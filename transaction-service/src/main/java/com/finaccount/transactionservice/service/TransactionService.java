package com.finaccount.transactionservice.service;

import com.finaccount.transactionservice.TransactionEvent;
import com.finaccount.transactionservice.client.AccountServiceClient;
import com.finaccount.transactionservice.dto.TransactionDto;
import com.finaccount.transactionservice.dto.TransactionStatus;
import com.finaccount.transactionservice.jpa.TransactionEntity;
import com.finaccount.transactionservice.jpa.TransactionRepository;
import com.finaccount.transactionservice.vo.AccountRequest;
import com.finaccount.transactionservice.vo.AccountResponse;
import com.finaccount.transactionservice.vo.AccountStatus;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.modelmapper.ModelMapper;
import org.springframework.cloud.client.circuitbreaker.CircuitBreaker;
import org.springframework.cloud.client.circuitbreaker.CircuitBreakerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;

@Service
public class TransactionService {
    private static final String TOPIC = "fin.transaction.events";

    private final TransactionRepository repository;
    private final AccountServiceClient accountService;
    private final KafkaProducer<String, TransactionEvent> kafkaProducer;
    private final CircuitBreakerFactory circuitBreakerFactory;

    public TransactionService(
            TransactionRepository repository,
            AccountServiceClient accountService,
            KafkaProducer<String, TransactionEvent> kafkaProducer,
            CircuitBreakerFactory circuitBreakerFactory
    ) {
        this.repository = repository;
        this.accountService = accountService;
        this.kafkaProducer = kafkaProducer;
        this.circuitBreakerFactory = circuitBreakerFactory;
    }

    @Transactional
    public TransactionDto addTransaction(TransactionDto dto) {
        TransactionDto pending = addPendingTransaction(dto);

        TransactionDto updated = updateFromPendingTransaction(pending);

        sendNotification(updated);

        return updated;
    }

    private TransactionDto addPendingTransaction(TransactionDto dto) {
        ModelMapper mapper = new ModelMapper();
        TransactionEntity entity = mapper.map(dto, TransactionEntity.class);

        entity.setType(dto.getType());
        entity.setStatus(TransactionStatus.PENDING);
        entity.setCreatedAt(Instant.now());
        entity = repository.save(entity);

        TransactionDto pending = mapper.map(entity, TransactionDto.class);

        return pending;
    }

    private TransactionDto updateFromPendingTransaction(TransactionDto pending) {
        try {
            this.updateAccount(pending);
            return updateToSuccessTransaction(pending);

        } catch (IllegalStateException e) {
            return updateToFailedTransaction(pending);
        }
    }

    private TransactionDto updateToSuccessTransaction(TransactionDto dto) {
        ModelMapper mapper = new ModelMapper();
        TransactionEntity entity = mapper.map(dto, TransactionEntity.class);

        entity.setStatus(TransactionStatus.SUCCESS);
        entity = repository.save(entity);

        TransactionDto success = mapper.map(entity, TransactionDto.class);

        return success;
    }

    private TransactionDto updateToFailedTransaction(TransactionDto dto) {
        ModelMapper mapper = new ModelMapper();
        TransactionEntity entity = mapper.map(dto, TransactionEntity.class);

        entity.setStatus(TransactionStatus.FAILED);
        entity = repository.save(entity);

        TransactionDto failed = mapper.map(entity, TransactionDto.class);

        return failed;
    }

    public TransactionDto getTransaction(Integer transactionId) throws NoSuchElementException {
        TransactionEntity entity = repository.findById(transactionId).orElseThrow();

        ModelMapper mapper = new ModelMapper();
        TransactionDto dto = mapper.map(entity, TransactionDto.class);

        return dto;
    }

    public List<TransactionDto> getTransactions(Integer accountId) {
        List<TransactionEntity> entities = repository.findByAccountId(accountId);

        ModelMapper mapper = new ModelMapper();
        List<TransactionDto> dtos = entities.stream()
                .map(entity -> mapper.map(entity, TransactionDto.class))
                .toList();

        return dtos;
    }

    private void updateAccount(TransactionDto transaction) throws IllegalStateException {
        switch (transaction.getType()) {
            case DEPOSIT -> updateAccountForDepositTransaction(transaction);
            case WITHDRAW -> updateAccountForWithdrawTransaction(transaction);
            case TRANSFER -> updateAccountForTransferTransaction(transaction);
        }
    }

    private void updateAccountForDepositTransaction(TransactionDto transaction) {
        Integer accountId = transaction.getToAccountId();
        Long amount = transaction.getAmount();

        CircuitBreaker circuitBreaker = circuitBreakerFactory.create("account-service");

        AccountResponse response = circuitBreaker.run(
                () -> accountService.getAccount(accountId),
                throwable -> {
                    throw new IllegalStateException();
                }
        );

        if (!isActiveAccount(response)) {
            throw new IllegalStateException();
        }

        AccountRequest.AccountRequestBuilder builder = AccountRequest.builder();
        builder.balance(response.getBalance() + amount);
        AccountRequest request = builder.build();

        circuitBreaker.run(
                () -> accountService.updateAccount(accountId, request),
                throwable -> {
                    throw new IllegalStateException();
                }
        );
    }

    private void updateAccountForWithdrawTransaction(TransactionDto transaction) {
        Integer accountId = transaction.getFromAccountId();
        Long amount = transaction.getAmount();

        CircuitBreaker circuitBreaker = circuitBreakerFactory.create("account-service");

        AccountResponse response = circuitBreaker.run(
                () -> accountService.getAccount(accountId),
                throwable -> {
                    throw new IllegalStateException();
                }
        );

        if (!response.getStatus().equals(AccountStatus.ACTIVE)) {
            throw new IllegalStateException();
        }

        if (!isEnoughBalance(response, transaction)) {
            throw new IllegalStateException();
        }

        AccountRequest.AccountRequestBuilder builder = AccountRequest.builder();
        builder.balance(response.getBalance() - amount);
        AccountRequest request = builder.build();

        circuitBreaker.run(
                () -> accountService.updateAccount(accountId, request),
                throwable -> {
                    throw new IllegalStateException();
                }
        );
    }

    private boolean isActiveAccount(AccountResponse account) {
        return account.getStatus().equals(AccountStatus.ACTIVE);
    }

    private boolean isEnoughBalance(AccountResponse account, TransactionDto transaction) {
        return account.getBalance() >= transaction.getAmount();
    }

    // TODO
    // updateAccountForDepositTransaction은 정상처리되었는데,
    // updateAccountForWithdrawTransaction에서 문제가 생긴다면 어떻게 해야하는가
    private void updateAccountForTransferTransaction(TransactionDto transaction) {
        updateAccountForDepositTransaction(transaction);
        updateAccountForWithdrawTransaction(transaction);
    }

    private void sendNotification(TransactionDto transaction) {
        TransactionEvent.Builder builder = TransactionEvent.newBuilder();
        builder.setTransactionId(transaction.getTransactionId());
        builder.setOwnerName(getOwnerNameForNotification(transaction)); // TODO
        builder.setTransactionType(transaction.getType().toString());
        builder.setAmount(transaction.getAmount());
        builder.setCreatedAt(transaction.getCreatedAt().toString());
        builder.setFromAccountId(transaction.getFromAccountId());
        builder.setToAccountId(transaction.getToAccountId());
        builder.setStatus(transaction.getStatus().toString());
        TransactionEvent event = builder.build();

        ProducerRecord<String, TransactionEvent> record = new ProducerRecord<>(TOPIC, event);

        CircuitBreaker circuitBreaker = circuitBreakerFactory.create("notification-service");
        circuitBreaker.run(() -> kafkaProducer.send(record), throwable -> null);
    }

    private String getOwnerNameForNotification(TransactionDto transaction) {
        Integer accountId = getAccountIdForNotification(transaction);

        CircuitBreaker circuitBreaker = circuitBreakerFactory.create("account-service");

        AccountResponse response = circuitBreaker.run(
                () -> accountService.getAccount(accountId),
                throwable -> {
                    throw new IllegalStateException();
                }
        );

        return response.getOwnerName();
    }

    private Integer getAccountIdForNotification(TransactionDto transaction) {
        return switch (transaction.getType()) {
            case DEPOSIT -> transaction.getToAccountId();
            case WITHDRAW -> transaction.getFromAccountId();
            case TRANSFER -> transaction.getFromAccountId();
        };
    }
}
