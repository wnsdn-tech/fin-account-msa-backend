package com.finaccount.accountservice.service;

import com.finaccount.accountservice.dto.AccountStatus;
import com.finaccount.accountservice.dto.AccountDto;
import com.finaccount.accountservice.jpa.AccountEntity;
import com.finaccount.accountservice.jpa.AccountRepository;
import org.modelmapper.ModelMapper;
import org.modelmapper.convention.MatchingStrategies;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.NoSuchElementException;

@Service
public class AccountService {
    private final AccountRepository repository;
    private final BCryptPasswordEncoder passwordEncoder;

    public AccountService(AccountRepository repository, BCryptPasswordEncoder passwordEncoder) {
        this.repository = repository;
        this.passwordEncoder = passwordEncoder;
    }

    public AccountDto createAccount(AccountDto dto) {
        ModelMapper mapper = new ModelMapper();
        mapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);

        AccountEntity entity = mapper.map(dto, AccountEntity.class);
        entity.setAccountNumber(new AccountNumberGenerator().generate());
        entity.setBalance(0L);
        entity.setStatus(AccountStatus.ACTIVE);
        entity.setPassword(passwordEncoder.encode(dto.getPassword()));

        repository.save(entity);

        AccountDto created = mapper.map(entity, AccountDto.class);

        return created;
    }

    public AccountDto getAccount(Integer accountId) throws NoSuchElementException {
        AccountEntity entity = repository.findById(accountId).orElseThrow();

        AccountDto response = new ModelMapper().map(entity, AccountDto.class);

        return response;
    }
}
