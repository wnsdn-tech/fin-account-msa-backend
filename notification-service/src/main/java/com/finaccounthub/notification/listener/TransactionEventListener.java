package com.finaccounthub.notification.listener;

import com.finaccounthub.avro.TransactionEvent;
import com.finaccounthub.notification.entity.NotificationEntity;
import com.finaccounthub.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * fin.transaction.events 토픽을 구독하여 거래 이벤트를 알림 로그로 변환/저장한다.
 *
 * Avro + Schema Registry 덕분에 컨슈머는 스키마 정의(TransactionEvent) 그대로
 * 타입 안전하게 역직렬화된 객체를 수신한다. (KafkaAvroDeserializer가 자동 처리)
 */
@Component
public class TransactionEventListener {

    private static final Logger log = LoggerFactory.getLogger(TransactionEventListener.class);

    private final NotificationRepository notificationRepository;

    public TransactionEventListener(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    @KafkaListener(topics = "${kafka.topic.transaction-events}", groupId = "${spring.kafka.consumer.group-id}")
    public void onTransactionEvent(TransactionEvent event) {
        log.info("Received TransactionEvent: {}", event);

        String transactionId = event.getTransactionId().toString();

        // 멱등성 보장: Kafka 재전송(at-least-once) / Consumer 재시작 시 동일 거래가 중복 저장되지 않도록
        // 저장 전에 존재 여부를 확인한다. transactionId에는 DB unique 제약도 걸려 있어 이중 방어된다.
        if (notificationRepository.existsByTransactionId(transactionId)) {
            log.warn("Duplicate TransactionEvent detected, skip saving: transactionId={}", transactionId);
            return;
        }

        String status = event.getStatus() != null ? event.getStatus().toString() : "SUCCESS";
        String message = buildMessage(event, status);

        NotificationEntity entity = new NotificationEntity(
                transactionId,
                event.getAccountId().toString(),
                event.getUserId().toString(),
                event.getTransactionType().toString(),
                event.getAmount(),
                message,
                status,
                event.getFromAccountId() != null ? event.getFromAccountId().toString() : null,
                event.getToAccountId() != null ? event.getToAccountId().toString() : null,
                LocalDateTime.now()
        );

        try {
            notificationRepository.save(entity);
        } catch (DataIntegrityViolationException e) {
            // 존재 여부 확인과 저장 사이의 race condition(동시 컨슈머 재처리 등)에 대한 마지막 방어선.
            log.warn("Duplicate TransactionEvent (unique constraint), skip saving: transactionId={}", transactionId);
        }
    }

    /**
     * 요구사항 정리 문서 기준 거래 타입: DEPOSIT | WITHDRAW | TRANSFER, 상태: PENDING | SUCCESS | FAILED.
     * TRANSFER는 fromAccountId/toAccountId(v2 스키마)를 이용해 출발/도착 계좌를 명시한다.
     * 구버전 프로듀서(from/to 필드 미포함, default null)와의 BACKWARD 호환을 위해 accountId로 폴백한다.
     */
    private String buildMessage(TransactionEvent event, String status) {
        if ("FAILED".equals(status)) {
            return String.format("계좌 %s 거래(%s) %,d원이 실패했습니다.", event.getAccountId(), event.getTransactionType(), event.getAmount());
        }
        if ("PENDING".equals(status)) {
            return String.format("계좌 %s 거래(%s) %,d원이 처리 중입니다.", event.getAccountId(), event.getTransactionType(), event.getAmount());
        }

        String from = event.getFromAccountId() != null ? event.getFromAccountId().toString() : null;
        String to = event.getToAccountId() != null ? event.getToAccountId().toString() : null;

        return switch (event.getTransactionType().toString()) {
            case "DEPOSIT" -> String.format("계좌 %s에 %,d원이 입금되었습니다.", to != null ? to : event.getAccountId(), event.getAmount());
            case "WITHDRAW" -> String.format("계좌 %s에서 %,d원이 출금되었습니다.", from != null ? from : event.getAccountId(), event.getAmount());
            case "TRANSFER" -> (from != null && to != null)
                    ? String.format("계좌 %s에서 계좌 %s로 %,d원이 이체되었습니다.", from, to, event.getAmount())
                    : String.format("계좌 %s에서 %,d원이 이체되었습니다.", event.getAccountId(), event.getAmount());
            default -> String.format("계좌 %s에 거래(%s) %,d원이 발생했습니다.", event.getAccountId(), event.getTransactionType(), event.getAmount());
        };
    }
}
