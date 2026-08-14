package com.finaccounthub.notification.entity;

import jakarta.persistence.*;

import java.time.LocalDateTime;

/**
 * 알림 로그 엔티티.
 * Kafka로부터 수신한 TransactionEvent를 사람이 읽을 수 있는 알림 메시지로
 * 변환하여 저장한다 (실제 이메일/SMS 발송 대신 로그 저장으로 대체).
 */
@Entity
@Table(name = "notification_log")
public class NotificationEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String transactionId;

    @Column(nullable = false)
    private String accountId;

    @Column(nullable = false)
    private String userId;

    @Column(nullable = false)
    private String transactionType;

    @Column(nullable = false)
    private Long amount;

    @Column(nullable = false, length = 500)
    private String message;

    @Column(nullable = false)
    private String status;

    @Column
    private String fromAccountId;

    @Column
    private String toAccountId;

    @Column(nullable = false)
    private LocalDateTime receivedAt;

    protected NotificationEntity() {
        // JPA
    }

    public NotificationEntity(String transactionId, String accountId, String userId,
                               String transactionType, Long amount, String message,
                               String status, String fromAccountId, String toAccountId,
                               LocalDateTime receivedAt) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.userId = userId;
        this.transactionType = transactionType;
        this.amount = amount;
        this.message = message;
        this.status = status;
        this.fromAccountId = fromAccountId;
        this.toAccountId = toAccountId;
        this.receivedAt = receivedAt;
    }

    public Long getId() {
        return id;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getUserId() {
        return userId;
    }

    public String getTransactionType() {
        return transactionType;
    }

    public Long getAmount() {
        return amount;
    }

    public String getMessage() {
        return message;
    }

    public String getStatus() {
        return status;
    }

    public String getFromAccountId() {
        return fromAccountId;
    }

    public String getToAccountId() {
        return toAccountId;
    }

    public LocalDateTime getReceivedAt() {
        return receivedAt;
    }
}
