package com.finaccounthub.notification.entity;

import jakarta.persistence.*;

import java.time.LocalDateTime;

/**
 * 알림 로그 엔티티.
 * Kafka로부터 수신한 TransactionEvent를 사람이 읽을 수 있는 알림 메시지로
 * 변환하여 저장한다 (실제 이메일/SMS 발송 대신 로그 저장으로 대체).
 *
 * v3: Avro 스키마 정합성 반영 — transactionId/fromAccountId/toAccountId를 Integer로 변경,
 * accountId 필드 제거.
 */
@Entity
@Table(name = "notification_log")
public class NotificationEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private Integer transactionId;

    @Column(nullable = false)
    private String ownerName;

    @Column(nullable = false)
    private String transactionType;

    @Column(nullable = false)
    private Long amount;

    @Column(nullable = false, length = 500)
    private String message;

    @Column(nullable = false)
    private String status;

    @Column
    private Integer fromAccountId;

    @Column
    private Integer toAccountId;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    protected NotificationEntity() {
        // JPA
    }

    public NotificationEntity(Integer transactionId, String ownerName,
                               String transactionType, Long amount, String message,
                               String status, Integer fromAccountId, Integer toAccountId,
                               LocalDateTime createdAt) {
        this.transactionId = transactionId;
        this.ownerName = ownerName;
        this.transactionType = transactionType;
        this.amount = amount;
        this.message = message;
        this.status = status;
        this.fromAccountId = fromAccountId;
        this.toAccountId = toAccountId;
        this.createdAt = createdAt;
    }

    public Long getId() {
        return id;
    }

    public Integer getTransactionId() {
        return transactionId;
    }

    public String getOwnerName() {
        return ownerName;
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

    public Integer getFromAccountId() {
        return fromAccountId;
    }

    public Integer getToAccountId() {
        return toAccountId;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }
}
