package com.finaccounthub.notification.repository;

import com.finaccounthub.notification.entity.NotificationEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface NotificationRepository extends JpaRepository<NotificationEntity, Long> {

    List<NotificationEntity> findByUserIdOrderByReceivedAtDesc(String userId);

    List<NotificationEntity> findAllByOrderByReceivedAtDesc();

    boolean existsByTransactionId(String transactionId);
}
