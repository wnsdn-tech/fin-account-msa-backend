package com.finaccounthub.notification.controller;

import com.finaccounthub.notification.entity.NotificationEntity;
import com.finaccounthub.notification.repository.NotificationRepository;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/")
public class NotificationController {

    private final Environment env;
    private final NotificationRepository notificationRepository;

    public NotificationController(Environment env, NotificationRepository notificationRepository) {
        this.env = env;
        this.notificationRepository = notificationRepository;
    }

    @GetMapping("/health-check")
    public String healthCheck() {
        return String.format("It's Working in Notification Service, port(local.server.port)=%s",
                env.getProperty("local.server.port"));
    }

    /** 전체 알림 로그 조회 (최신순) */
    @GetMapping("/notifications")
    public List<NotificationEntity> getAllNotifications() {
        return notificationRepository.findAllByOrderByReceivedAtDesc();
    }

    /** 사용자별 알림 로그 조회 (최신순) */
    @GetMapping("/notifications/{userId}")
    public List<NotificationEntity> getNotificationsByUser(@PathVariable String userId) {
        return notificationRepository.findByUserIdOrderByReceivedAtDesc(userId);
    }
}
