package com.finaccounthub.notification;

import com.finaccount.transactionservice.TransactionEvent;
import com.finaccounthub.notification.controller.NotificationController;
import com.finaccounthub.notification.entity.NotificationEntity;
import com.finaccounthub.notification.repository.NotificationRepository;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 통합 테스트 시나리오 (요구사항 정리 문서 담당 범위: Notification/Kafka/Schema Registry):
 *
 *   Producer 발행(EmbeddedKafka + MockSchemaRegistryClient)
 *   -> Schema Registry 등록/호환성 확인(Mock 레지스트리가 자동 등록/검증)
 *   -> Notification Consumer 저장(KafkaAvroDeserializer -> TransactionEventListener -> DB)
 *   -> 조회 API 확인(NotificationRepository/Controller 결과 검증)
 *
 * 실제 docker-compose의 Kafka/Schema Registry 대신 spring-kafka-test의 EmbeddedKafka와
 * Confluent의 MockSchemaRegistryClient(schema.registry.url=mock://test-scope)를 사용해
 * 외부 인프라 없이 CI에서도 실행 가능하게 구성했다.
 *
 * v3: Avro 스키마 정합성 반영 — transactionId/accountId(테스트용 키)/fromAccountId/toAccountId를
 * int로 변경, accountId 필드는 스키마에서 제거됨.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"fin.transaction.events.test"})
@DirtiesContext
class NotificationServiceIntegrationTest {

    @Autowired
    private NotificationRepository notificationRepository;

    @Autowired
    private NotificationController notificationController;

    @Autowired
    private org.springframework.kafka.test.EmbeddedKafkaBroker embeddedKafkaBroker;

    @Value("${kafka.topic.transaction-events}")
    private String topic;

    private static final AtomicInteger TX_ID_SEQ = new AtomicInteger(900_000);

    private static int nextTransactionId() {
        return TX_ID_SEQ.incrementAndGet();
    }

    private KafkaTemplate<String, Object> testProducerTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "mock://test-scope"); // 컨슈머와 동일 mock scope 공유
        ProducerFactory<String, Object> pf = new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(pf);
    }

    private TransactionEvent buildEvent(int transactionId, String type,
                                         long amount, Integer from, Integer to, String status) {
        return TransactionEvent.newBuilder()
                .setTransactionId(transactionId)
                .setOwnerName("John Doe")
                .setTransactionType(type)
                .setAmount(amount)
                .setCreatedAt(Instant.now().toString())
                .setFromAccountId(from)
                .setToAccountId(to)
                .setStatus(status)
                .build();
    }

    @Test
    void producerToConsumerToQueryApi_endToEnd() {
        int transactionId = nextTransactionId();
        int accountId = 1001;

        TransactionEvent event = buildEvent(transactionId, "DEPOSIT", 50_000L,
                null, accountId, "SUCCESS");

        // 1) Producer 발행 (Avro + Mock Schema Registry에 자동 등록)
        testProducerTemplate().send(topic, String.valueOf(accountId), event);

        // 2) Consumer가 수신하여 notification_log에 저장할 때까지 대기
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(notificationRepository.existsByTransactionId(transactionId)).isTrue()
        );

        // 3) 조회 API 확인 — 저장된 내용이 이벤트와 일치하는지 검증
        List<NotificationEntity> all = notificationController.getAllNotifications();
        NotificationEntity saved = all.stream()
                .filter(n -> n.getTransactionId().equals(transactionId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("저장된 알림을 찾을 수 없음: " + transactionId));

        assertThat(saved.getTransactionType()).isEqualTo("DEPOSIT");
        assertThat(saved.getAmount()).isEqualTo(50_000L);
        assertThat(saved.getStatus()).isEqualTo("SUCCESS");
        assertThat(saved.getToAccountId()).isEqualTo(accountId);
        assertThat(saved.getMessage()).contains(String.valueOf(accountId)).contains("입금");

        List<NotificationEntity> byUser = notificationController.getNotificationsByUser("John Doe");
        assertThat(byUser).anyMatch(n -> n.getTransactionId().equals(transactionId));
    }

    @Test
    void duplicateTransactionId_isNotSavedTwice_idempotency() {
        int transactionId = nextTransactionId();
        int accountId = 2002;

        TransactionEvent event = buildEvent(transactionId, "WITHDRAW", 10_000L,
                accountId, null, "SUCCESS");

        KafkaTemplate<String, Object> producer = testProducerTemplate();

        // 동일 transactionId를 두 번 발행 (Kafka 재전송/재처리 시나리오 시뮬레이션)
        producer.send(topic, String.valueOf(accountId), event);
        producer.send(topic, String.valueOf(accountId), event);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(notificationRepository.existsByTransactionId(transactionId)).isTrue()
        );

        // Consumer가 두 메시지를 모두 처리할 시간을 준 뒤 저장 건수가 1건인지 확인
        await().pollDelay(2, TimeUnit.SECONDS).atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            long count = notificationRepository.findAllByOrderByCreatedAtDesc().stream()
                    .filter(n -> n.getTransactionId().equals(transactionId))
                    .count();
            assertThat(count).isEqualTo(1);
        });
    }

    @Test
    void transferEvent_showsFromAndToAccountInMessage() {
        int transactionId = nextTransactionId();
        int from = 3001;
        int to = 3002;

        TransactionEvent event = buildEvent(transactionId, "TRANSFER", 30_000L, from, to, "SUCCESS");
        testProducerTemplate().send(topic, String.valueOf(from), event);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(notificationRepository.existsByTransactionId(transactionId)).isTrue()
        );

        NotificationEntity saved = notificationRepository.findAllByOrderByCreatedAtDesc().stream()
                .filter(n -> n.getTransactionId().equals(transactionId))
                .findFirst()
                .orElseThrow();

        assertThat(saved.getFromAccountId()).isEqualTo(from);
        assertThat(saved.getToAccountId()).isEqualTo(to);
        assertThat(saved.getMessage()).contains(String.valueOf(from)).contains(String.valueOf(to));
    }
}
