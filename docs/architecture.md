# 시스템 구성도 — Fin-Account Hub (담당 파트: Notification / Kafka / Schema Registry)

> 이 문서는 전체 팀 프로젝트 아키텍처를 보여주되, 이 리포지토리에는 **notification-service /
> Kafka / Schema Registry**만 포함되어 있습니다. account-service, transaction-service,
> discovery-service, config-service, apigateway-service는 팀원 담당(전체 스택 리포지토리 참고).

## 전체 아키텍처 (텍스트 다이어그램, 참고용)

```
┌──────────┐        ┌───────────────────────┐        ┌──────────────────┐
│  Client  │──────▶│  apigateway-service    │──────▶│  discovery-service │
│ (Postman)│        │  (Spring Cloud Gateway)│        │  (Eureka Server)   │
└──────────┘        │  :8000                 │        │  :8761              │
                     └───────────┬────────────┘        └──────────┬─────────┘
                                 │ lb://SERVICE-NAME               │ 서비스 등록/검색
                                 ▼                                 │
        ┌────────────────────────────────────────────────┐         │
        │                                                 │◀────────┘
        ▼                        ▼                        ▼
┌───────────────┐      ┌───────────────────┐     ┌────────────────────┐
│ account-service│      │ transaction-service│     │ notification-service│
│ :8081 (TODO)   │◀────▶│ :8082               │────▶│ :8085 (완성)         │
│ 계좌 원장 DB   │Feign │ 거래 내역 DB        │Kafka│ 알림 로그 DB         │
└───────┬────────┘      └─────────┬──────────┘     └──────────┬──────────┘
        │                          │ Producer(Avro)              ▲ Consumer(Avro)
        │                          ▼                              │
        │                 ┌─────────────────────┐                 │
        │                 │   Kafka Broker       │─────────────────┘
        │                 │   (KRaft, :29092)     │
        │                 │   topic: fin.transaction.events
        │                 └──────────┬────────────┘
        │                            │ 스키마 등록/조회/호환성 검증
        │                            ▼
        │                 ┌─────────────────────────┐
        │                 │ Confluent Schema Registry│
        │                 │ :8091 (host) / 8081(내부) │
        │                 │ subject: fin.transaction.events-value
        │                 │ compatibility: BACKWARD  │
        │                 └─────────────────────────┘
        │
        ▼
┌────────────────────┐
│   config-service     │  (모든 서비스가 기동 시 설정 조회)
│   (Config Server)     │
│   :8888, native backend, config-repo/*.yml
└────────────────────┘
```

## 컴포넌트 책임 (Bounded Context)

| 서비스 | 책임 | 통신 방식 | 상태 |
|---|---|---|---|
| discovery-service | 서비스 등록/검색 (Eureka) | - | 완성 |
| config-service | 중앙화된 설정 관리 (native file backend) | - | 완성(간이) |
| apigateway-service | 단일 진입점, 라우팅 | HTTP | 완성(인증 필터 TODO) |
| account-service | 계좌 원장 관리 | REST (내부 Feign 수신) | 스켈레톤 |
| transaction-service | 입출금/이체, 거래 이벤트 발행 | Feign(→account), Kafka Producer | Kafka 파이프라인만 완성 |
| notification-service | 거래 이벤트 구독, 알림 로그 생성/조회 | Kafka Consumer, REST | **완성** |
| Kafka | 거래 이벤트 비동기 파이프라인 | - | 완성 |
| Schema Registry | Avro 스키마 버전/호환성 관리 | - | 완성 |

## 이벤트 흐름 (Notification 파이프라인)

1. Client → `POST /transaction-service/transactions/simulate` (Gateway 경유) 또는 transaction-service 직접 호출
2. transaction-service: `TransactionEvent` Avro 객체 생성 → `KafkaTemplate.send()`
3. `KafkaAvroSerializer` 가 Schema Registry에 스키마 등록/버전 확인 후 직렬화
4. Kafka Broker의 `fin.transaction.events` 토픽에 메시지 적재
5. notification-service의 `TransactionEventListener` 가 `@KafkaListener` 로 구독
6. `KafkaAvroDeserializer` 가 Schema Registry에서 스키마를 가져와 `TransactionEvent` 객체로 역직렬화
7. 거래 유형별 알림 메시지 생성 후 `NotificationEntity` 로 저장
8. Client → `GET /notification-service/notifications` 로 알림 로그 확인

## Database per Service 원칙

- account-service: `accountdb` (계좌 원장) — TODO
- transaction-service: `transactiondb` (거래 내역) — TODO
- notification-service: `notificationdb` (알림 로그) — 완성

각 서비스는 자신의 H2 인메모리 DB만 직접 접근하며, 서비스 간 데이터 공유는 반드시
API(Feign) 또는 이벤트(Kafka)를 통해서만 이루어진다.
