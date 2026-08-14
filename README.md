# Fin-Account Hub — 담당 파트 (Notification Service / Kafka / Schema Registry)

- 프로젝트 코드: **[FIN-M]**
- 도메인: 금융 (중) / 핵심 차별화 기술: **Schema Registry**
- 참조 문서: `[CNS 5기] 미니PJT 2 기획서.pdf` (10장), `C:\inspire` 요구사항 정리 PDF 3종

이 리포지토리는 팀 프로젝트 중 **개인 담당 파트만** 추려 담았습니다. 전체 MSA(discovery/config/gateway/
account-service/transaction-service 포함)는 별도 리포지토리(`fin-account-msa-backend-personal`)를
참고하세요.

## 담당 범위

| 영역 | 상태 | 설명 |
|---|---|---|
| **notification-service** | ✅ 완성 | Kafka(Avro) 컨슈머, 알림 로그 저장/조회 API, 멱등성 보장, DLT 처리 |
| **Kafka 인프라 + 이벤트 파이프라인** | ✅ 완성 | Kafka(KRaft, 단일 노드), Consumer 파이프라인 |
| **Schema Registry** | ✅ 완성 | Confluent Schema Registry, Avro 스키마 등록/조회, BACKWARD 호환성 검증 스크립트 |

## 기술 스택

- Java **21**, Spring Boot 3.3.4, Spring Cloud 2023.0.3
- Kafka (Apache Kafka 3.8, KRaft 모드, Zookeeper 불필요)
- Confluent Schema Registry 7.6.1 + Avro
- H2 (임베디드 DB)
- Docker / Docker Compose

## 폴더 구조

```
fin-account-msa-backend/
├── docker-compose.yml            # Kafka, Schema Registry, Kafka UI, notification-service
├── docs/
│   ├── architecture.md           # 시스템 구성도 (텍스트 다이어그램)
│   └── schema-registry-guide.md  # Avro 스키마 등록/호환성 검증 가이드 (v2 스키마 반영)
├── scripts/
│   └── schema-registry-demo.sh   # 스키마 등록 + BACKWARD 호환성 테스트 curl 스크립트
└── notification-service/         # 알림 MS (완성, 멱등성/DLT/통합테스트 포함)
```

## Avro 스키마 (v2)

`notification-service/src/main/avro/transaction_event.avsc` — 요구사항 정리 문서 기준으로 아래 필드 확정:

- `transactionType`: `DEPOSIT | WITHDRAW | TRANSFER` (WITHDRAWAL 아님, 요구사항 문서 기준)
- `fromAccountId` / `toAccountId` (nullable, default null) — 서비스별 DB 분리 구조에서 이체 시 출발/도착 계좌 구분
- `status` (default "SUCCESS") — `PENDING | SUCCESS | FAILED`, Saga 보상 트랜잭션 대응
- 신규 필드는 모두 default 값을 가져 **BACKWARD 호환성 유지**

⚠️ **팀 조율 필요**: Producer 역할인 transaction-service가 이 v2 스키마와 `WITHDRAW` 값으로 발행하도록
동기화되어야 합니다 (전체 스택 리포지토리인 `fin-account-msa-backend-personal`에는 이미 반영됨).

## notification-service 신뢰성 개선

- **멱등성**: `notification_log.transaction_id` UNIQUE 제약 + 저장 전 존재 확인 → Kafka 재전송/Consumer
  재시작 시 중복 알림 방지
- **실패 처리/DLT**: `ErrorHandlingDeserializer` + `DefaultErrorHandler`(1초 간격 3회 재시도) +
  `DeadLetterPublishingRecoverer` → 역직렬화/저장 실패 시 `fin.transaction.events.DLT` 토픽으로 발행
- **통합 테스트**: `NotificationServiceIntegrationTest` — EmbeddedKafka + MockSchemaRegistryClient로
  외부 인프라 없이 "발행 → Schema Registry 등록 → Consumer 저장 → 조회 API" 전 과정과 멱등성을 검증

## 실행 방법

### 1) Docker Compose로 기동 (Kafka + Schema Registry + notification-service)

```bash
cd fin-account-msa-backend
docker compose build
docker compose up -d
```

기동 순서: `kafka` → `schema-registry` → `notification-service`

### 2) 알림 로그 조회 API 확인

```bash
curl http://localhost:8085/notifications
curl http://localhost:8085/notifications/{userId}
```

실제 거래 이벤트를 발행하려면 transaction-service(Producer, 별도 리포지토리)가 `fin.transaction.events`
토픽에 Avro 이벤트를 발행해야 합니다.

### 3) 통합 테스트 실행

```bash
cd notification-service
mvn test -Dtest=NotificationServiceIntegrationTest
```

Docker/외부 Kafka 없이 EmbeddedKafka + Mock Schema Registry로 전체 파이프라인을 검증합니다
(발행 → 스키마 등록 → 소비 → 저장 → 조회 API, 멱등성, TRANSFER 메시지 포맷).

### 4) Schema Registry 확인

```bash
curl http://localhost:8091/subjects
curl http://localhost:8091/subjects/fin.transaction.events-value/versions/latest
curl http://localhost:8091/config/fin.transaction.events-value
```

BACKWARD 호환성 검증 데모는 `scripts/schema-registry-demo.sh` 및 `docs/schema-registry-guide.md` 참고.

## 서비스 & 포트

| 서비스 | 포트 | 비고 |
|---|---|---|
| notification-service | 8085 | |
| Kafka broker | 9092 (내부), 29092 (호스트) | KRaft 단일 노드 |
| Schema Registry | 8081(컨테이너 내부) → **8091**(호스트) | account-service와 포트 충돌 방지 |
| Kafka UI (선택) | 8090 | http://localhost:8090 |

## 남은 작업 (팀 조율)

1. **transaction-service(Producer) 스키마 동기화**: `WITHDRAW` 값 및 `fromAccountId`/`toAccountId`/`status`
   필드를 채워 발행하도록 확인 (전체 스택 리포지토리에는 이미 반영됨)
2. DLT(`fin.transaction.events.DLT`)로 들어온 메시지의 모니터링/재처리 절차는 별도 미정
