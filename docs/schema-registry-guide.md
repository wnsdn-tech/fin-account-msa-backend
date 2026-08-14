# Schema Registry 가이드 — 핵심 차별화 기술 [FIN-M]

## 개요

Confluent Schema Registry는 Kafka 토픽에 발행되는 메시지의 **Avro 스키마**를 중앙에서
버전 관리하고, 스키마 변경 시 **호환성(compatibility)**을 강제하는 역할을 한다.

- Subject: `fin.transaction.events-value` (토픽명-value 컨벤션)
- Compatibility Mode: **BACKWARD** (기본값, docker-compose에서 설정)
  - 새 스키마로 만든 데이터를 **이전 스키마를 쓰는 컨슈머**가 읽을 수 있어야 함
  - 즉, 새 필드 추가 시 **반드시 default 값**을 가져야 하고, 필드 삭제는 자유롭지만
    필수(default 없는) 필드 추가는 금지됨

## 스키마 정의 위치

- `transaction-service/src/main/avro/transaction_event.avsc`
- `notification-service/src/main/avro/transaction_event.avsc` (동일 파일, Producer/Consumer 양쪽에 필요)

**v2 변경사항** (요구사항 정리 문서 반영, 이번 조율 작업으로 transaction-service까지 통일 완료):
- `transactionType` 값 통일: `WITHDRAWAL` → `WITHDRAW` (요구사항 정리 문서 기준) — Producer/Consumer 양쪽 반영 완료
- `fromAccountId` / `toAccountId` (nullable string, default null) 추가 — 서비스별 DB 분리 구조에서 이체(TRANSFER) 시 출발/도착 계좌를 구분하기 위함 (요구사항 정리 문서의 Transaction DB `fromAccount`/`toAccount` FK 대응). transaction-service `/transactions/simulate`가 거래 타입에 따라 값을 채워 발행함
- `status` (string, default "SUCCESS") 추가 — PENDING/SUCCESS/FAILED, Saga 보상 트랜잭션 대응
- 위 필드들은 모두 default 값을 가지므로 BACKWARD 호환성 유지됨
- Producer(transaction-service)/Consumer(notification-service) 스키마 파일이 동일하게 동기화됨 (이전에는 Consumer만 v2였던 불일치를 해소)

**추가 신뢰성 개선** (notification-service):
- `notification_log.transaction_id` UNIQUE 제약 + 저장 전 존재 확인으로 멱등성 보장 (Kafka 재전송/Consumer 재시작 시 중복 저장 방지)
- `ErrorHandlingDeserializer` + `DefaultErrorHandler`(FixedBackOff 1초×3회) + `DeadLetterPublishingRecoverer`로 역직렬화/저장 실패 시 `fin.transaction.events.DLT` 토픽으로 재발행
- `NotificationServiceIntegrationTest`: EmbeddedKafka + MockSchemaRegistryClient 기반 end-to-end 통합 테스트 (발행→등록→소비→조회 API, 멱등성, TRANSFER 메시지)

`avro-maven-plugin` 이 빌드 시 `.avsc` → Java 클래스(`com.finaccounthub.avro.TransactionEvent`)를
자동 생성한다 (`mvn generate-sources`).

## 1) 스키마 등록 확인

애플리케이션이 처음 이벤트를 발행하는 순간 `KafkaAvroSerializer` 가 자동으로 스키마를
Schema Registry에 등록한다. 수동으로 확인/등록하려면:

```bash
# 등록된 subject 목록
curl -s http://localhost:8091/subjects | jq

# 특정 subject의 최신 버전 스키마 확인
curl -s http://localhost:8091/subjects/fin.transaction.events-value/versions/latest | jq

# 호환성 모드 확인
curl -s http://localhost:8091/config/fin.transaction.events-value | jq
```

## 2) BACKWARD 호환성 검증 시나리오

기획서 요구사항 #10(선택 항목 #10 "스키마 호환성 시뮬레이션")에 대응하는 실습:

### Step 1. 현재 스키마로 이벤트 발행 (v1)
```bash
curl -X POST http://localhost:8082/transactions/simulate \
  -H "Content-Type: application/json" \
  -d '{"accountId":"ACC-0001","userId":"USER-0001","transactionType":"DEPOSIT","amount":10000}'
```

### Step 2. 필드 추가 시나리오 — 호환 O (권장)

`transaction_event.avsc` 에 **default 값이 있는** 필드를 추가:

```json
{ "name": "channel", "type": "string", "default": "MOBILE", "doc": "거래 발생 채널" }
```

이 상태로 스키마 호환성만 미리 검증하려면(실제 등록 전):

```bash
curl -X POST http://localhost:8091/compatibility/subjects/fin.transaction.events-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "<위 필드가 추가된 .avsc 내용을 JSON 문자열로 이스케이프>"}'
```
→ `{"is_compatible": true}` 예상

### Step 3. 필드 추가 시나리오 — 호환 X (실패 유도, 학습용)

default 없이 필드를 추가하면:

```json
{ "name": "channel", "type": "string" }
```

동일한 호환성 체크 API 호출 시:
→ `{"is_compatible": false}` — BACKWARD 모드에서는 default 없는 필드 추가가 거부됨

### Step 4. 결과 문서화 (기획서 요구사항 #10 대응)

아래와 같은 표로 결과를 정리해 제출:

| 시나리오 | 변경 내용 | 호환성 결과 | 이유 |
|---|---|---|---|
| A | `channel` 필드 추가 (default: "MOBILE") | ✅ Compatible | 기존 컨슈머는 이 필드를 몰라도 무시(구버전 리더가 신버전 데이터 읽을 때 무관), 신버전 컨슈머는 default로 채움 |
| B | `channel` 필드 추가 (default 없음) | ❌ Incompatible | 구버전 데이터에 해당 필드가 없어 신버전 컨슈머가 역직렬화 시 값 없음 → BACKWARD 원칙 위반 |
| C | 기존 필드 삭제 (`amount`) | ❌ Incompatible (일반적으로) | 컨슈머가 필드 참조 시 NPE 위험 |

## 3) 구버전 컨슈머가 신버전 이벤트를 정상 처리하는지 시뮬레이션 (선택 요구사항 #10)

1. notification-service를 **현재 스키마(v1)** 로 기동해둔 상태 유지
2. transaction-service만 스키마에 `channel`(default 포함) 필드를 추가하고 재배포
3. transaction-service가 v2 스키마로 이벤트 발행
4. notification-service(v1 컨슈머)가 여전히 정상적으로 이벤트를 수신/저장하는지 확인
   → BACKWARD 호환성 덕분에 컨슈머 재배포 없이도 정상 동작해야 함 (신규 필드는 무시됨)

## 참고

- 강의노트 Section 10 (Encryption), Section 12 (Kafka) 참고
- Confluent 공식 문서: https://docs.confluent.io/platform/current/schema-registry/avro.html
- 호환성 타입: BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, NONE
