#!/usr/bin/env bash
#
# Schema Registry 데모 스크립트
# - 현재 등록된 subject/스키마 확인
# - BACKWARD 호환성 있는 필드 추가 시나리오 검증
# - BACKWARD 호환성 없는 필드 추가 시나리오 검증
#
# 사전 조건: docker-compose로 schema-registry, kafka, transaction-service가 기동 중이어야 하며
#            /transactions/simulate 를 최소 1회 호출해 스키마가 등록되어 있어야 함.

set -euo pipefail

SR_URL="${SR_URL:-http://localhost:8091}"
SUBJECT="fin.transaction.events-value"

echo "=== 1) 등록된 subject 목록 ==="
curl -s "${SR_URL}/subjects" | tee /dev/stderr
echo

echo "=== 2) ${SUBJECT} 최신 스키마 ==="
curl -s "${SR_URL}/subjects/${SUBJECT}/versions/latest" | tee /dev/stderr
echo

echo "=== 3) 호환성 모드 확인 ==="
curl -s "${SR_URL}/config/${SUBJECT}" | tee /dev/stderr
echo

# --- 시나리오 A: default 값이 있는 필드 추가 -> 호환 가능해야 함 ---
COMPATIBLE_SCHEMA=$(cat <<'EOF'
{
  "type": "record",
  "name": "TransactionEvent",
  "namespace": "com.finaccounthub.avro",
  "fields": [
    { "name": "transactionId", "type": "string" },
    { "name": "accountId", "type": "string" },
    { "name": "userId", "type": "string" },
    { "name": "transactionType", "type": "string" },
    { "name": "amount", "type": "long" },
    { "name": "occurredAt", "type": "string" },
    { "name": "fromAccountId", "type": ["null", "string"], "default": null },
    { "name": "toAccountId", "type": ["null", "string"], "default": null },
    { "name": "status", "type": "string", "default": "SUCCESS" },
    { "name": "channel", "type": "string", "default": "MOBILE" }
  ]
}
EOF
)

echo "=== 4) [시나리오 A] default 있는 필드 추가 -> 호환성 체크 (기대: true) ==="
python3 - "$COMPATIBLE_SCHEMA" "$SR_URL" "$SUBJECT" <<'PYEOF'
import json, sys, urllib.request

schema, sr_url, subject = sys.argv[1], sys.argv[2], sys.argv[3]
payload = json.dumps({"schema": schema}).encode()
req = urllib.request.Request(
    f"{sr_url}/compatibility/subjects/{subject}/versions/latest",
    data=payload,
    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    method="POST",
)
with urllib.request.urlopen(req) as resp:
    print(resp.read().decode())
PYEOF
echo

# --- 시나리오 B: default 없는 필드 추가 -> 호환 불가해야 함 ---
INCOMPATIBLE_SCHEMA=$(cat <<'EOF'
{
  "type": "record",
  "name": "TransactionEvent",
  "namespace": "com.finaccounthub.avro",
  "fields": [
    { "name": "transactionId", "type": "string" },
    { "name": "accountId", "type": "string" },
    { "name": "userId", "type": "string" },
    { "name": "transactionType", "type": "string" },
    { "name": "amount", "type": "long" },
    { "name": "occurredAt", "type": "string" },
    { "name": "fromAccountId", "type": ["null", "string"], "default": null },
    { "name": "toAccountId", "type": ["null", "string"], "default": null },
    { "name": "status", "type": "string", "default": "SUCCESS" },
    { "name": "channel", "type": "string" }
  ]
}
EOF
)

echo "=== 5) [시나리오 B] default 없는 필드 추가 -> 호환성 체크 (기대: false) ==="
python3 - "$INCOMPATIBLE_SCHEMA" "$SR_URL" "$SUBJECT" <<'PYEOF'
import json, sys, urllib.request

schema, sr_url, subject = sys.argv[1], sys.argv[2], sys.argv[3]
payload = json.dumps({"schema": schema}).encode()
req = urllib.request.Request(
    f"{sr_url}/compatibility/subjects/{subject}/versions/latest",
    data=payload,
    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    method="POST",
)
with urllib.request.urlopen(req) as resp:
    print(resp.read().decode())
PYEOF
echo

echo "=== 완료. docs/schema-registry-guide.md 의 결과표에 위 출력을 반영하세요. ==="
