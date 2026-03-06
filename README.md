# Dev PubSub

로컬 개발용 Google Cloud Pub/Sub 에뮬레이터. 실시간 웹 대시보드 포함.

`PUBSUB_EMULATOR_HOST=localhost:8085` 환경변수 하나만 설정하면 Go/Python 서비스가 GCP 대신 로컬 에뮬레이터에 자동 연결됩니다. 인증 불필요.

---

## 기능

- **gRPC API** — `google.pubsub.v1` Publisher/Subscriber 서비스 구현 (StreamingPull 포함)
- **실시간 대시보드** — 토픽/구독 상태, 메시지 흐름을 WebSocket으로 실시간 모니터링
- **YAML 설정** — 서버 시작 시 토픽/구독 자동 생성
- **REST API** — HTTP로 상태 조회, 수동 메시지 발행
- **인메모리** — 가볍게 동작, 재시작 시 초기화

---

## 빠른 시작

### 요구사항

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)

### 설치 및 실행

```bash
uv sync
uv run dev-pubsub
```

```
INFO  Created topic: projects/postmath-dev/topics/ai-service-request-emulator
INFO  Created subscription: projects/postmath-dev/subscriptions/ai-service-request-sub-emulator
INFO  gRPC server listening on :8085
INFO  Dashboard: http://localhost:8086
```

- gRPC: `:8085` — Pub/Sub 클라이언트 연결용
- HTTP: `:8086` — 대시보드 및 REST API

### 설정 파일 지정

```bash
uv run dev-pubsub --config path/to/my-config.yaml
```

---

## 설정 (config.yaml) 예시

```yaml
project_id: postmath-dev
grpc_port: 8085
web_port: 8086

topics:
  - name: ai-service-request-emulator
    subscriptions:
      - ai-service-request-sub-emulator

  - name: ai-service-response-emulator
    subscriptions:
      - ai-service-response-ags-sub-emulator
      - ai-service-response-dps-sub-emulator
      - ai-service-response-cms-sub-emulator

  - name: bms2dps-emulator
    subscriptions:
      - bms2dps-sub-emulator

  - name: dps2cms-migration-emulator
    subscriptions:
      - dps2cms-migration-sub-emulator
```

서버 시작 시 각 토픽/구독이 `projects/{project_id}/topics/{name}`, `projects/{project_id}/subscriptions/{name}` 형식으로 자동 생성됩니다. 런타임에 gRPC `CreateTopic`/`CreateSubscription` 호출로 추가 생성도 가능합니다.

---

## 클라이언트 연결

### Go

코드 변경 없이 환경변수만 설정하면 됩니다.

```env
PUBSUB_EMULATOR_HOST=localhost:8085
GCP_PROJECT_ID=postmath-dev
```

`cloud.google.com/go/pubsub/v2` 라이브러리가 `PUBSUB_EMULATOR_HOST`를 감지하면 해당 주소로 인증 없이 자동 연결합니다.

### Python

`google-cloud-pubsub` 라이브러리도 동일하게 `PUBSUB_EMULATOR_HOST`를 지원합니다.

```python
import os
from google.cloud import pubsub_v1

if os.getenv("PUBSUB_EMULATOR_HOST"):
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
else:
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_file(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
```

---

## 대시보드

[http://localhost:8086](http://localhost:8086)

| 영역 | 내용 |
|------|------|
| **Topics** | 토픽 목록, 연결된 구독 표시 |
| **Subscriptions** | 구독별 pending/outstanding 메시지 수, 활성 스트림 수 |
| **Messages** | 실시간 메시지 스트림, ACK/UNACK 상태 배지, 전체 데이터 표시 |
| **Stats** | Published / Delivered / Acked / Pending 카운터 |
| **Publish** | 토픽 선택 후 수동 메시지 발행 (테스트용) |

메시지 패널에서 **All / ACK / UNACK** 필터로 상태별 필터링이 가능합니다.

---

## REST API

| Method | Path | 설명 |
|--------|------|------|
| `GET` | `/api/topics` | 토픽 목록 |
| `GET` | `/api/subscriptions` | 구독 목록 (pending/outstanding/streams) |
| `GET` | `/api/stats` | 통계 (published, delivered, acked, pending) |
| `GET` | `/api/messages` | 최근 메시지 히스토리 (최대 100개) |
| `POST` | `/api/publish/{topic_name}` | 토픽에 메시지 발행 |
| `WS` | `/ws` | 실시간 메시지 이벤트 스트림 |

```bash
# 통계 조회
curl http://localhost:8086/api/stats

# 메시지 발행
curl -X POST http://localhost:8086/api/publish/ai-service-request-emulator \
  -H 'Content-Type: application/json' \
  -d '{
    "data": "{\"task_id\": \"test-123\", \"task_type\": \"cell_ocr\"}",
    "attributes": {"source": "manual-test"}
  }'
```

---

## gRPC API

`google.pubsub.v1` 표준 API를 구현합니다.

### Publisher

| RPC | 상태 |
|-----|------|
| `CreateTopic` | 지원 |
| `GetTopic` | 지원 (없으면 자동 생성) |
| `Publish` | 지원 (모든 구독에 fan-out) |
| `ListTopics` | 지원 |
| `ListTopicSubscriptions` | 지원 |
| `DeleteTopic` | 지원 |
| `UpdateTopic` | 지원 (labels) |
| `DetachSubscription` | Stub |

### Subscriber

| RPC | 상태 |
|-----|------|
| `CreateSubscription` | 지원 |
| `GetSubscription` | 지원 |
| `Pull` | 지원 |
| `StreamingPull` | 지원 (양방향 스트리밍) |
| `Acknowledge` | 지원 |
| `ModifyAckDeadline` | 지원 (deadline=0으로 nack) |
| `DeleteSubscription` | 지원 |
| `ListSubscriptions` | 지원 |
| `UpdateSubscription` | 지원 |
| `Seek` / `Snapshot` | Stub |

### StreamingPull 동작

Go pubsub v2의 `Receive()`가 사용하는 핵심 RPC입니다.

1. 클라이언트가 subscription 이름과 ack deadline을 담은 초기 요청 전송
2. 서버가 해당 구독에 `asyncio.Queue`를 등록
3. 두 개의 비동기 루프가 동시 실행:
   - **recv_loop** — 클라이언트의 ack/nack/deadline 연장 요청 처리
   - **send_loop** — 큐에서 메시지를 가져와 클라이언트에 전달
4. 구독에 여러 스트림이 연결된 경우 (Go가 여러 goroutine을 띄움) **round-robin**으로 메시지를 하나의 스트림에만 전달
5. 연결 해제 시 자동 정리

---

## 메시지 흐름

```
Publish(topic, message)
        │
        ▼
  MessageBroker: 해당 topic의 모든 subscription에 fan-out
        │
        ├─ 활성 스트림 있음 → round-robin으로 하나의 스트림에 전달
        └─ 활성 스트림 없음 → pending 큐에 저장 (스트림 연결 시 즉시 전달)
        │
        ▼
  클라이언트가 메시지 수신
        │
        ├─ Ack     → outstanding에서 제거
        ├─ Nack    → 즉시 재전달
        └─ Timeout → deadline 만료 후 자동 재전달 (기본 10초)
```

---

## 프로젝트 구조

```
src/dev_pubsub/
├── __main__.py              # 엔트리포인트 (gRPC + FastAPI + deadline checker)
├── config.py                # YAML 설정 로딩 + CLI 인자
├── broker.py                # 인메모리 메시지 브로커 (fan-out, ack, deadline)
├── grpc_server.py           # gRPC 서버 부트스트랩
├── publisher_servicer.py    # Publisher gRPC 서비스
├── subscriber_servicer.py   # Subscriber gRPC 서비스 (StreamingPull)
├── web_server.py            # FastAPI REST + WebSocket
├── generated/               # proto 컴파일 결과 (git-tracked)
│   └── google/pubsub/v1/
└── static/
    └── index.html           # 대시보드 (vanilla JS)
```

---

## Proto 재컴파일

생성된 proto 파일은 git에 포함되어 있어 보통 재컴파일이 필요 없습니다. 업데이트가 필요한 경우:

```bash
uv run bash scripts/generate_protos.sh
```

---

## 트러블슈팅

**구독을 찾을 수 없다는 오류가 발생할 때**
- `config.yaml` 파일이 정상적으로 로드되는지 확인. 존재하지 않는 경로를 `--config`로 지정하면 빈 설정으로 시작됩니다.
- `curl http://localhost:8086/api/subscriptions`로 구독 목록 확인

**메시지가 중복 수신될 때**
- ack deadline 내에 처리를 완료하지 못하면 메시지가 재전달됩니다. 서비스의 처리 시간을 확인하세요.

**WebSocket 관련 경고가 나올 때**
- `uvicorn[standard]`이 설치되어야 합니다. `uv sync`로 의존성을 재설치하세요.

---

## License

MIT
