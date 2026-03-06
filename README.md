# Dev PubSub

로컬 개발용 Google Cloud Pub/Sub gRPC 에뮬레이터

## 이게 뭔가요?

Go 서비스(DPS 등)가 Google Cloud Pub/Sub를 사용해서 메시지를 발행/구독하는데, 개발할 때마다 GCP에 연결하는 건 번거롭습니다. 이 에뮬레이터를 로컬에 띄우면 **GCP 연결 없이** 바로 Pub/Sub를 사용할 수 있습니다.

Go pubsub v2 클라이언트 라이브러리(`cloud.google.com/go/pubsub/v2`)는 `PUBSUB_EMULATOR_HOST` 환경변수가 설정되면 해당 주소의 gRPC 서버로 **인증 없이** 자동 연결합니다. 이 프로젝트는 그 gRPC API(`google.pubsub.v1`)를 Python으로 구현한 것입니다.

## 아키텍처

```
┌─────────────────────────────────────────────┐
│         Dev PubSub (Python/uv)              │
│                                             │
│  ┌──────────────────┐  ┌─────────────────┐  │
│  │  gRPC Server     │  │  FastAPI + WS   │  │
│  │  :8085           │  │  :8086          │  │
│  │  (Publisher API)  │  │  (Dashboard)    │  │
│  │  (Subscriber API) │  │  (REST + WS)   │  │
│  └────────┬─────────┘  └───────┬─────────┘  │
│           │                     │            │
│  ┌────────┴─────────────────────┴─────────┐  │
│  │       MessageBroker (in-memory)         │  │
│  │  Topics → Subscriptions → Messages      │  │
│  └─────────────────────────────────────────┘  │
└─────────────────────────────────────────────┘

Go Service (DPS 등)
  PUBSUB_EMULATOR_HOST=localhost:8085
  → gRPC로 Publish/StreamingPull 수행
```

## 기능

- **gRPC API** (`:8085`)
  - `google.pubsub.v1.Publisher`: CreateTopic, Publish, GetTopic, ListTopics, DeleteTopic 등
  - `google.pubsub.v1.Subscriber`: CreateSubscription, Pull, **StreamingPull**, Acknowledge, ModifyAckDeadline 등
  - Go pubsub v2 클라이언트가 사용하는 모든 핵심 RPC 지원
  - 런타임에 gRPC로 토픽/구독 추가 생성 가능
- **웹 대시보드** (`:8086`)
  - 토픽/구독 상태 실시간 모니터링
  - 메시지 흐름 실시간 표시 (WebSocket)
  - 수동 메시지 발행 (테스트용)
  - Published / Delivered / Acked 카운터
- **YAML 설정**: 서버 시작 시 토픽/구독 자동 생성
- **인메모리**: 영속성 없이 가볍게 동작 (개발용이므로 재시작하면 초기화)
- **Ack Deadline 관리**: 만료된 메시지 자동 재전달

## 빠른 시작

### 1. 의존성 설치

```bash
cd /workspace/dev_pubsub
uv sync
```

### 2. 에뮬레이터 실행

```bash
uv run dev-pubsub
```

시작하면 다음과 같이 출력됩니다:

```
19:57:01 [INFO] dev_pubsub.broker: Created topic: projects/postmath-dev/topics/ai-request-topic
19:57:01 [INFO] dev_pubsub.broker: Created topic: projects/postmath-dev/topics/ai-response-topic
19:57:01 [INFO] dev_pubsub.broker: Created subscription: projects/postmath-dev/subscriptions/ai-response-sub -> projects/postmath-dev/topics/ai-response-topic
19:57:01 [INFO] dev_pubsub.broker: Created topic: projects/postmath-dev/topics/bms2dps-topic
19:57:01 [INFO] dev_pubsub.broker: Created subscription: projects/postmath-dev/subscriptions/bms2dps-sub -> projects/postmath-dev/topics/bms2dps-topic
19:57:01 [INFO] dev_pubsub.broker: Created topic: projects/postmath-dev/topics/dps2cms-migration-topic
19:57:01 [INFO] dev_pubsub: gRPC server listening on :8085
19:57:01 [INFO] dev_pubsub: Dashboard: http://localhost:8086
```

### 3. Go 서비스 연결

Go 서비스의 `.env` 파일에 환경변수를 추가합니다:

```env
# 에뮬레이터 연결 (이 변수가 있으면 Go pubsub 라이브러리가 GCP 대신 로컬로 연결)
PUBSUB_EMULATOR_HOST=localhost:8085

# 프로젝트 ID (config.yaml의 project_id와 일치해야 함)
GCP_PROJECT_ID=postmath-dev

# 토픽/구독 이름 (config.yaml에 정의된 이름과 일치해야 함)
REQUEST_TOPIC_ID=ai-request-topic
RESPONSE_SUBSCRIPTION_ID=ai-response-sub
BMS2DPS_SUBSCRIPTION_ID=bms2dps-sub
DPS2CMS_MIGRATION_TOPIC_ID=dps2cms-migration-topic
```

그 다음 Go 서비스를 실행하면 자동으로 에뮬레이터에 연결됩니다. 코드 변경은 필요 없습니다.

### 4. 대시보드 확인

브라우저에서 [http://localhost:8086](http://localhost:8086) 접속:

- **Topics 패널**: 생성된 토픽 목록과 연결된 구독
- **Subscriptions 패널**: 구독별 pending/outstanding 메시지 수, 활성 스트림 수
- **Messages 패널**: 발행된 메시지 실시간 스트리밍 (데이터, 속성, 타임스탬프)
- **하단 발행 폼**: 토픽 선택 후 수동으로 메시지 발행 가능 (테스트용)

## 설정 (config.yaml)

```yaml
# 에뮬레이터 서버 설정
project_id: postmath-dev      # GCP 프로젝트 ID (Go 서비스의 GCP_PROJECT_ID와 일치)
grpc_port: 8085               # gRPC 서버 포트
web_port: 8086                # 대시보드 HTTP 포트

# 토픽-구독 매핑
# 서버 시작 시 자동으로 생성됩니다.
topics:
  - name: ai-request-topic
    # 구독 없음: DPS가 발행, AI 서비스가 구독하는 토픽

  - name: ai-response-topic
    subscriptions:
      - ai-response-sub        # Go 서비스의 RESPONSE_SUBSCRIPTION_ID

  - name: bms2dps-topic
    subscriptions:
      - bms2dps-sub            # Go 서비스의 BMS2DPS_SUBSCRIPTION_ID

  - name: dps2cms-migration-topic
    # 구독 없음: DPS가 발행, CMS가 구독하는 토픽
```

### 동작 원리

1. 서버 시작 시 `config.yaml`을 읽어 토픽/구독을 `projects/{project_id}/topics/{name}` 형식으로 생성
2. Go 서비스가 `PUBSUB_EMULATOR_HOST=localhost:8085` 설정 후 접속하면, 토픽/구독이 이미 존재
3. `Publish` 호출 시 해당 토픽에 연결된 모든 구독에 메시지를 fan-out
4. `StreamingPull`로 연결된 구독자에게 실시간 전달, 미연결 시 pending 큐에 저장
5. 런타임에 gRPC `CreateTopic`/`CreateSubscription` 호출로 추가 생성도 가능

### 커스텀 설정 파일

```bash
uv run dev-pubsub --config my-config.yaml
```

## REST API

대시보드 외에 HTTP API로도 상태 조회 및 메시지 발행이 가능합니다.

### 상태 조회

```bash
# 토픽 목록
curl http://localhost:8086/api/topics

# 구독 목록 (pending/outstanding 메시지 수, 활성 스트림 수 포함)
curl http://localhost:8086/api/subscriptions

# 전체 통계 (published, delivered, acked, pending, outstanding)
curl http://localhost:8086/api/stats

# 최근 메시지 히스토리 (최대 100개)
curl http://localhost:8086/api/messages
```

### 수동 메시지 발행 (테스트용)

```bash
# 토픽에 메시지 발행
curl -X POST http://localhost:8086/api/publish/ai-response-topic \
  -H 'Content-Type: application/json' \
  -d '{
    "data": "{\"task_id\": \"test-123\", \"status\": \"success\"}",
    "attributes": {"source": "test", "task": "page-detect"}
  }'
```

### WebSocket

`ws://localhost:8086/ws`에 연결하면 메시지 발행 이벤트를 실시간으로 수신합니다.

## 프로젝트 구조

```
src/dev_pubsub/
├── __main__.py              # 엔트리포인트 (gRPC + FastAPI + deadline checker 동시 실행)
├── config.py                # config.yaml 로딩 + CLI 인자 파싱
├── broker.py                # 인메모리 메시지 브로커 (fan-out, ack, deadline 관리)
├── grpc_server.py           # gRPC 서버 부트스트랩
├── publisher_servicer.py    # gRPC Publisher 서비스 구현
├── subscriber_servicer.py   # gRPC Subscriber 서비스 구현 (StreamingPull 포함)
├── web_server.py            # FastAPI REST API + WebSocket
├── generated/               # proto 컴파일 결과 (git-tracked)
│   └── google/pubsub/v1/
│       ├── pubsub_pb2.py
│       └── pubsub_pb2_grpc.py
└── static/
    └── index.html           # 대시보드 (vanilla JS + WebSocket)
```

## 지원하는 gRPC 메서드

### Publisher (`google.pubsub.v1.Publisher`)

| 메서드 | 설명 |
|--------|------|
| `CreateTopic` | 토픽 생성 |
| `GetTopic` | 토픽 조회 (없으면 자동 생성) |
| `Publish` | 메시지 발행 (fan-out) |
| `ListTopics` | 토픽 목록 |
| `ListTopicSubscriptions` | 토픽에 연결된 구독 목록 |
| `DeleteTopic` | 토픽 삭제 |
| `UpdateTopic` | 토픽 라벨 업데이트 |

### Subscriber (`google.pubsub.v1.Subscriber`)

| 메서드 | 설명 |
|--------|------|
| `CreateSubscription` | 구독 생성 |
| `GetSubscription` | 구독 조회 |
| `Pull` | 메시지 가져오기 (단발성) |
| `StreamingPull` | 양방향 스트리밍으로 메시지 수신 (Go v2 기본 방식) |
| `Acknowledge` | 메시지 처리 완료 확인 |
| `ModifyAckDeadline` | Ack deadline 연장 / Nack (deadline=0) |
| `DeleteSubscription` | 구독 삭제 |
| `ListSubscriptions` | 구독 목록 |
| `UpdateSubscription` | 구독 설정 업데이트 |

## Proto 재컴파일

생성된 proto 파일은 `src/dev_pubsub/generated/`에 git-tracked되어 있어 보통은 재컴파일이 필요 없습니다. proto 정의를 업데이트해야 할 경우:

```bash
uv run bash scripts/generate_protos.sh
```

googleapis에서 최신 `google/pubsub/v1/pubsub.proto`를 다운로드하고, `grpc_tools.protoc`으로 컴파일한 뒤, import 경로를 패키지 상대경로로 자동 수정합니다.

## 기술 스택

| 항목 | 선택 | 이유 |
|------|------|------|
| gRPC 모드 | `grpc.aio` (async) | StreamingPull 양방향 스트리밍에 async 필수 |
| 메시지 저장 | 인메모리 | 개발용이므로 영속성 불필요 |
| 토픽/구독 관리 | YAML 설정 + gRPC API | 시작 시 초기화 + 런타임 추가 가능 |
| 대시보드 | vanilla JS | 외부 의존성 없이 단일 HTML |
| HTTP 프레임워크 | FastAPI | async 지원 + WebSocket 내장 |
| proto 결과물 | git-tracked | 매번 재컴파일 불필요 |
