# Sogang Notices to Notion

서강대학교 장학/학사 공지를 수집해 Notion 데이터베이스로 동기화하는 개인용 CLI 프로젝트다. 새 공지 등록 여부를 Notion 쪽 자동화와 함께 확인할 수 있도록, 공지 목록 수집부터 상세 본문·첨부파일 처리, 기존 페이지 갱신까지 한 번에 수행한다.

## 개요

- 기본 대상 게시판은 장학공지(`141`)와 학사공지(`2`)다.
- 공지 목록은 API를 우선 사용하고, 필요할 때 Playwright 기반 수집으로 보완한다.
- Notion 페이지 생성·업데이트와 중복 URL 정리, 본문 해시 기반 변경 감지를 함께 수행한다.
- 운영 로그에서 어느 공지와 어느 단계에서 실패했는지 바로 확인할 수 있도록 진단 로그를 강화했다.
- 로컬 실행과 GitHub Actions 정기 실행을 모두 지원한다.

## 프로젝트 구조

```text
.
├─ .github/
│  └─ workflows/
│     └─ crawler.yml
├─ project/
│  └─ README.md
├─ scripts/
│  ├─ README.md
│  ├─ bbs_parser.py
│  ├─ crawler.py
│  ├─ log.py
│  ├─ main.py
│  ├─ notion_client.py
│  ├─ settings.py
│  ├─ sync.py
│  └─ utils.py
├─ .env.example
├─ .gitignore
├─ main.py
├─ requirements.txt
└─ README.md
```

- 루트 `main.py`는 기존 `python main.py` 실행 습관을 유지하기 위한 얇은 진입점이다.
- 실제 실행 로직은 `scripts/main.py`와 관련 모듈에 모여 있다.
- `project/`는 로컬 내부 운영 문서를 두기 위한 디렉토리이며, 현재 공개 저장소에는 `README.md`만 남기고 나머지는 `.gitignore`로 제외한다.

## 요구 사항

- Python `3.11+`
- Notion 통합 토큰과 데이터베이스 ID
- Playwright용 Chromium 브라우저

## 설치

### 1. 가상환경 생성

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. 의존성 설치

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m playwright install --with-deps chromium
```

## 설정

먼저 예시 파일을 복사한 뒤, 필수 값만 채우면 된다.

```bash
cp .env.example .env
```

필수 환경변수:

```env
NOTION_TOKEN=your_notion_token
NOTION_DB_ID=your_database_id
```

불리언 성격의 옵션은 `1/0`, `true/false`, `yes/no`, `on/off` 중 하나로 줄 수 있다.

## 실행

기본 실행:

```bash
python main.py
```

로컬 HTML 파일을 입력으로 사용할 때:

```bash
HTML_PATH=sample.html python main.py
```

첨부파일 정책 자체 점검만 수행할 때:

```bash
ATTACHMENT_SELFTEST=1 python main.py
```

## Notion 데이터베이스 동기화 기준

다음 속성을 확인하거나 필요 시 생성·보정한다.

- `공지사항` `title`
- `TOP` `checkbox`
- `작성일` `date`
- `작성자` `select`
- `URL` `url`
- `유형` `select`
- `분류` `select`
- `조회수` `number`
- `첨부파일` `files`
- `본문 해시` `rich_text`

이미 `공지사항`이 아닌 다른 이름의 제목 속성이 있다면, 실행 중 해당 제목 속성 이름을 `공지사항`으로 맞춘다. 반대로 같은 이름의 비제목 속성이 이미 있거나 기대 타입이 맞지 않으면 동기화가 실패할 수 있다.

## 환경 변수

| 변수 | 필수 여부 | 기본값 | 설명 |
| --- | --- | --- | --- |
| `NOTION_TOKEN` | 필수 | 없음 | Notion 통합 토큰 |
| `NOTION_DB_ID` | 필수 | 없음 | 대상 Notion 데이터베이스 ID |
| `NOTION_API_VERSION` | 선택 | `2022-06-28` | Notion API 버전 |
| `BBS_CONFIG_FKS` | 선택 | `141,2` | 기본 수집 대상 게시판 목록 |
| `BBS_CONFIG_CLASSIFY` | 선택 | `141:장학공지,2:학사공지` | 게시판 ID별 분류명 매핑 |
| `BBS_CONFIG_LIST_URLS` | 선택 | 코드 내 기본 URL | 게시판 ID별 목록 URL 재정의 |
| `BBS_CONFIG_FK` | 선택 | 첫 번째 게시판 ID | 단일 게시판 기준이 필요한 경우의 보조 설정 |
| `BBS_PAGE_SIZE` | 선택 | `20` | API 요청 시 페이지당 항목 수 |
| `SYNC_MODE` | 선택 | `overwrite` | 본문 동기화 방식. `overwrite` 또는 `preserve` |
| `NOTION_DEDUPE_ON_START` | 선택 | `1` | 시작 시 URL 기준 중복 페이지 정리 여부 |
| `NOTION_UPLOAD_FILES` | 선택 | `1` | 이미지 첨부를 Notion 파일로 업로드할지 여부 |
| `INCLUDE_NON_TOP` | 선택 | `1` | 비TOP 공지까지 포함할지 여부 |
| `NON_TOP_MAX_PAGES` | 선택 | `2` | 비TOP 공지 탐색 최대 페이지 수. `0`이면 제한 없음 |
| `BROWSER` | 선택 | `chromium` | Playwright 브라우저 종류 |
| `HEADLESS` | 선택 | `1` | 브라우저를 헤드리스 모드로 실행할지 여부 |
| `USER_AGENT` | 선택 | 기본 사용자 에이전트 | 브라우저/HTTP 요청 시 사용자 에이전트 |
| `ATTACHMENT_ALLOWED_DOMAINS` | 선택 | `sogang.ac.kr` | 첨부파일 다운로드 허용 도메인 목록 |
| `ATTACHMENT_MAX_COUNT` | 선택 | `15` | 공지당 첨부파일 최대 반영 개수 |
| `ATTACHMENT_SELFTEST` | 선택 | 꺼짐 | 첨부 정책 자체 점검만 수행하고 종료 |
| `HTML_PATH` | 선택 | 없음 | 수집 대신 로컬 HTML 파일을 입력으로 사용 |

## GitHub Actions

`.github/workflows/crawler.yml`은 다음 두 방식으로 실행된다.

- 매시 정각 스케줄 실행
- 수동 `workflow_dispatch`

GitHub Actions에서 실행하려면 저장소 `Secrets`에 아래 값을 등록해야 한다.

- `NOTION_TOKEN`
- `NOTION_DB_ID`

## 검증

현재 별도의 자동 테스트 스위트는 없다. 대신 아래 정도를 기본 점검 경로로 본다.

```bash
python -m py_compile main.py scripts/*.py
ATTACHMENT_SELFTEST=1 python main.py
```
