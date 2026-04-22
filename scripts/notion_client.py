import json
import mimetypes
import os
import re
import socket
import time
import urllib.error
import urllib.request
import uuid
from io import BytesIO
from typing import Callable, Optional, TypeVar
from urllib.parse import urlencode, urlsplit

from log import LOGGER
from settings import (
    ATTACHMENT_PROPERTY,
    AUTHOR_PROPERTY,
    BODY_HASH_PROPERTY,
    CLASSIFICATION_PROPERTY,
    DATE_PROPERTY,
    FALLBACK_TYPE,
    PAGE_ICON_EMOJI,
    TITLE_PROPERTY,
    TOP_PROPERTY,
    TYPE_TAGS,
    TYPE_PROPERTY,
    URL_PROPERTY,
    VIEWS_PROPERTY,
    get_notion_api_version,
    should_upload_files_to_notion,
)
from utils import (
    build_file_block,
    build_pdf_block,
    build_site_headers,
    build_uploaded_file_hash_block,
    build_uploaded_image_hash_block,
    derive_filename_from_url,
    extract_attachment_name,
    is_allowed_external_download_url,
    is_embed_file_candidate,
    is_image_name_or_url,
    is_pdf_name_or_url,
    normalize_content_type,
    sanitize_filename,
)

FILE_UPLOAD_CACHE: dict[str, str] = {}
WORKSPACE_UPLOAD_LIMIT: Optional[int] = None
# Notion 문서 기준 평균 3 req/s 수준을 넘지 않도록 프로세스 전체 요청 간격을 완만하게 제한한다.
NOTION_MIN_REQUEST_INTERVAL_SECONDS = 0.35
NOTION_MAX_RETRIES = 5
NOTION_RATE_LIMIT_BASE_DELAY_SECONDS = 3.0
NOTION_TRANSIENT_BASE_DELAY_SECONDS = 1.0
NEXT_NOTION_REQUEST_AT = 0.0
EXTERNAL_FETCH_MAX_RETRIES = 3
EXTERNAL_UPLOAD_MAX_RETRIES = 3
# DB 공유 상태가 직전에 바뀌었거나 Notion이 일시적으로 object_not_found를 돌려줄 때를 대비해 같은 ID만 짧게 재확인한다.
NOTION_DATABASE_OBJECT_NOT_FOUND_MAX_ATTEMPTS = 3
DATABASE_OBJECT_NOT_FOUND_BASE_DELAY_SECONDS = 1.0

_T = TypeVar("_T")


class NotionRequestError(RuntimeError):
    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        reason: Optional[str] = None,
        method: Optional[str] = None,
        target: Optional[str] = None,
        notion_code: Optional[str] = None,
        request_id: Optional[str] = None,
        hint: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.reason = reason
        self.method = method
        self.target = target
        self.notion_code = notion_code
        self.request_id = request_id
        self.hint = hint


def is_database_object_not_found_error(exc: NotionRequestError) -> bool:
    return exc.status_code == 404 and exc.notion_code == "object_not_found"


# 같은 database_id로만 재확인해서 일시적 접근 이상과 실제 설정 오류를 운영 로그에서 구분하기 쉽게 만든다.
def run_database_request_with_object_not_found_retry(
    request_fn: Callable[[], _T],
    *,
    method: str,
    database_id: str,
    action_name: str,
) -> _T:
    total_attempts = NOTION_DATABASE_OBJECT_NOT_FOUND_MAX_ATTEMPTS
    last_exc: Optional[NotionRequestError] = None
    for attempt in range(total_attempts):
        try:
            return request_fn()
        except NotionRequestError as exc:
            if not is_database_object_not_found_error(exc):
                raise
            last_exc = exc
            if attempt + 1 >= total_attempts:
                raise
            sleep_s = get_database_object_not_found_retry_sleep_seconds(attempt)
            LOGGER.warning(
                "Notion 데이터베이스 재확인: 동작=%s, method=%s, database_id=%s, 다음 시도=%s/%s, 대기=%.1fs",
                action_name,
                method,
                summarize_database_id(database_id),
                attempt + 2,
                total_attempts,
                sleep_s,
            )
            time.sleep(sleep_s)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Notion 데이터베이스 재확인 로직이 비정상 종료되었습니다")


# Notion 오류는 응답 JSON 형태가 일정해서, 상위 로그에 바로 도움이 되는 정보만 추려둔다.
def summarize_request_target(url: str) -> str:
    parsed = urlsplit(url)
    return parsed.path or "/"


def truncate_error_text(text: str, limit: int = 240) -> str:
    cleaned = " ".join((text or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: limit - 3]}..."


def parse_notion_error_payload(body_text: str) -> dict:
    try:
        payload = json.loads(body_text)
    except json.JSONDecodeError:
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def build_notion_error_hint(
    status_code: Optional[int],
    notion_code: Optional[str],
) -> str:
    if status_code == 401:
        return "토큰 값과 만료 여부를 확인"
    if status_code == 403:
        return "integration 권한과 데이터베이스 공유 상태를 확인"
    if status_code == 404 and notion_code == "object_not_found":
        return "대상 ID, integration 공유 상태, 토큰이 연결된 워크스페이스를 확인"
    if status_code == 400:
        return "속성 이름, 속성 타입, 요청 payload를 확인"
    if status_code == 409:
        return "동시 수정 충돌 가능성이 있어 잠시 후 재시도"
    if status_code == 429:
        return "요청량 제한이 걸려 재시도가 필요"
    return ""


def format_notion_error_message(
    method: str,
    target: str,
    status_code: Optional[int],
    notion_code: Optional[str],
    reason: str,
    request_id: Optional[str],
    hint: Optional[str],
) -> str:
    parts = [f"Notion API error: {method} {target}"]
    if status_code is not None:
        parts.append(f"HTTP {status_code}")
    if notion_code:
        parts.append(notion_code)
    if reason:
        parts.append(f"message={truncate_error_text(reason)}")
    if request_id:
        parts.append(f"request_id={request_id}")
    if hint:
        parts.append(f"hint={hint}")
    return " | ".join(parts)


# Retry-After 헤더는 초 단위 문자열이므로, 정수·실수 형태를 모두 안전하게 읽어둔다.
def parse_retry_after_seconds(raw_value: Optional[str]) -> Optional[float]:
    if raw_value is None:
        return None
    try:
        seconds = float(str(raw_value).strip())
    except ValueError:
        return None
    return max(0.0, seconds)


# 요청 시작 시점을 일정 간격으로 벌려서 연속 조회와 본문 동기화가 한꺼번에 몰리지 않게 한다.
def wait_for_notion_request_slot() -> None:
    global NEXT_NOTION_REQUEST_AT
    now = time.monotonic()
    sleep_s = NEXT_NOTION_REQUEST_AT - now
    if sleep_s > 0:
        time.sleep(sleep_s)
        now = time.monotonic()
    NEXT_NOTION_REQUEST_AT = now + NOTION_MIN_REQUEST_INTERVAL_SECONDS


# 429는 일반 네트워크 오류보다 더 길게 기다려야 다시 성공하는 경우가 많아서 별도 backoff를 둔다.
def get_retry_sleep_seconds(
    attempt: int,
    status_code: Optional[int] = None,
    retry_after: Optional[str] = None,
) -> float:
    if status_code == 429:
        header_delay = parse_retry_after_seconds(retry_after) or 0.0
        backoff_delay = min(
            NOTION_RATE_LIMIT_BASE_DELAY_SECONDS * (2**attempt),
            30.0,
        )
        return max(header_delay, backoff_delay)
    return min(NOTION_TRANSIENT_BASE_DELAY_SECONDS * (2**attempt), 8.0)


def get_database_object_not_found_retry_sleep_seconds(attempt: int) -> float:
    return min(DATABASE_OBJECT_NOT_FOUND_BASE_DELAY_SECONDS * (2**attempt), 4.0)


# 업로드 URL에 토큰 헤더를 붙일 때는 부분 문자열이 아니라 스킴과 호스트를 함께 검사해야 한다.
def is_notion_api_url(url: str) -> bool:
    parsed = urlsplit(url)
    return parsed.scheme == "https" and parsed.hostname == "api.notion.com"


def summarize_external_request_target(url: str) -> str:
    parsed = urlsplit(url)
    host = parsed.netloc or "-"
    path = parsed.path or "/"
    return f"{host}{path}"


def summarize_database_id(database_id: str) -> str:
    cleaned = str(database_id or "").strip()
    if not cleaned:
        return "-"
    if len(cleaned) <= 12:
        return cleaned
    return f"{cleaned[:8]}...{cleaned[-4:]}"


def is_retryable_http_status(status_code: int) -> bool:
    return status_code in {429, 500, 502, 503, 504}


def get_external_retry_sleep_seconds(
    attempt: int,
    retry_after: Optional[str] = None,
) -> float:
    header_delay = parse_retry_after_seconds(retry_after) or 0.0
    backoff_delay = min(1.0 * (2**attempt), 8.0)
    return max(header_delay, backoff_delay)


def download_file_bytes(
    url: str,
    require_file_hint: bool = False,
) -> tuple[Optional[bytes], Optional[str]]:
    # 실제 다운로드 직전에도 allowlist를 다시 확인해, 첨부와 본문 미디어가 같은 정책을 따르도록 한다.
    if not is_allowed_external_download_url(url, require_file_hint=require_file_hint):
        LOGGER.warning("외부 파일 다운로드 차단: %s", url)
        return None, None
    request_target = summarize_external_request_target(url)
    for attempt in range(EXTERNAL_FETCH_MAX_RETRIES + 1):
        req = urllib.request.Request(url, headers=build_site_headers())
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                content_type = (resp.headers.get("Content-Type") or "").split(";", 1)[0].strip()
                data = resp.read()
                return data, content_type or None
        except urllib.error.HTTPError as exc:
            if is_retryable_http_status(exc.code) and attempt < EXTERNAL_FETCH_MAX_RETRIES:
                sleep_s = get_external_retry_sleep_seconds(
                    attempt,
                    retry_after=exc.headers.get("Retry-After"),
                )
                LOGGER.info(
                    "외부 파일 다운로드 재시도(%s/%s): %s -> HTTP %s, 대기=%.1fs",
                    attempt + 1,
                    EXTERNAL_FETCH_MAX_RETRIES,
                    request_target,
                    exc.code,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue
            LOGGER.info("파일 다운로드 실패: %s (HTTP %s)", url, exc.code)
        except urllib.error.URLError as exc:
            is_timeout = isinstance(exc.reason, socket.timeout)
            if attempt < EXTERNAL_FETCH_MAX_RETRIES and is_timeout:
                sleep_s = get_external_retry_sleep_seconds(attempt)
                LOGGER.info(
                    "외부 파일 다운로드 재시도(%s/%s): %s -> timeout, 대기=%.1fs",
                    attempt + 1,
                    EXTERNAL_FETCH_MAX_RETRIES,
                    request_target,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue
            if is_timeout:
                LOGGER.info("파일 다운로드 실패: %s (timeout)", url)
            else:
                LOGGER.info("파일 다운로드 실패: %s (%s)", url, exc.reason)
        except socket.timeout:
            if attempt < EXTERNAL_FETCH_MAX_RETRIES:
                sleep_s = get_external_retry_sleep_seconds(attempt)
                LOGGER.info(
                    "외부 파일 다운로드 재시도(%s/%s): %s -> timeout, 대기=%.1fs",
                    attempt + 1,
                    EXTERNAL_FETCH_MAX_RETRIES,
                    request_target,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue
            LOGGER.info("파일 다운로드 실패: %s (timeout)", url)
    return None, None


def compress_image_to_limit(
    payload: bytes,
    content_type: str,
    max_bytes: int,
) -> Optional[tuple[bytes, str]]:
    if max_bytes <= 0:
        return None
    try:
        from PIL import Image
    except ImportError:
        LOGGER.info("이미지 압축 스킵: Pillow 미설치")
        return None
    try:
        with Image.open(BytesIO(payload)) as image:
            image.load()
            working = image.copy()
    except Exception as exc:
        LOGGER.info("이미지 압축 실패: 열기 실패 (%s)", exc)
        return None
    if working.size[0] <= 0 or working.size[1] <= 0:
        return None
    if working.mode in {"RGBA", "LA"}:
        background = Image.new("RGB", working.size, (255, 255, 255))
        background.paste(working, mask=working.split()[-1])
        working = background
    elif working.mode != "RGB":
        working = working.convert("RGB")
    quality_steps = [85, 75, 65, 55, 45]
    scale_steps = [1.0, 0.9, 0.8, 0.7, 0.6]
    original_size = len(payload)
    width, height = working.size
    for scale in scale_steps:
        if scale < 1.0:
            # Pylance와 최신 Pillow 타입 정의 기준에 맞춰 enum 형태의 리샘플링 상수를 사용한다.
            resized = working.resize(
                (max(1, int(width * scale)), max(1, int(height * scale))),
                Image.Resampling.LANCZOS,
            )
        else:
            resized = working
        for quality in quality_steps:
            buffer = BytesIO()
            try:
                resized.save(buffer, format="JPEG", quality=quality, optimize=True)
            except Exception as exc:
                LOGGER.info("이미지 압축 실패: 저장 실패 (%s)", exc)
                return None
            data = buffer.getvalue()
            if len(data) <= max_bytes:
                LOGGER.info(
                    "이미지 압축 적용: %s -> %s bytes (q=%s, scale=%.2f)",
                    original_size,
                    len(data),
                    quality,
                    scale,
                )
                return data, "image/jpeg"
    LOGGER.info("이미지 압축 실패: %s bytes -> limit %s bytes", original_size, max_bytes)
    return None


def get_workspace_upload_limit(token: str) -> Optional[int]:
    global WORKSPACE_UPLOAD_LIMIT
    if WORKSPACE_UPLOAD_LIMIT is not None:
        return WORKSPACE_UPLOAD_LIMIT
    try:
        data = notion_request("GET", "https://api.notion.com/v1/users/me", token)
    except NotionRequestError as exc:
        LOGGER.info("업로드 제한 조회 실패: %s", exc)
        WORKSPACE_UPLOAD_LIMIT = None
        return None
    limit = data.get("bot", {}).get("workspace_limits", {}).get(
        "max_file_upload_size_in_bytes"
    )
    if isinstance(limit, int):
        WORKSPACE_UPLOAD_LIMIT = limit
        return limit
    WORKSPACE_UPLOAD_LIMIT = None
    return None


def encode_multipart_form_data(
    filename: str,
    content_type: str,
    payload: bytes,
    part_number: Optional[int] = None,
) -> tuple[bytes, str]:
    boundary = f"----NotionUpload{uuid.uuid4().hex}"
    lines: list[bytes] = []
    if part_number is not None:
        lines.append(
            f"--{boundary}\r\n"
            "Content-Disposition: form-data; name=\"part_number\"\r\n\r\n"
            f"{part_number}\r\n".encode("utf-8")
        )
    safe_name = re.sub(r"[^ -~]", "_", filename)
    lines.append(
        f"--{boundary}\r\n"
        f"Content-Disposition: form-data; name=\"file\"; filename=\"{safe_name}\"\r\n"
        f"Content-Type: {content_type}\r\n\r\n".encode("utf-8")
    )
    lines.append(payload)
    lines.append(f"\r\n--{boundary}--\r\n".encode("utf-8"))
    body = b"".join(lines)
    return body, f"multipart/form-data; boundary={boundary}"

def notion_request(
    method: str,
    url: str,
    token: str,
    payload: Optional[dict] = None,
) -> dict:
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    max_retries = NOTION_MAX_RETRIES
    request_target = summarize_request_target(url)

    for attempt in range(max_retries + 1):
        wait_for_notion_request_slot()
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Notion-Version", get_notion_api_version())
        req.add_header("Content-Type", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            error_payload = parse_notion_error_payload(body)
            notion_code = str(error_payload.get("code") or "").strip() or None
            notion_message = str(error_payload.get("message") or "").strip()
            reason_text = notion_message or body
            request_id = (
                exc.headers.get("x-request-id")
                or exc.headers.get("X-Request-Id")
                or str(error_payload.get("request_id") or "").strip()
                or None
            )
            hint = build_notion_error_hint(exc.code, notion_code)
            retryable = exc.code in {429, 500, 502, 503, 504}
            if retryable and attempt < max_retries:
                retry_after = exc.headers.get("Retry-After")
                sleep_s = get_retry_sleep_seconds(
                    attempt,
                    status_code=exc.code,
                    retry_after=retry_after,
                )
                LOGGER.info(
                    "Notion API 재시도(%s/%s): %s %s -> HTTP %s%s, 대기=%.1fs",
                    attempt + 1,
                    max_retries,
                    method,
                    request_target,
                    exc.code,
                    f" {notion_code}" if notion_code else "",
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue
            raise NotionRequestError(
                format_notion_error_message(
                    method,
                    request_target,
                    exc.code,
                    notion_code,
                    reason_text,
                    request_id,
                    hint,
                ),
                status_code=exc.code,
                reason=reason_text,
                method=method,
                target=request_target,
                notion_code=notion_code,
                request_id=request_id,
                hint=hint,
            ) from exc
        except (socket.timeout, TimeoutError) as exc:
            if attempt < max_retries:
                sleep_s = get_retry_sleep_seconds(attempt)
                LOGGER.info(
                    "Notion API 재시도(%s/%s): %s %s -> timeout, 대기=%.1fs",
                    attempt + 1,
                    max_retries,
                    method,
                    request_target,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue
            raise NotionRequestError(
                format_notion_error_message(
                    method,
                    request_target,
                    None,
                    None,
                    "timeout",
                    None,
                    "",
                ),
                reason="timeout",
                method=method,
                target=request_target,
            ) from exc
        except urllib.error.URLError as exc:
            is_timeout = isinstance(exc.reason, socket.timeout)
            if attempt < max_retries:
                sleep_s = get_retry_sleep_seconds(attempt)
                LOGGER.info(
                    "Notion API 재시도(%s/%s): %s %s -> %s, 대기=%.1fs",
                    attempt + 1,
                    max_retries,
                    method,
                    request_target,
                    "timeout" if is_timeout else exc.reason,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue
            if is_timeout:
                raise NotionRequestError(
                    format_notion_error_message(
                        method,
                        request_target,
                        None,
                        None,
                        "timeout",
                        None,
                        "",
                    ),
                    reason="timeout",
                    method=method,
                    target=request_target,
                ) from exc
            raise NotionRequestError(
                format_notion_error_message(
                    method,
                    request_target,
                    None,
                    None,
                    str(exc.reason),
                    None,
                    "",
                ),
                reason=str(exc.reason),
                method=method,
                target=request_target,
            ) from exc
    # 위 루프는 성공 시 return, 실패 시 예외를 발생시키는 구조지만 정적 분석기에도 종료 조건을 명시한다.
    raise RuntimeError(f"Notion API 요청 종료 상태 불명: {method} {request_target}")


def create_file_upload(
    token: str,
    filename: str,
    content_type: str,
    mode: str = "single_part",
) -> Optional[dict]:
    payload = {"mode": mode, "filename": filename, "content_type": content_type}
    try:
        return notion_request("POST", "https://api.notion.com/v1/file_uploads", token, payload)
    except NotionRequestError as exc:
        LOGGER.info("파일 업로드 생성 실패: %s (%s)", filename, exc)
        return None


def send_file_upload(
    token: str,
    upload_url: str,
    filename: str,
    content_type: str,
    payload: bytes,
    part_number: Optional[int] = None,
) -> Optional[dict]:
    body, content_header = encode_multipart_form_data(
        filename, content_type, payload, part_number=part_number
    )
    request_target = summarize_external_request_target(upload_url)
    for attempt in range(EXTERNAL_UPLOAD_MAX_RETRIES + 1):
        req = urllib.request.Request(upload_url, data=body, method="POST")
        req.add_header("Content-Type", content_header)
        req.add_header("Content-Length", str(len(body)))
        if is_notion_api_url(upload_url):
            req.add_header("Authorization", f"Bearer {token}")
            req.add_header("Notion-Version", get_notion_api_version())
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="replace")
            if is_retryable_http_status(exc.code) and attempt < EXTERNAL_UPLOAD_MAX_RETRIES:
                sleep_s = get_external_retry_sleep_seconds(
                    attempt,
                    retry_after=exc.headers.get("Retry-After"),
                )
                LOGGER.info(
                    "파일 업로드 전송 재시도(%s/%s): %s -> HTTP %s, 대기=%.1fs",
                    attempt + 1,
                    EXTERNAL_UPLOAD_MAX_RETRIES,
                    request_target,
                    exc.code,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue
            LOGGER.info("파일 업로드 전송 실패: HTTP %s (%s)", exc.code, body_text)
        except urllib.error.URLError as exc:
            is_timeout = isinstance(exc.reason, socket.timeout)
            if attempt < EXTERNAL_UPLOAD_MAX_RETRIES and is_timeout:
                sleep_s = get_external_retry_sleep_seconds(attempt)
                LOGGER.info(
                    "파일 업로드 전송 재시도(%s/%s): %s -> timeout, 대기=%.1fs",
                    attempt + 1,
                    EXTERNAL_UPLOAD_MAX_RETRIES,
                    request_target,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue
            if is_timeout:
                LOGGER.info("파일 업로드 전송 실패: timeout")
            else:
                LOGGER.info("파일 업로드 전송 실패: %s", exc.reason)
        except socket.timeout:
            if attempt < EXTERNAL_UPLOAD_MAX_RETRIES:
                sleep_s = get_external_retry_sleep_seconds(attempt)
                LOGGER.info(
                    "파일 업로드 전송 재시도(%s/%s): %s -> timeout, 대기=%.1fs",
                    attempt + 1,
                    EXTERNAL_UPLOAD_MAX_RETRIES,
                    request_target,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue
            LOGGER.info("파일 업로드 전송 실패: timeout")
    return None


def upload_external_file_to_notion(
    token: str,
    url: str,
    filename_hint: Optional[str] = None,
    expect_image: bool = True,
) -> Optional[str]:
    if not url:
        return None
    cached = FILE_UPLOAD_CACHE.get(url)
    if cached:
        return cached

    payload, content_type = download_file_bytes(url, require_file_hint=not expect_image)
    if not payload:
        return None
    filename = sanitize_filename(
        filename_hint or derive_filename_from_url(url, fallback="file")
    )
    content_type = normalize_content_type(content_type, filename, url)
    if expect_image and not content_type.lower().startswith("image/"):
        LOGGER.info("이미지 업로드 스킵: content_type=%s (%s)", content_type, url)
        return None
    file_size = len(payload)
    max_bytes = get_workspace_upload_limit(token)
    if max_bytes and file_size > max_bytes and expect_image:
        compressed = compress_image_to_limit(payload, content_type, max_bytes)
        if compressed:
            payload, content_type = compressed
            file_size = len(payload)
    if max_bytes and file_size > max_bytes:
        LOGGER.info("업로드 용량 초과: %s bytes (limit=%s)", file_size, max_bytes)
        return None
    if file_size > 20 * 1024 * 1024:
        LOGGER.info("업로드 스킵(멀티파트 필요): %s bytes", file_size)
        return None

    if not filename:
        filename = sanitize_filename(derive_filename_from_url(url, fallback="file"))
    if "." not in filename:
        ext = mimetypes.guess_extension(content_type) or ""
        if ext:
            filename = f"{filename}{ext}"
    if content_type.lower() == "image/jpeg":
        stem, ext = os.path.splitext(filename)
        if ext.lower() not in {".jpg", ".jpeg"}:
            filename = f"{stem}.jpg"

    created = create_file_upload(token, filename, content_type)
    if not created:
        return None
    upload_id = created.get("id")
    upload_url = created.get("upload_url")
    if isinstance(upload_url, str):
        upload_url = upload_url.strip("`")
    upload_url = upload_url or (
        f"https://api.notion.com/v1/file_uploads/{upload_id}/send"
        if upload_id
        else None
    )
    if not upload_id or not upload_url:
        LOGGER.info("파일 업로드 응답 누락: id=%s url=%s", upload_id, upload_url)
        return None
    sent = send_file_upload(
        token, upload_url, filename, content_type, payload, part_number=None
    )
    if not sent or sent.get("status") != "uploaded":
        LOGGER.info(
            "파일 업로드 상태 이상: %s (%s)", url, sent.get("status") if sent else "no_response"
        )
        return None
    FILE_UPLOAD_CACHE[url] = upload_id
    return upload_id


def prepare_attachments_for_sync(token: str, attachments: list[dict]) -> list[dict]:
    if not attachments or not should_upload_files_to_notion():
        return attachments
    updated: list[dict] = []
    for attachment in attachments:
        if attachment.get("type") != "external":
            updated.append(attachment)
            continue
        url = attachment.get("external", {}).get("url") or ""
        name = attachment.get("name") or extract_attachment_name(attachment)
        if not is_image_name_or_url(name, url):
            updated.append(attachment)
            continue
        upload_id = upload_external_file_to_notion(token, url, name, expect_image=True)
        if upload_id:
            updated.append(
                {"name": name, "type": "file_upload", "file_upload": {"id": upload_id}}
            )
        else:
            updated.append(attachment)
    return updated


def prepare_body_blocks_for_sync(token: str, blocks: list[dict]) -> tuple[list[dict], list[dict]]:
    if not blocks or not should_upload_files_to_notion():
        return blocks, blocks
    updated: list[dict] = []
    hash_blocks: list[dict] = []
    for block in blocks:
        block_type = block.get("type")
        if block_type == "image":
            image = block.get("image", {})
            if image.get("type") != "external":
                updated.append(block)
                hash_blocks.append(block)
                continue
            url = image.get("external", {}).get("url") or ""
            if not url:
                updated.append(block)
                hash_blocks.append(block)
                continue
            # 본문 이미지도 첨부와 같은 allowlist를 적용해 스케줄 러너가 임의 외부 호스트를 읽지 않게 한다.
            if not is_allowed_external_download_url(url):
                LOGGER.info("본문 이미지 업로드 스킵: 허용되지 않은 외부 URL (%s)", url)
                updated.append(block)
                hash_blocks.append(block)
                continue
            filename = derive_filename_from_url(url, fallback="image")
            upload_id = upload_external_file_to_notion(
                token, url, filename, expect_image=True
            )
            if not upload_id:
                updated.append(block)
                # 업로드 실패 시에는 실제로 남는 external 블록 상태를 해시에 그대로 반영해야 다음 실행에서 다시 시도할 수 있다.
                hash_blocks.append(block)
                continue
            new_block = {
                "object": "block",
                "type": "image",
                "image": {"type": "file_upload", "file_upload": {"id": upload_id}},
            }
            if image.get("caption"):
                new_block["image"]["caption"] = image["caption"]
            updated.append(new_block)
            hash_blocks.append(
                build_uploaded_image_hash_block(url, image.get("caption"))
            )
            continue
        if block_type == "embed":
            embed = block.get("embed", {})
            url = embed.get("url") or ""
            if not url or not is_embed_file_candidate(url):
                updated.append(block)
                hash_blocks.append(block)
                continue
            # 임베드도 도메인과 파일 신호가 둘 다 맞을 때만 내려받아 업로드한다.
            if not is_allowed_external_download_url(url, require_file_hint=True):
                LOGGER.info("본문 임베드 업로드 스킵: 허용되지 않은 외부 URL (%s)", url)
                updated.append(block)
                hash_blocks.append(block)
                continue
            filename = derive_filename_from_url(url, fallback="file")
            upload_id = upload_external_file_to_notion(
                token, url, filename, expect_image=False
            )
            if not upload_id:
                updated.append(block)
                # 업로드 실패 시에는 실제 결과가 embed 유지이므로 해시도 같은 상태로 남긴다.
                hash_blocks.append(block)
                continue
            if is_pdf_name_or_url(filename, url):
                updated.append(build_pdf_block(upload_id))
                hash_blocks.append(build_uploaded_file_hash_block(url, as_pdf=True))
            else:
                updated.append(build_file_block(upload_id))
                hash_blocks.append(build_uploaded_file_hash_block(url, as_pdf=False))
            continue
        updated.append(block)
        hash_blocks.append(block)
    return updated, hash_blocks
def fetch_database(token: str, database_id: str) -> dict:
    url = f"https://api.notion.com/v1/databases/{database_id}"
    return run_database_request_with_object_not_found_retry(
        lambda: notion_request("GET", url, token),
        method="GET",
        database_id=database_id,
        action_name="데이터베이스 조회",
    )


def update_database(token: str, database_id: str, properties: dict) -> dict:
    url = f"https://api.notion.com/v1/databases/{database_id}"
    payload = {"properties": properties}
    return run_database_request_with_object_not_found_retry(
        lambda: notion_request("PATCH", url, token, payload),
        method="PATCH",
        database_id=database_id,
        action_name="데이터베이스 속성 수정",
    )


def ensure_title_property(token: str, database_id: str, database: dict) -> dict:
    properties = database.get("properties", {})
    if TITLE_PROPERTY in properties:
        prop = properties.get(TITLE_PROPERTY) or {}
        if prop.get("type") != "title":
            raise RuntimeError(
                f"Notion 속성 타입 불일치: {TITLE_PROPERTY} (title 아님)"
            )
        return database
    title_name = None
    for name, prop in properties.items():
        if prop.get("type") == "title":
            title_name = name
            break
    if not title_name:
        raise RuntimeError("Notion title 속성을 찾을 수 없습니다")
    LOGGER.info("Notion 속성 이름 변경: %s -> %s", title_name, TITLE_PROPERTY)
    return update_database(token, database_id, {title_name: {"name": TITLE_PROPERTY}})


def ensure_top_property(token: str, database_id: str, database: dict) -> dict:
    prop = database.get("properties", {}).get(TOP_PROPERTY)
    if prop:
        if prop.get("type") != "checkbox":
            raise RuntimeError(
                f"Notion 속성 타입 불일치: {TOP_PROPERTY} (checkbox 아님)"
            )
        return database
    LOGGER.info("Notion 속성 추가: %s", TOP_PROPERTY)
    return update_database(token, database_id, {TOP_PROPERTY: {"checkbox": {}}})


def ensure_date_property(token: str, database_id: str, database: dict) -> dict:
    prop = database.get("properties", {}).get(DATE_PROPERTY)
    if prop:
        if prop.get("type") != "date":
            raise RuntimeError(
                f"Notion 속성 타입 불일치: {DATE_PROPERTY} (date 아님)"
            )
        return database
    LOGGER.info("Notion 속성 추가: %s", DATE_PROPERTY)
    return update_database(token, database_id, {DATE_PROPERTY: {"date": {}}})


def ensure_author_property(token: str, database_id: str, database: dict) -> dict:
    prop = database.get("properties", {}).get(AUTHOR_PROPERTY)
    if prop:
        if prop.get("type") != "select":
            raise RuntimeError(
                f"Notion 속성 타입 불일치: {AUTHOR_PROPERTY} (select 아님)"
            )
        return database
    LOGGER.info("Notion 속성 추가: %s", AUTHOR_PROPERTY)
    return update_database(
        token, database_id, {AUTHOR_PROPERTY: {"select": {"options": []}}}
    )


def ensure_classification_property(token: str, database_id: str, database: dict) -> dict:
    prop = database.get("properties", {}).get(CLASSIFICATION_PROPERTY)
    if prop:
        if prop.get("type") != "select":
            raise RuntimeError(
                f"Notion 속성 타입 불일치: {CLASSIFICATION_PROPERTY} (select 아님)"
            )
        return database
    LOGGER.info("Notion 속성 추가: %s", CLASSIFICATION_PROPERTY)
    return update_database(
        token, database_id, {CLASSIFICATION_PROPERTY: {"select": {"options": []}}}
    )


def ensure_views_property(token: str, database_id: str, database: dict) -> dict:
    prop = database.get("properties", {}).get(VIEWS_PROPERTY)
    if prop:
        if prop.get("type") != "number":
            raise RuntimeError(
                f"Notion 속성 타입 불일치: {VIEWS_PROPERTY} (number 아님)"
            )
        return database
    LOGGER.info("Notion 속성 추가: %s", VIEWS_PROPERTY)
    return update_database(token, database_id, {VIEWS_PROPERTY: {"number": {}}})


def ensure_url_property(token: str, database_id: str, database: dict) -> dict:
    prop = database.get("properties", {}).get(URL_PROPERTY)
    if prop:
        if prop.get("type") != "url":
            raise RuntimeError(f"Notion 속성 타입 불일치: {URL_PROPERTY} (url 아님)")
        return database
    LOGGER.info("Notion 속성 추가: %s", URL_PROPERTY)
    return update_database(token, database_id, {URL_PROPERTY: {"url": {}}})


def ensure_type_property(token: str, database_id: str, database: dict) -> dict:
    prop = database.get("properties", {}).get(TYPE_PROPERTY)
    if prop:
        if prop.get("type") != "select":
            raise RuntimeError(f"Notion 속성 타입 불일치: {TYPE_PROPERTY} (select 아님)")
        return database
    LOGGER.info("Notion 속성 추가: %s", TYPE_PROPERTY)
    options = [{"name": name} for name in (*TYPE_TAGS, FALLBACK_TYPE)]
    return update_database(token, database_id, {TYPE_PROPERTY: {"select": {"options": options}}})


def ensure_attachment_property(token: str, database_id: str, database: dict) -> dict:
    prop = database.get("properties", {}).get(ATTACHMENT_PROPERTY)
    if prop:
        if prop.get("type") != "files":
            raise RuntimeError(
                f"Notion 속성 타입 불일치: {ATTACHMENT_PROPERTY} (files 아님)"
            )
        return database
    LOGGER.info("Notion 속성 추가: %s", ATTACHMENT_PROPERTY)
    return update_database(token, database_id, {ATTACHMENT_PROPERTY: {"files": {}}})


def ensure_body_hash_property(token: str, database_id: str, database: dict) -> dict:
    prop = database.get("properties", {}).get(BODY_HASH_PROPERTY)
    if prop:
        if prop.get("type") != "rich_text":
            raise RuntimeError(
                f"Notion 속성 타입 불일치: {BODY_HASH_PROPERTY} (rich_text 아님)"
            )
        return database
    LOGGER.info("Notion 속성 추가: %s", BODY_HASH_PROPERTY)
    return update_database(token, database_id, {BODY_HASH_PROPERTY: {"rich_text": {}}})


def ensure_required_properties(token: str, database_id: str, database: dict) -> dict:
    database = ensure_title_property(token, database_id, database)
    database = ensure_top_property(token, database_id, database)
    database = ensure_date_property(token, database_id, database)
    database = ensure_author_property(token, database_id, database)
    database = ensure_url_property(token, database_id, database)
    database = ensure_type_property(token, database_id, database)
    return database
def validate_optional_property_type(
    database: dict,
    property_name: str,
    expected_type: str,
) -> bool:
    prop = database.get("properties", {}).get(property_name)
    if not prop:
        return False
    actual = prop.get("type")
    if actual != expected_type:
        LOGGER.info(
            "Notion 속성 타입 불일치: %s (기대 %s, 실제 %s) -> 업데이트 생략",
            property_name,
            expected_type,
            actual,
        )
        return False
    return True
def get_select_options(database: dict, property_name: str) -> list[dict]:
    prop = database.get("properties", {}).get(property_name)
    if not prop:
        raise RuntimeError(f"Notion 속성 누락: {property_name}")
    if prop.get("type") != "select":
        raise RuntimeError(f"Notion 속성 타입 오류: {property_name} (select 아님)")
    return prop.get("select", {}).get("options", [])


def sanitize_select_options(options: list[dict]) -> list[dict]:
    sanitized: list[dict] = []
    for option in options:
        name = option.get("name")
        if not name:
            continue
        item = {"name": name}
        if option.get("id"):
            item["id"] = option["id"]
        color = option.get("color")
        if color:
            item["color"] = color
        sanitized.append(item)
    return sanitized


def ensure_select_option(
    token: str,
    database_id: str,
    property_name: str,
    option_name: str,
    options_cache: list[dict],
) -> list[dict]:
    if not option_name:
        return options_cache
    sanitized_options = sanitize_select_options(options_cache)
    existing = {opt.get("name") for opt in sanitized_options}
    if option_name in existing:
        return options_cache
    updated_options = sanitized_options + [{"name": option_name}]
    LOGGER.info("Notion 옵션 추가: %s=%s", property_name, option_name)
    data = update_database(
        token,
        database_id,
        {property_name: {"select": {"options": updated_options}}},
    )
    return get_select_options(data, property_name)


def ensure_select_options_batch(
    token: str,
    database_id: str,
    property_name: str,
    options_cache: list[dict],
    desired_names: set[str],
) -> list[dict]:
    sanitized_options = sanitize_select_options(options_cache)
    existing = {opt.get("name") for opt in sanitized_options}
    missing = sorted(name for name in desired_names if name and name not in existing)
    if not missing:
        return options_cache
    updated_options = sanitized_options + [{"name": name} for name in missing]
    LOGGER.info("Notion 옵션 일괄 추가: %s=%s", property_name, ", ".join(missing))
    data = update_database(
        token,
        database_id,
        {property_name: {"select": {"options": updated_options}}},
    )
    return get_select_options(data, property_name)


def query_database(token: str, database_id: str, filter_payload: dict) -> list[dict]:
    payload = {"filter": filter_payload}
    data = query_database_page(token, database_id, payload)
    return data.get("results", [])


def query_database_page(token: str, database_id: str, payload: dict) -> dict:
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    return run_database_request_with_object_not_found_retry(
        lambda: notion_request("POST", url, token, payload),
        method="POST",
        database_id=database_id,
        action_name="데이터베이스 쿼리",
    )


def append_block_children(token: str, block_id: str, children: list[dict]) -> dict:
    url = f"https://api.notion.com/v1/blocks/{block_id}/children"
    payload = {"children": children}
    return notion_request("PATCH", url, token, payload)


def list_block_children(token: str, block_id: str) -> list[dict]:
    base_url = f"https://api.notion.com/v1/blocks/{block_id}/children"
    results: list[dict] = []
    cursor: Optional[str] = None
    while True:
        params = {"page_size": 100}
        if cursor:
            params["start_cursor"] = cursor
        url = f"{base_url}?{urlencode(params)}"
        data = notion_request("GET", url, token)
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return results


def delete_block(token: str, block_id: str) -> None:
    url = f"https://api.notion.com/v1/blocks/{block_id}"
    fallback_statuses = {403, 405, 409, 429, 500, 502, 503, 504}
    try:
        notion_request("DELETE", url, token)
    except NotionRequestError as exc:
        if exc.status_code == 404:
            LOGGER.info("블록 이미 삭제됨: %s", block_id)
            return
        if exc.status_code in fallback_statuses:
            LOGGER.info(
                "블록 DELETE 실패 -> archived 폴백: %s (HTTP %s)",
                block_id,
                exc.status_code,
            )
            notion_request("PATCH", url, token, {"archived": True})
            return
        raise
def archive_page(token: str, page_id: str) -> None:
    notion_request("PATCH", f"https://api.notion.com/v1/pages/{page_id}", token, {"archived": True})

def build_icon() -> dict:
    return {"type": "emoji", "emoji": PAGE_ICON_EMOJI}


def create_page(token: str, database_id: str, properties: dict) -> str:
    payload = {
        "parent": {"database_id": database_id},
        "properties": properties,
        "icon": build_icon(),
    }
    data = notion_request("POST", "https://api.notion.com/v1/pages", token, payload)
    # Notion 응답에 page id가 없으면 후속 update 요청이 전부 깨지므로 즉시 실패시킨다.
    page_id = data.get("id")
    if not isinstance(page_id, str) or not page_id:
        raise RuntimeError("Notion 페이지 생성 응답에 id가 없습니다.")
    return page_id


def update_page(token: str, page_id: str, properties: dict) -> None:
    payload = {"properties": properties, "icon": build_icon()}
    notion_request("PATCH", f"https://api.notion.com/v1/pages/{page_id}", token, payload)
