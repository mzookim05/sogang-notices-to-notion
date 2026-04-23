import copy
import json
import re
from typing import Optional
from urllib.parse import urlparse

from common import (
    ATTACHMENTS_STATUS_KNOWN,
    ATTACHMENTS_STATUS_UNKNOWN,
    ensure_item_title,
    is_empty_paragraph_block,
    normalize_body_blocks,
    rich_text_plain_text,
)
from log import LOGGER
from notion_client import (
    NotionRequestError,
    append_block_children,
    archive_page,
    delete_block,
    list_block_children,
    notion_request,
    query_database,
    query_database_page,
    update_page,
)
from settings import (
    ATTACHMENT_PROPERTY,
    ATTACHMENT_STATE_PROPERTY,
    AUTHOR_PROPERTY,
    BODY_MEDIA_STATE_PROPERTY,
    CLASSIFICATION_PROPERTY,
    DATE_PROPERTY,
    FALLBACK_TYPE,
    SYNC_CONTAINER_MARKER,
    TITLE_PROPERTY,
    TOP_PROPERTY,
    TYPE_PROPERTY,
    URL_PROPERTY,
    VIEWS_PROPERTY,
    should_allow_title_only_match,
)
from utils import (
    DEFAULT_ANNOTATIONS,
    build_container_block,
    build_file_block,
    build_pdf_block,
    build_space_rich_text,
    chunks,
    normalize_date_key,
    normalize_detail_url,
)

def extract_type_from_title(title: str) -> str:
    def normalize_type_label(raw: str) -> str:
        cleaned = (raw or "").strip()
        if not cleaned:
            return ""
        cleaned = cleaned.replace(",", "/")
        cleaned = re.sub(r"\s*/\s*", "/", cleaned)
        cleaned = re.sub(r"/{2,}", "/", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.strip()

    match = re.match(r"\s*\[([^\]]+)\]", title)
    if match:
        label = normalize_type_label(match.group(1))
        if label:
            return label
    return FALLBACK_TYPE


def is_image_only_blocks(blocks: list[dict]) -> bool:
    if not blocks:
        return False
    has_image = False
    for block in blocks:
        if is_empty_paragraph_block(block):
            continue
        if block.get("type") != "image":
            return False
        has_image = True
    return has_image


def has_sync_marker(rich_text: list[dict]) -> bool:
    if not rich_text:
        return False
    plain = rich_text_plain_text(rich_text)
    if not plain:
        return False
    first_line = plain.splitlines()[0].strip()
    return first_line == SYNC_CONTAINER_MARKER


def ensure_sync_marker_in_rich_text(rich_text: list[dict]) -> list[dict]:
    if has_sync_marker(rich_text):
        return rich_text
    marker_segment = {
        "type": "text",
        "text": {"content": f"{SYNC_CONTAINER_MARKER}\n"},
        "annotations": dict(DEFAULT_ANNOTATIONS),
    }
    if rich_text:
        return [marker_segment] + rich_text
    return [marker_segment]


def find_sync_container_id(token: str, page_id: str) -> Optional[str]:
    queue = list_block_children(token, page_id)
    while queue:
        block = queue.pop(0)
        if block.get("type") == "quote":
            rich_text = block.get("quote", {}).get("rich_text", [])
            if has_sync_marker(rich_text):
                return block.get("id")
        if block.get("has_children"):
            block_id = block.get("id")
            if not block_id:
                continue
            try:
                queue.extend(list_block_children(token, block_id))
            except NotionRequestError as exc:
                LOGGER.info("하위 블록 조회 실패: %s (%s)", block_id, exc)
    return None


# preserve/overwrite 공통으로 지금 페이지가 관리 중인 컨테이너를 찾는다.
def find_sync_container_block(token: str, page_id: str) -> Optional[dict]:
    top_blocks = list_block_children(token, page_id)
    quote_blocks: list[dict] = []
    for block in top_blocks:
        if block.get("type") != "quote":
            continue
        quote_blocks.append(block)
        rich_text = block.get("quote", {}).get("rich_text", [])
        if has_sync_marker(rich_text):
            return block
    # overwrite 모드에는 마커가 없으므로, 최상위 블록이 quote 하나뿐일 때만 컨테이너로 간주한다.
    # 여러 최상위 블록이 섞여 있으면 사용자가 수동으로 추가한 quote를 잘못 재사용할 수 있으니 보수적으로 포기한다.
    if len(top_blocks) == 1 and len(quote_blocks) == 1:
        return quote_blocks[0]
    return None


def is_notion_hosted_media_block(block: dict) -> bool:
    block_type = block.get("type")
    if block_type == "image":
        return block.get("image", {}).get("type") in {"file", "file_upload"}
    if block_type in {"file", "pdf"}:
        return block.get(block_type, {}).get("type") in {"file", "file_upload"}
    return False


def sanitize_uploaded_media_block(block: dict, upload_id: str) -> Optional[dict]:
    block_type = block.get("type")
    clean_upload_id = str(upload_id or "").strip()
    if not clean_upload_id:
        return None
    # list_block_children 응답에는 id, created_time 같은 읽기 전용 필드가 섞여 오므로,
    # 재사용할 때는 append 가능한 최소 payload만 다시 구성해야 잘못된 블록 상태가 전파되지 않는다.
    # 실제 Notion read 응답은 file_upload 대신 file 타입으로 돌아올 수 있으므로,
    # 쓰기용 payload는 현재 read shape가 아니라 저장해 둔 upload_id를 기준으로 다시 만든다.
    if block_type == "image":
        image = block.get("image", {})
        if image.get("type") not in {"file", "file_upload"}:
            return None
        sanitized = {
            "object": "block",
            "type": "image",
            "image": {"type": "file_upload", "file_upload": {"id": clean_upload_id}},
        }
        caption = image.get("caption")
        if caption:
            sanitized["image"]["caption"] = copy.deepcopy(caption)
        return sanitized
    if block_type == "file":
        payload = block.get("file", {})
        if payload.get("type") not in {"file", "file_upload"}:
            return None
        return build_file_block(clean_upload_id)
    if block_type == "pdf":
        payload = block.get("pdf", {})
        if payload.get("type") not in {"file", "file_upload"}:
            return None
        return build_pdf_block(clean_upload_id)
    return None


def extract_body_media_state(properties: dict) -> list[dict]:
    raw = extract_rich_text_value(properties, BODY_MEDIA_STATE_PROPERTY)
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        LOGGER.info("본문 미디어 상태 파싱 실패: JSON decode error")
        return []
    if not isinstance(payload, list):
        LOGGER.info("본문 미디어 상태 파싱 실패: list 아님")
        return []
    items: list[dict] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        media_type = str(entry.get("type") or "").strip()
        source_url = str(entry.get("source_url") or "").strip()
        upload_id = str(entry.get("upload_id") or "").strip()
        block_id = str(entry.get("block_id") or "").strip()
        hosted_file_key = str(entry.get("hosted_file_key") or "").strip()
        if media_type not in {"image", "file", "pdf"} or not source_url:
            continue
        normalized_entry = {"type": media_type, "source_url": source_url}
        if upload_id:
            normalized_entry["upload_id"] = upload_id
        if block_id:
            normalized_entry["block_id"] = block_id
        if hosted_file_key:
            normalized_entry["hosted_file_key"] = hosted_file_key
        items.append(normalized_entry)
    return items


def extract_attachment_state(properties: dict) -> list[dict]:
    raw = extract_rich_text_value(properties, ATTACHMENT_STATE_PROPERTY)
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        LOGGER.info("첨부 상태 파싱 실패: JSON decode error")
        return []
    if not isinstance(payload, list):
        LOGGER.info("첨부 상태 파싱 실패: list 아님")
        return []
    items: list[dict] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        source_url = str(entry.get("source_url") or "").strip()
        upload_id = str(entry.get("upload_id") or "").strip()
        name = str(entry.get("name") or "").strip()
        hosted_file_key = str(entry.get("hosted_file_key") or "").strip()
        if not source_url or not upload_id:
            continue
        normalized_entry = {"source_url": source_url, "upload_id": upload_id}
        if name:
            normalized_entry["name"] = name
        if hosted_file_key:
            normalized_entry["hosted_file_key"] = hosted_file_key
        items.append(normalized_entry)
    return items


def normalize_item_attachments(item: dict) -> None:
    status = str(item.get("attachments_status") or "").strip()
    if status == ATTACHMENTS_STATUS_UNKNOWN:
        # 첨부 확인 실패는 "첨부 없음"으로 확정할 수 없으므로 files=[]를 보내지 않는다.
        item.pop("attachments", None)
        return
    if "attachments" in item:
        item["attachments"] = list(item.get("attachments") or [])
        item["attachments_status"] = ATTACHMENTS_STATUS_KNOWN
        return
    if status == ATTACHMENTS_STATUS_KNOWN:
        # 수집 단계가 명확히 "첨부 없음"을 확인한 경우에만 빈 리스트를 만들어
        # 기존 Notion 첨부파일 속성을 안전하게 비울 수 있게 한다.
        item["attachments"] = []


def normalize_notion_hosted_file_key(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or not parsed.path:
        return ""
    # signed query는 자주 바뀌므로, 현재 read 응답에서 비교 가능한 식별 힌트는 호스트+경로까지만 사용한다.
    return f"{parsed.netloc}{parsed.path}"


def extract_notion_hosted_file_key_from_block(block: dict) -> str:
    block_type = str(block.get("type") or "").strip()
    if block_type not in {"image", "file", "pdf"}:
        return ""
    payload = block.get(block_type, {})
    # body read 응답도 page files 속성과 마찬가지로 file 타입으로 돌아올 수 있으므로,
    # 현재 블록이 실제로 어떤 hosted 파일을 가리키는지는 file.url의 호스트+경로로 비교한다.
    if payload.get("type") != "file":
        return ""
    return normalize_notion_hosted_file_key(payload.get("file", {}).get("url") or "")


def enrich_attachment_state_with_properties(properties: dict, attachment_state: list[dict]) -> list[dict]:
    if not attachment_state:
        return attachment_state
    files_prop = properties.get(ATTACHMENT_PROPERTY, {})
    files = files_prop.get("files", [])
    if not isinstance(files, list):
        return attachment_state
    current_uploaded_entries: list[dict] = []
    for file_info in files:
        if not isinstance(file_info, dict):
            return attachment_state
        file_type = str(file_info.get("type") or "").strip()
        if file_type == "external":
            continue
        name = str(file_info.get("name") or "").strip()
        if not name:
            return attachment_state
        if file_type == "file":
            hosted_file_key = normalize_notion_hosted_file_key(
                file_info.get("file", {}).get("url") or ""
            )
            if not hosted_file_key:
                return attachment_state
            current_uploaded_entries.append(
                {"type": "file", "name": name, "hosted_file_key": hosted_file_key}
            )
            continue
        if file_type == "file_upload":
            upload_id = str(file_info.get("file_upload", {}).get("id") or "").strip()
            if not upload_id:
                return attachment_state
            current_uploaded_entries.append(
                {"type": "file_upload", "name": name, "upload_id": upload_id}
            )
            continue
        return attachment_state
    if len(current_uploaded_entries) != len(attachment_state):
        return attachment_state
    enriched: list[dict] = []
    for state_entry, current_entry in zip(attachment_state, current_uploaded_entries):
        state_name = str(state_entry.get("name") or "").strip()
        if not state_name or state_name != current_entry["name"]:
            return attachment_state
        enriched_entry = dict(state_entry)
        if current_entry["type"] == "file":
            enriched_entry["hosted_file_key"] = current_entry["hosted_file_key"]
        enriched.append(enriched_entry)
    return enriched


def enrich_attachment_state_with_page(
    token: str,
    page_id: str,
    attachment_state: list[dict],
) -> list[dict]:
    if not page_id or not attachment_state:
        return attachment_state
    try:
        page = notion_request("GET", f"https://api.notion.com/v1/pages/{page_id}", token)
    except NotionRequestError as exc:
        LOGGER.info("첨부 상태 hosted_file_key 보강 스킵: 페이지 조회 실패 (%s)", exc)
        return attachment_state
    return enrich_attachment_state_with_properties(page.get("properties", {}), attachment_state)


def extract_existing_uploaded_attachment_ids(
    properties: dict,
    attachment_state: list[dict],
) -> dict[str, list[str]]:
    if not attachment_state:
        return {}
    files_prop = properties.get(ATTACHMENT_PROPERTY, {})
    files = files_prop.get("files", [])
    if not isinstance(files, list):
        LOGGER.info("기존 첨부 재사용 스킵: files 속성 형식 불일치")
        return {}
    current_upload_ids: set[str] = set()
    current_uploaded_names: set[str] = set()
    current_hosted_file_keys: set[str] = set()
    current_uploaded_count = 0
    saw_file_read_shape = False
    for file_info in files:
        if not isinstance(file_info, dict):
            LOGGER.info("기존 첨부 재사용 스킵: files 항목 형식 불일치")
            return {}
        file_type = str(file_info.get("type") or "").strip()
        # 현재 정책상 이미지 첨부만 file_upload로 바꾸고 나머지는 external로 남길 수 있으므로,
        # mixed attachment 페이지에서도 업로드된 첨부만 부분 재사용할 수 있게 external은 무시한다.
        if file_type == "external":
            continue
        current_uploaded_count += 1
        name = str(file_info.get("name") or "").strip()
        if not name:
            LOGGER.info("기존 첨부 재사용 스킵: 현재 첨부 이름 누락")
            return {}
        if file_type != "file_upload":
            if file_type != "file":
                LOGGER.info(
                    "기존 첨부 재사용 스킵: 알 수 없는 첨부 타입 감지 (%s)",
                    file_type or "unknown",
                )
                return {}
            # page files 속성은 실제 read 응답에서 file_upload 대신 file 타입으로 돌아올 수 있으므로,
            # 이런 경우에는 상태에 저장한 hosted_file_key를 현재 file.url과 비교해 stale 재사용을 막는다.
            saw_file_read_shape = True
            hosted_file_key = normalize_notion_hosted_file_key(
                file_info.get("file", {}).get("url") or ""
            )
            if not hosted_file_key:
                LOGGER.info("기존 첨부 재사용 스킵: 현재 첨부 hosted_file_key 누락")
                return {}
            if hosted_file_key in current_hosted_file_keys:
                LOGGER.info(
                    "기존 첨부 재사용 스킵: 현재 첨부 중복 hosted_file_key 감지 (%s)",
                    hosted_file_key,
                )
                return {}
            current_hosted_file_keys.add(hosted_file_key)
        if name in current_uploaded_names:
            LOGGER.info(
                "기존 첨부 재사용 스킵: 현재 첨부 중복 이름 감지 (%s)",
                name,
            )
            return {}
        current_uploaded_names.add(name)
        if file_type == "file_upload":
            upload_id = str(file_info.get("file_upload", {}).get("id") or "").strip()
            if not upload_id:
                LOGGER.info("기존 첨부 재사용 스킵: 현재 첨부 upload_id 누락")
                return {}
            if upload_id in current_upload_ids:
                LOGGER.info(
                    "기존 첨부 재사용 스킵: 현재 첨부 중복 upload_id 감지 (%s)",
                    upload_id,
                )
                return {}
            current_upload_ids.add(upload_id)
    if current_uploaded_count != len(attachment_state):
        LOGGER.info(
            "기존 첨부 재사용 스킵: 업로드 첨부 개수 불일치 (state=%s, current=%s)",
            len(attachment_state),
            current_uploaded_count,
        )
        return {}
    reusable: dict[str, list[str]] = {}
    seen_upload_ids: set[str] = set()
    seen_names: set[str] = set()
    for entry in attachment_state:
        source_url = str(entry.get("source_url") or "").strip()
        upload_id = str(entry.get("upload_id") or "").strip()
        name = str(entry.get("name") or "").strip()
        hosted_file_key = str(entry.get("hosted_file_key") or "").strip()
        if not source_url or not upload_id:
            LOGGER.info("기존 첨부 재사용 스킵: 상태 값 누락")
            return {}
        if upload_id in seen_upload_ids:
            LOGGER.info(
                "기존 첨부 재사용 스킵: 상태 중복 upload_id 감지 (%s)",
                upload_id,
            )
            return {}
        if not name:
            LOGGER.info("기존 첨부 재사용 스킵: 상태 첨부 이름 누락")
            return {}
        if name in seen_names:
            LOGGER.info("기존 첨부 재사용 스킵: 상태 중복 이름 감지 (%s)", name)
            return {}
        if name not in current_uploaded_names:
            LOGGER.info("기존 첨부 재사용 스킵: 현재 첨부 속성에 없는 이름 (%s)", name)
            return {}
        if saw_file_read_shape:
            if not hosted_file_key:
                LOGGER.info("기존 첨부 재사용 스킵: 상태 hosted_file_key 누락")
                return {}
            if hosted_file_key not in current_hosted_file_keys:
                LOGGER.info(
                    "기존 첨부 재사용 스킵: 현재 첨부 속성에 없는 hosted_file_key (%s)",
                    hosted_file_key,
                )
                return {}
        elif upload_id not in current_upload_ids:
            LOGGER.info(
                "기존 첨부 재사용 스킵: 현재 첨부 속성에 없는 upload_id (%s)",
                upload_id,
            )
            return {}
        seen_upload_ids.add(upload_id)
        seen_names.add(name)
        reusable.setdefault(source_url, []).append(upload_id)
    return reusable


# 이전 sync에서 이미 성공한 업로드 블록을 실제 upload_id까지 확인해 재사용해,
# 부분 성공 뒤 다음 실행에서 같은 파일을 또 올리지 않으면서도 수동 편집 오매핑을 막는다.
def extract_existing_uploaded_media_blocks(
    token: str,
    page_id: str,
    media_state: list[dict],
) -> dict[tuple[str, str], list[dict]]:
    if not page_id or not media_state:
        return {}
    try:
        container = find_sync_container_block(token, page_id)
    except NotionRequestError as exc:
        LOGGER.info("기존 본문 컨테이너 조회 실패: %s (%s)", page_id, exc)
        return {}
    if not container:
        return {}
    container_id = container.get("id")
    if not container_id:
        return {}
    try:
        children = list_block_children(token, container_id)
    except NotionRequestError as exc:
        LOGGER.info("기존 본문 미디어 조회 실패: %s (%s)", container_id, exc)
        return {}
    hosted_blocks_in_order = [block for block in children if is_notion_hosted_media_block(block)]
    if len(hosted_blocks_in_order) != len(media_state):
        LOGGER.info(
            "기존 본문 미디어 재사용 스킵: 미디어 개수 불일치 (state=%s, blocks=%s)",
            len(media_state),
            len(hosted_blocks_in_order),
        )
        return {}
    blocks_by_id: dict[str, dict] = {}
    for block in hosted_blocks_in_order:
        block_id = str(block.get("id") or "").strip()
        if not block_id:
            LOGGER.info("기존 본문 미디어 재사용 스킵: 현재 블록 id 누락")
            return {}
        if block_id in blocks_by_id:
            LOGGER.info("기존 본문 미디어 재사용 스킵: 현재 블록 id 중복 (%s)", block_id)
            return {}
        blocks_by_id[block_id] = block
    reusable: dict[tuple[str, str], list[dict]] = {}
    seen_upload_ids: set[str] = set()
    state_has_block_ids = all(str(meta.get("block_id") or "").strip() for meta in media_state)
    if state_has_block_ids:
        # file_upload는 write-only일 수 있으므로, 현재 read 응답에서는 block_id로 동일 블록을 찾고
        # append payload는 저장된 upload_id로 다시 만든다.
        # 여기에 hosted_file_key까지 함께 맞춰 두면, 같은 block_id를 유지한 채 본문 파일만 수동 교체한 경우도 재사용 전에 차단할 수 있다.
        for meta in media_state:
            upload_id = str(meta.get("upload_id") or "").strip()
            block_id = str(meta.get("block_id") or "").strip()
            hosted_file_key = str(meta.get("hosted_file_key") or "").strip()
            if not upload_id or not block_id:
                LOGGER.info("기존 본문 미디어 재사용 스킵: 상태 값 누락")
                return {}
            if upload_id in seen_upload_ids:
                LOGGER.info(
                    "기존 본문 미디어 재사용 스킵: 상태 중복 upload_id 감지 (%s)",
                    upload_id,
                )
                return {}
            seen_upload_ids.add(upload_id)
            block = blocks_by_id.get(block_id)
            if not block:
                LOGGER.info(
                    "기존 본문 미디어 재사용 스킵: 현재 컨테이너에 없는 block_id (%s)",
                    block_id,
                )
                return {}
            if str(block.get("type") or "") != meta["type"]:
                LOGGER.info(
                    "기존 본문 미디어 재사용 스킵: block_id 타입 불일치 (%s, state=%s, block=%s)",
                    block_id,
                    meta["type"],
                    block.get("type"),
                )
                return {}
            if hosted_file_key:
                current_hosted_file_key = extract_notion_hosted_file_key_from_block(block)
                if not current_hosted_file_key:
                    LOGGER.info(
                        "기존 본문 미디어 재사용 스킵: 현재 블록 hosted_file_key 누락 (%s)",
                        block_id,
                    )
                    return {}
                if current_hosted_file_key != hosted_file_key:
                    LOGGER.info(
                        "기존 본문 미디어 재사용 스킵: hosted_file_key 불일치 (%s)",
                        block_id,
                    )
                    return {}
            sanitized = sanitize_uploaded_media_block(block, upload_id)
            if not sanitized:
                LOGGER.info(
                    "기존 본문 미디어 재사용 스킵: 생성용 블록 정리 실패 (%s)",
                    block.get("type"),
                )
                return {}
            key = (meta["type"], meta["source_url"])
            reusable.setdefault(key, []).append(copy.deepcopy(sanitized))
        return reusable
    # block_id가 없는 예전 상태는 read 응답이 file/file_upload 중 무엇이든 올 수 있으므로,
    # 같은 타입이 반복되지 않는 경우에만 현재 블록 순서와 타입 시퀀스를 보수적으로 맞춰 재사용한다.
    state_types = [str(meta.get("type") or "").strip() for meta in media_state]
    block_types = [str(block.get("type") or "").strip() for block in hosted_blocks_in_order]
    if state_types != block_types:
        LOGGER.info(
            "기존 본문 미디어 재사용 스킵: 타입 시퀀스 불일치 (state=%s, blocks=%s)",
            state_types,
            block_types,
        )
        return {}
    if len(set(state_types)) != len(state_types):
        LOGGER.info("기존 본문 미디어 재사용 스킵: block_id 없는 동일 타입 반복 상태")
        return {}
    for meta, block in zip(media_state, hosted_blocks_in_order):
        upload_id = str(meta.get("upload_id") or "").strip()
        hosted_file_key = str(meta.get("hosted_file_key") or "").strip()
        if not upload_id:
            LOGGER.info("기존 본문 미디어 재사용 스킵: 상태 upload_id 누락")
            return {}
        if upload_id in seen_upload_ids:
            LOGGER.info(
                "기존 본문 미디어 재사용 스킵: 상태 중복 upload_id 감지 (%s)",
                upload_id,
            )
            return {}
        seen_upload_ids.add(upload_id)
        if hosted_file_key:
            current_hosted_file_key = extract_notion_hosted_file_key_from_block(block)
            if not current_hosted_file_key:
                LOGGER.info("기존 본문 미디어 재사용 스킵: 현재 블록 hosted_file_key 누락")
                return {}
            if current_hosted_file_key != hosted_file_key:
                LOGGER.info("기존 본문 미디어 재사용 스킵: hosted_file_key 불일치")
                return {}
        sanitized = sanitize_uploaded_media_block(block, upload_id)
        if not sanitized:
            LOGGER.info(
                "기존 본문 미디어 재사용 스킵: 생성용 블록 정리 실패 (%s)",
                block.get("type"),
            )
            return {}
        key = (meta["type"], meta["source_url"])
        reusable.setdefault(key, []).append(copy.deepcopy(sanitized))
    return reusable


def enrich_body_media_state_with_block_ids(
    token: str,
    page_id: str,
    media_state: list[dict],
) -> list[dict]:
    if not page_id or not media_state:
        return media_state
    try:
        container = find_sync_container_block(token, page_id)
    except NotionRequestError as exc:
        LOGGER.info("본문 미디어 상태 block_id 보강 스킵: 컨테이너 조회 실패 (%s)", exc)
        return media_state
    if not container:
        return media_state
    container_id = str(container.get("id") or "").strip()
    if not container_id:
        return media_state
    try:
        children = list_block_children(token, container_id)
    except NotionRequestError as exc:
        LOGGER.info("본문 미디어 상태 block_id 보강 스킵: 하위 블록 조회 실패 (%s)", exc)
        return media_state
    hosted_blocks_in_order = [block for block in children if is_notion_hosted_media_block(block)]
    if len(hosted_blocks_in_order) != len(media_state):
        return media_state
    state_types = [str(meta.get("type") or "").strip() for meta in media_state]
    block_types = [str(block.get("type") or "").strip() for block in hosted_blocks_in_order]
    if state_types != block_types:
        return media_state
    enriched: list[dict] = []
    seen_block_ids: set[str] = set()
    for meta, block in zip(media_state, hosted_blocks_in_order):
        block_id = str(block.get("id") or "").strip()
        if not block_id or block_id in seen_block_ids:
            return media_state
        enriched_entry = dict(meta)
        enriched_entry["block_id"] = block_id
        # 다음 실행에서 같은 block_id를 가진 다른 hosted 파일로 수동 교체된 경우도 잡을 수 있게,
        # 현재 read 응답 기준 hosted_file_key를 함께 저장한다.
        hosted_file_key = extract_notion_hosted_file_key_from_block(block)
        if hosted_file_key:
            enriched_entry["hosted_file_key"] = hosted_file_key
        enriched.append(enriched_entry)
        seen_block_ids.add(block_id)
    return enriched


def update_quote_block(token: str, block_id: str, rich_text: list[dict]) -> None:
    url = f"https://api.notion.com/v1/blocks/{block_id}"
    payload = {"quote": {"rich_text": rich_text, "color": "default"}}
    notion_request("PATCH", url, token, payload)


def sync_page_body_blocks(
    token: str,
    page_id: str,
    blocks: list[dict],
    sync_mode: str = "overwrite",
) -> None:
    if not blocks:
        return
    idx = 0
    while idx < len(blocks) and is_empty_paragraph_block(blocks[idx]):
        idx += 1
    container_rich_text: list[dict] = []
    if idx < len(blocks) and blocks[idx].get("type") == "paragraph":
        container_rich_text = blocks[idx].get("paragraph", {}).get("rich_text", [])
        idx += 1
    remaining_blocks = blocks[idx:]
    if is_image_only_blocks(remaining_blocks):
        remaining_blocks = [
            block for block in remaining_blocks if not is_empty_paragraph_block(block)
        ]
        if not container_rich_text:
            container_rich_text = build_space_rich_text()
    if (sync_mode or "overwrite").strip().lower() == "preserve":
        container_rich_text = ensure_sync_marker_in_rich_text(container_rich_text)
    sync_mode = (sync_mode or "overwrite").strip().lower()

    if sync_mode == "preserve":
        container_payload = build_container_block(container_rich_text)
        container_id = find_sync_container_id(token, page_id)
        if container_id:
            update_quote_block(token, container_id, container_payload["quote"]["rich_text"])
        else:
            response = append_block_children(token, page_id, [container_payload])
            results = response.get("results", []) if isinstance(response, dict) else []
            container_id = results[0].get("id") if results else None
        if not container_id:
            LOGGER.info("컨테이너 생성 실패: %s", page_id)
            return
        for block in list_block_children(token, container_id):
            block_id = block.get("id")
            if block_id:
                try:
                    delete_block(token, block_id)
                except RuntimeError as exc:
                    LOGGER.info("블록 삭제 실패: %s (%s)", block_id, exc)
        for chunk in chunks(remaining_blocks, 80):
            append_block_children(token, container_id, chunk)
        return

    if not container_rich_text:
        container_rich_text = build_space_rich_text()
    container_payload = build_container_block(container_rich_text)
    children = list_block_children(token, page_id)
    for block in children:
        block_id = block.get("id")
        if block_id:
            try:
                delete_block(token, block_id)
            except RuntimeError as exc:
                LOGGER.info("블록 삭제 실패: %s (%s)", block_id, exc)
    response = append_block_children(token, page_id, [container_payload])
    container_id = None
    results = response.get("results", []) if isinstance(response, dict) else []
    if results:
        container_id = results[0].get("id")
    if not container_id:
        LOGGER.info("컨테이너 생성 실패: %s", page_id)
        return
    for chunk in chunks(remaining_blocks, 80):
        append_block_children(token, container_id, chunk)


def build_properties(
    item: dict,
    has_views_property: bool,
    has_attachments_property: bool,
    has_classification_property: bool,
) -> dict:
    title_text = {"content": item["title"]}
    if item.get("url"):
        title_text["link"] = {"url": item["url"]}
    props = {
        TITLE_PROPERTY: {"title": [{"type": "text", "text": title_text}]},
        TOP_PROPERTY: {"checkbox": item["top"]},
    }

    if item.get("date"):
        props[DATE_PROPERTY] = {"date": {"start": item["date"]}}
    if item.get("author"):
        props[AUTHOR_PROPERTY] = {"select": {"name": item["author"]}}
    if item.get("type"):
        props[TYPE_PROPERTY] = {"select": {"name": item["type"]}}
    if has_attachments_property and "attachments" in item:
        # 원본에서 첨부가 사라진 경우에도 files=[]를 명시적으로 보내야,
        # 예전 실행에서 남은 Notion 첨부파일 속성이 그대로 잔존하지 않는다.
        props[ATTACHMENT_PROPERTY] = {"files": item.get("attachments") or []}
    if has_views_property and item.get("views") is not None:
        props[VIEWS_PROPERTY] = {"number": item["views"]}
    if has_classification_property and item.get("classification"):
        props[CLASSIFICATION_PROPERTY] = {
            "select": {"name": item["classification"]}
        }
    if item.get("url"):
        props[URL_PROPERTY] = {"url": item["url"]}
    return props


def extract_title(properties: dict) -> str:
    title_prop = properties.get(TITLE_PROPERTY, {})
    title_parts = title_prop.get("title", [])
    text = "".join(part.get("plain_text", "") for part in title_parts).strip()
    return text


def extract_date(properties: dict) -> Optional[str]:
    date_prop = properties.get(DATE_PROPERTY, {})
    date_data = date_prop.get("date")
    if not date_data:
        return None
    start = date_data.get("start")
    if not start:
        return None
    return start


def extract_url(properties: dict) -> Optional[str]:
    url_prop = properties.get(URL_PROPERTY, {})
    url_value = url_prop.get("url")
    if not url_value:
        return None
    return normalize_detail_url(url_value)


def extract_rich_text_value(properties: dict, property_name: str) -> str:
    prop = properties.get(property_name, {})
    rich_text = prop.get("rich_text", [])
    return "".join(part.get("plain_text", "") for part in rich_text).strip()


def pick_primary_page(pages: list[dict]) -> Optional[dict]:
    if not pages:
        return None
    return max(
        pages,
        key=lambda page: (
            page.get("last_edited_time") or "",
            page.get("created_time") or "",
            page.get("id") or "",
        ),
    )


def dedupe_pages(
    token: str,
    pages: list[dict],
    reason: str,
    archive_duplicates: bool = True,
) -> Optional[dict]:
    primary = pick_primary_page(pages)
    if not primary:
        return None
    keep_id = primary.get("id")
    archived = 0
    if archive_duplicates:
        for page in pages:
            page_id = page.get("id")
            if not page_id or page_id == keep_id:
                continue
            if page.get("archived"):
                continue
            try:
                archive_page(token, page_id)
                archived += 1
            except NotionRequestError as exc:
                LOGGER.info("중복 페이지 아카이브 실패: %s (%s)", page_id, exc)
    LOGGER.info(
        "중복 페이지 정리: %s (유지=%s, 제거=%s, 총=%s)",
        reason,
        keep_id,
        archived,
        len(pages),
    )
    return primary
def iter_database_pages(token: str, database_id: str) -> list[dict]:
    payload: dict = {"page_size": 100}
    results: list[dict] = []
    while True:
        # 시작 중복 정리도 실제 운영 쿼리와 같은 재확인 경로를 타도록 맞춘다.
        data = query_database_page(token, database_id, payload)
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data.get("next_cursor")
    return results
def dedupe_database_by_url(token: str, database_id: str) -> int:
    pages = iter_database_pages(token, database_id)
    grouped: dict[str, list[dict]] = {}
    for page in pages:
        props = page.get("properties", {})
        url = extract_url(props)
        if not url:
            continue
        grouped.setdefault(url, []).append(page)
    archived = 0
    for url, group in grouped.items():
        if len(group) < 2:
            continue
        primary = pick_primary_page(group)
        if not primary:
            continue
        keep_id = primary.get("id")
        for page in group:
            page_id = page.get("id")
            if not page_id or page_id == keep_id:
                continue
            if page.get("archived"):
                continue
            try:
                archive_page(token, page_id)
                archived += 1
            except NotionRequestError as exc:
                LOGGER.info("중복 페이지 아카이브 실패: %s (%s)", page_id, exc)
        LOGGER.info("URL 중복 정리: %s (유지=%s, 중복=%s)", url, keep_id, len(group) - 1)
    return archived


# 조회 단계명을 함께 남겨서 기존 페이지 탐색이 어디에서 실패했는지 바로 구분한다.
def query_existing_pages_with_stage_log(
    token: str,
    database_id: str,
    filter_payload: dict,
    stage_name: str,
    detail_url: Optional[str],
    title: str,
    date_iso: Optional[str],
) -> list[dict]:
    try:
        return query_database(token, database_id, filter_payload)
    except NotionRequestError as exc:
        LOGGER.error(
            "기존 페이지 조회 실패: 단계=%s, 제목=%s, 작성일=%s, url=%s (%s)",
            stage_name,
            title or "제목없음",
            date_iso or "날짜없음",
            detail_url or "없음",
            exc,
        )
        raise


def find_existing_page(
    token: str,
    database_id: str,
    detail_url: Optional[str],
    title: str,
    date_iso: Optional[str],
) -> Optional[dict]:
    if detail_url:
        results = query_existing_pages_with_stage_log(
            token,
            database_id,
            {"property": URL_PROPERTY, "url": {"equals": detail_url}},
            "URL 일치 조회",
            detail_url,
            title,
            date_iso,
        )
        if len(results) == 1:
            return results[0]
        if len(results) > 1:
            return dedupe_pages(token, results, f"URL={detail_url}", archive_duplicates=True)

    if title and date_iso:
        results = query_existing_pages_with_stage_log(
            token,
            database_id,
            {
                "and": [
                    {"property": TITLE_PROPERTY, "title": {"equals": title}},
                    {"property": DATE_PROPERTY, "date": {"equals": date_iso}},
                ]
            },
            "제목+작성일 조회",
            detail_url,
            title,
            date_iso,
        )
        if len(results) == 1:
            return results[0]
        if len(results) > 1:
            return dedupe_pages(
                token,
                results,
                f"제목+작성일={title} ({date_iso})",
                archive_duplicates=True,
            )

    # 제목 단독 매칭은 오탐 업데이트 위험이 커서 설정으로 명시적으로 켠 경우에만 허용한다.
    if title and should_allow_title_only_match():
        results = query_existing_pages_with_stage_log(
            token,
            database_id,
            {"property": TITLE_PROPERTY, "title": {"equals": title}},
            "제목 단독 조회",
            detail_url,
            title,
            date_iso,
        )
        if len(results) == 1:
            return results[0]
        if len(results) > 1:
            primary = pick_primary_page(results)
            if primary:
                LOGGER.info(
                    "제목 중복 감지(삭제 생략): %s (유지=%s, 총=%s)",
                    title,
                    primary.get("id"),
                    len(results),
                )
                return primary
    return None
def iter_top_pages(token: str, database_id: str):
    payload = {
        "filter": {"property": TOP_PROPERTY, "checkbox": {"equals": True}},
        "page_size": 100,
    }

    while True:
        # TOP 정리도 DB 조회 실패 유형을 동일한 기준으로 해석할 수 있게 공통 helper를 사용한다.
        data = query_database_page(token, database_id, payload)
        for page in data.get("results", []):
            yield page
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data.get("next_cursor")


def disable_missing_top(
    token: str,
    database_id: str,
    current_top_urls: set[str],
    current_top_dates: dict[str, set[str]],
) -> int:
    disabled = 0
    for page in iter_top_pages(token, database_id):
        props = page.get("properties", {})
        page_url = extract_url(props)
        if page_url and current_top_urls:
            if page_url in current_top_urls:
                continue
        title = extract_title(props)
        if not title:
            continue
        date_iso = extract_date(props)
        date_key = normalize_date_key(date_iso)
        title_dates = current_top_dates.get(title)
        if title_dates is not None and date_key in title_dates:
            continue
        update_page(token, page["id"], {TOP_PROPERTY: {"checkbox": False}})
        disabled += 1
        LOGGER.info("TOP 해제: %s (%s)", title, date_iso or "날짜없음")
    return disabled
