import re
from typing import Optional

from common import (
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
    query_database,
    query_database_page,
    update_page,
)
from settings import (
    ATTACHMENT_PROPERTY,
    AUTHOR_PROPERTY,
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
    if has_attachments_property and item.get("attachments"):
        props[ATTACHMENT_PROPERTY] = {"files": item["attachments"]}
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
