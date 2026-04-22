import re
from html import unescape
from typing import Optional
from urllib.parse import parse_qs, urlparse

from settings import (
    DETAIL_ID_CAPTURE_PATTERN,
    DETAIL_ID_DATA_ATTR_PATTERN,
    DETAIL_ID_FUNCTION_PATTERN,
    DETAIL_ID_PARAM_PATTERN,
    DETAIL_PATH_PATTERN,
    LIST_ROW_SELECTOR,
    build_detail_url,
)
from utils import (
    normalize_date_key,
    normalize_detail_url,
    normalize_title_key,
    parse_datetime,
    parse_int,
)


# 상세 ID와 URL 판별 규칙은 HTML 파서와 Playwright 수집기가 같은 기준을 써야 하므로 공통 모듈로 모은다.
def extract_detail_id_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    match = DETAIL_ID_CAPTURE_PATTERN.search(text)
    if match:
        return match.group(1)
    match = DETAIL_ID_FUNCTION_PATTERN.search(text)
    if match:
        return match.group(1)
    match = DETAIL_ID_PARAM_PATTERN.search(text)
    if match:
        return match.group(1)
    match = DETAIL_ID_DATA_ATTR_PATTERN.search(text)
    if match:
        return match.group(1)
    return None


def is_detail_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    path = parsed.path or ""
    if DETAIL_PATH_PATTERN.search(path):
        return True
    qs = parse_qs(parsed.query)
    return "bbsConfigFk" in qs


def extract_detail_url_from_row_html(
    row_html: str,
    config_fk: Optional[str] = None,
) -> Optional[str]:
    for match in re.finditer(r'href="([^"]+)"', row_html):
        href = unescape(match.group(1))
        candidate = normalize_detail_url(href)
        if candidate and is_detail_url(candidate):
            return candidate
        detail_id = extract_detail_id_from_text(href)
        if detail_id:
            return normalize_detail_url(build_detail_url(detail_id, config_fk))
    match = re.search(r"/detail/(\d+)", row_html)
    if match:
        return normalize_detail_url(build_detail_url(match.group(1), config_fk))
    return None


def get_browser_launcher(playwright, browser: str):
    browser = browser.lower()
    if browser in {"chromium", "chrome", "edge"}:
        return playwright.chromium
    if browser == "firefox":
        return playwright.firefox
    if browser in {"webkit", "safari"}:
        return playwright.webkit
    raise RuntimeError(f"Unsupported BROWSER: {browser}")


def extract_list_rows(page, config_fk: Optional[str] = None) -> list[dict]:
    rows = page.locator(LIST_ROW_SELECTOR)
    count = rows.count()
    items = []

    for index in range(count):
        row = rows.nth(index)
        cells = row.locator("td")
        cell_count = cells.count()
        if cell_count < 5:
            continue

        num_or_top = cells.nth(0).inner_text().strip()
        title = cells.nth(1).inner_text().strip()
        author = cells.nth(2).inner_text().strip()
        date_text = cells.nth(cell_count - 2).inner_text().strip()
        views_text = cells.nth(cell_count - 1).inner_text().strip()

        date_iso = parse_datetime(date_text)
        views = parse_int(views_text)
        if views is None:
            continue

        top = num_or_top.strip().upper() == "TOP"
        detail_url = None
        link = row.locator("a[href]")
        link_count = link.count()
        if link_count:
            for idx in range(link_count):
                href = link.nth(idx).get_attribute("href")
                if not href:
                    continue
                candidate = normalize_detail_url(href)
                if candidate and is_detail_url(candidate):
                    detail_url = candidate
                    break
                detail_id = extract_detail_id_from_text(href)
                if detail_id:
                    detail_url = normalize_detail_url(build_detail_url(detail_id, config_fk))
                    break
        if not detail_url:
            onclick = row.get_attribute("onclick") or ""
            detail_id = extract_detail_id_from_text(onclick)
            if detail_id:
                detail_url = normalize_detail_url(build_detail_url(detail_id, config_fk))
            else:
                try:
                    row_html = row.evaluate("row => row.outerHTML")
                except Exception:
                    row_html = ""
                detail_id = extract_detail_id_from_text(row_html or "")
                if detail_id:
                    detail_url = normalize_detail_url(build_detail_url(detail_id, config_fk))
        items.append(
            {
                "title": title,
                "author": author,
                "date": date_iso,
                "views": views,
                "top": top,
                "row_index": index,
                "detail_url": detail_url,
            }
        )

    return items


# 본문 블록 정규화와 제목 보정은 수집기와 동기화기가 같은 기준을 써야 해시와 중복 판별이 흔들리지 않는다.
def is_empty_paragraph_block(block: dict) -> bool:
    if block.get("type") != "paragraph":
        return False
    rich_text = block.get("paragraph", {}).get("rich_text", [])
    if not rich_text:
        return True
    content = "".join(
        item.get("text", {}).get("content", "") for item in rich_text
    )
    return content.replace("\u00a0", "").strip() == ""


def strip_trailing_empty_paragraphs(blocks: list[dict]) -> list[dict]:
    if not blocks:
        return blocks
    end = len(blocks)
    while end > 0 and is_empty_paragraph_block(blocks[end - 1]):
        end -= 1
    if end == len(blocks):
        return blocks
    return blocks[:end]


def trim_trailing_whitespace_rich_text(rich_text: list[dict]) -> None:
    idx = len(rich_text) - 1
    while idx >= 0:
        item = rich_text[idx]
        if item.get("type") != "text":
            break
        text_payload = item.get("text", {})
        content = text_payload.get("content", "")
        trimmed = content.rstrip()
        if trimmed == content:
            break
        if trimmed:
            text_payload["content"] = trimmed
            break
        rich_text.pop()
        idx -= 1


def normalize_body_blocks(blocks: list[dict]) -> list[dict]:
    normalized = strip_trailing_empty_paragraphs(blocks or [])
    if not normalized:
        return normalized
    last = normalized[-1]
    block_type = last.get("type")
    if block_type in {"paragraph", "bulleted_list_item"}:
        rich_text = last.get(block_type, {}).get("rich_text", [])
        if rich_text:
            trim_trailing_whitespace_rich_text(rich_text)
            if not rich_text:
                normalized = strip_trailing_empty_paragraphs(normalized[:-1])
    return normalized


def rich_text_plain_text(rich_text: list[dict]) -> str:
    return "".join(item.get("text", {}).get("content", "") for item in rich_text)


def extract_first_nonempty_line(text: str) -> str:
    if not text:
        return ""
    for line in text.splitlines():
        cleaned = line.replace("\u00a0", " ").strip()
        if cleaned:
            return cleaned
    return text.replace("\u00a0", " ").strip()


def derive_title_from_blocks(blocks: list[dict]) -> str:
    for block in blocks or []:
        block_type = block.get("type")
        if block_type not in {"paragraph", "bulleted_list_item"}:
            continue
        rich_text = block.get(block_type, {}).get("rich_text", [])
        if not rich_text:
            continue
        text = rich_text_plain_text(rich_text)
        candidate = extract_first_nonempty_line(text)
        if candidate:
            return candidate
    return ""


def build_fallback_title(detail_url: Optional[str], date_iso: Optional[str]) -> str:
    detail_id = extract_detail_id_from_text(detail_url or "")
    if detail_id:
        return f"제목없음-{detail_id}"
    date_key = normalize_date_key(date_iso)
    if date_key:
        return f"제목없음-{date_key}"
    return "제목없음"


def ensure_item_title(
    item: dict,
    body_blocks: list[dict],
    detail_url: Optional[str] = None,
) -> None:
    title = normalize_title_key(item.get("title", ""))
    if title:
        item["title"] = title
        return
    derived = derive_title_from_blocks(body_blocks)
    if derived:
        item["title"] = normalize_title_key(derived)
        return
    item["title"] = build_fallback_title(detail_url or item.get("url"), item.get("date"))
