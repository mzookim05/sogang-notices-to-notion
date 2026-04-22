import json
import os
import re
import socket
import urllib.error
import urllib.request
from html import unescape
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse

from log import LOGGER
from bbs_parser import (
    detect_body_has_content,
    extract_attachments_from_detail,
    extract_attachments_from_page,
    extract_body_blocks_from_html,
    extract_detail_id_from_row,
    extract_detail_id_from_text,
    extract_written_at_from_detail,
    extract_written_at_from_page,
    is_detail_url,
    parse_rows,
    ensure_item_title,
)
from settings import (
    ATTACHMENT_LINK_PATTERN,
    BASE_URL,
    BBS_API_BASE,
    BBS_LIST_API_URL,
    BODY_CONTAINER_PATTERN,
    DATE_TIME_JS_PATTERN,
    DEFAULT_QUERY,
    LIST_ROW_SELECTOR,
    USER_AGENT,
    build_detail_url,
    get_attachment_allowed_domains,
    get_attachment_max_count,
    get_bbs_config_fk,
    get_bbs_config_fks,
    get_classification_for_config,
    get_list_base_url,
    get_non_top_max_pages,
    should_include_non_top,
)
from utils import (
    build_site_headers,
    is_allowed_attachment_host,
    is_attachment_candidate,
    normalize_detail_url,
    normalize_file_url,
    normalize_title_key,
    parse_compact_datetime,
    parse_datetime,
    parse_int,
    replace_body_image_urls,
)

def run_attachment_policy_selftest() -> None:
    LOGGER.info("첨부파일 정책 셀프테스트 시작")
    keys = ("ATTACHMENT_ALLOWED_DOMAINS",)
    original_env = {key: os.environ.get(key) for key in keys}
    os.environ["ATTACHMENT_ALLOWED_DOMAINS"] = "sogang.ac.kr"
    try:
        html = (
            '<div>첨부파일</div>'
            '<a href="https://example.com/file.pdf">file.pdf</a>'
        )
        html_attachments = extract_attachments_from_detail(html)
        api_attachments = extract_attachments_from_api_data(
            {"fileValue1": "https://example.com/file.pdf"}
        )
        page_candidates = [("https://example.com/file.pdf", "file.pdf")]
        page_attachments = []
        for href, text in page_candidates:
            url = normalize_file_url(href)
            if not url:
                continue
            allowed, _ = is_attachment_candidate(
                url, text, allow_domain_only=True
            )
            if allowed:
                page_attachments.append(url)
        strict_allowed, _ = is_attachment_candidate(
            "https://example.com/file.pdf",
            "file.pdf",
            allow_domain_only=True,
        )
        if html_attachments or api_attachments or strict_allowed or page_attachments:
            LOGGER.info(
                "첨부파일 정책 셀프테스트 실패: html=%s, api=%s, strict_allowed=%s, page=%s",
                len(html_attachments),
                len(api_attachments),
                int(strict_allowed),
                len(page_attachments),
            )
            raise RuntimeError("첨부파일 정책 셀프테스트 실패")
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            LOGGER.info("Playwright 미설치: 셀프테스트(Playwright) 스킵")
        else:
            pw_attachments: list[dict] = []
            with sync_playwright() as playwright:
                try:
                    browser = playwright.chromium.launch(headless=True)
                except Exception as exc:
                    LOGGER.info(
                        "Playwright 브라우저 실행 실패: %s (셀프테스트 스킵)",
                        exc,
                    )
                    browser = None
                if browser:
                    try:
                        page = browser.new_page()
                        page.set_content(html, wait_until="domcontentloaded")
                        pw_attachments = extract_attachments_from_page(page)
                    finally:
                        browser.close()
            if pw_attachments:
                LOGGER.info(
                    "첨부파일 정책 셀프테스트 실패(Playwright): %s개",
                    len(pw_attachments),
                )
                raise RuntimeError("첨부파일 정책 셀프테스트 실패(Playwright)")
        LOGGER.info("첨부파일 정책 셀프테스트 통과")
    finally:
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

def log_attachments(label: str, attachments: list[dict]) -> None:
    if not attachments:
        return
    LOGGER.info("첨부파일 추출: %s (총 %s개)", label, len(attachments))
    for attachment in attachments:
        url = attachment.get("external", {}).get("url") or ""
        name = attachment.get("name") or ""
        LOGGER.info("첨부파일 링크: %s (%s)", url, name)


def cap_attachments(attachments: list[dict], label: str) -> list[dict]:
    max_count = get_attachment_max_count()
    if max_count <= 0:
        return attachments
    if len(attachments) > max_count:
        LOGGER.info(
            "첨부파일 상한 적용: %s (원본 %s개 -> %s개)",
            label,
            len(attachments),
            max_count,
        )
        return attachments[:max_count]
    return attachments
def fetch_site_json(url: str) -> Optional[dict]:
    req = urllib.request.Request(url, headers=build_site_headers())
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
        text = raw.decode("utf-8", errors="replace")
        return json.loads(text)
    except urllib.error.HTTPError as exc:
        LOGGER.info("API 요청 실패: %s (HTTP %s)", url, exc.code)
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, socket.timeout):
            LOGGER.info("API 요청 실패: %s (timeout)", url)
        else:
            LOGGER.info("API 요청 실패: %s (%s)", url, exc.reason)
    except socket.timeout:
        LOGGER.info("API 요청 실패: %s (timeout)", url)
    except json.JSONDecodeError:
        LOGGER.info("API 응답 파싱 실패: %s", url)
    return None


def fetch_bbs_list(
    page_num: int,
    page_size: int = 20,
    config_fk: Optional[str] = None,
) -> list[dict]:
    config_fk = (config_fk or get_bbs_config_fk()).strip()
    params = {
        "pageNum": str(page_num),
        "pageSize": str(page_size),
        "bbsConfigFks": config_fk,
        "title": "",
        "content": "",
        "username": "",
        "category": "",
    }
    url = f"{BBS_LIST_API_URL}?{urlencode(params)}"
    data = fetch_site_json(url)
    if not data:
        return []
    return data.get("data", {}).get("list", []) or []


def fetch_bbs_detail(pk_id: str, config_fk: Optional[str] = None) -> Optional[dict]:
    config_fk = (config_fk or get_bbs_config_fk()).strip()
    params = {"pkId": pk_id}
    if config_fk:
        params["bbsConfigFk"] = config_fk
    url = f"{BBS_API_BASE}?{urlencode(params)}"
    data = fetch_site_json(url)
    if not data:
        return None
    detail = data.get("data")
    if not isinstance(detail, dict):
        return None
    return detail


def extract_attachments_from_api_data(data: dict) -> list[dict]:
    attachments: list[dict] = []
    seen: set[str] = set()
    allowed_domains = get_attachment_allowed_domains()
    for idx in range(1, 6):
        raw = data.get(f"fileValue{idx}")
        if not raw:
            continue
        url = normalize_file_url(str(raw))
        if not url or url in seen:
            continue
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        if not is_allowed_attachment_host(host, allowed_domains):
            continue
        seen.add(url)
        params = parse_qs(urlparse(url).query)
        name = params.get("sg", [""])[0].strip()
        if not name:
            name = Path(urlparse(url).path).name or "첨부파일"
        attachments.append({"name": name, "type": "external", "external": {"url": url}})
    return attachments

def build_list_url(page: int, base_url: Optional[str] = None) -> str:
    query = dict(DEFAULT_QUERY)
    query["page"] = str(page)
    base_url = base_url or BASE_URL
    return f"{base_url}?{urlencode(query)}"


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


def return_to_list_page(page, list_url: str) -> None:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    try:
        page.go_back()
        page.wait_for_selector(LIST_ROW_SELECTOR, timeout=30000)
    except PlaywrightTimeoutError:
        if not goto_list_page(page, list_url):
            LOGGER.info("목록 복귀 실패: %s", list_url)


def wait_for_written_at(page, timeout_ms: int = 30000) -> bool:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    try:
        page.wait_for_function(
            "pattern => new RegExp(pattern).test(document.body.innerText)",
            arg=DATE_TIME_JS_PATTERN,
            timeout=timeout_ms,
        )
        return True
    except PlaywrightTimeoutError:
        return False


def wait_for_detail_url(page, list_url: str) -> Optional[str]:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    try:
        page.wait_for_url(lambda url: is_detail_url(url) and url != list_url, timeout=30000)
    except PlaywrightTimeoutError:
        return None
    return page.url

def fetch_detail_metadata_via_playwright(
    page,
    list_url: str,
    detail_url: str,
) -> tuple[Optional[str], list[dict], list[dict]]:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    written_at = None
    attachments: list[dict] = []
    body_blocks: list[dict] = []
    try:
        page.goto(detail_url, wait_until="domcontentloaded", timeout=30000)
        if not wait_for_written_at(page):
            LOGGER.info("작성일 로드 대기 실패: %s", detail_url)
        try:
            page.wait_for_selector("text=첨부파일", timeout=5000)
        except PlaywrightTimeoutError:
            pass
        label_visible = page.locator("text=첨부파일").count()
        if not label_visible:
            try:
                label_visible = page.wait_for_selector(
                    "text=첨부파일", timeout=10000, state="attached"
                )
                label_visible = 1 if label_visible else 0
            except PlaywrightTimeoutError:
                label_visible = 0
        LOGGER.info("첨부파일 라벨 감지: %s (%s)", label_visible, detail_url)
        written_at = extract_written_at_from_page(page)
        if not written_at:
            written_at = extract_written_at_from_detail(page.content())
        attachments = extract_attachments_from_page(page)
        if not attachments:
            attachments = extract_attachments_from_detail(page.content())
        body_blocks = extract_body_blocks_from_html(page.content())
        if attachments and body_blocks:
            body_blocks = replace_body_image_urls(body_blocks, attachments)
    except PlaywrightTimeoutError:
        LOGGER.info("상세 페이지 로드 실패: %s", detail_url)
    finally:
        return_to_list_page(page, list_url)
    return written_at, attachments, body_blocks


def fetch_detail_for_row(
    page,
    list_url: str,
    row_index: int,
    detail_url: Optional[str],
    config_fk: Optional[str] = None,
) -> tuple[Optional[str], Optional[str], list[dict], list[dict]]:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    if detail_url:
        detail_url = normalize_detail_url(detail_url) or detail_url
        if detail_url and not is_detail_url(detail_url):
            LOGGER.info("상세 URL 경로 아님: %s", detail_url)
            detail_url = None
    if detail_url:
        written_at, attachments, body_blocks, signals = fetch_detail_metadata_from_url(
            detail_url
        )
        if should_retry_detail_fetch(written_at, attachments, body_blocks, signals):
            pw_written_at, pw_attachments, pw_body_blocks = fetch_detail_metadata_via_playwright(
                page, list_url, detail_url
            )
            if not written_at and pw_written_at:
                written_at = pw_written_at
            if pw_attachments:
                attachments = pw_attachments
            if pw_body_blocks:
                body_blocks = pw_body_blocks
        return written_at, detail_url, attachments, body_blocks

    rows = page.locator(LIST_ROW_SELECTOR)
    if row_index >= rows.count():
        return None, None, [], []

    row = rows.nth(row_index)
    row.scroll_into_view_if_needed()
    detail_id = extract_detail_id_from_row(row)
    if detail_id:
        # 상세 ID에서 만든 URL도 정규화 결과가 없을 수 있으니, str로 확정된 뒤에만 메타데이터 조회를 진행한다.
        normalized_detail_url = normalize_detail_url(build_detail_url(detail_id, config_fk))
        if normalized_detail_url:
            written_at, attachments, body_blocks, signals = fetch_detail_metadata_from_url(
                normalized_detail_url
            )
            if should_retry_detail_fetch(written_at, attachments, body_blocks, signals):
                pw_written_at, pw_attachments, pw_body_blocks = (
                    fetch_detail_metadata_via_playwright(
                        page,
                        list_url,
                        normalized_detail_url,
                    )
                )
                if not written_at and pw_written_at:
                    written_at = pw_written_at
                if pw_attachments:
                    attachments = pw_attachments
                if pw_body_blocks:
                    body_blocks = pw_body_blocks
            if written_at or attachments or body_blocks:
                return written_at, normalized_detail_url, attachments, body_blocks
    row.click()

    detail_url = wait_for_detail_url(page, list_url)
    if not detail_url:
        LOGGER.info("상세 URL 전환 실패: row %s", row_index)
        return_to_list_page(page, list_url)
        return None, None, [], []

    normalized_detail_url = normalize_detail_url(detail_url) or detail_url
    written_at, attachments, body_blocks, _signals = fetch_detail_metadata_from_url(
        normalized_detail_url
    )
    if not wait_for_written_at(page):
        LOGGER.info("작성일 로드 대기 실패: %s", detail_url)
    if not written_at:
        written_at = extract_written_at_from_page(page)
        if not written_at:
            written_at = extract_written_at_from_detail(page.content())
    page_attachments = extract_attachments_from_page(page)
    if page_attachments:
        attachments = page_attachments
    elif not attachments:
        attachments = extract_attachments_from_detail(page.content())
    page_blocks = extract_body_blocks_from_html(page.content())
    if page_blocks:
        body_blocks = page_blocks
    if attachments and body_blocks:
        body_blocks = replace_body_image_urls(body_blocks, attachments)
    return_to_list_page(page, list_url)
    return written_at, normalized_detail_url, attachments, body_blocks


def goto_list_page(page, url: str) -> bool:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    try:
        response = page.goto(url, wait_until="domcontentloaded", timeout=45000)
    except PlaywrightTimeoutError:
        LOGGER.info("페이지 로드 타임아웃: %s", url)
        return False
    if response is not None and response.status >= 400:
        LOGGER.info("페이지 응답 코드: %s (%s)", response.status, url)
    try:
        page.wait_for_selector(LIST_ROW_SELECTOR, timeout=30000)
    except PlaywrightTimeoutError:
        LOGGER.info("목록 셀렉터 미검출: %s", url)
        return False
    return True


def crawl_top_items_api(
    config_fk: str,
    include_non_top: bool,
    non_top_max_pages: int,
) -> list[dict]:
    items: list[dict] = []
    seen: set[str] = set()
    page_number = 1
    classification = get_classification_for_config(config_fk)
    page_size_raw = os.environ.get("BBS_PAGE_SIZE", "20")
    try:
        page_size = max(1, int(page_size_raw))
    except ValueError:
        page_size = 20

    while True:
        if include_non_top and non_top_max_pages > 0 and page_number > non_top_max_pages:
            LOGGER.info("비TOP 페이지 상한 도달(API): %s", non_top_max_pages)
            break
        LOGGER.info("페이지 로드 시작(API): %s", page_number)
        page_entries = fetch_bbs_list(page_number, page_size, config_fk=config_fk)
        LOGGER.info("페이지 %s 항목 수(API): %s", page_number, len(page_entries))
        if not page_entries:
            break

        if include_non_top:
            entries_to_process = page_entries
        else:
            entries_to_process = [
                entry
                for entry in page_entries
                if str(entry.get("isTop", "")).upper() == "Y"
            ]
        new_count = 0

        for entry in entries_to_process:
            pk_id = str(entry.get("pkId") or "").strip()
            if not pk_id:
                continue
            detail_url = normalize_detail_url(
                build_detail_url(pk_id, config_fk)
            ) or build_detail_url(pk_id, config_fk)
            detail = fetch_bbs_detail(pk_id, config_fk=config_fk)
            if detail is None:
                LOGGER.info("상세 API 로드 실패: %s", pk_id)
                detail = {}

            title = normalize_title_key(detail.get("title") or entry.get("title") or "")
            author = detail.get("userName") or entry.get("userName") or entry.get("userNickName") or ""
            written_at = parse_compact_datetime(detail.get("regDate") or entry.get("regDate"))
            views_raw = detail.get("viewCount", entry.get("viewCount"))
            views = parse_int(str(views_raw)) if views_raw is not None else None
            top = str(entry.get("isTop", "")).upper() == "Y"
            if not include_non_top and not top:
                continue

            attachments = extract_attachments_from_api_data(detail or entry)
            content_html = detail.get("content") or ""
            body_blocks = extract_body_blocks_from_html(content_html) if content_html else []
            if attachments and body_blocks:
                body_blocks = replace_body_image_urls(body_blocks, attachments)

            item = {
                "title": title,
                "author": author,
                "date": written_at,
                "views": views,
                "top": top,
                "url": detail_url,
            }
            if body_blocks:
                item["body_blocks"] = body_blocks
            if classification:
                item["classification"] = classification
            ensure_item_title(item, body_blocks, detail_url)
            if attachments:
                attachments = cap_attachments(attachments, item["title"])
                item["attachments"] = attachments
                log_attachments(item["title"], attachments)

            key = detail_url or f"{item['title']}|{written_at or ''}"
            if key in seen:
                continue
            seen.add(key)
            items.append(item)
            new_count += 1

        LOGGER.info("페이지 %s 신규 수집 수(API): %s", page_number, new_count)
        if not include_non_top:
            has_non_top = any(
                str(entry.get("isTop", "")).upper() != "Y" for entry in page_entries
            )
            if has_non_top:
                LOGGER.info("페이지 %s에서 비TOP 발견, 다음 페이지 탐색 중단(API)", page_number)
                break
        page_number += 1

    return items


def crawl_top_items_playwright(
    config_fk: str,
    include_non_top: bool,
    non_top_max_pages: int,
) -> list[dict]:
    base_url = get_list_base_url(config_fk)
    classification = get_classification_for_config(config_fk)
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    except ImportError:
        LOGGER.info("Playwright 미설치: HTTP 모드로 전환")
        return crawl_top_items_http(config_fk, include_non_top, non_top_max_pages)

    items = []
    seen = set()
    browser_name = os.environ.get("BROWSER", "chromium")
    headless_raw = os.environ.get("HEADLESS", "1").strip().lower()
    headless = headless_raw not in {"0", "false", "no", "off"}
    user_agent = os.environ.get("USER_AGENT", USER_AGENT)
    fallback_to_http = False

    with sync_playwright() as playwright:
        try:
            launcher = get_browser_launcher(playwright, browser_name)
            browser = launcher.launch(headless=headless)
        except Exception as exc:
            LOGGER.info("Playwright 브라우저 실행 실패: %s (HTTP 모드로 전환)", exc)
            return crawl_top_items_http(config_fk, include_non_top, non_top_max_pages)
        try:
            context = browser.new_context(
                user_agent=user_agent,
                viewport={"width": 1920, "height": 1080},
            )
            page = context.new_page()

            page_number = 1
            while True:
                if include_non_top and non_top_max_pages > 0 and page_number > non_top_max_pages:
                    LOGGER.info("비TOP 페이지 상한 도달: %s", non_top_max_pages)
                    break
                url = build_list_url(page_number, base_url)
                LOGGER.info("페이지 로드 시작: %s", url)
                if not goto_list_page(page, url):
                    LOGGER.info("페이지 %s 로드 실패", page_number)
                    if page_number == 1:
                        LOGGER.info("Playwright 페이지 로드 실패: HTTP 모드로 전환")
                        fallback_to_http = True
                    break

                page_items = extract_list_rows(page, config_fk)
                LOGGER.info("페이지 %s 항목 수: %s", page_number, len(page_items))
                if not page_items:
                    break

                if include_non_top:
                    items_to_process = page_items
                else:
                    items_to_process = [item for item in page_items if item.get("top")]
                new_count = 0
                for item in items_to_process:
                    body_blocks: list[dict] = []
                    attachments: list[dict] = []
                    written_at, detail_url, attachments, body_blocks = fetch_detail_for_row(
                        page,
                        url,
                        item["row_index"],
                        item.get("detail_url"),
                        config_fk,
                    )
                    if written_at:
                        item["date"] = written_at
                    if detail_url:
                        item["url"] = normalize_detail_url(detail_url)
                    if body_blocks:
                        item["body_blocks"] = body_blocks
                    if classification:
                        item["classification"] = classification
                    ensure_item_title(item, body_blocks, detail_url or item.get("url"))
                    if not detail_url:
                        LOGGER.info("상세 URL 미확보: %s", item["title"])
                    if not written_at:
                        LOGGER.info(
                            "작성일 미검출: %s (%s)",
                            item["title"],
                            detail_url or "URL없음",
                        )
                    if attachments:
                        attachments = cap_attachments(attachments, item["title"])
                        item["attachments"] = attachments
                        log_attachments(item["title"], attachments)
                    key = item.get("url") or f"{item['title']}|{item.get('date') or ''}"
                    if key in seen:
                        continue
                    seen.add(key)
                    items.append(item)
                    new_count += 1

                LOGGER.info("페이지 %s 신규 수집 수: %s", page_number, new_count)
                if not include_non_top:
                    has_non_top = any(not item.get("top") for item in page_items)
                    if has_non_top:
                        LOGGER.info("페이지 %s에서 비TOP 발견, 다음 페이지 탐색 중단", page_number)
                        break
                page_number += 1
        finally:
            browser.close()

    if fallback_to_http:
        return crawl_top_items_http(config_fk, include_non_top, non_top_max_pages)
    return items


def crawl_top_items() -> list[dict]:
    include_non_top = should_include_non_top()
    non_top_max_pages = get_non_top_max_pages()
    if include_non_top:
        limit_label = "제한없음" if non_top_max_pages <= 0 else str(non_top_max_pages)
        LOGGER.info("비TOP 포함 모드: 최대 페이지=%s", limit_label)

    config_fks = get_bbs_config_fks()
    if not config_fks:
        return []

    items: list[dict] = []
    seen: set[str] = set()
    for config_fk in config_fks:
        classification = get_classification_for_config(config_fk)
        LOGGER.info(
            "수집 설정: bbsConfigFk=%s, 분류=%s",
            config_fk,
            classification or "없음",
        )
        config_items = crawl_top_items_api(config_fk, include_non_top, non_top_max_pages)
        if not config_items:
            config_items = crawl_top_items_playwright(
                config_fk, include_non_top, non_top_max_pages
            )
        for item in config_items:
            key = item.get("url") or f"{item.get('title','')}|{item.get('date') or ''}"
            if key in seen:
                continue
            seen.add(key)
            items.append(item)
    return items


def crawl_top_items_http(
    config_fk: str,
    include_non_top: bool,
    non_top_max_pages: int,
) -> list[dict]:
    items = []
    seen = set()
    page_number = 1
    base_url = get_list_base_url(config_fk)
    classification = get_classification_for_config(config_fk)

    while True:
        if include_non_top and non_top_max_pages > 0 and page_number > non_top_max_pages:
            LOGGER.info("비TOP 페이지 상한 도달(HTTP): %s", non_top_max_pages)
            break
        url = build_list_url(page_number, base_url)
        LOGGER.info("페이지 로드 시작(HTTP): %s", url)
        html_text = fetch_html(url)
        if not html_text:
            LOGGER.info("페이지 %s 로드 실패(HTTP)", page_number)
            break
        page_items = parse_rows(html_text, config_fk)
        LOGGER.info("페이지 %s 항목 수(HTTP): %s", page_number, len(page_items))
        if not page_items:
            break

        if include_non_top:
            items_to_process = page_items
        else:
            items_to_process = [item for item in page_items if item.get("top")]
        new_count = 0
        for item in items_to_process:
            body_blocks: list[dict] = []
            attachments: list[dict] = []
            if item.get("url"):
                written_at, attachments, body_blocks, _signals = fetch_detail_metadata_from_url(
                    item["url"]
                )
                if written_at:
                    item["date"] = written_at
                if body_blocks:
                    item["body_blocks"] = body_blocks
            if classification:
                item["classification"] = classification
            ensure_item_title(item, body_blocks, item.get("url"))
            if attachments:
                attachments = cap_attachments(attachments, item["title"])
                item["attachments"] = attachments
                log_attachments(item["title"], attachments)
            key = item.get("url") or f"{item['title']}|{item.get('date') or ''}"
            if key in seen:
                continue
            seen.add(key)
            items.append(item)
            new_count += 1

        LOGGER.info("페이지 %s 신규 수집 수(HTTP): %s", page_number, new_count)
        if not include_non_top:
            has_non_top = any(not item.get("top") for item in page_items)
            if has_non_top:
                LOGGER.info("페이지 %s에서 비TOP 발견, 다음 페이지 탐색 중단(HTTP)", page_number)
                break
        page_number += 1

    return items

def fetch_html(url: str) -> Optional[str]:
    req = urllib.request.Request(url, headers=build_site_headers())
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        LOGGER.info("상세 HTML 요청 실패: %s (HTTP %s)", url, exc.code)
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, socket.timeout):
            LOGGER.info("상세 HTML 요청 실패: %s (timeout)", url)
        else:
            LOGGER.info("상세 HTML 요청 실패: %s (%s)", url, exc.reason)
    except socket.timeout:
        LOGGER.info("상세 HTML 요청 실패: %s (timeout)", url)
    return None


def build_detail_signals(html_text: str) -> dict:
    return {
        "has_html": True,
        "has_attachment_label": "첨부파일" in html_text,
        "has_attachment_link": bool(ATTACHMENT_LINK_PATTERN.search(html_text)),
        "has_body_container": bool(BODY_CONTAINER_PATTERN.search(html_text)),
        "body_has_content": detect_body_has_content(html_text),
    }


def should_retry_detail_fetch(
    written_at: Optional[str],
    attachments: list[dict],
    body_blocks: list[dict],
    signals: dict,
) -> bool:
    reasons: list[str] = []
    if not written_at:
        reasons.append("작성일")
    if (signals.get("has_attachment_label") or signals.get("has_attachment_link")) and not attachments:
        reasons.append("첨부파일")
    if (
        signals.get("has_body_container")
        and signals.get("body_has_content")
        and not body_blocks
    ):
        reasons.append("본문")
    retry = bool(reasons)
    LOGGER.info(
        "상세 재시도 판단: %s (reasons=%s, written_at=%s, attachments=%s, body_blocks=%s, signals=label=%s,link=%s,body_container=%s,body_content=%s)",
        "Y" if retry else "N",
        ",".join(reasons) if reasons else "-",
        "Y" if written_at else "N",
        len(attachments),
        len(body_blocks),
        int(bool(signals.get("has_attachment_label"))),
        int(bool(signals.get("has_attachment_link"))),
        int(bool(signals.get("has_body_container"))),
        int(bool(signals.get("body_has_content"))),
    )
    return retry


def fetch_detail_metadata_from_url(
    detail_url: str,
) -> tuple[Optional[str], list[dict], list[dict], dict]:
    html_text = fetch_html(detail_url)
    if not html_text:
        return None, [], [], {
            "has_html": False,
            "has_attachment_label": False,
            "has_attachment_link": False,
            "has_body_container": False,
            "body_has_content": False,
        }
    signals = build_detail_signals(html_text)
    if signals.get("has_attachment_label"):
        LOGGER.info("첨부파일 HTML 감지: %s", detail_url)
    written_at = extract_written_at_from_detail(html_text)
    attachments = extract_attachments_from_detail(html_text)
    body_blocks = extract_body_blocks_from_html(html_text)
    if attachments and body_blocks:
        body_blocks = replace_body_image_urls(body_blocks, attachments)
    return written_at, attachments, body_blocks, signals
