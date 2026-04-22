import hashlib
import json
import mimetypes
import os
import re
from html import unescape
from pathlib import Path
from typing import Optional
from urllib.parse import (
    parse_qs,
    quote,
    urlencode,
    unquote,
    urljoin,
    urlparse,
    urlsplit,
    urlunparse,
    urlunsplit,
)

from log import LOGGER
from settings import (
    ATTACHMENT_EXT_PATTERN,
    ATTACHMENT_HINTS,
    ATTACHMENT_LINK_PATTERN,
    BASE_SITE,
    BASE_URL,
    CONTENT_TYPE_OVERRIDES,
    IMAGE_EXT_PATTERN,
    USER_AGENT,
    get_attachment_allowed_domains,
    has_attachment_query_key,
)

DEFAULT_ANNOTATIONS = {
    "bold": False,
    "italic": False,
    "strikethrough": False,
    "underline": False,
    "code": False,
    "color": "default",
}

CSS_COLOR_MAP = {
    "black": (0, 0, 0),
    "white": (255, 255, 255),
    "red": (255, 0, 0),
    "blue": (0, 0, 255),
    "green": (0, 128, 0),
    "yellow": (255, 255, 0),
    "orange": (255, 165, 0),
    "purple": (128, 0, 128),
    "pink": (255, 192, 203),
    "gray": (128, 128, 128),
    "grey": (128, 128, 128),
    "brown": (165, 42, 42),
}
URL_TEXT_PATTERN = re.compile(
    r"(https?://[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+|"
    r"www\.[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+)"
)
TRAILING_URL_PUNCTUATION = ").,;]"

def clean_text(html_text: str) -> str:
    text = re.sub(r"<[^>]+>", "", html_text)
    text = unescape(text).replace("\u00a0", " ")
    return text.strip()


def normalize_title_key(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def parse_datetime(date_text: str) -> Optional[str]:
    match = re.search(r"(\d{4})[.\-](\d{2})[.\-](\d{2})", date_text)
    if not match:
        return None
    year, month, day = match.groups()
    time_match = re.search(r"(\d{2}):(\d{2})(?::(\d{2}))?", date_text)
    if time_match:
        hour, minute, second = time_match.groups()
        if not second:
            second = "00"
        return f"{year}-{month}-{day}T{hour}:{minute}:{second}+09:00"
    return f"{year}-{month}-{day}T00:00:00+09:00"


def parse_compact_datetime(date_text: Optional[str]) -> Optional[str]:
    if not date_text:
        return None
    digits = re.sub(r"[^0-9]", "", str(date_text))
    if len(digits) >= 14:
        year, month, day = digits[0:4], digits[4:6], digits[6:8]
        hour, minute, second = digits[8:10], digits[10:12], digits[12:14]
        return f"{year}-{month}-{day}T{hour}:{minute}:{second}+09:00"
    if len(digits) >= 8:
        year, month, day = digits[0:4], digits[4:6], digits[6:8]
        return f"{year}-{month}-{day}T00:00:00+09:00"
    return parse_datetime(str(date_text))


def normalize_date_key(date_text: Optional[str]) -> str:
    if not date_text:
        return ""
    match = re.search(r"\d{4}-\d{2}-\d{2}", date_text)
    if match:
        return match.group(0)
    return date_text[:10]


def compute_body_hash(blocks: list[dict], image_mode: str = "") -> str:
    payload_value: object
    if image_mode:
        payload_value = {"image_mode": image_mode, "blocks": blocks}
    else:
        payload_value = blocks
    payload = json.dumps(
        payload_value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def has_image_blocks(blocks: list[dict]) -> bool:
    if not blocks:
        return False
    for block in blocks:
        if block.get("type") == "image":
            return True
    return False


# 본문 해시는 file_upload id 대신 원본 URL을 기준으로 남겨야, 같은 파일을 다시 올려도 해시가 흔들리지 않는다.
def build_uploaded_image_hash_block(
    source_url: str,
    caption: Optional[list[dict]] = None,
) -> dict:
    block = {
        "type": "image",
        "image": {
            "type": "uploaded_external",
            "source_url": source_url,
        },
    }
    if caption:
        block["image"]["caption"] = caption
    return block


# 파일/ PDF 업로드도 upload id가 아니라 source_url로 정규화해야 실제 sync 결과를 안정적으로 비교할 수 있다.
def build_uploaded_file_hash_block(source_url: str, as_pdf: bool) -> dict:
    block_type = "pdf" if as_pdf else "file"
    return {
        "type": block_type,
        block_type: {
            "type": "uploaded_external",
            "source_url": source_url,
        },
    }


def normalize_body_blocks_for_hash(
    blocks: list[dict], upload_files: bool
) -> list[dict]:
    if not blocks:
        return []
    normalized: list[dict] = []
    for block in blocks:
        block_type = block.get("type")
        if block_type == "image":
            image = block.get("image", {})
            url = image.get("external", {}).get("url") or ""
            if (
                upload_files
                and image.get("type") == "external"
                and url
                and is_allowed_external_download_url(url)
            ):
                normalized.append(
                    build_uploaded_image_hash_block(url, image.get("caption"))
                )
            else:
                normalized.append(block)
            continue
        if block_type == "embed":
            embed = block.get("embed", {})
            url = embed.get("url") or ""
            if (
                upload_files
                and is_embed_file_candidate(url)
                and is_allowed_external_download_url(url, require_file_hint=True)
            ):
                filename = derive_filename_from_url(url, fallback="file")
                marker_type = "pdf" if is_pdf_name_or_url(filename, url) else "file"
                normalized.append(
                    build_uploaded_file_hash_block(url, as_pdf=marker_type == "pdf")
                )
            else:
                normalized.append(block)
            continue
        normalized.append(block)
    return normalized


def normalize_detail_url(raw_url: Optional[str]) -> Optional[str]:
    if not raw_url:
        return None
    raw_url = raw_url.strip()
    lowered = raw_url.lower()
    if lowered in {"#", "#/", "javascript:void(0)", "javascript:void(0);"}:
        return None
    if lowered.startswith(("javascript:", "mailto:", "tel:", "data:")):
        return None
    if raw_url.startswith("//"):
        raw_url = "https:" + raw_url
    parsed = urlparse(raw_url)
    if parsed.scheme in {"javascript", "mailto", "tel", "data"}:
        return None
    if not parsed.scheme or not parsed.netloc:
        if raw_url.startswith("/"):
            base = urlparse(BASE_URL)
            parsed = urlparse(f"{base.scheme}://{base.netloc}{raw_url}")
        else:
            return None
    query = parse_qs(parsed.query)
    drop_keys = {"introPkId", "option", "page"}
    query_items: list[tuple[str, str]] = []
    for key in sorted(query):
        if key in drop_keys:
            continue
        for value in query[key]:
            query_items.append((key, value))
    new_query = urlencode(query_items, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", new_query, ""))


def normalize_file_url(raw_url: Optional[str]) -> Optional[str]:
    if not raw_url:
        return None
    raw_url = raw_url.strip()
    lowered = raw_url.lower()
    if lowered in {"#", "#/", "javascript:void(0)", "javascript:void(0);"}:
        return None
    if lowered.startswith(("javascript:", "mailto:", "tel:", "data:")):
        return None
    if raw_url.startswith("//"):
        raw_url = "https:" + raw_url
    absolute = urljoin(BASE_SITE, raw_url)
    parsed = urlsplit(absolute)
    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        return None
    if parsed.scheme in {"javascript", "mailto", "tel", "data"}:
        return None
    encoded = encode_url(absolute)
    encoded_parts = urlsplit(encoded)
    return urlunsplit(
        (encoded_parts.scheme, encoded_parts.netloc, encoded_parts.path, encoded_parts.query, "")
    )


# Attachment policy:
# - ATTACHMENT_ALLOWED_DOMAINS: comma-separated allowed hosts (default: sogang.ac.kr)
def is_allowed_attachment_host(host: str, allowed_domains: tuple[str, ...]) -> bool:
    if not host:
        return False
    host = host.split(":", 1)[0]
    for domain in allowed_domains:
        if host == domain or host.endswith(f".{domain}"):
            return True
    return False


def is_attachment_candidate(
    url: str,
    text: str,
    allow_domain_only: bool = False,
) -> tuple[bool, bool]:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    allowed_domains = get_attachment_allowed_domains()
    lowered_url = url.lower()
    ext_match = bool(
        ATTACHMENT_EXT_PATTERN.search(url) or ATTACHMENT_EXT_PATTERN.search(text)
    )
    hint_match = any(hint in lowered_url for hint in ATTACHMENT_HINTS)
    link_match = bool(ATTACHMENT_LINK_PATTERN.search(url))
    path_match = "/file-fe-prd/board/" in parsed.path
    strong_match = ext_match or hint_match or link_match or path_match
    text_hint = "첨부" in text or "다운로드" in text
    query_hint = has_attachment_query_key(url)
    minimal_signal = strong_match or text_hint or query_hint
    allowed_host = is_allowed_attachment_host(host, allowed_domains)

    if not allowed_host:
        return False, False

    if allow_domain_only:
        if not minimal_signal:
            return False, False
        return True, not strong_match

    if strong_match:
        return True, False
    return False, False


# 실제 다운로드 직전에도 동일한 allowlist를 강제해, 본문 미디어와 첨부파일 정책이 어긋나지 않게 한다.
def is_allowed_external_download_url(
    url: str,
    require_file_hint: bool = False,
) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.netloc or "").lower()
    allowed_domains = get_attachment_allowed_domains()
    if not is_allowed_attachment_host(host, allowed_domains):
        return False
    if not require_file_hint:
        return True
    allowed, _ = is_attachment_candidate(url, url, allow_domain_only=True)
    return allowed


def normalize_attachment_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "")).strip().lower()


def extract_attachment_name(attachment: dict) -> str:
    name = attachment.get("name") or ""
    if name:
        return name
    url = attachment.get("external", {}).get("url") or ""
    if not url:
        return ""
    params = parse_qs(urlparse(url).query)
    name = params.get("sg", [""])[0].strip()
    if name:
        return name
    return Path(urlparse(url).path).name


def strip_dataview_prefix(filename: str) -> str:
    if re.match(r"^\d{10}", filename):
        return filename[10:]
    return filename


def replace_body_image_urls(body_blocks: list[dict], attachments: list[dict]) -> list[dict]:
    if not body_blocks or not attachments:
        return body_blocks
    name_map: dict[str, str] = {}
    for attachment in attachments:
        name = extract_attachment_name(attachment)
        key = normalize_attachment_name(name)
        url = attachment.get("external", {}).get("url") or ""
        if key and url and key not in name_map:
            name_map[key] = url
    if not name_map:
        return body_blocks
    replaced = 0
    for block in body_blocks:
        if block.get("type") != "image":
            continue
        image = block.get("image", {})
        if image.get("type") != "external":
            continue
        url = image.get("external", {}).get("url") or ""
        if not url:
            continue
        parsed = urlparse(url)
        if "/dataview/board/" not in parsed.path:
            continue
        filename = unquote(Path(parsed.path).name)
        if not filename:
            continue
        normalized = normalize_attachment_name(strip_dataview_prefix(filename))
        replacement = name_map.get(normalized)
        if replacement and replacement != url:
            image["external"]["url"] = replacement
            replaced += 1
    if replaced:
        LOGGER.info("본문 이미지 URL 치환: %s개", replaced)
    return body_blocks

def build_site_headers() -> dict:
    return {"User-Agent": USER_AGENT, "Referer": BASE_URL}


def is_image_name_or_url(name: str, url: str) -> bool:
    if IMAGE_EXT_PATTERN.search(name or ""):
        return True
    return bool(IMAGE_EXT_PATTERN.search(url or ""))


def is_pdf_name_or_url(name: str, url: str) -> bool:
    # PDF 판별도 쿼리 문자열 앞까지만 정확히 끊어야 .pdfx 같은 오탐을 막을 수 있다.
    if re.search(r"\.pdf(?:$|\?)", name or "", re.IGNORECASE):
        return True
    return bool(re.search(r"\.pdf(?:$|\?)", url or "", re.IGNORECASE))


def is_embed_file_candidate(url: str) -> bool:
    if not url:
        return False
    if ATTACHMENT_EXT_PATTERN.search(url):
        return True
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    for key in ("filename", "fileName", "file_name", "sg"):
        value = params.get(key)
        if not value:
            continue
        candidate = value[0]
        if candidate and ATTACHMENT_EXT_PATTERN.search(candidate):
            return True
    allowed, _ = is_attachment_candidate(url, url, allow_domain_only=True)
    return allowed
def truncate_utf8(text: str, max_bytes: int) -> str:
    if max_bytes <= 0:
        return ""
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    truncated = encoded[:max_bytes]
    while truncated and (truncated[-1] & 0xC0) == 0x80:
        truncated = truncated[:-1]
    return truncated.decode("utf-8", errors="ignore")


def sanitize_filename(name: str, fallback: str = "file") -> str:
    cleaned = re.sub(r"[\r\n]+", " ", (name or "")).strip()
    if not cleaned:
        return fallback
    cleaned = cleaned.replace("\"", "'")
    max_bytes = 900
    if len(cleaned.encode("utf-8")) <= max_bytes:
        return cleaned
    stem, ext = os.path.splitext(cleaned)
    if ext:
        ext_bytes = len(ext.encode("utf-8"))
        trimmed_stem = truncate_utf8(stem, max_bytes - ext_bytes)
        return f"{trimmed_stem}{ext}" if trimmed_stem else truncate_utf8(cleaned, max_bytes)
    return truncate_utf8(cleaned, max_bytes)


def derive_filename_from_url(url: str, fallback: str = "file") -> str:
    parsed = urlparse(url)
    name = unquote(Path(parsed.path).name)
    params = parse_qs(parsed.query)
    query_name = ""
    for key in ("filename", "fileName", "file_name", "sg", "name"):
        value = params.get(key)
        if value:
            candidate = value[0].strip()
            if candidate:
                query_name = candidate
                break
    if query_name:
        if not name:
            return query_name
        if "." not in name and "." in query_name:
            return query_name
        if name.lower() in {"download", "download3", "file"}:
            return query_name
    if name:
        return name
    return fallback


def guess_content_type_from_filename(filename: str) -> Optional[str]:
    ext = os.path.splitext(filename)[1].lower()
    if ext in CONTENT_TYPE_OVERRIDES:
        return CONTENT_TYPE_OVERRIDES[ext]
    return mimetypes.guess_type(filename)[0]


def normalize_content_type(
    raw_content_type: Optional[str],
    filename: str,
    url: str,
) -> str:
    cleaned = (raw_content_type or "").split(";", 1)[0].strip().lower()
    if cleaned and cleaned not in {"application/octet-stream", "binary/octet-stream"}:
        return cleaned
    guessed = guess_content_type_from_filename(filename)
    if not guessed:
        guessed = mimetypes.guess_type(url)[0]
    if guessed:
        return guessed
    return cleaned or "application/octet-stream"
def normalize_content_url(raw_url: Optional[str]) -> Optional[str]:
    if not raw_url:
        return None
    raw_url = raw_url.strip()
    if raw_url.startswith("//"):
        raw_url = "https:" + raw_url
    parsed = urlparse(raw_url)
    if parsed.scheme in {"javascript", "mailto", "tel", "data"}:
        return None
    if not parsed.scheme:
        raw_url = urljoin(BASE_SITE, raw_url)
    return encode_url(raw_url)


QUERY_SAFE_CHARS = "/?:@-._~!$&'()*+,;=%"


def encode_url(raw_url: str) -> str:
    parsed = urlsplit(raw_url)
    path = quote(parsed.path, safe="/%")
    query = quote(parsed.query, safe=QUERY_SAFE_CHARS)
    fragment = quote(parsed.fragment, safe=QUERY_SAFE_CHARS)
    return urlunsplit((parsed.scheme, parsed.netloc, path, query, fragment))


def is_valid_notion_url(url: Optional[str], allow_mailto: bool = True) -> bool:
    if not url or any(ch.isspace() for ch in url):
        return False
    parsed = urlsplit(url)
    scheme = parsed.scheme.lower()
    if scheme in {"http", "https"}:
        return bool(parsed.netloc)
    if allow_mailto and scheme in {"mailto", "tel"}:
        return bool(parsed.path)
    return False


def resolve_iframe_embed_url(raw_url: Optional[str]) -> Optional[str]:
    normalized = normalize_content_url(raw_url)
    if not normalized:
        return None
    parsed = urlsplit(normalized)
    qs = parse_qs(parsed.query)
    file_values = qs.get("file")
    if file_values:
        candidate = file_values[0]
        if candidate:
            file_url = normalize_content_url(candidate)
            if file_url:
                return file_url
    return normalized


def normalize_link_url(raw_url: Optional[str]) -> Optional[str]:
    if not raw_url:
        return None
    cleaned = raw_url.strip()
    lowered = cleaned.lower()
    if lowered.startswith(("mailto:", "tel:")):
        return cleaned
    return normalize_content_url(cleaned)


def split_text_with_links(text: str) -> list[tuple[str, Optional[str]]]:
    if not text:
        return []
    parts: list[tuple[str, Optional[str]]] = []
    last_index = 0
    for match in URL_TEXT_PATTERN.finditer(text):
        start, end = match.span()
        if start > last_index:
            parts.append((text[last_index:start], None))
        url_text = match.group(0)
        trimmed = url_text.rstrip(TRAILING_URL_PUNCTUATION)
        suffix = url_text[len(trimmed) :]
        if trimmed:
            link = trimmed
            if link.lower().startswith("www."):
                link = "https://" + link
            normalized = normalize_content_url(link)
            link = normalized if normalized else None
            parts.append((trimmed, link))
        if suffix:
            parts.append((suffix, None))
        last_index = end
    if last_index < len(text):
        parts.append((text[last_index:], None))
    return parts


def build_image_block(url: str) -> dict:
    return {
        "object": "block",
        "type": "image",
        "image": {"type": "external", "external": {"url": url}},
    }


def build_embed_block(url: str) -> dict:
    return {
        "object": "block",
        "type": "embed",
        "embed": {"url": url},
    }


def build_file_block(upload_id: str) -> dict:
    return {
        "object": "block",
        "type": "file",
        "file": {"type": "file_upload", "file_upload": {"id": upload_id}},
    }


def build_pdf_block(upload_id: str) -> dict:
    return {
        "object": "block",
        "type": "pdf",
        "pdf": {"type": "file_upload", "file_upload": {"id": upload_id}},
    }


def build_space_rich_text() -> list[dict]:
    return [
        {
            "type": "text",
            "text": {"content": "\u00a0"},
            "annotations": dict(DEFAULT_ANNOTATIONS),
        }
    ]


def build_container_block(rich_text: Optional[list[dict]] = None) -> dict:
    return {
        "object": "block",
        "type": "quote",
        "quote": {
            "rich_text": rich_text or [],
            "color": "default",
        },
    }


def build_table_row_block(cells: list[list[dict]]) -> dict:
    return {
        "object": "block",
        "type": "table_row",
        "table_row": {"cells": cells},
    }


def build_table_block(
    rows: list[list[list[dict]]],
    has_column_header: bool,
    has_row_header: bool,
) -> Optional[dict]:
    if not rows:
        return None
    table_width = max((len(row) for row in rows), default=0)
    if table_width <= 0:
        return None
    normalized_rows: list[dict] = []
    for row in rows:
        if len(row) < table_width:
            row = row + [[] for _ in range(table_width - len(row))]
        normalized_rows.append(build_table_row_block(row))
    return {
        "object": "block",
        "type": "table",
        "table": {
            "table_width": table_width,
            "has_column_header": has_column_header,
            "has_row_header": has_row_header,
            "children": normalized_rows,
        },
    }
def chunks(items: list[dict], size: int) -> list[list[dict]]:
    return [items[i : i + size] for i in range(0, len(items), size)]
def parse_int(value: str) -> Optional[int]:
    digits = re.sub(r"[^0-9]", "", value)
    if not digits:
        return None
    return int(digits)
