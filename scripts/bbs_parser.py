import re
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

from common import (
    ensure_item_title,
    extract_detail_id_from_text,
    extract_detail_url_from_row_html,
    extract_list_rows,
    get_browser_launcher,
    is_detail_url,
    normalize_body_blocks,
)

from log import LOGGER
from settings import (
    ATTACHMENT_EXT_PATTERN,
    ATTACHMENT_HINTS,
    DATE_PATTERN,
    DATE_TIME_PATTERN,
    DETAIL_PATH_PATTERN,
    build_detail_url,
)
from utils import (
    CSS_COLOR_MAP,
    DEFAULT_ANNOTATIONS,
    build_embed_block,
    build_image_block,
    build_table_block,
    clean_text,
    is_attachment_candidate,
    is_valid_notion_url,
    normalize_content_url,
    normalize_detail_url,
    normalize_file_url,
    normalize_link_url,
    normalize_title_key,
    parse_datetime,
    parse_int,
    replace_body_image_urls,
    resolve_iframe_embed_url,
    split_text_with_links,
)


def parse_css_color(value: str) -> Optional[tuple[int, int, int]]:
    if not value:
        return None
    raw = value.strip().lower()
    if raw in {"inherit", "transparent", "currentcolor"}:
        return None
    if raw in CSS_COLOR_MAP:
        return CSS_COLOR_MAP[raw]
    if raw.startswith("#"):
        hex_value = raw[1:]
        if len(hex_value) == 3:
            try:
                r = int(hex_value[0] * 2, 16)
                g = int(hex_value[1] * 2, 16)
                b = int(hex_value[2] * 2, 16)
                return r, g, b
            except ValueError:
                return None
        if len(hex_value) == 6:
            try:
                r = int(hex_value[0:2], 16)
                g = int(hex_value[2:4], 16)
                b = int(hex_value[4:6], 16)
                return r, g, b
            except ValueError:
                return None
        return None
    match = re.match(r"rgba?\(([^)]+)\)", raw)
    if match:
        parts = re.split(r"[,\s/]+", match.group(1).strip())
        if len(parts) >= 3:
            rgb: list[int] = []
            for part in parts[:3]:
                if part.endswith("%"):
                    try:
                        rgb.append(int(float(part[:-1]) * 2.55))
                    except ValueError:
                        return None
                else:
                    try:
                        rgb.append(int(float(part)))
                    except ValueError:
                        return None
            # Pylance가 길이 미정 tuple로 보지 않도록, 3채널 RGB를 고정 길이로 반환한다.
            normalized_rgb = [max(0, min(255, val)) for val in rgb[:3]]
            return normalized_rgb[0], normalized_rgb[1], normalized_rgb[2]
    return None


def rgb_to_hsl(r: int, g: int, b: int) -> tuple[float, float, float]:
    rf = r / 255.0
    gf = g / 255.0
    bf = b / 255.0
    max_c = max(rf, gf, bf)
    min_c = min(rf, gf, bf)
    l = (max_c + min_c) / 2.0
    if max_c == min_c:
        return 0.0, 0.0, l
    d = max_c - min_c
    s = d / (2.0 - max_c - min_c) if l > 0.5 else d / (max_c + min_c)
    if max_c == rf:
        h = (gf - bf) / d + (6.0 if gf < bf else 0.0)
    elif max_c == gf:
        h = (bf - rf) / d + 2.0
    else:
        h = (rf - gf) / d + 4.0
    h *= 60.0
    return h, s, l


def notion_color_from_rgb(rgb: tuple[int, int, int]) -> str:
    r, g, b = rgb
    h, s, l = rgb_to_hsl(r, g, b)
    if s < 0.15:
        if l < 0.35:
            return "default"
        return "gray"
    if h < 20 or h >= 345:
        return "red"
    if h < 45:
        return "orange"
    if h < 65:
        return "yellow"
    if h < 150:
        return "green"
    if h < 250:
        return "blue"
    if h < 290:
        return "purple"
    return "pink"


def extract_inline_color(style: str) -> Optional[str]:
    if not style:
        return None
    found = False
    color_value: Optional[str] = None
    for chunk in style.split(";"):
        if ":" not in chunk:
            continue
        prop, value = chunk.split(":", 1)
        if prop.strip().lower() != "color":
            continue
        found = True
        rgb = parse_css_color(value)
        if not rgb:
            color_value = None
            continue
        mapped = notion_color_from_rgb(rgb)
        color_value = mapped if mapped != "default" else None
    if not found:
        return None
    return color_value


def normalize_inline_text(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def build_rich_text_from_segments(segments: list[dict]) -> list[dict]:
    rich_text: list[dict] = []
    has_content = False
    for segment in segments:
        text = segment.get("text", "")
        if not text:
            continue
        is_whitespace_only = text.isspace() and "\u00a0" not in text
        if is_whitespace_only and "\n" not in text:
            continue
        if is_whitespace_only and "\n" in text and not has_content:
            continue
        annotations = segment.get("annotations", DEFAULT_ANNOTATIONS)
        link = segment.get("link")
        if link and not is_valid_notion_url(link, allow_mailto=True):
            link = None
        remaining = text
        while remaining:
            chunk = remaining[:2000]
            remaining = remaining[2000:]
            text_payload = {"content": chunk}
            if link:
                text_payload["link"] = {"url": link}
            rich_text.append(
                {
                    "type": "text",
                    "text": text_payload,
                    "annotations": annotations,
                }
            )
            has_content = True
    return rich_text


def build_paragraph_block_from_rich_text(rich_text: list[dict]) -> Optional[dict]:
    if not rich_text:
        return None
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {"rich_text": rich_text},
    }


def build_bulleted_block_from_rich_text(rich_text: list[dict]) -> Optional[dict]:
    if not rich_text:
        return None
    return {
        "object": "block",
        "type": "bulleted_list_item",
        "bulleted_list_item": {"rich_text": rich_text},
    }


def build_empty_paragraph_block() -> dict:
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": "\u00a0"},
                    "annotations": dict(DEFAULT_ANNOTATIONS),
                }
            ]
        },
    }
class TiptapBlockParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_tiptap = False
        self.tiptap_depth = 0
        self.seen_tiptap = False
        self.in_list_item = False
        self.current_block_type: Optional[str] = None
        self.rich_text: list[dict] = []
        self.bold_depth = 0
        self.italic_depth = 0
        self.underline_depth = 0
        self.strike_depth = 0
        self.code_depth = 0
        self.link_stack: list[Optional[str]] = []
        self.color_stack: list[str] = ["default"]
        self.color_push_stack: list[bool] = []
        self.blocks: list[dict] = []
        self.in_table = False
        self.table_depth = 0
        self.in_table_row = False
        self.in_table_cell = False
        self.table_rows: list[list[list[dict]]] = []
        self.table_cells: list[list[dict]] = []
        self.table_cell_segments: list[dict] = []
        self.table_cell_is_header = False
        self.table_row_index = -1
        self.table_cell_index = 0
        self.table_has_column_header = False
        self.table_has_row_header = False
        self.void_tags = {
            "area",
            "base",
            "br",
            "col",
            "embed",
            "hr",
            "iframe",
            "img",
            "input",
            "link",
            "meta",
            "param",
            "source",
            "track",
            "wbr",
        }

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        attrs_dict = {key: value or "" for key, value in attrs}
        if not self.in_tiptap and tag == "div":
            classes = attrs_dict.get("class", "")
            if "tiptap" in classes.split():
                self.seen_tiptap = True
                self.in_tiptap = True
                self.tiptap_depth = 1
                return
        if not self.in_tiptap:
            return
        if tag not in self.void_tags:
            self.tiptap_depth += 1
            color = extract_inline_color(attrs_dict.get("style", ""))
            if color:
                self.color_stack.append(color)
                self.color_push_stack.append(True)
            else:
                self.color_push_stack.append(False)
        if tag == "table":
            if not self.in_table:
                self.flush_block()
                self.in_table = True
                self.table_depth = 1
                self.table_rows = []
                self.table_cells = []
                self.table_cell_segments = []
                self.table_cell_is_header = False
                self.table_row_index = -1
                self.table_cell_index = 0
                self.table_has_column_header = False
                self.table_has_row_header = False
            else:
                self.table_depth += 1
            return
        if self.in_table:
            if tag == "tr":
                self.in_table_row = True
                self.table_row_index += 1
                self.table_cell_index = 0
                self.table_cells = []
                return
            if tag in {"td", "th"}:
                self.in_table_cell = True
                self.table_cell_segments = []
                self.table_cell_is_header = tag == "th"
                return
            if tag == "p":
                if self.in_table_cell and self.table_cell_segments:
                    self.append_line_break()
                return
            if tag == "li":
                if self.in_table_cell and self.table_cell_segments:
                    self.append_line_break()
                return
            if tag in {"img", "iframe"}:
                return
        if tag == "li":
            if not self.in_list_item:
                self.flush_block()
                self.in_list_item = True
                self.current_block_type = "li"
        elif tag == "p":
            if not self.in_list_item and self.current_block_type != "p":
                self.flush_block()
                self.current_block_type = "p"
        elif tag in {"strong", "b"}:
            self.bold_depth += 1
        elif tag in {"em", "i"}:
            self.italic_depth += 1
        elif tag == "u":
            self.underline_depth += 1
        elif tag in {"s", "del", "strike"}:
            self.strike_depth += 1
        elif tag == "code":
            self.code_depth += 1
        elif tag == "a":
            href = attrs_dict.get("href") or ""
            link = normalize_link_url(href)
            if link and not is_valid_notion_url(link, allow_mailto=True):
                link = None
            self.link_stack.append(link)
        elif tag == "iframe":
            if self.in_table:
                return
            src = attrs_dict.get("src") or ""
            url = resolve_iframe_embed_url(src)
            if url and is_valid_notion_url(url, allow_mailto=False):
                self.flush_block()
                self.blocks.append(build_embed_block(url))
        elif tag == "img":
            src = attrs_dict.get("src") or ""
            url = normalize_content_url(src)
            if url:
                self.flush_block()
                self.blocks.append(build_image_block(url))
        elif tag == "br":
            self.append_line_break()

    def handle_endtag(self, tag: str) -> None:
        if not self.in_tiptap:
            return
        if tag not in self.void_tags and self.color_push_stack:
            pushed = self.color_push_stack.pop()
            if pushed and len(self.color_stack) > 1:
                self.color_stack.pop()
        if self.in_table:
            if tag in {"td", "th"}:
                self.flush_table_cell()
            elif tag == "tr":
                self.flush_table_row()
            elif tag == "table":
                self.table_depth = max(0, self.table_depth - 1)
                if self.table_depth == 0:
                    self.flush_table()
        if tag == "li" and self.in_list_item:
            self.flush_block()
            self.in_list_item = False
            self.current_block_type = None
        elif (
            tag == "p"
            and not self.in_list_item
            and self.current_block_type == "p"
            and not self.in_table
        ):
            if self.rich_text:
                self.flush_block()
            else:
                self.blocks.append(build_empty_paragraph_block())
            self.current_block_type = None
        elif tag in {"strong", "b"}:
            self.bold_depth = max(0, self.bold_depth - 1)
        elif tag in {"em", "i"}:
            self.italic_depth = max(0, self.italic_depth - 1)
        elif tag == "u":
            self.underline_depth = max(0, self.underline_depth - 1)
        elif tag in {"s", "del", "strike"}:
            self.strike_depth = max(0, self.strike_depth - 1)
        elif tag == "code":
            self.code_depth = max(0, self.code_depth - 1)
        elif tag == "a":
            if self.link_stack:
                self.link_stack.pop()
        if tag not in self.void_tags:
            self.tiptap_depth -= 1
            if self.tiptap_depth <= 0:
                self.flush_block()
                self.in_tiptap = False
                self.tiptap_depth = 0
                self.in_list_item = False
                self.current_block_type = None
                self.bold_depth = 0
                self.italic_depth = 0
                self.underline_depth = 0
                self.strike_depth = 0
                self.code_depth = 0
                self.link_stack.clear()
                self.color_stack = ["default"]
                self.color_push_stack = []
                self.in_table = False
                self.table_depth = 0
                self.in_table_row = False
                self.in_table_cell = False
                self.table_rows = []
                self.table_cells = []
                self.table_cell_segments = []
                self.table_cell_is_header = False
                self.table_row_index = -1
                self.table_cell_index = 0
                self.table_has_column_header = False
                self.table_has_row_header = False

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if not self.in_tiptap:
            return
        if tag == "br":
            self.append_line_break()
        elif tag == "iframe":
            if self.in_table:
                return
            attrs_dict = {key: value or "" for key, value in attrs}
            src = attrs_dict.get("src") or ""
            url = resolve_iframe_embed_url(src)
            if url and is_valid_notion_url(url, allow_mailto=False):
                self.flush_block()
                self.blocks.append(build_embed_block(url))
        elif tag == "img":
            if self.in_table:
                return
            attrs_dict = {key: value or "" for key, value in attrs}
            src = attrs_dict.get("src") or ""
            url = normalize_content_url(src)
            if url:
                self.flush_block()
                self.blocks.append(build_image_block(url))

    def handle_data(self, data: str) -> None:
        if not self.in_tiptap:
            return
        self.append_text(data)

    def append_text(self, data: str) -> None:
        if self.in_table and not self.in_table_cell:
            return
        text = normalize_inline_text(data)
        if not text:
            return
        annotations = dict(DEFAULT_ANNOTATIONS)
        annotations["bold"] = self.bold_depth > 0
        annotations["italic"] = self.italic_depth > 0
        annotations["underline"] = self.underline_depth > 0
        annotations["strikethrough"] = self.strike_depth > 0
        annotations["code"] = self.code_depth > 0
        annotations["color"] = self.color_stack[-1] if self.color_stack else "default"
        link = self.link_stack[-1] if self.link_stack else None
        if link:
            self.append_segment(text, annotations, link)
            return
        for segment_text, segment_link in split_text_with_links(text):
            self.append_segment(segment_text, annotations, segment_link)

    def append_segment(self, text: str, annotations: dict, link: Optional[str]) -> None:
        if not text:
            return
        segments = self.table_cell_segments if self.in_table_cell else self.rich_text
        if text.isspace() and "\u00a0" not in text:
            if not segments:
                return
            if not segments[-1]["text"].endswith((" ", "\n")):
                segments[-1]["text"] += " "
            return
        if segments:
            last = segments[-1]
            if last.get("annotations") == annotations and last.get("link") == link:
                last["text"] += text
                return
        segments.append({"text": text, "annotations": annotations, "link": link})

    def append_line_break(self) -> None:
        if self.in_table and not self.in_table_cell:
            return
        if (
            self.current_block_type is None
            and not self.in_list_item
            and not self.in_table
        ):
            self.current_block_type = "p"
        annotations = dict(DEFAULT_ANNOTATIONS)
        annotations["bold"] = self.bold_depth > 0
        annotations["italic"] = self.italic_depth > 0
        annotations["underline"] = self.underline_depth > 0
        annotations["strikethrough"] = self.strike_depth > 0
        annotations["code"] = self.code_depth > 0
        annotations["color"] = self.color_stack[-1] if self.color_stack else "default"
        link = self.link_stack[-1] if self.link_stack else None
        segments = self.table_cell_segments if self.in_table_cell else self.rich_text
        if segments:
            last = segments[-1]
            if last.get("annotations") == annotations and last.get("link") == link:
                last["text"] += "\n"
                return
        segments.append({"text": "\n", "annotations": annotations, "link": link})

    def flush_block(self) -> None:
        if not self.rich_text:
            return
        rich_text = build_rich_text_from_segments(self.rich_text)
        self.rich_text = []
        if self.in_list_item or self.current_block_type == "li":
            block = build_bulleted_block_from_rich_text(rich_text)
        else:
            block = build_paragraph_block_from_rich_text(rich_text)
        if block:
            self.blocks.append(block)

    def flush_table_cell(self) -> None:
        if not self.in_table_cell:
            return
        rich_text = build_rich_text_from_segments(self.table_cell_segments)
        self.table_cells.append(rich_text)
        if self.table_cell_is_header:
            if self.table_row_index == 0:
                self.table_has_column_header = True
            if self.table_cell_index == 0:
                self.table_has_row_header = True
        self.table_cell_segments = []
        self.table_cell_is_header = False
        self.in_table_cell = False
        self.table_cell_index += 1

    def flush_table_row(self) -> None:
        if not self.in_table_row:
            return
        if self.in_table_cell:
            self.flush_table_cell()
        if self.table_cells:
            self.table_rows.append(self.table_cells)
        self.table_cells = []
        self.in_table_row = False

    def flush_table(self) -> None:
        if self.in_table_cell:
            self.flush_table_cell()
        if self.in_table_row:
            self.flush_table_row()
        table_block = build_table_block(
            self.table_rows, self.table_has_column_header, self.table_has_row_header
        )
        if table_block:
            self.blocks.append(table_block)
        self.in_table = False
        self.table_depth = 0
        self.in_table_row = False
        self.in_table_cell = False
        self.table_rows = []
        self.table_cells = []
        self.table_cell_segments = []
        self.table_cell_is_header = False
        self.table_row_index = -1
        self.table_cell_index = 0
        self.table_has_column_header = False
        self.table_has_row_header = False


def extract_body_blocks_from_html(html_text: str) -> list[dict]:
    if not html_text:
        return []
    parser = TiptapBlockParser()
    parser.feed(html_text)
    parser.close()
    if parser.blocks:
        return normalize_body_blocks(parser.blocks)
    lowered = html_text.lower()
    looks_like_fragment = "<html" not in lowered and "<body" not in lowered
    if not parser.seen_tiptap and looks_like_fragment:
        wrapped = f'<div class="tiptap">{html_text}</div>'
        fallback = TiptapBlockParser()
        fallback.feed(wrapped)
        fallback.close()
        return normalize_body_blocks(fallback.blocks)
    return normalize_body_blocks(parser.blocks)


def chunks(items: list[dict], size: int) -> list[list[dict]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


class BodyContentDetector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_container = False
        self.depth = 0
        self.has_content = False
        self.void_tags = {
            "area",
            "base",
            "br",
            "col",
            "embed",
            "hr",
            "iframe",
            "img",
            "input",
            "link",
            "meta",
            "param",
            "source",
            "track",
            "wbr",
        }

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        attrs_dict = {key: value or "" for key, value in attrs}
        if not self.in_container and tag == "div":
            classes = attrs_dict.get("class", "")
            if "tiptap" in classes.split() or "custom-css-tag-a" in classes.split():
                self.in_container = True
                self.depth = 1
                return
        if not self.in_container:
            return
        if tag in {"img", "a", "iframe"}:
            self.has_content = True
        if tag not in self.void_tags:
            self.depth += 1

    def handle_endtag(self, tag: str) -> None:
        if not self.in_container:
            return
        if tag not in self.void_tags:
            self.depth -= 1
        if self.depth <= 0:
            self.in_container = False
            self.depth = 0

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if not self.in_container:
            return
        if tag in {"img", "iframe"}:
            self.has_content = True

    def handle_data(self, data: str) -> None:
        if not self.in_container:
            return
        text = unescape(data).replace("\u00a0", " ").strip()
        if text:
            self.has_content = True


def detect_body_has_content(html_text: str) -> bool:
    detector = BodyContentDetector()
    detector.feed(html_text)
    detector.close()
    return detector.has_content

class TableRowParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_tr = False
        self.in_td = False
        self.current_cells: list[str] = []
        self.current_parts: list[str] = []
        self.current_meta: list[str] = []
        self.rows: list[dict] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        attrs_dict = {key: value or "" for key, value in attrs}
        if tag == "tr":
            self.in_tr = True
            self.current_cells = []
            self.current_meta = []
            onclick = attrs_dict.get("onclick")
            if onclick:
                self.current_meta.append(onclick)
            for key, value in attrs:
                if key.startswith("data-") and value:
                    self.current_meta.append(f"{key}={value}")
        if not self.in_tr:
            return
        onclick = attrs_dict.get("onclick")
        if onclick:
            self.current_meta.append(onclick)
        if tag == "td":
            self.in_td = True
            self.current_parts = []
        if tag == "a":
            href = attrs_dict.get("href") or ""
            if href:
                self.current_meta.append(href)

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self.in_td:
            text = "".join(self.current_parts)
            text = unescape(text).replace("\u00a0", " ")
            self.current_cells.append(text.strip())
            self.in_td = False
            self.current_parts = []
        if tag == "tr" and self.in_tr:
            if self.current_cells:
                self.rows.append({"cells": self.current_cells, "meta": self.current_meta})
            self.in_tr = False
            self.current_cells = []
            self.current_meta = []

    def handle_data(self, data: str) -> None:
        if self.in_tr and self.in_td:
            self.current_parts.append(data)


def parse_rows(html_text: str, config_fk: Optional[str] = None) -> list[dict]:
    parser = TableRowParser()
    parser.feed(html_text)
    parser.close()
    items: list[dict] = []

    for row in parser.rows:
        cells = row.get("cells", [])
        if len(cells) < 5:
            continue
        num_or_top = cells[0]
        title = cells[1]
        author = cells[2]
        date_text = cells[-2]
        views_text = cells[-1]

        date_iso = parse_datetime(date_text)
        views = parse_int(views_text)
        if not date_iso or views is None:
            continue

        top = num_or_top.strip().upper() == "TOP"
        detail_url = None
        for meta in row.get("meta", []):
            candidate = normalize_detail_url(meta)
            if candidate and is_detail_url(candidate):
                detail_url = candidate
                break
            detail_id = extract_detail_id_from_text(meta)
            if detail_id:
                detail_url = normalize_detail_url(build_detail_url(detail_id, config_fk))
                break

        items.append(
            {
                "title": title,
                "author": author,
                "date": date_iso,
                "views": views,
                "top": top,
                "url": detail_url,
            }
        )

    return items


def extract_written_at_from_detail(html_text: str) -> Optional[str]:
    matches = re.findall(
        r"(작성일|등록일).*?(\d{4}[.\-]\d{2}[.\-]\d{2}(?:\s+\d{2}:\d{2}(?::\d{2})?)?)",
        html_text,
        re.DOTALL,
    )
    if not matches:
        return None
    for _, value in matches:
        if DATE_TIME_PATTERN.search(value):
            return parse_datetime(value)
    return parse_datetime(matches[0][1])


def extract_attachments_from_detail(html_text: str) -> list[dict]:
    attachments: list[dict] = []
    seen_urls: set[str] = set()
    allowlist_only_urls: list[str] = []

    def add_attachment(href: str, text: str, allow_domain_only: bool) -> None:
        url = normalize_file_url(href)
        if not url or url in seen_urls:
            return
        allowed, allowlist_only = is_attachment_candidate(
            url, text, allow_domain_only=allow_domain_only
        )
        if not allowed:
            return
        seen_urls.add(url)
        if allowlist_only:
            allowlist_only_urls.append(url)
        if text:
            name = text
        else:
            params = parse_qs(urlparse(url).query)
            name = params.get("sg", [""])[0]
        if not name:
            name = Path(urlparse(url).path).name or "첨부파일"
        attachments.append({"name": name, "type": "external", "external": {"url": url}})

    def extract_from_chunk(chunk: str, strict: bool) -> None:
        for match in re.finditer(
            r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
            chunk,
            re.IGNORECASE | re.DOTALL,
        ):
            href = unescape(match.group(1)).strip()
            if not href:
                continue
            text = clean_text(match.group(2))
            if not strict:
                snippet = chunk[max(0, match.start() - 200) : match.end() + 200]
                lowered_href = href.lower()
                if (
                    "첨부" not in text
                    and "첨부" not in snippet
                    and "다운로드" not in text
                    and "다운로드" not in snippet
                    and not ATTACHMENT_EXT_PATTERN.search(href)
                    and not ATTACHMENT_EXT_PATTERN.search(text)
                    and not any(hint in lowered_href for hint in ATTACHMENT_HINTS)
                ):
                    continue
            add_attachment(href, text, allow_domain_only=strict)

    label_matches = list(re.finditer(r"첨부파일", html_text))
    has_label = bool(label_matches)
    if has_label:
        for match in label_matches:
            start = max(0, match.start() - 800)
            end = min(len(html_text), match.end() + 6000)
            extract_from_chunk(html_text[start:end], strict=True)
    if not attachments:
        extract_from_chunk(html_text, strict=has_label)
    if allowlist_only_urls:
        sample = ", ".join(allowlist_only_urls[:3])
        LOGGER.info(
            "allow_domain_only 첨부: %s개 (샘플=%s)",
            len(allowlist_only_urls),
            sample or "없음",
        )

    return attachments


def extract_attachments_from_page(page) -> list[dict]:
    result = page.evaluate(
        """
        () => {
            const results = [];
            const seen = new Set();
            let labelCount = 0;
            let labelLinkCount = 0;
            let labelCandidateCount = 0;
            const labelCandidateSamples = [];
            const extPattern = /\\.(pdf|hwp|hwpx|docx?|xlsx?|pptx?|zip|rar|7z|txt|csv|jpg|jpeg|png|gif|bmp)(?:$|\\?)/i;
            const hintPattern = /(file-fe-prd\\/board|filedown|filedownload|bbsfile|download|attach)/i;
            const queryKeyPattern = /(sg=|fileid=|file_id=|fileno=|file_no=|fileseq=|file_seq=|attachid=|attach_id=|attachno=|attach_no=)/i;
            const textHintPattern = /(첨부|다운로드)/;
            const isCandidate = (href, text) => (
                extPattern.test(href) ||
                hintPattern.test(href) ||
                queryKeyPattern.test(href) ||
                textHintPattern.test(text || "")
            );
            const collectLabelNodes = (root) => Array.from(root.querySelectorAll("*"))
                .filter(el => el.textContent && el.textContent.includes("첨부파일"));
            const containers = Array.from(document.querySelectorAll(".tiptap, .custom-css-tag-a"));
            let labels = [];
            if (containers.length) {
                const labelSet = new Set();
                containers.forEach(container => {
                    collectLabelNodes(container).forEach(label => labelSet.add(label));
                });
                labels = Array.from(labelSet);
            }
            if (!labels.length) {
                labels = collectLabelNodes(document.body);
            }
            labelCount = labels.length;
            const collectLinks = (root, trackCandidates) => {
                const links = root.querySelectorAll("a[href]");
                links.forEach(a => {
                    const href = a.getAttribute("href") || "";
                    const text = (a.textContent || "").trim();
                    if (!href) return;
                    const key = href + "|" + text;
                    if (seen.has(key)) return;
                    seen.add(key);
                    results.push({href, text});
                    if (trackCandidates && isCandidate(href, text)) {
                        labelCandidateCount += 1;
                        if (labelCandidateSamples.length < 3) {
                            labelCandidateSamples.push(href);
                        }
                    }
                });
                return links.length;
            };
            for (const label of labels) {
                let node = label;
                for (let i = 0; i < 6 && node; i += 1) {
                    const count = collectLinks(node, true);
                    if (count) {
                        labelLinkCount += count;
                        break;
                    }
                    node = node.parentElement;
                }
            }
            if (!results.length) {
                const links = document.querySelectorAll("a[href]");
                links.forEach(a => {
                    const href = a.getAttribute("href") || "";
                    const text = (a.textContent || "").trim();
                    if (!href) return;
                    const key = href + "|" + text;
                    if (seen.has(key)) return;
                    seen.add(key);
                    results.push({href, text});
                });
            }
            return {
                links: results,
                labelCount,
                labelLinkCount,
                labelCandidateCount,
                labelCandidateSamples,
            };
        }
        """
    )
    candidates = result.get("links", []) if isinstance(result, dict) else []
    label_count = result.get("labelCount", 0) if isinstance(result, dict) else 0
    label_link_count = result.get("labelLinkCount", 0) if isinstance(result, dict) else 0
    label_candidate_count = (
        result.get("labelCandidateCount", 0) if isinstance(result, dict) else 0
    )
    allow_domain_only = label_count > 0
    def build_attachments(candidate_list: list[dict]) -> tuple[list[dict], list[str]]:
        attachments: list[dict] = []
        seen_urls: set[str] = set()
        allowlist_only_urls: list[str] = []
        for candidate in candidate_list:
            href = candidate.get("href", "")
            text = candidate.get("text", "")
            url = normalize_file_url(href)
            if not url or url in seen_urls:
                continue
            allowed, allowlist_only = is_attachment_candidate(
                url, text, allow_domain_only=allow_domain_only
            )
            if not allowed:
                continue
            seen_urls.add(url)
            if allowlist_only:
                allowlist_only_urls.append(url)
            name = text
            if not name:
                params = parse_qs(urlparse(url).query)
                name = params.get("sg", [""])[0]
            if not name:
                name = Path(urlparse(url).path).name or "첨부파일"
            attachments.append(
                {"name": name, "type": "external", "external": {"url": url}}
            )
        return attachments, allowlist_only_urls

    attachments, allowlist_only_urls = build_attachments(candidates)
    if allow_domain_only and label_link_count > 0 and not attachments:
        all_candidates = page.evaluate(
            """
            () => {
                const results = [];
                const seen = new Set();
                const links = document.querySelectorAll("a[href]");
                links.forEach(a => {
                    const href = a.getAttribute("href") || "";
                    const text = (a.textContent || "").trim();
                    if (!href) return;
                    const key = href + "|" + text;
                    if (seen.has(key)) return;
                    seen.add(key);
                    results.push({href, text});
                });
                return results;
            }
            """
        )
        if isinstance(all_candidates, list) and all_candidates:
            LOGGER.info("첨부파일 폴백: 라벨 있음, 전체 링크 재스캔")
            candidates = all_candidates
            attachments, allowlist_only_urls = build_attachments(candidates)
    if allowlist_only_urls:
        sample = ", ".join(allowlist_only_urls[:3])
        LOGGER.info(
            "allow_domain_only 첨부: %s개 (샘플=%s)",
            len(allowlist_only_urls),
            sample or "없음",
        )
    if not attachments:
        if not candidates:
            LOGGER.info("첨부파일 후보 링크 없음 (라벨=%s)", label_count)
        else:
            sample = ", ".join(
                f"{c.get('href','')}" for c in candidates[:3] if c.get("href")
            )
            LOGGER.info(
                "첨부파일 필터링 결과 0개 (라벨=%s, 라벨링크=%s, 후보=%s, 샘플=%s)",
                label_count,
                label_link_count,
                len(candidates),
                sample or "없음",
            )
    return attachments
def extract_detail_id_from_row(row) -> Optional[str]:
    for key in ("data-id", "data-no", "data-board-id", "data-article-id", "data-detail-id"):
        value = row.get_attribute(key)
        if value and value.isdigit():
            return value
    onclick = row.get_attribute("onclick") or ""
    detail_id = extract_detail_id_from_text(onclick)
    if detail_id:
        return detail_id
    try:
        dataset = row.evaluate("row => ({...row.dataset})")
        for value in dataset.values():
            if isinstance(value, str) and value.isdigit():
                return value
    except Exception:
        dataset = {}
    try:
        row_html = row.evaluate("row => row.outerHTML")
    except Exception:
        return None
    detail_id = extract_detail_id_from_text(row_html or "")
    if detail_id:
        return detail_id
    return None


def extract_written_at_from_page(page) -> Optional[str]:
    for label_text in ("작성일", "등록일"):
        locator = page.locator(f"text={label_text}")
        for idx in range(locator.count()):
            label_node = locator.nth(idx)
            try:
                container_text = label_node.locator("xpath=..").inner_text()
            except Exception:
                container_text = ""
            match = DATE_TIME_PATTERN.search(container_text)
            if match:
                return parse_datetime(match.group(0))
            try:
                sibling_texts = label_node.locator(
                    "xpath=following-sibling::*"
                ).all_inner_texts()
            except Exception:
                sibling_texts = []
            for text in sibling_texts:
                match = DATE_TIME_PATTERN.search(text)
                if match:
                    return parse_datetime(match.group(0))
    body_text = page.locator("body").inner_text()
    match = re.search(
        rf"(작성일|등록일).*?({DATE_TIME_PATTERN.pattern})",
        body_text,
    )
    if match:
        return parse_datetime(match.group(2))
    match = DATE_TIME_PATTERN.search(body_text)
    if match:
        return parse_datetime(match.group(0))
    match = DATE_PATTERN.search(body_text)
    if match:
        return parse_datetime(match.group(0))
    return None

def is_detail_path_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    path = parsed.path or ""
    return bool(DETAIL_PATH_PATTERN.search(path))
