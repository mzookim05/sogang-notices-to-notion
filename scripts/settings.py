import os
import re
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

DEFAULT_NOTION_API_VERSION = "2022-06-28"
BASE_URL = "https://www.sogang.ac.kr/ko/scholarship-notice"
ACADEMIC_BASE_URL = "https://www.sogang.ac.kr/ko/academic-support/notices"
DEFAULT_QUERY = {"introPkId": "All", "option": "TITLE"}
# 외부 요청 로그와 저장소 이름이 어긋나지 않도록 프로젝트 전용 사용자 에이전트 이름을 맞춘다.
USER_AGENT = "Mozilla/5.0 (compatible; SogangNoticesCrawler/1.0)"
PAGE_ICON_EMOJI = "📢"
TITLE_PROPERTY = "공지사항"
AUTHOR_PROPERTY = "작성자"
DATE_PROPERTY = "작성일"
TOP_PROPERTY = "TOP"
URL_PROPERTY = "URL"
VIEWS_PROPERTY = "조회수"
ATTACHMENT_PROPERTY = "첨부파일"
TYPE_PROPERTY = "유형"
CLASSIFICATION_PROPERTY = "분류"
BODY_HASH_PROPERTY = "본문 해시"
BODY_HASH_IMAGE_MODE_UPLOAD = "upload-files-v1"
SYNC_CONTAINER_MARKER = "[SYNC_CONTAINER]"
BASE_SITE = "https://www.sogang.ac.kr"
BBS_API_BASE = f"{BASE_SITE}/api/api/v1/mainKo/BbsData"
BBS_LIST_API_URL = f"{BBS_API_BASE}/boardListMultiConfigId"
DATE_PATTERN = re.compile(
    r"\d{4}[.\-]\d{2}[.\-]\d{2}(?:\s+\d{2}:\d{2}(?::\d{2})?)?"
)
DATE_TIME_PATTERN = re.compile(r"\d{4}[.\-]\d{2}[.\-]\d{2}\s+\d{2}:\d{2}(?::\d{2})?")
DATE_TIME_JS_PATTERN = r"\d{4}[.\-]\d{2}[.\-]\d{2}\s+\d{2}:\d{2}(?::\d{2})?"
DETAIL_PATH_PATTERN = re.compile(r"/detail/\d+")
DETAIL_ID_CAPTURE_PATTERN = re.compile(r"/detail/(\d+)")
DETAIL_ID_FUNCTION_PATTERN = re.compile(
    r"(?:view|detail|article)\s*\(\s*'?(\d{5,})'?",
    re.IGNORECASE,
)
DETAIL_ID_PARAM_PATTERN = re.compile(
    r"(?:detailId|detail_id|articleId|article_id|boardNo|board_no|contentId|content_id)\D{0,5}(\d{5,})",
    re.IGNORECASE,
)
DETAIL_ID_DATA_ATTR_PATTERN = re.compile(
    r"data-(?:id|no|board-id|board-no|article-id|article-no|detail-id|detail-no)=['\"](\d{5,})['\"]",
    re.IGNORECASE,
)
LIST_ROW_SELECTOR = "tr[data-v-6debbb14], table tbody tr"
ATTACHMENT_EXT_PATTERN = re.compile(
    # Python raw regex에서는 \?만 literal 물음표가 되므로, 쿼리 문자열 구분도 정확히 잡도록 수정한다.
    r"\.(pdf|hwp|hwpx|docx?|xlsx?|pptx?|zip|rar|7z|txt|csv|jpg|jpeg|png|gif|bmp)(?:$|\?)",
    re.IGNORECASE,
)
IMAGE_EXT_PATTERN = re.compile(
    # 이미지 URL도 .jpgx 같은 오탐을 막기 위해 동일한 종료 조건을 사용한다.
    r"\.(jpg|jpeg|png|gif|bmp|webp|svg)(?:$|\?)",
    re.IGNORECASE,
)
CONTENT_TYPE_OVERRIDES = {
    ".pdf": "application/pdf",
    ".doc": "application/msword",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xls": "application/vnd.ms-excel",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".ppt": "application/vnd.ms-powerpoint",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ".hwp": "application/vnd.hancom.hwp",
    ".hwpx": "application/vnd.hancom.hwpx",
    ".zip": "application/zip",
    ".rar": "application/vnd.rar",
    ".7z": "application/x-7z-compressed",
    ".csv": "text/csv",
    ".txt": "text/plain",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".bmp": "image/bmp",
    ".svg": "image/svg+xml",
}
ATTACHMENT_HINTS = (
    "download",
    "filedown",
    "filedownload",
    "fileid",
    "fileno",
    "bbsfile",
    "attach",
    "file-fe-prd/board",
    "sg=",
)
ATTACHMENT_LINK_PATTERN = re.compile(
    r"(file-fe-prd/board|filedown|filedownload|bbsfile|download)",
    re.IGNORECASE,
)
ATTACHMENT_QUERY_KEYS = {
    "sg",
    "fileid",
    "file_id",
    "fileno",
    "file_no",
    "fileseq",
    "file_seq",
    "attachid",
    "attach_id",
    "attachno",
    "attach_no",
}
BODY_CONTAINER_PATTERN = re.compile(r"\b(tiptap|custom-css-tag-a)\b", re.IGNORECASE)
TYPE_TAGS = (
    "교내/국가",
    "교외",
    "국가근로",
    "학자금대출",
    "대청교",
    "발전기금",
    "동문회",
    "주거지원",
)
FALLBACK_TYPE = "공통"
DEFAULT_CONFIG_CLASSIFICATIONS = {"141": "장학공지", "2": "학사공지"}
DEFAULT_CONFIG_LIST_URLS = {"141": BASE_URL, "2": ACADEMIC_BASE_URL}
DEFAULT_BBS_CONFIG_FKS = ["141", "2"]
def load_dotenv(path: str = ".env") -> None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    continue
                if len(value) >= 2 and value[0] == value[-1] and value[0] in {"\"", "'"}:
                    value = value[1:-1]
                os.environ.setdefault(key, value)
    except FileNotFoundError:
        return


def get_notion_api_version() -> str:
    return os.environ.get("NOTION_API_VERSION", DEFAULT_NOTION_API_VERSION)
def get_attachment_allowed_domains() -> tuple[str, ...]:
    raw = os.environ.get("ATTACHMENT_ALLOWED_DOMAINS", "sogang.ac.kr")
    domains = [part.strip().lower() for part in raw.split(",") if part.strip()]
    return tuple(domains)


def get_attachment_max_count() -> int:
    raw = os.environ.get("ATTACHMENT_MAX_COUNT", "15").strip()
    try:
        value = int(raw)
    except ValueError:
        return 15
    return max(1, value)


def has_attachment_query_key(url: str) -> bool:
    params = parse_qs(urlparse(url).query)
    for key in params.keys():
        if key.lower() in ATTACHMENT_QUERY_KEYS:
            return True
    return False

def should_run_attachment_selftest() -> bool:
    raw = os.environ.get("ATTACHMENT_SELFTEST", "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


# Notion file upload:
# - NOTION_UPLOAD_FILES: enable uploading image files to Notion (default: true)
def should_upload_files_to_notion() -> bool:
    raw = os.environ.get("NOTION_UPLOAD_FILES", "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


# Crawl policy:
# - INCLUDE_NON_TOP: include non-top posts when true (default: true)
# - NON_TOP_MAX_PAGES: max pages to scan when including non-top (default: 2, 0=unlimited)
def should_include_non_top() -> bool:
    raw = os.environ.get("INCLUDE_NON_TOP", "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def get_non_top_max_pages() -> int:
    raw = os.environ.get("NON_TOP_MAX_PAGES", "2").strip()
    try:
        value = int(raw)
    except ValueError:
        return 2
    return max(0, value)

def parse_config_map(raw: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    if not raw:
        return mapping
    for chunk in re.split(r"[;,]+", raw):
        chunk = chunk.strip()
        if not chunk:
            continue
        if ":" in chunk:
            key, value = chunk.split(":", 1)
        elif "=" in chunk:
            key, value = chunk.split("=", 1)
        else:
            continue
        key = key.strip()
        value = value.strip()
        if key and value:
            mapping[key] = value
    return mapping


def get_bbs_config_fk() -> str:
    raw = os.environ.get("BBS_CONFIG_FK", "").strip()
    if raw:
        return raw
    raw_list = os.environ.get("BBS_CONFIG_FKS", "").strip()
    if raw_list:
        parts = re.split(r"[,\s]+", raw_list)
        for part in parts:
            if part:
                return part
    return DEFAULT_BBS_CONFIG_FKS[0]


def get_bbs_config_fks() -> list[str]:
    raw = os.environ.get("BBS_CONFIG_FKS", "").strip()
    if raw:
        parts = re.split(r"[,\s]+", raw)
        return [part for part in parts if part]
    single = os.environ.get("BBS_CONFIG_FK", "").strip()
    if single:
        return [single]
    return list(DEFAULT_BBS_CONFIG_FKS)


def get_config_classification_map() -> dict[str, str]:
    mapping = dict(DEFAULT_CONFIG_CLASSIFICATIONS)
    raw = os.environ.get("BBS_CONFIG_CLASSIFY", "").strip()
    if raw:
        mapping.update(parse_config_map(raw))
    return mapping


def get_classification_for_config(config_fk: str) -> Optional[str]:
    key = str(config_fk or "").strip()
    if not key:
        return None
    return get_config_classification_map().get(key)


def get_config_list_url_map() -> dict[str, str]:
    mapping = dict(DEFAULT_CONFIG_LIST_URLS)
    raw = os.environ.get("BBS_CONFIG_LIST_URLS", "").strip()
    if raw:
        mapping.update(parse_config_map(raw))
    return mapping


def get_list_base_url(config_fk: str) -> str:
    key = str(config_fk or "").strip()
    return get_config_list_url_map().get(key, BASE_URL)


def build_detail_url(detail_id: str, config_fk: Optional[str] = None) -> str:
    config_fk = (config_fk or get_bbs_config_fk()).strip()
    return f"{BASE_SITE}/ko/detail/{detail_id}?bbsConfigFk={config_fk}"


def get_sync_mode() -> str:
    raw = os.environ.get("SYNC_MODE", "overwrite").strip().lower()
    if raw in {"overwrite", "preserve"}:
        return raw
    return "overwrite"


def should_dedupe_on_start() -> bool:
    # 전체 DB 스캔은 요청량과 실행 시간을 크게 늘릴 수 있어 기본값은 보수적으로 끈다.
    raw = os.environ.get("NOTION_DEDUPE_ON_START", "0").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def should_allow_title_only_match() -> bool:
    # 제목만 같은 공지가 반복되는 게시판에서는 오탐 업데이트가 날 수 있어 기본값은 끈다.
    raw = os.environ.get("NOTION_ALLOW_TITLE_ONLY_MATCH", "0").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def resolve_html_path() -> Optional[Path]:
    if len(sys.argv) > 1:
        return Path(sys.argv[1])
    env_path = os.environ.get("HTML_PATH")
    if env_path:
        return Path(env_path)
    return None
