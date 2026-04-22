import importlib.util
import logging
import os
import sys

from settings import (
    get_bbs_config_fks,
    get_config_classification_map,
    get_notion_api_version,
    get_sync_mode,
    should_upload_files_to_notion,
)

# 저장소와 로그 이름을 맞춰 운영 로그 검색 시 프로젝트 식별이 바로 되게 한다.
LOGGER = logging.getLogger("sogang-notices-crawler")
def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

def log_environment_info() -> None:
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    playwright_installed = importlib.util.find_spec("playwright") is not None
    browser = os.environ.get("BROWSER", "chromium")
    headless_raw = os.environ.get("HEADLESS", "1").strip().lower()
    headless = headless_raw not in {"0", "false", "no", "off"}
    sync_mode = get_sync_mode()
    upload_files = should_upload_files_to_notion()
    LOGGER.info(
        "환경: Python=%s, Playwright=%s",
        python_version,
        "설치됨" if playwright_installed else "미설치",
    )
    config_fks = get_bbs_config_fks()
    config_label = ",".join(config_fks) if config_fks else "없음"
    class_map = get_config_classification_map()
    class_label = ", ".join(
        f"{key}:{value}" for key, value in class_map.items() if key in config_fks
    )
    LOGGER.info(
        "환경: BROWSER=%s, HEADLESS=%s, BBS_CONFIG_FKS=%s, SYNC_MODE=%s",
        browser,
        "1" if headless else "0",
        config_label,
        sync_mode,
    )
    if class_label:
        LOGGER.info("환경: BBS_CONFIG_CLASSIFY=%s", class_label)
    LOGGER.info(
        "환경: NOTION_VERSION=%s, NOTION_UPLOAD_FILES=%s",
        get_notion_api_version(),
        "1" if upload_files else "0",
    )
