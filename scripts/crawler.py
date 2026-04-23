import json
import os
import re
import socket
import time
import urllib.error
import urllib.request
from html import unescape
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse

from common import (
    ATTACHMENTS_STATUS_KNOWN,
    ATTACHMENTS_STATUS_UNKNOWN,
    ensure_item_title,
    extract_detail_id_from_text,
    extract_detail_url_from_row_html,
    extract_list_rows,
    get_browser_launcher,
    is_detail_url,
)
from log import LOGGER
from bbs_parser import (
    detect_body_has_content,
    extract_attachments_from_detail,
    extract_attachments_from_page,
    extract_body_blocks_from_html,
    extract_detail_id_from_row,
    extract_written_at_from_detail,
    extract_written_at_from_page,
    parse_rows,
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

SITE_FETCH_MAX_RETRIES = 3

def run_attachment_policy_selftest() -> None:
    LOGGER.info("첨부파일 정책 셀프테스트 시작")
    keys = ("ATTACHMENT_ALLOWED_DOMAINS",)
    original_env = {key: os.environ.get(key) for key in keys}
    os.environ["ATTACHMENT_ALLOWED_DOMAINS"] = "sogang.ac.kr"
    notion_upload_backup = None
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
        # 본문 image/embed 경로도 allowlist와 업로드 결과가 해시에 반영되는지 함께 확인한다.
        import notion_client as notion_client_module
        from notion_client import (
            prepare_attachments_for_sync,
            prepare_body_blocks_for_sync,
        )
        import sync as sync_module
        from utils import build_pdf_block, compute_body_hash, normalize_body_blocks_for_hash

        notion_upload_backup = notion_client_module.upload_external_file_to_notion

        def fake_upload_success(
            token: str,
            url: str,
            filename: str,
            expect_image: bool = False,
            filename_hint: Optional[str] = None,
        ) -> Optional[str]:
            suffix = "image" if expect_image else "file"
            return f"{suffix}-{filename}"

        notion_client_module.upload_external_file_to_notion = fake_upload_success
        allowed_blocks = [
            {
                "object": "block",
                "type": "image",
                "image": {
                    "type": "external",
                    "external": {
                        "url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg"
                    },
                    "caption": [{"type": "text", "text": {"content": "sample"}}],
                },
            },
            {
                "object": "block",
                "type": "embed",
                "embed": {
                    "url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.pdf?sg=test.pdf"
                },
            },
        ]
        prepared_blocks, prepared_hash_blocks, prepared_media_state = prepare_body_blocks_for_sync(
            "selftest-token", allowed_blocks
        )
        desired_hash = compute_body_hash(
            normalize_body_blocks_for_hash(allowed_blocks, True),
            image_mode="upload-files-v1",
        )
        actual_hash = compute_body_hash(
            prepared_hash_blocks,
            image_mode="upload-files-v1",
        )
        if (
            prepared_blocks[0].get("image", {}).get("type") != "file_upload"
            or prepared_blocks[1].get("type") != "pdf"
            or prepared_media_state
            != [
                {
                    "type": "image",
                    "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                    "upload_id": "image-test.jpg",
                },
                {
                    "type": "pdf",
                    "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.pdf?sg=test.pdf",
                    "upload_id": "file-test.pdf",
                },
            ]
            or desired_hash != actual_hash
        ):
            raise RuntimeError("본문 업로드 셀프테스트 실패(allowlist/hash)")

        def fake_upload_partial(
            token: str,
            url: str,
            filename: str,
            expect_image: bool = False,
            filename_hint: Optional[str] = None,
        ) -> Optional[str]:
            if url.endswith("test.pdf?sg=test.pdf"):
                return None
            suffix = "image" if expect_image else "file"
            return f"{suffix}-{filename}"

        notion_client_module.upload_external_file_to_notion = fake_upload_partial
        partial_blocks, partial_hash_blocks, partial_media_state = prepare_body_blocks_for_sync(
            "selftest-token", allowed_blocks
        )
        partial_hash = compute_body_hash(
            partial_hash_blocks,
            image_mode="upload-files-v1",
        )
        if (
            partial_blocks[0].get("image", {}).get("type") != "file_upload"
            or partial_blocks[1].get("type") != "embed"
            or partial_media_state
            != [
                {
                    "type": "image",
                    "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                    "upload_id": "image-test.jpg",
                }
            ]
            or partial_hash == desired_hash
        ):
            raise RuntimeError("본문 업로드 셀프테스트 실패(부분 실패 재시도)")

        def fake_upload_should_not_run(
            token: str,
            url: str,
            filename: str,
            expect_image: bool = False,
            filename_hint: Optional[str] = None,
        ) -> Optional[str]:
            raise RuntimeError("재사용 가능한 업로드 블록이 있는데 업로드가 다시 호출됨")

        notion_client_module.upload_external_file_to_notion = fake_upload_should_not_run
        reusable_image = {
            "object": "block",
            "type": "image",
            "image": {"type": "file_upload", "file_upload": {"id": "reused-test-image"}},
        }
        reusable_pdf = build_pdf_block("reused-test-pdf")
        reused_blocks, reused_hash_blocks, reused_media_state = prepare_body_blocks_for_sync(
            "selftest-token",
            allowed_blocks,
            reusable_uploaded_media={
                (
                    "image",
                    "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                ): [reusable_image],
                (
                    "pdf",
                    "https://www.sogang.ac.kr/file-fe-prd/board/1/test.pdf?sg=test.pdf",
                ): [reusable_pdf]
            },
        )
        reused_hash = compute_body_hash(
            reused_hash_blocks,
            image_mode="upload-files-v1",
        )
        if (
            reused_blocks[0].get("image", {}).get("file_upload", {}).get("id") != "reused-test-image"
            or reused_blocks[0].get("image", {}).get("caption", [{}])[0].get("text", {}).get("content") != "sample"
            or
            reused_blocks[1].get("type") != "pdf"
            or reused_blocks[1].get("pdf", {}).get("file_upload", {}).get("id") != "reused-test-pdf"
            or reused_media_state
            != [
                {
                    "type": "image",
                    "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                    "upload_id": "reused-test-image",
                },
                {
                    "type": "pdf",
                    "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.pdf?sg=test.pdf",
                    "upload_id": "reused-test-pdf",
                },
            ]
            or reused_hash != desired_hash
        ):
            raise RuntimeError("본문 업로드 셀프테스트 실패(기존 업로드 재사용)")

        captioned_reusable_image = {
            "object": "block",
            "type": "image",
            "image": {
                "type": "file_upload",
                "file_upload": {"id": "caption-test-image"},
                "caption": [{"type": "text", "text": {"content": "old caption"}}],
            },
        }
        (
            caption_removed_blocks,
            caption_removed_hash_blocks,
            caption_removed_media_state,
        ) = prepare_body_blocks_for_sync(
            "selftest-token",
            [
                {
                    "object": "block",
                    "type": "image",
                    "image": {
                        "type": "external",
                        "external": {
                            "url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg"
                        },
                    },
                }
            ],
            reusable_uploaded_media={
                (
                    "image",
                    "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                ): [captioned_reusable_image]
            },
        )
        if (
            "caption" in caption_removed_blocks[0].get("image", {})
            or "caption" in caption_removed_hash_blocks[0].get("image", {})
            or caption_removed_media_state
            != [
                {
                    "type": "image",
                    "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                    "upload_id": "caption-test-image",
                }
            ]
        ):
            raise RuntimeError("본문 업로드 셀프테스트 실패(캡션 제거 반영)")

        reused_attachments, reused_attachment_state = prepare_attachments_for_sync(
            "selftest-token",
            [
                {
                    "name": "sample.jpg",
                    "type": "external",
                    "external": {
                        "url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg"
                    },
                }
            ],
            reusable_uploaded_attachments={
                "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg": [
                    "attachment-upload-1"
                ]
            },
        )
        if (
            reused_attachments
            != [
                {
                    "name": "sample.jpg",
                    "type": "file_upload",
                    "file_upload": {"id": "attachment-upload-1"},
                }
            ]
            or reused_attachment_state
            != [
                {
                    "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                    "name": "sample.jpg",
                    "upload_id": "attachment-upload-1",
                }
            ]
        ):
            raise RuntimeError("첨부 업로드 셀프테스트 실패(기존 업로드 재사용)")

        notion_client_module.upload_external_file_to_notion = fake_upload_success
        blocked_blocks = [
            {
                "object": "block",
                "type": "image",
                "image": {
                    "type": "external",
                    "external": {"url": "https://example.com/test.jpg"},
                },
            },
            {
                "object": "block",
                "type": "embed",
                "embed": {"url": "https://example.com/test.pdf"},
            },
        ]
        blocked_prepared, blocked_hash_blocks, blocked_media_state = prepare_body_blocks_for_sync(
            "selftest-token", blocked_blocks
        )
        if (
            blocked_prepared != blocked_blocks
            or blocked_hash_blocks != blocked_blocks
            or blocked_media_state
        ):
            raise RuntimeError("본문 업로드 셀프테스트 실패(차단 URL 유지)")
        if get_detail_html_fallback_reason(
            {
                "title": "",
                "regDate": "20260422103030",
                "content": "<p>fragment body</p>",
            },
            entry_title="목록 제목",
        ):
            raise RuntimeError("상세 폴백 셀프테스트 실패(fragment body/title fallback)")
        original_list_block_children = sync_module.list_block_children
        try:
            def fake_list_block_children(_token: str, _page_id: str):
                return [
                    {
                        "id": "quote-user",
                        "type": "quote",
                        "quote": {
                            "rich_text": [{"plain_text": "사용자 quote"}]
                        },
                    },
                    {
                        "id": "quote-sync",
                        "type": "quote",
                        "quote": {
                            "rich_text": [{"plain_text": "본문 컨테이너"}]
                        },
                    },
                ]

            sync_module.list_block_children = fake_list_block_children
            if sync_module.find_sync_container_block("selftest-token", "selftest-page"):
                raise RuntimeError("본문 업로드 셀프테스트 실패(overwrite 컨테이너 추정)")
        finally:
            sync_module.list_block_children = original_list_block_children
        original_find_sync_container_block = sync_module.find_sync_container_block
        original_list_block_children = sync_module.list_block_children
        try:
            sync_module.find_sync_container_block = (
                lambda _token, _page_id: {"id": "sync-container"}
            )

            def fake_misaligned_uploaded_children(_token: str, _page_id: str):
                return [
                    {
                        "id": "file-1",
                        "type": "file",
                        "file": {
                            "type": "file",
                            "file": {
                                "url": "https://s3.us-west-2.amazonaws.com/secure.notion-static.com/file-1/test.bin?X-Amz-Signature=abc"
                            },
                        },
                    },
                    {
                        "id": "image-1",
                        "type": "image",
                        "image": {
                            "type": "file",
                            "file": {
                                "url": "https://s3.us-west-2.amazonaws.com/secure.notion-static.com/image-1/test.jpg?X-Amz-Signature=abc"
                            },
                        },
                    },
                    {
                        "id": "pdf-1",
                        "type": "pdf",
                        "pdf": {
                            "type": "file",
                            "file": {
                                "url": "https://s3.us-west-2.amazonaws.com/secure.notion-static.com/pdf-1/test.pdf?X-Amz-Signature=abc"
                            },
                        },
                    },
                ]

            sync_module.list_block_children = fake_misaligned_uploaded_children
            if sync_module.extract_existing_uploaded_media_blocks(
                "selftest-token",
                "selftest-page",
                [
                    {
                        "type": "image",
                        "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                        "upload_id": "image-upload-state",
                        "block_id": "image-1",
                    },
                    {
                        "type": "pdf",
                        "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.pdf?sg=test.pdf",
                        "upload_id": "pdf-upload-state",
                        "block_id": "pdf-1",
                    },
                ],
            ):
                raise RuntimeError("본문 업로드 셀프테스트 실패(재사용 fail-closed)")

            def fake_uploaded_image_child(_token: str, _page_id: str):
                return [
                    {
                        "id": "image-raw",
                        "type": "image",
                        "has_children": False,
                        "archived": False,
                        "created_time": "2026-04-22T00:00:00.000Z",
                        "image": {
                            "type": "file",
                            "file": {
                                "url": "https://s3.us-west-2.amazonaws.com/secure.notion-static.com/image-raw/test.jpg?X-Amz-Signature=abc"
                            },
                            "caption": [
                                {"type": "text", "text": {"content": "caption kept"}}
                            ],
                        },
                    }
                ]

            sync_module.list_block_children = fake_uploaded_image_child
            sanitized_reusable = sync_module.extract_existing_uploaded_media_blocks(
                "selftest-token",
                "selftest-page",
                [
                    {
                        "type": "image",
                        "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                        "upload_id": "image-upload-sanitized",
                        "block_id": "image-raw",
                    }
                ],
            )
            expected_reusable = {
                (
                    "image",
                    "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                ): [
                    {
                        "object": "block",
                        "type": "image",
                        "image": {
                            "type": "file_upload",
                            "file_upload": {"id": "image-upload-sanitized"},
                            "caption": [
                                {"type": "text", "text": {"content": "caption kept"}}
                            ],
                        },
                    }
                ]
            }
            if sanitized_reusable != expected_reusable:
                raise RuntimeError("본문 업로드 셀프테스트 실패(재사용 sanitize)")

            # 상태에 upload_id를 같이 저장하면 같은 타입이 반복되어도 순서가 아니라 식별자로 정확히 재사용할 수 있다.
            def fake_reordered_same_type_children(_token: str, _page_id: str):
                return [
                    {
                        "id": "image-b",
                        "type": "image",
                        "image": {
                            "type": "file",
                            "file": {
                                "url": "https://s3.us-west-2.amazonaws.com/secure.notion-static.com/image-b/B.jpg?X-Amz-Signature=abc"
                            },
                        },
                    },
                    {
                        "id": "image-a",
                        "type": "image",
                        "image": {
                            "type": "file",
                            "file": {
                                "url": "https://s3.us-west-2.amazonaws.com/secure.notion-static.com/image-a/A.jpg?X-Amz-Signature=abc"
                            },
                        },
                    },
                ]

            sync_module.list_block_children = fake_reordered_same_type_children
            reordered_reusable = sync_module.extract_existing_uploaded_media_blocks(
                "selftest-token",
                "selftest-page",
                [
                    {
                        "type": "image",
                        "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/A.jpg?sg=A.jpg",
                        "upload_id": "image-upload-a",
                        "block_id": "image-a",
                    },
                    {
                        "type": "image",
                        "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/B.jpg?sg=B.jpg",
                        "upload_id": "image-upload-b",
                        "block_id": "image-b",
                    },
                ],
            )
            if reordered_reusable != {
                (
                    "image",
                    "https://www.sogang.ac.kr/file-fe-prd/board/1/A.jpg?sg=A.jpg",
                ): [
                    {
                        "object": "block",
                        "type": "image",
                        "image": {
                            "type": "file_upload",
                            "file_upload": {"id": "image-upload-a"},
                        },
                    }
                ],
                (
                    "image",
                    "https://www.sogang.ac.kr/file-fe-prd/board/1/B.jpg?sg=B.jpg",
                ): [
                    {
                        "object": "block",
                        "type": "image",
                        "image": {
                            "type": "file_upload",
                            "file_upload": {"id": "image-upload-b"},
                        },
                    }
                ],
            }:
                raise RuntimeError("본문 업로드 셀프테스트 실패(upload_id 기반 재사용)")

            # 같은 개수/같은 타입 시퀀스라도 저장된 upload_id와 현재 블록이 다르면 수동 편집이 섞인 상태이므로 재사용을 끈다.
            def fake_same_shape_but_stale_ids(_token: str, _page_id: str):
                return [
                    {
                        "id": "image-current",
                        "type": "image",
                        "image": {
                            "type": "file",
                            "file": {
                                "url": "https://s3.us-west-2.amazonaws.com/secure.notion-static.com/image-current/test.jpg?X-Amz-Signature=abc"
                            },
                        },
                    },
                    {
                        "id": "pdf-current",
                        "type": "pdf",
                        "pdf": {
                            "type": "file",
                            "file": {
                                "url": "https://s3.us-west-2.amazonaws.com/secure.notion-static.com/pdf-current/test.pdf?X-Amz-Signature=abc"
                            },
                        },
                    },
                ]

            sync_module.list_block_children = fake_same_shape_but_stale_ids
            if sync_module.extract_existing_uploaded_media_blocks(
                "selftest-token",
                "selftest-page",
                [
                    {
                        "type": "image",
                        "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                        "upload_id": "image-upload-stale",
                        "block_id": "image-stale",
                    },
                    {
                        "type": "pdf",
                        "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.pdf?sg=test.pdf",
                        "upload_id": "pdf-upload-stale",
                        "block_id": "pdf-stale",
                    },
                ],
            ):
                raise RuntimeError("본문 업로드 셀프테스트 실패(수동 편집 stale block_id 차단)")

            # 같은 block_id를 유지한 채 hosted 파일만 바뀐 경우도 수동 편집으로 보고 재사용을 꺼야,
            # 내부 컨테이너를 사람이 수정했을 때 이전 upload_id를 조용히 재사용하는 문제를 막을 수 있다.
            def fake_same_block_id_but_changed_hosted_file(_token: str, _page_id: str):
                return [
                    {
                        "id": "image-current",
                        "type": "image",
                        "image": {
                            "type": "file",
                            "file": {
                                "url": "https://s3.us-west-2.amazonaws.com/secure.notion-static.com/image-current/changed.jpg?X-Amz-Signature=abc"
                            },
                        },
                    }
                ]

            sync_module.list_block_children = fake_same_block_id_but_changed_hosted_file
            if sync_module.extract_existing_uploaded_media_blocks(
                "selftest-token",
                "selftest-page",
                [
                    {
                        "type": "image",
                        "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                        "upload_id": "image-upload-current",
                        "block_id": "image-current",
                        "hosted_file_key": "s3.us-west-2.amazonaws.com/secure.notion-static.com/image-current/original.jpg",
                    }
                ],
            ):
                raise RuntimeError("본문 업로드 셀프테스트 실패(같은 block_id 다른 hosted 파일 차단)")

            # 첨부 재사용도 body와 마찬가지로, 저장된 상태만 믿지 말고 현재 첨부 속성에 실제로 남아 있는 upload_id만 허용해야 한다.
            valid_attachment_reuse = sync_module.extract_existing_uploaded_attachment_ids(
                {
                    "첨부파일": {
                        "files": [
                            {
                                "name": "sample.jpg",
                                "type": "file",
                                "file": {
                                    "url": "https://s3.us-west-2.amazonaws.com/secure.notion-static.com/attachment-upload-1/sample.jpg?X-Amz-Signature=abc"
                                },
                            }
                        ]
                    }
                },
                [
                    {
                        "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                        "name": "sample.jpg",
                        "upload_id": "attachment-upload-1",
                        "hosted_file_key": "s3.us-west-2.amazonaws.com/secure.notion-static.com/attachment-upload-1/sample.jpg",
                    }
                ],
            )
            if valid_attachment_reuse != {
                "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg": [
                    "attachment-upload-1"
                ]
            }:
                raise RuntimeError("첨부 업로드 셀프테스트 실패(현재 첨부 검증)")

            # 현재 첨부 속성에 external과 file_upload가 섞여 있어도,
            # 업로드된 이미지 첨부에 대해서는 부분 재사용이 가능해야 실제 운영 비용 절감 효과가 유지된다.
            mixed_attachment_reuse = sync_module.extract_existing_uploaded_attachment_ids(
                {
                    "첨부파일": {
                        "files": [
                            {
                                "name": "sample.jpg",
                                "type": "file",
                                "file": {
                                    "url": "https://s3.us-west-2.amazonaws.com/secure.notion-static.com/attachment-upload-1/sample.jpg?X-Amz-Signature=abc"
                                },
                            },
                            {
                                "name": "sample.pdf",
                                "type": "external",
                                "external": {
                                    "url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.pdf?sg=test.pdf"
                                },
                            },
                        ]
                    }
                },
                [
                    {
                        "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                        "name": "sample.jpg",
                        "upload_id": "attachment-upload-1",
                        "hosted_file_key": "s3.us-west-2.amazonaws.com/secure.notion-static.com/attachment-upload-1/sample.jpg",
                    }
                ],
            )
            if mixed_attachment_reuse != {
                "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg": [
                    "attachment-upload-1"
                ]
            }:
                raise RuntimeError("첨부 업로드 셀프테스트 실패(mixed 부분 재사용)")

            # 상태에 적힌 upload_id가 현재 첨부 속성에 없으면 stale 상태이므로 재사용을 끈다.
            if sync_module.extract_existing_uploaded_attachment_ids(
                {
                    "첨부파일": {
                        "files": [
                            {
                                "name": "sample.jpg",
                                "type": "file",
                                "file": {
                                    "url": "https://s3.us-west-2.amazonaws.com/secure.notion-static.com/attachment-upload-current/sample.jpg?X-Amz-Signature=abc"
                                },
                            }
                        ]
                    }
                },
                [
                    {
                        "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                        "name": "sample.jpg",
                        "upload_id": "attachment-upload-stale",
                        "hosted_file_key": "s3.us-west-2.amazonaws.com/secure.notion-static.com/attachment-upload-stale/sample.jpg",
                    }
                ],
            ):
                raise RuntimeError("첨부 업로드 셀프테스트 실패(stale 상태 차단)")

            # 실제 런타임에서는 첨부가 없을 때 attachments 키가 생략될 수 있으므로,
            # "첨부 없음"이 확정된 항목만 files=[] clear payload가 생성되는지 확인한다.
            attachment_removed_item = {
                "title": "첨부 제거 테스트",
                "top": False,
                "attachments_status": ATTACHMENTS_STATUS_KNOWN,
            }
            sync_module.normalize_item_attachments(attachment_removed_item)
            if (
                attachment_removed_item.get("attachments") != []
                or sync_module.build_properties(
                    attachment_removed_item,
                    has_views_property=False,
                    has_attachments_property=True,
                    has_classification_property=False,
                ).get("첨부파일")
                != {"files": []}
            ):
                raise RuntimeError("첨부 업로드 셀프테스트 실패(빈 첨부 clear)")

            # 반대로 첨부 확인 실패 상태는 빈 첨부로 오해하면 안 되므로,
            # properties payload에서 첨부파일 속성 자체가 빠져 기존 값을 보존해야 한다.
            attachment_unknown_item = {
                "title": "첨부 미확인 테스트",
                "top": False,
                "attachments_status": ATTACHMENTS_STATUS_UNKNOWN,
            }
            sync_module.normalize_item_attachments(attachment_unknown_item)
            if (
                "attachments" in attachment_unknown_item
                or sync_module.build_properties(
                    attachment_unknown_item,
                    has_views_property=False,
                    has_attachments_property=True,
                    has_classification_property=False,
                ).get("첨부파일")
                is not None
            ):
                raise RuntimeError("첨부 업로드 셀프테스트 실패(미확인 첨부 보존)")

            # 라벨만 있는 빈 첨부 영역은 실제 "첨부 없음"으로 보고, 파일 링크 흔적이 있는데 비면 미확인으로 본다.
            if classify_attachment_status_from_signals(
                [],
                {
                    "has_html": True,
                    "has_attachment_label": True,
                    "has_attachment_link": False,
                },
            ) != ATTACHMENTS_STATUS_KNOWN or classify_attachment_status_from_signals(
                [],
                {
                    "has_html": True,
                    "has_attachment_label": True,
                    "has_attachment_link": True,
                },
            ) != ATTACHMENTS_STATUS_UNKNOWN:
                raise RuntimeError("첨부 업로드 셀프테스트 실패(첨부 상태 분류)")

            # 재사용 후보 조회는 최적화일 뿐이므로, 루트 컨테이너 조회 실패가 항목 전체 실패로 번지지 않고
            # 새 업로드 경로로 자연스럽게 되돌아가는지 확인한다.
            def fake_top_level_failure(_token: str, _page_id: str):
                raise sync_module.NotionRequestError("selftest-root-failure", status_code=500)

            sync_module.find_sync_container_block = original_find_sync_container_block
            sync_module.list_block_children = fake_top_level_failure
            if sync_module.extract_existing_uploaded_media_blocks(
                "selftest-token",
                "selftest-page",
                [
                    {
                        "type": "image",
                        "source_url": "https://www.sogang.ac.kr/file-fe-prd/board/1/test.jpg?sg=test.jpg",
                        "upload_id": "image-upload-sanitized",
                    }
                ],
            ):
                raise RuntimeError("본문 업로드 셀프테스트 실패(컨테이너 조회 best-effort)")
        finally:
            sync_module.find_sync_container_block = original_find_sync_container_block
            sync_module.list_block_children = original_list_block_children
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
        if notion_upload_backup is not None:
            import notion_client as notion_client_module

            notion_client_module.upload_external_file_to_notion = notion_upload_backup
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


def classify_attachment_status_from_signals(
    attachments: list[dict],
    signals: dict,
) -> str:
    if attachments:
        return ATTACHMENTS_STATUS_KNOWN
    if not signals.get("has_html"):
        return ATTACHMENTS_STATUS_UNKNOWN
    if signals.get("has_attachment_link"):
        # 실제 파일 링크 흔적은 보였는데 추출 결과가 비면 "없음"이 아니라 "확인 실패"로 남겨
        # 기존 Notion 첨부파일 속성을 실수로 비우지 않는다. 라벨만 있는 빈 첨부 영역은 clear를 허용한다.
        return ATTACHMENTS_STATUS_UNKNOWN
    return ATTACHMENTS_STATUS_KNOWN


def classify_attachment_status_from_api_detail(
    detail: Optional[dict],
    attachments: list[dict],
    fallback_reason: Optional[str],
    fallback_attachment_status: str,
) -> str:
    if attachments:
        return ATTACHMENTS_STATUS_KNOWN
    if not isinstance(detail, dict) or not detail:
        return fallback_attachment_status

    content_html = str(detail.get("content") or "")
    has_attachment_hint = bool(
        content_html and ATTACHMENT_LINK_PATTERN.search(content_html)
    )
    has_file_value = any(
        str(detail.get(f"fileValue{idx}") or "").strip() for idx in range(1, 6)
    )
    if has_attachment_hint or has_file_value:
        # API가 첨부 흔적을 줬는데 최종 첨부가 비면 일시 실패나 정책 차단일 수 있으므로,
        # HTML 폴백이 명확히 빈 첨부를 확인한 경우에만 clear를 허용한다.
        return (
            ATTACHMENTS_STATUS_KNOWN
            if fallback_attachment_status == ATTACHMENTS_STATUS_KNOWN
            else ATTACHMENTS_STATUS_UNKNOWN
        )
    if fallback_reason and "attachment_missing" in fallback_reason.split(","):
        return fallback_attachment_status
    return ATTACHMENTS_STATUS_KNOWN


def apply_item_attachments(
    item: dict,
    attachments: list[dict],
    attachment_status: str,
) -> None:
    status = (
        ATTACHMENTS_STATUS_KNOWN
        if attachments or attachment_status == ATTACHMENTS_STATUS_KNOWN
        else ATTACHMENTS_STATUS_UNKNOWN
    )
    item["attachments_status"] = status
    if status != ATTACHMENTS_STATUS_KNOWN:
        # 이번 실행에서 첨부 확인이 실패한 항목은 attachments 키 자체를 제거해
        # build_properties가 Notion files=[] clear payload를 만들지 못하게 한다.
        item.pop("attachments", None)
        return

    capped = cap_attachments(attachments, item["title"]) if attachments else []
    item["attachments"] = capped
    if capped:
        log_attachments(item["title"], capped)


def parse_retry_after_seconds(raw_value: Optional[str]) -> Optional[float]:
    if raw_value is None:
        return None
    try:
        seconds = float(str(raw_value).strip())
    except ValueError:
        return None
    return max(0.0, seconds)


def is_retryable_site_status(status_code: int) -> bool:
    return status_code in {429, 500, 502, 503, 504}


def get_site_retry_sleep_seconds(
    attempt: int,
    retry_after: Optional[str] = None,
) -> float:
    header_delay = parse_retry_after_seconds(retry_after) or 0.0
    backoff_delay = min(1.0 * (2**attempt), 8.0)
    return max(header_delay, backoff_delay)


# 사이트 JSON/HTML 요청도 재시도 기준을 맞춰야 외부 네트워크 흔들림이 곧바로 수집 실패가 되지 않는다.
def fetch_site_bytes(url: str, label: str) -> Optional[bytes]:
    for attempt in range(SITE_FETCH_MAX_RETRIES + 1):
        req = urllib.request.Request(url, headers=build_site_headers())
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            if is_retryable_site_status(exc.code) and attempt < SITE_FETCH_MAX_RETRIES:
                sleep_s = get_site_retry_sleep_seconds(
                    attempt,
                    retry_after=exc.headers.get("Retry-After"),
                )
                LOGGER.info(
                    "%s 재시도(%s/%s): %s -> HTTP %s, 대기=%.1fs",
                    label,
                    attempt + 1,
                    SITE_FETCH_MAX_RETRIES,
                    url,
                    exc.code,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue
            LOGGER.info("%s 실패: %s (HTTP %s)", label, url, exc.code)
        except urllib.error.URLError as exc:
            is_timeout = isinstance(exc.reason, socket.timeout)
            if attempt < SITE_FETCH_MAX_RETRIES and is_timeout:
                sleep_s = get_site_retry_sleep_seconds(attempt)
                LOGGER.info(
                    "%s 재시도(%s/%s): %s -> timeout, 대기=%.1fs",
                    label,
                    attempt + 1,
                    SITE_FETCH_MAX_RETRIES,
                    url,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue
            if is_timeout:
                LOGGER.info("%s 실패: %s (timeout)", label, url)
            else:
                LOGGER.info("%s 실패: %s (%s)", label, url, exc.reason)
        except socket.timeout:
            if attempt < SITE_FETCH_MAX_RETRIES:
                sleep_s = get_site_retry_sleep_seconds(attempt)
                LOGGER.info(
                    "%s 재시도(%s/%s): %s -> timeout, 대기=%.1fs",
                    label,
                    attempt + 1,
                    SITE_FETCH_MAX_RETRIES,
                    url,
                    sleep_s,
                )
                time.sleep(sleep_s)
                continue
            LOGGER.info("%s 실패: %s (timeout)", label, url)
    return None


def fetch_site_json(url: str) -> Optional[dict]:
    raw = fetch_site_bytes(url, "API 요청")
    if raw is None:
        return None
    try:
        text = raw.decode("utf-8", errors="replace")
        return json.loads(text)
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


# 상세 API가 200을 돌려줘도 핵심 필드가 비면 HTML 보완 조회를 태워서 조용한 부분 실패를 줄인다.
def get_detail_html_fallback_reason(
    detail: Optional[dict],
    entry_title: str = "",
) -> Optional[str]:
    if detail is None:
        return "api_missing"
    if not isinstance(detail, dict):
        return "api_invalid"
    reasons: list[str] = []
    detail_title = normalize_title_key(str(detail.get("title") or ""))
    list_title = normalize_title_key(entry_title)
    if not detail_title and not list_title:
        reasons.append("title_missing")
    if not parse_compact_datetime(detail.get("regDate")):
        reasons.append("date_missing")
    content_html = str(detail.get("content") or "")
    # wrapper 없는 fragment도 실제 파서로는 읽을 수 있으니, 감지기보다 블록 추출 결과를 기준으로 폴백 여부를 맞춘다.
    body_blocks = extract_body_blocks_from_html(content_html) if content_html else []
    if not body_blocks:
        reasons.append("body_missing")
    attachments = extract_attachments_from_api_data(detail)
    if not attachments and content_html and ATTACHMENT_LINK_PATTERN.search(content_html):
        reasons.append("attachment_missing")
    if not reasons:
        return None
    return ",".join(reasons)


def fetch_detail_metadata_with_html_fallback(
    pk_id: str,
    detail_url: str,
    reason: str,
) -> tuple[Optional[str], list[dict], list[dict], str, str]:
    # 단순 API 미응답과 부분 성공을 로그에서 구분할 수 있게 폴백 사유를 함께 남긴다.
    LOGGER.warning("상세 API 보완 조회: %s (%s) -> HTML 폴백 시도", pk_id, reason)
    written_at, attachments, body_blocks, signals = fetch_detail_metadata_from_url(detail_url)
    attachment_status = classify_attachment_status_from_signals(attachments, signals)
    if written_at or attachments or body_blocks:
        LOGGER.info(
            "상세 HTML 폴백 성공: %s (작성일=%s, 첨부=%s, 본문=%s)",
            pk_id,
            "Y" if written_at else "N",
            len(attachments),
            len(body_blocks),
        )
        status = "html_fallback" if reason == "api_missing" else "html_fallback_partial"
        return written_at, attachments, body_blocks, status, attachment_status
    LOGGER.warning("상세 HTML 폴백 실패: %s (%s)", pk_id, detail_url)
    status = "detail_missing" if reason == "api_missing" else "detail_incomplete"
    return None, [], [], status, ATTACHMENTS_STATUS_UNKNOWN


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
) -> tuple[Optional[str], list[dict], list[dict], str]:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

    written_at = None
    attachments: list[dict] = []
    body_blocks: list[dict] = []
    attachment_status = ATTACHMENTS_STATUS_UNKNOWN
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
        html_text = page.content()
        written_at = extract_written_at_from_page(page)
        if not written_at:
            written_at = extract_written_at_from_detail(html_text)
        attachments = extract_attachments_from_page(page)
        if not attachments:
            attachments = extract_attachments_from_detail(html_text)
        body_blocks = extract_body_blocks_from_html(html_text)
        # Playwright까지 도달한 페이지는 HTML 신호를 다시 분류해
        # 빈 첨부가 확정인지 추출 실패인지 상위 동기화 단계가 구분하게 한다.
        attachment_status = classify_attachment_status_from_signals(
            attachments,
            build_detail_signals(html_text),
        )
        if attachments and body_blocks:
            body_blocks = replace_body_image_urls(body_blocks, attachments)
    except PlaywrightTimeoutError:
        LOGGER.info("상세 페이지 로드 실패: %s", detail_url)
    finally:
        return_to_list_page(page, list_url)
    return written_at, attachments, body_blocks, attachment_status


def fetch_detail_for_row(
    page,
    list_url: str,
    row_index: int,
    detail_url: Optional[str],
    config_fk: Optional[str] = None,
) -> tuple[Optional[str], Optional[str], list[dict], list[dict], str]:
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
        attachment_status = classify_attachment_status_from_signals(attachments, signals)
        if should_retry_detail_fetch(written_at, attachments, body_blocks, signals):
            pw_written_at, pw_attachments, pw_body_blocks, pw_attachment_status = (
                fetch_detail_metadata_via_playwright(page, list_url, detail_url)
            )
            if not written_at and pw_written_at:
                written_at = pw_written_at
            if pw_attachments:
                attachments = pw_attachments
            if pw_attachment_status == ATTACHMENTS_STATUS_KNOWN or pw_attachments:
                attachment_status = pw_attachment_status
            if pw_body_blocks:
                body_blocks = pw_body_blocks
        return written_at, detail_url, attachments, body_blocks, attachment_status

    rows = page.locator(LIST_ROW_SELECTOR)
    if row_index >= rows.count():
        return None, None, [], [], ATTACHMENTS_STATUS_UNKNOWN

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
            attachment_status = classify_attachment_status_from_signals(
                attachments,
                signals,
            )
            if should_retry_detail_fetch(written_at, attachments, body_blocks, signals):
                pw_written_at, pw_attachments, pw_body_blocks, pw_attachment_status = (
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
                if pw_attachment_status == ATTACHMENTS_STATUS_KNOWN or pw_attachments:
                    attachment_status = pw_attachment_status
                if pw_body_blocks:
                    body_blocks = pw_body_blocks
            if written_at or attachments or body_blocks:
                return (
                    written_at,
                    normalized_detail_url,
                    attachments,
                    body_blocks,
                    attachment_status,
                )
    row.click()

    detail_url = wait_for_detail_url(page, list_url)
    if not detail_url:
        LOGGER.info("상세 URL 전환 실패: row %s", row_index)
        return_to_list_page(page, list_url)
        return None, None, [], [], ATTACHMENTS_STATUS_UNKNOWN

    normalized_detail_url = normalize_detail_url(detail_url) or detail_url
    written_at, attachments, body_blocks, signals = fetch_detail_metadata_from_url(
        normalized_detail_url
    )
    attachment_status = classify_attachment_status_from_signals(attachments, signals)
    if not wait_for_written_at(page):
        LOGGER.info("작성일 로드 대기 실패: %s", detail_url)
    html_text = page.content()
    if not written_at:
        written_at = extract_written_at_from_page(page)
        if not written_at:
            written_at = extract_written_at_from_detail(html_text)
    page_attachments = extract_attachments_from_page(page)
    if page_attachments:
        attachments = page_attachments
    elif not attachments:
        attachments = extract_attachments_from_detail(html_text)
    page_blocks = extract_body_blocks_from_html(html_text)
    if page_blocks:
        body_blocks = page_blocks
    attachment_status = classify_attachment_status_from_signals(
        attachments,
        build_detail_signals(html_text),
    )
    if attachments and body_blocks:
        body_blocks = replace_body_image_urls(body_blocks, attachments)
    return_to_list_page(page, list_url)
    return written_at, normalized_detail_url, attachments, body_blocks, attachment_status


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
            detail_fetch_status = "api"
            fallback_written_at: Optional[str] = None
            fallback_attachments: list[dict] = []
            fallback_body_blocks: list[dict] = []
            fallback_attachment_status = ATTACHMENTS_STATUS_UNKNOWN
            fallback_reason = get_detail_html_fallback_reason(
                detail,
                entry_title=str(entry.get("title") or ""),
            )
            if fallback_reason:
                detail = detail if isinstance(detail, dict) else {}
                (
                    fallback_written_at,
                    fallback_attachments,
                    fallback_body_blocks,
                    detail_fetch_status,
                    fallback_attachment_status,
                ) = fetch_detail_metadata_with_html_fallback(
                    pk_id,
                    detail_url,
                    fallback_reason,
                )

            # detail은 HTML fallback 전후로 Optional로 보일 수 있어서, 여기서 dict로 한 번 좁혀
            # 아래 필드 접근을 안전하게 만들고 Pylance의 Optional 경고도 함께 없앤다.
            detail_data = detail if isinstance(detail, dict) else {}

            title = normalize_title_key(detail_data.get("title") or entry.get("title") or "")
            author = detail_data.get("userName") or entry.get("userName") or entry.get("userNickName") or ""
            written_at = parse_compact_datetime(detail_data.get("regDate") or entry.get("regDate"))
            if not written_at and fallback_written_at:
                written_at = fallback_written_at
            views_raw = detail_data.get("viewCount", entry.get("viewCount"))
            views = parse_int(str(views_raw)) if views_raw is not None else None
            top = str(entry.get("isTop", "")).upper() == "Y"
            if not include_non_top and not top:
                continue

            attachments = extract_attachments_from_api_data(detail_data or entry)
            if not attachments and fallback_attachments:
                attachments = fallback_attachments
            attachment_status = classify_attachment_status_from_api_detail(
                detail_data,
                attachments,
                fallback_reason,
                fallback_attachment_status,
            )
            content_html = detail_data.get("content") or ""
            body_blocks = extract_body_blocks_from_html(content_html) if content_html else []
            if not body_blocks and fallback_body_blocks:
                body_blocks = fallback_body_blocks
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
            if detail_fetch_status != "api":
                # 상위 동기화 로그에서 부분 실패 여부를 바로 구분할 수 있도록 항목 문맥에 상태를 남긴다.
                item["detail_fetch_status"] = detail_fetch_status
            if body_blocks:
                item["body_blocks"] = body_blocks
            if classification:
                item["classification"] = classification
            ensure_item_title(item, body_blocks, detail_url)
            apply_item_attachments(item, attachments, attachment_status)

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
                    (
                        written_at,
                        detail_url,
                        attachments,
                        body_blocks,
                        attachment_status,
                    ) = fetch_detail_for_row(
                        page, url, item["row_index"], item.get("detail_url"), config_fk
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
                    apply_item_attachments(item, attachments, attachment_status)
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
            attachment_status = ATTACHMENTS_STATUS_UNKNOWN
            if item.get("url"):
                written_at, attachments, body_blocks, signals = fetch_detail_metadata_from_url(
                    item["url"]
                )
                attachment_status = classify_attachment_status_from_signals(
                    attachments,
                    signals,
                )
                if written_at:
                    item["date"] = written_at
                if body_blocks:
                    item["body_blocks"] = body_blocks
            if classification:
                item["classification"] = classification
            ensure_item_title(item, body_blocks, item.get("url"))
            apply_item_attachments(item, attachments, attachment_status)
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
    raw = fetch_site_bytes(url, "상세 HTML 요청")
    if raw is None:
        return None
    return raw.decode("utf-8", errors="replace")


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
