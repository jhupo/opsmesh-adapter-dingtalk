"""OSS streaming download (.part + Range resume), media/upload, and DingPan."""

from collections.abc import AsyncIterator
import mimetypes
import os
from pathlib import Path
import re
import tempfile
from typing import IO
from urllib.parse import urljoin, urlsplit
import uuid

import anyio
from opsmesh.drivers import Request, Timeout

from dingtalk.event.models import DownloadedMedia
from dingtalk.exception import DingTalkNetworkError, MediaError, MediaLimitExceeded
from dingtalk.typing import MediaType, T_JsonDict
from dingtalk.utils import sanitize_filename

from .client import ApiClient

_OSS_HTTP_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*\.oss-[a-z0-9-]+\.aliyuncs\.com$")
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_MAX_REDIRECTS = 3
_MAX_RANGE_RETRIES = 2

MEDIA_UPLOAD_LIMITS: dict[str, int] = {
    MediaType.IMAGE: 20 * 1024 * 1024,
    MediaType.VIDEO: 20 * 1024 * 1024,
    MediaType.FILE: 20 * 1024 * 1024,
    MediaType.VOICE: 2 * 1024 * 1024,
}


def upgrade_oss_https(url: str) -> str:
    """Losslessly upgrade native http OSS links to https without rewriting queries."""
    parsed = urlsplit(url)
    if parsed.scheme == "http" and _OSS_HTTP_PATTERN.fullmatch(parsed.netloc.lower()):
        return "https" + url[4:]
    return url


def validate_download_origin(url: str, allowed_domains: tuple[str, ...]) -> None:
    """Validate every hop against HTTPS and vendor domain allowlist (SSRF guard)."""
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise MediaError(f"Malformed media download URL: {url!r}") from exc

    host = (parsed.hostname or "").lower()
    if (
        parsed.scheme != "https"
        or parsed.username
        or parsed.password
        or port not in (None, 443)
        or not any(host == d or host.endswith(f".{d}") for d in allowed_domains)
    ):
        raise MediaError(f"Media download origin is not allowed: {host!r}")


def _resolve_filename(
    filename: str | None,
    content_type: str | None,
    fallback_ext: str | None = None,
) -> str:
    clean = sanitize_filename(filename, fallback="attachment")
    suffix = Path(clean).suffix.lower()
    if suffix and suffix != ".file":
        return clean

    guessed: str | None = None
    if fallback_ext and fallback_ext.lstrip(".").lower() != "file":
        guessed = f".{fallback_ext.lstrip('.')}"
    elif content_type:
        mime = content_type.split(";", 1)[0].strip().lower()
        guessed = mimetypes.guess_extension(mime)

    stem = Path(clean).stem if suffix == ".file" else clean
    return f"{stem}{guessed or '.bin'}"


class MediaService:
    """Handle GB-scale streaming downloads, media/upload, and DingPan large uploads."""

    def __init__(self, client: ApiClient) -> None:
        self._client = client

    async def exchange_download_url(
        self,
        *,
        access_token: str,
        robot_code: str,
        download_code: str,
    ) -> str:
        """Exchange a short-lived DingTalk downloadCode for a signed OSS URL."""
        body = await self._client.request_openapi(
            "POST",
            "/v1.0/robot/messageFiles/download",
            access_token=access_token,
            json_body={
                "downloadCode": download_code,
                "robotCode": robot_code,
            },
        )
        raw_url = body.get("downloadUrl")
        if not isinstance(raw_url, str) or not raw_url:
            raise MediaError("DingTalk messageFiles/download returned no downloadUrl.")
        return upgrade_oss_https(raw_url)

    async def iter_url_chunks(
        self,
        download_url: str,
        *,
        chunk_size: int | None = None,
        max_bytes: int | None = None,
        start_offset: int = 0,
    ) -> AsyncIterator[tuple[bytes, str | None, int]]:
        """Stream bytes from a verified vendor URL with bounded redirect hops."""
        cfg = self._client.config
        effective_chunk = chunk_size or cfg.dingtalk_download_chunk_size
        effective_limit = (
            max_bytes if max_bytes is not None else cfg.dingtalk_max_download_bytes
        )
        current_url = upgrade_oss_https(download_url)
        timeout = Timeout(
            total=None,
            connect=10.0,
            read=cfg.dingtalk_download_read_timeout,
            close=10.0,
        )

        for hop in range(_MAX_REDIRECTS + 1):
            validate_download_origin(current_url, cfg.dingtalk_allowed_download_domains)
            headers: dict[str, str] = {}
            if start_offset > 0:
                headers["Range"] = f"bytes={start_offset}-"

            req = Request(
                "GET",
                current_url,
                headers=headers or None,
                timeout=timeout,
                auto_redirects=False,
            )

            redirected: str | None = None
            total_received = start_offset

            async for resp in self._client.adapter.stream_request(
                req, chunk_size=effective_chunk
            ):
                if resp.status_code in _REDIRECT_STATUSES:
                    location = resp.headers.get("location")
                    if not location or hop == _MAX_REDIRECTS:
                        raise MediaError(
                            "Media download redirect limit exceeded or missing Location"
                        )
                    redirected = upgrade_oss_https(urljoin(current_url, location))
                    break

                if resp.status_code not in (200, 206):
                    raise MediaError(
                        f"Media download failed with HTTP {resp.status_code}"
                    )

                if resp.status_code == 200 and start_offset > 0:
                    total_received = 0

                raw_chunk = resp.content or b""
                chunk = (
                    raw_chunk.encode("utf-8")
                    if isinstance(raw_chunk, str)
                    else raw_chunk
                )
                if not chunk:
                    continue

                total_received += len(chunk)
                if effective_limit is not None and total_received > effective_limit:
                    raise MediaLimitExceeded(
                        f"Downloaded media exceeds limit of {effective_limit} bytes.",
                        limit_bytes=effective_limit,
                        actual_bytes=total_received,
                    )
                yield chunk, resp.headers.get("content-type"), resp.status_code

            if redirected is not None:
                current_url = redirected
                continue
            return

        raise MediaError("Media download redirect loop terminated unexpectedly.")

    async def download_to_disk(
        self,
        *,
        access_token: str,
        robot_code: str,
        download_code: str,
        dest_dir: str | Path | None = None,
        filename: str | None = None,
        fallback_ext: str | None = None,
        max_bytes: int | None = None,
    ) -> DownloadedMedia:
        """Stream a DingTalk attachment directly to disk with Range resume."""
        download_url = await self.exchange_download_url(
            access_token=access_token,
            robot_code=robot_code,
            download_code=download_code,
        )

        target_dir = (
            Path(dest_dir) if dest_dir is not None else Path(tempfile.gettempdir())
        )
        target_dir.mkdir(parents=True, exist_ok=True)
        part_path = target_dir / f".dingtalk_{uuid.uuid4().hex}.part"

        written = 0
        content_type: str | None = None
        retries = 0

        try:
            while True:
                mode = "r+b" if written > 0 else "wb"
                try:
                    async with await anyio.open_file(part_path, mode) as fp:
                        if written > 0:
                            await fp.seek(written)
                        reset_done = False
                        async for chunk, ctype, status in self.iter_url_chunks(
                            download_url,
                            max_bytes=max_bytes,
                            start_offset=written,
                        ):
                            if ctype and not content_type:
                                content_type = ctype
                            if status == 200 and written > 0 and not reset_done:
                                await fp.seek(0)
                                await fp.truncate(0)
                                written = 0
                                reset_done = True
                            await fp.write(chunk)
                            written += len(chunk)
                    break
                except MediaError:
                    raise
                except Exception as exc:
                    if written > 0 and retries < _MAX_RANGE_RETRIES:
                        retries += 1
                        continue
                    raise DingTalkNetworkError(
                        f"Streaming media download interrupted: {exc}"
                    ) from exc

            final_name = _resolve_filename(filename, content_type, fallback_ext)
            final_path = target_dir / final_name
            if final_path.exists():
                stem = final_path.stem
                suffix = final_path.suffix
                final_path = target_dir / f"{stem}_{uuid.uuid4().hex[:8]}{suffix}"

            os.replace(part_path, final_path)
            return DownloadedMedia(
                path=final_path,
                name=final_path.name,
                size=written,
                mime=content_type.split(";", 1)[0].strip() if content_type else None,
            )
        except BaseException:
            if part_path.exists():
                part_path.unlink(missing_ok=True)
            raise

    async def read_bytes(
        self,
        *,
        access_token: str,
        robot_code: str,
        download_code: str,
        max_bytes: int = 32 * 1024 * 1024,
    ) -> bytes:
        """Read a small attachment into memory with a strict byte ceiling."""
        download_url = await self.exchange_download_url(
            access_token=access_token,
            robot_code=robot_code,
            download_code=download_code,
        )
        buf = bytearray()
        async for chunk, _, _ in self.iter_url_chunks(
            download_url, max_bytes=max_bytes
        ):
            buf.extend(chunk)
        return bytes(buf)

    async def upload_media(
        self,
        *,
        access_token: str,
        file: str | Path | bytes | IO[bytes],
        media_type: str | MediaType = MediaType.FILE,
        filename: str | None = None,
    ) -> str:
        """Upload media to oapi /media/upload with local 20MB/2MB pre-checks."""
        kind = MediaType(media_type)
        limit = MEDIA_UPLOAD_LIMITS[kind]

        if isinstance(file, str | Path):
            async_path = anyio.Path(file)
            if not await async_path.is_file():
                raise MediaError(f"Media file does not exist: {file}")
            size = (await async_path.stat()).st_size
            if size > limit:
                raise MediaLimitExceeded(
                    f"{kind.value} exceeds DingTalk {limit // (1024 * 1024)}MB "
                    f"media/upload limit ({size} bytes).",
                    limit_bytes=limit,
                    actual_bytes=size,
                )
            resolved_name = sanitize_filename(filename or async_path.name)
            payload = await async_path.read_bytes()
        elif isinstance(file, bytes):
            size = len(file)
            if size > limit:
                raise MediaLimitExceeded(
                    f"{kind.value} exceeds DingTalk {limit // (1024 * 1024)}MB "
                    f"media/upload limit ({size} bytes).",
                    limit_bytes=limit,
                    actual_bytes=size,
                )
            resolved_name = sanitize_filename(filename, fallback=f"upload.{kind.value}")
            payload = file
        else:
            payload = await anyio.to_thread.run_sync(file.read)
            size = len(payload)
            if size > limit:
                raise MediaLimitExceeded(
                    f"{kind.value} exceeds DingTalk {limit // (1024 * 1024)}MB "
                    f"media/upload limit ({size} bytes).",
                    limit_bytes=limit,
                    actual_bytes=size,
                )
            resolved_name = sanitize_filename(filename, fallback=f"upload.{kind.value}")

        mime = mimetypes.guess_type(resolved_name)[0] or "application/octet-stream"
        body = await self._client.request_oapi(
            "POST",
            "/media/upload",
            access_token=access_token,
            data={"type": kind.value},
            files={"media": (resolved_name, payload, mime)},
        )
        media_id = body.get("media_id")
        if not isinstance(media_id, str) or not media_id:
            raise MediaError("DingTalk /media/upload response missing 'media_id'.")
        return media_id

    async def _query_group_space_id(
        self, *, access_token: str, union_id: str, open_conversation_id: str
    ) -> str:
        resp = await self._client.request_openapi(
            "POST",
            "/v1.0/convFile/conversations/spaces/query",
            access_token=access_token,
            params={"unionId": union_id},
            json_body={"openConversationId": open_conversation_id},
        )
        space = resp.get("space")
        space_id = space.get("spaceId") if isinstance(space, dict) else None
        if not isinstance(space_id, str) or not space_id:
            raise MediaError("Failed to query DingPan group spaceId.")
        return space_id

    async def _query_dingpan_upload_info(
        self,
        *,
        access_token: str,
        union_id: str,
        space_id: str,
        filename: str,
        size: int,
    ) -> tuple[str, str, dict[str, str]]:
        info = await self._client.request_openapi(
            "POST",
            f"/v2.0/storage/spaces/{space_id}/files/0/uploadInfos/query",
            access_token=access_token,
            params={"unionId": union_id},
            json_body={
                "protocol": "HEADER_SIGNATURE",
                "option": {"preCheckParam": {"name": filename, "size": size}},
            },
        )
        upload_key = info.get("uploadKey")
        sig_info = info.get("headerSignatureInfo")
        if not isinstance(upload_key, str) or not isinstance(sig_info, dict):
            raise MediaError("Invalid DingPan uploadInfos response.")
        resource_urls = sig_info.get("resourceUrls")
        if not isinstance(resource_urls, list) or not resource_urls:
            raise MediaError("DingPan uploadInfos missing resourceUrls.")
        oss_url = upgrade_oss_https(str(resource_urls[0]))
        validate_download_origin(
            oss_url, self._client.config.dingtalk_allowed_download_domains
        )
        oss_headers = {
            str(k): str(v) for k, v in (sig_info.get("headers") or {}).items()
        }
        return upload_key, oss_url, oss_headers

    async def _put_oss_bytes(
        self, *, oss_url: str, headers: dict[str, str], raw_bytes: bytes
    ) -> None:
        put_req = Request(
            "PUT",
            oss_url,
            headers=headers,
            content=raw_bytes,
            timeout=Timeout(
                total=None,
                connect=10.0,
                read=self._client.config.dingtalk_download_read_timeout,
                close=10.0,
            ),
        )
        put_resp = await self._client.adapter.request(put_req)
        if not (200 <= put_resp.status_code < 300):
            raise MediaError(
                f"DingPan OSS PUT upload failed (HTTP {put_resp.status_code})"
            )

    async def _commit_dingpan_file(
        self,
        *,
        access_token: str,
        union_id: str,
        space_id: str,
        upload_key: str,
        filename: str,
    ) -> str:
        resp = await self._client.request_openapi(
            "POST",
            f"/v2.0/storage/spaces/{space_id}/files/0/commit",
            access_token=access_token,
            params={"unionId": union_id},
            json_body={"uploadKey": upload_key, "name": filename, "parentId": "0"},
        )
        dentry = resp.get("dentry")
        dentry_id = dentry.get("id") if isinstance(dentry, dict) else None
        if not isinstance(dentry_id, str) or not dentry_id:
            raise MediaError("DingPan commit response missing dentry.id.")
        return dentry_id

    async def send_dingpan_file(
        self,
        *,
        access_token: str,
        open_conversation_id: str,
        file_path: str | Path,
        union_id: str,
        filename: str | None = None,
    ) -> T_JsonDict:
        """Upload a >20MB file to a group DingPan space via OSS PUT and deliver it."""
        async_path = anyio.Path(file_path)
        if not await async_path.is_file():
            raise MediaError(f"Large file does not exist: {file_path}")
        size = (await async_path.stat()).st_size
        resolved_name = sanitize_filename(filename or async_path.name)

        space_id = await self._query_group_space_id(
            access_token=access_token,
            union_id=union_id,
            open_conversation_id=open_conversation_id,
        )
        upload_key, oss_url, oss_headers = await self._query_dingpan_upload_info(
            access_token=access_token,
            union_id=union_id,
            space_id=space_id,
            filename=resolved_name,
            size=size,
        )
        await self._put_oss_bytes(
            oss_url=oss_url,
            headers=oss_headers,
            raw_bytes=await async_path.read_bytes(),
        )
        dentry_id = await self._commit_dingpan_file(
            access_token=access_token,
            union_id=union_id,
            space_id=space_id,
            upload_key=upload_key,
            filename=resolved_name,
        )
        return await self._client.request_openapi(
            "POST",
            "/v1.0/convFile/conversations/files/send",
            access_token=access_token,
            params={"unionId": union_id},
            json_body={
                "spaceId": space_id,
                "dentryId": dentry_id,
                "openConversationId": open_conversation_id,
            },
        )


__all__ = [
    "MEDIA_UPLOAD_LIMITS",
    "MediaService",
    "upgrade_oss_https",
    "validate_download_origin",
]
