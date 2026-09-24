from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import struct
import time
import zlib

VERSION = "007"


def _u16(value: int) -> bytes:
    return struct.pack("<H", int(value))


def _u32(value: int) -> bytes:
    return struct.pack("<I", int(value))


def _s16(value: int) -> bytes:
    return struct.pack("<h", int(value))


def _str(value: bytes | str) -> bytes:
    if isinstance(value, str):
        value = value.encode("utf-8")
    return _u16(len(value)) + value


class _Service:
    def __init__(self, service_type: int) -> None:
        self.service_type = service_type
        self.privileges: dict[int, int] = {}

    def add_privilege(self, privilege: int, expire_ts: int) -> None:
        self.privileges[int(privilege)] = int(expire_ts)

    def pack(self) -> bytes:
        body = _u16(self.service_type)
        body += _u16(len(self.privileges))
        for key in sorted(self.privileges):
            body += _u16(key) + _u32(self.privileges[key])
        return body


class _RtcService(_Service):
    JOIN_CHANNEL = 1
    PUBLISH_AUDIO = 2
    PUBLISH_VIDEO = 3
    PUBLISH_DATA = 4

    def __init__(self, channel_name: str, uid: int) -> None:
        super().__init__(1)
        self.channel_name = channel_name.encode("utf-8")
        self.uid = str(uid).encode("utf-8")

    def pack(self) -> bytes:
        return (
            super().pack()
            + _str(self.channel_name)
            + _str(self.uid)
        )


def build_rtc_token_with_uid(
    app_id: str,
    app_certificate: str,
    channel_name: str,
    uid: int,
    token_expire_seconds: int = 3600,
    privilege_expire_seconds: int | None = None,
) -> str:
    app_id = app_id.strip()
    app_certificate = app_certificate.strip()
    if len(app_id) != 32 or len(app_certificate) != 32:
        raise ValueError("Agora App ID and App Certificate must each be 32 characters.")
    try:
        bytes.fromhex(app_id)
        bytes.fromhex(app_certificate)
    except ValueError as exc:
        raise ValueError("Agora App ID and App Certificate must be hexadecimal strings.") from exc

    if not channel_name or len(channel_name.encode("utf-8")) >= 64:
        raise ValueError("Invalid Agora channel name.")
    if not (1 <= int(uid) <= 0xFFFFFFFF):
        raise ValueError("Agora UID must be between 1 and 4294967295.")

    token_expire_seconds = max(60, min(int(token_expire_seconds), 86400))
    privilege_expire_seconds = (
        token_expire_seconds
        if privilege_expire_seconds is None
        else max(60, min(int(privilege_expire_seconds), token_expire_seconds))
    )

    issue_ts = int(time.time())
    salt = secrets.randbelow(99_999_998) + 1

    app_id_bytes = app_id.encode("utf-8")
    app_certificate_bytes = app_certificate.encode("utf-8")
    signing = hmac.new(_u32(issue_ts), app_certificate_bytes, hashlib.sha256).digest()
    signing = hmac.new(_u32(salt), signing, hashlib.sha256).digest()

    rtc = _RtcService(channel_name, int(uid))
    rtc.add_privilege(_RtcService.JOIN_CHANNEL, issue_ts + privilege_expire_seconds)
    rtc.add_privilege(_RtcService.PUBLISH_AUDIO, issue_ts + privilege_expire_seconds)

    services = [rtc]
    signing_info = (
        _str(app_id_bytes)
        + _u32(issue_ts)
        + _u32(token_expire_seconds)
        + _u32(salt)
        + _u16(len(services))
        + b"".join(service.pack() for service in services)
    )
    signature = hmac.new(signing, signing_info, hashlib.sha256).digest()
    payload = _str(signature) + signing_info

    return VERSION + base64.b64encode(zlib.compress(payload)).decode("ascii")
