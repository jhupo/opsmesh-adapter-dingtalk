"""Tests for Config, BotConfig, DedupeRing, filename sanitization, and driver checks."""

from opsmesh.config import Config as OpsMeshConfig
from opsmesh.config import Env
from opsmesh.drivers.none import Driver as NoneDriver
from pydantic import ValidationError
import pytest

from dingtalk import Adapter, BotConfig, Config
from dingtalk.utils import DedupeRing, sanitize_filename


def test_bot_config_and_duplicate_validation() -> None:
    cfg = Config(
        dingtalk_bots=[
            BotConfig(client_id="ding_a", client_secret="sec_a"),
            BotConfig(client_id="ding_b", client_secret="sec_b", robot_code="bot_b"),
        ]
    )
    assert cfg.dingtalk_bots[0].gateway_id == "dingtalk:ding_a"
    assert cfg.dingtalk_bots[1].gateway_id == "dingtalk:bot_b"
    assert "sec_a" not in repr(cfg.dingtalk_bots[0])
    assert cfg.dingtalk_max_download_bytes is None

    with pytest.raises(ValidationError, match="Duplicate DingTalk client_id"):
        Config(
            dingtalk_bots=[
                BotConfig(client_id="dup", client_secret="s1"),
                BotConfig(client_id="dup", client_secret="s2"),
            ]
        )


def test_dedupe_ring_lru_and_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 100.0
    monkeypatch.setattr("dingtalk.utils.time.monotonic", lambda: now)

    ring = DedupeRing(capacity=2, ttl=10.0)
    assert ring.seen_or_record("m1") is False
    assert ring.seen_or_record("m1") is True
    assert ring.seen_or_record("m2") is False
    assert ring.seen_or_record("m3") is False  # evicts m1 (LRU)
    assert ring.seen_or_record("m1") is False

    now += 15.0  # expire all
    assert len(ring) == 0
    assert ring.seen_or_record("m2") is False


def test_sanitize_filename_blocks_traversal() -> None:
    assert sanitize_filename("../../etc/passwd") == "passwd"
    assert sanitize_filename("..\\..\\secret.txt") == "secret.txt"
    assert sanitize_filename("bad<>:name?.pdf") == "bad___name_.pdf"
    assert sanitize_filename("...") == "attachment.bin"
    assert sanitize_filename(None) == "attachment.bin"


def test_adapter_requires_http_and_ws_mixins() -> None:
    driver = NoneDriver(Env(), OpsMeshConfig())
    with pytest.raises(TypeError, match="HTTPClientMixin and WebSocketClientMixin"):
        Adapter(driver, dingtalk_config=Config())
