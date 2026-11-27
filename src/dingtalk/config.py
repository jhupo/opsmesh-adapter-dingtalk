"""Configuration models (BotConfig & Config) for OpsMesh get_plugin_config."""

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from .typing import ADAPTER_NAME


class BotConfig(BaseModel):
    """Single DingTalk enterprise internal bot credentials and subscriptions."""

    model_config = ConfigDict(extra="ignore")

    client_id: str = Field(..., min_length=1)
    client_secret: SecretStr
    robot_code: str | None = None
    subscribe_card_callbacks: bool = True
    subscribe_events: bool = False
    operator_union_id: str | None = None

    @property
    def effective_robot_code(self) -> str:
        return self.robot_code or self.client_id

    @property
    def gateway_id(self) -> str:
        return f"{ADAPTER_NAME}:{self.effective_robot_code}"


class Config(BaseModel):
    """Global DingTalk adapter configuration loaded via opsmesh.get_plugin_config."""

    model_config = ConfigDict(extra="ignore")

    dingtalk_bots: list[BotConfig] = Field(default_factory=list)
    dingtalk_api_base: str = "https://api.dingtalk.com"
    dingtalk_oapi_base: str = "https://oapi.dingtalk.com"
    dingtalk_request_timeout: float = Field(default=30.0, gt=0)
    dingtalk_max_download_bytes: int | None = Field(default=None, gt=0)
    dingtalk_download_chunk_size: int = Field(default=256 * 1024, ge=4096)
    dingtalk_download_read_timeout: float = Field(default=60.0, gt=0)
    dingtalk_allowed_download_domains: tuple[str, ...] = (
        "dingtalk.com",
        "alicdn.com",
        "aliyuncs.com",
        "ddurl.cn",
    )
    dingtalk_card_ack_timeout: float = Field(default=2.0, gt=0)
    dingtalk_dedupe_capacity: int = Field(default=2048, ge=1)
    dingtalk_dedupe_ttl: float = Field(default=600.0, gt=0)
    dingtalk_user_cache_ttl: float = Field(default=300.0, ge=0)
    dingtalk_reconnect_max_delay: float = Field(default=30.0, gt=0)

    @field_validator("dingtalk_bots")
    @classmethod
    def _validate_unique_bots(cls, bots: list[BotConfig]) -> list[BotConfig]:
        seen_ids: set[str] = set()
        seen_clients: set[str] = set()
        for bot in bots:
            if bot.client_id in seen_clients:
                raise ValueError(f"Duplicate DingTalk client_id: {bot.client_id!r}")
            if bot.gateway_id in seen_ids:
                raise ValueError(f"Duplicate DingTalk gateway_id: {bot.gateway_id!r}")
            seen_clients.add(bot.client_id)
            seen_ids.add(bot.gateway_id)
        return bots


__all__ = ["BotConfig", "Config"]
