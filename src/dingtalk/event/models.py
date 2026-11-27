"""Typed value models: AtUser, RoleItem, UserProfile, and DownloadedMedia."""

from dataclasses import dataclass
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class AtUser(BaseModel):
    """Mentioned user metadata inside a DingTalk group message."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    dingtalk_id: str = Field(alias="dingtalkId")
    staff_id: str | None = Field(default=None, alias="staffId")


class RoleItem(BaseModel):
    """Enterprise role entry from topapi/v2/user/get."""

    model_config = ConfigDict(extra="allow")

    id: int
    name: str
    group_name: str | None = None


class UserProfile(BaseModel):
    """Enterprise employee directory profile returned by topapi/v2/user/get."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    userid: str
    name: str = ""
    unionid: str | None = None
    title: str | None = None
    job_number: str | None = None
    email: str | None = None
    mobile: str | None = None
    dept_id_list: list[int] = Field(default_factory=list)
    role_list: list[RoleItem] = Field(default_factory=list)
    admin: bool = False
    boss: bool = False
    active: bool = True

    @property
    def roles(self) -> list[RoleItem]:
        """Alias for role_list for ergonomic RBAC checks."""
        return self.role_list


@dataclass(slots=True, kw_only=True, frozen=True)
class DownloadedMedia:
    """Result metadata of a streamed attachment download to local disk."""

    path: Path
    name: str
    size: int
    mime: str | None = None


__all__ = ["AtUser", "DownloadedMedia", "RoleItem", "UserProfile"]
