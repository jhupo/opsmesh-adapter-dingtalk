"""Smoke test for initialized dingtalk modular package skeleton."""

import dingtalk
import dingtalk.api
import dingtalk.event
import dingtalk.message
import dingtalk.stream


def test_package_skeleton_import() -> None:
    assert dingtalk.__version__ is not None
    for subpkg in (dingtalk.api, dingtalk.event, dingtalk.message, dingtalk.stream):
        assert subpkg.__doc__
