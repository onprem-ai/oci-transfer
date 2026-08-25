"""Hatch hook for correctly tagging wheels that bundle the Go service."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

_ALLOWED_PLATFORMS = frozenset({"manylinux_2_28_x86_64", "manylinux_2_28_aarch64"})


class CustomBuildHook(BuildHookInterface):
    """Mark release wheels as platform-specific and non-pure."""

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        del version
        platform = os.environ.get("OPAI_OCI_WHEEL_PLATFORM")
        if platform is None:
            # Editable development installs do not bundle a service binary.
            return
        if platform not in _ALLOWED_PLATFORMS:
            raise ValueError(f"unsupported OPAI_OCI_WHEEL_PLATFORM: {platform}")
        binary = Path(self.root) / "src/opai_oci_transfer/_bin/opai-oci-transferd"
        if not binary.is_file():
            raise RuntimeError("opai-oci-transferd must be built before the wheel")
        build_data["pure_python"] = False
        build_data["tag"] = f"py3-none-{platform}"
