from __future__ import annotations

import json
import os
from typing import Any


def get_env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value not in (None, ""):
        return value

    if os.name == "nt":
        try:
            import winreg

            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Environment") as key:
                registry_value, _ = winreg.QueryValueEx(key, name)
            if registry_value not in (None, ""):
                return str(registry_value)
        except OSError:
            return default

    return default


def get_bool(name: str, default: bool) -> bool:
    value = get_env(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def get_int(name: str, default: int) -> int:
    value = get_env(name)
    return int(value) if value is not None else default


def get_float(name: str, default: float) -> float:
    value = get_env(name)
    return float(value) if value is not None else default


def get_csv(name: str, default: list[str]) -> list[str]:
    value = get_env(name)
    if value is None:
        return default
    return [item.strip() for item in value.split(",") if item.strip()]


def get_json(name: str, default: Any) -> Any:
    value = get_env(name)
    if value is None:
        return default
    return json.loads(value)
