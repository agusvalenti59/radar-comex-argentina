# Copyright (c) 2026 Agustin Valenti Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Carga de configuración por entorno (conf/base.yml + conf/<env>.yml)."""

from __future__ import annotations

import copy
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

CONF_DIR = Path(os.environ.get("TRADE_INTEL_CONF_DIR", Path(__file__).resolve().parents[2] / "conf"))


def _deep_merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _render(value: Any, ctx: dict) -> Any:
    if isinstance(value, str):
        try:
            return value.format(**ctx)
        except (KeyError, IndexError):
            return value
    if isinstance(value, dict):
        return {k: _render(v, ctx) for k, v in value.items()}
    if isinstance(value, list):
        return [_render(v, ctx) for v in value]
    return value


@dataclass(frozen=True)
class Settings:
    env: str
    raw: dict = field(repr=False)

    @property
    def catalog(self) -> str | None:
        return self.raw.get("catalog")

    @property
    def storage_format(self) -> str:
        return self.raw["storage"]["format"]

    @property
    def home_iso3(self) -> str:
        return self.raw["home_country_iso3"]

    def schema(self, layer: str) -> str:
        return self.raw["schemas"][layer]

    def source(self, name: str) -> dict:
        return self.raw["sources"][name]

    @property
    def gold(self) -> dict:
        return self.raw["gold"]

    @property
    def product_universe(self) -> list[str]:
        return list(self.raw["product_universe"]["hs6"])

    @property
    def landing_root(self) -> str:
        return self.raw["storage"]["landing_root"]

    @property
    def checkpoint_root(self) -> str:
        return self.raw["storage"]["checkpoint_root"]


def load_settings(env: str | None = None, conf_dir: Path | None = None) -> Settings:
    env = env or os.environ.get("TRADE_INTEL_ENV", "dev")
    conf_dir = conf_dir or CONF_DIR
    base = yaml.safe_load((conf_dir / "base.yml").read_text())
    env_file = conf_dir / f"{env}.yml"
    override = yaml.safe_load(env_file.read_text()) if env_file.exists() else {}
    merged = _deep_merge(base, override or {})
    merged.setdefault("env", env)
    ctx = {"env": merged["env"], "storage_account": merged.get("storage_account", "")}
    return Settings(env=merged["env"], raw=_render(merged, ctx))
