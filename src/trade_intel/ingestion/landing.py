# Copyright (c) 2026 Agustin Valenti Boursiac. Todos los derechos reservados.
# Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
"""Escritura en la zona landing (ADLS Gen2 en la nube, filesystem en local).

En Databricks se escribe vía `dbutils.fs.put` sobre un Volume / external location de UC,
así las credenciales las resuelve el Access Connector (managed identity), no el código.
"""

from __future__ import annotations

from pathlib import Path


def write_landing(root: str, key: str, payload: bytes, dbutils=None) -> str:
    path = f"{root.rstrip('/')}/{key}"
    if dbutils is not None:
        dbutils.fs.put(path, payload.decode("utf-8"), overwrite=True)
        return path
    local = Path(path)
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(payload)
    return str(local)
