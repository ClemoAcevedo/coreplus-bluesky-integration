# attributes.py

"""
attributes.py
=============

• Funciones *create_* →  construyen las listas de atributos (PyValue-s) que
  exige CORE+ a partir de los records Bluesky.

• Funciones *get_robust_* →  obtienen timestamps “robustos” en nanosegundos:
  intentan primero el valor primario (createdAt) y, si falla, usan el del
  commit.  Son usadas por los *attribute_creator*.

No hay dependencias de red ni E/S; sólo utilidades puras.
"""

from __future__ import annotations

import time
from typing import Any

from dateutil.parser import isoparse

# Activar para debug puntual (no se imprime nada si está en False)
_VERBOSE = False


# ──────────────────────────────────────────────────────────
# Utilidades de timestamp
# ──────────────────────────────────────────────────────────
def _dbg(msg: str) -> None:
    if _VERBOSE:
        print(f"[attributes] {msg}")


def get_robust_nanosecond_timestamp_as_int(
    primary_ts_str: str | None,
    fallback_ts_str: str | None,
    path_for_logging: str = "",
) -> int:
    """
    Devuelve un **int** epoch-ns robusto:

    1. Intenta `primary_ts_str`
    2. Si falla, intenta `fallback_ts_str`
    3. Si ambos fallan, retorna **0**

    El parámetro *path_for_logging* sólo se usa para mensajes de depuración.
    """
    ts_ns: float | None = None

    if primary_ts_str:
        try:
            ts_ns = isoparse(primary_ts_str).timestamp() * 1e9
        except Exception:
            _dbg(f"Falló parse primary_ts «{primary_ts_str}» @{path_for_logging}")

    if ts_ns is None and fallback_ts_str:
        try:
            ts_ns = isoparse(fallback_ts_str).timestamp() * 1e9
        except Exception:
            _dbg(f"Falló parse fallback_ts «{fallback_ts_str}» @{path_for_logging}")

    return int(ts_ns) if ts_ns is not None else 0


def get_robust_nanosecond_timestamp_as_float(
    primary_ts_str: str | None,
    fallback_ts_str: str | None,
    path_for_logging: str = "",
) -> float:
    """
    Igual que la anterior, pero devuelve **float** (útil para columnas double).
    """
    ts_ns = float(get_robust_nanosecond_timestamp_as_int(primary_ts_str, fallback_ts_str, path_for_logging))
    return ts_ns if ts_ns != 0 else 0.0


# ──────────────────────────────────────────────────────────
# Creadores de atributos por tipo de evento
# ──────────────────────────────────────────────────────────
def create_bluesky_post_attributes(
    op_details: dict[str, Any],
    record_data: dict[str, Any],
    commit_payload: dict[str, Any],
    pycore_module,
) -> list:
    """
    Construye la lista de PyValue-s para BlueskyPosts.CreatePost
    (orden 1-a-1 con streams.py)
    """
    path = op_details.get("path", "")
    # ------------------------------------------------------------------ básicos
    uri              = pycore_module.PyStringValue(f"at://{commit_payload.get('repo', '')}/{path}")
    commit_cid       = pycore_module.PyStringValue(op_details.get("cid_str", ""))
    repo             = pycore_module.PyStringValue(commit_payload.get("repo", ""))
    seq              = pycore_module.PyIntValue(commit_payload.get("seq", -1))

    # ------------------------------------------------------------------ tiempos
    rca_str = record_data.get("createdAt") if isinstance(record_data, dict) else None
    c_time_str = commit_payload.get("time")

    record_created_at_ns = get_robust_nanosecond_timestamp_as_int(rca_str, c_time_str, path)
    record_created_at    = pycore_module.PyDateValue(record_created_at_ns)

    commit_time_ns_float = get_robust_nanosecond_timestamp_as_float(c_time_str, None, path)
    if commit_time_ns_float == 0.0 and c_time_str is None:          # fallback a «now»
        commit_time_ns_float = time.time() * 1e9
    commit_time          = pycore_module.PyDoubleValue(commit_time_ns_float)

    # ------------------------------------------------------------------ opcionales
    text                 = ""
    langs_str            = ""
    reply_root_uri_str   = ""
    reply_parent_uri_str = ""
    embed_type_str       = ""
    embed_image_count    = 0
    embed_external_uri   = ""
    embed_record_uri     = ""

    if isinstance(record_data, dict):
        # Text
        text = record_data.get("text", "")

        # Langs → lista → CSV
        langs = record_data.get("langs", [])
        if isinstance(langs, list):
            langs_str = ",".join(langs)

        # Reply
        if (reply := record_data.get("reply")) and isinstance(reply, dict):
            if (root := reply.get("root"))   and root.get("uri"):   reply_root_uri_str   = root["uri"]
            if (parent := reply.get("parent")) and parent.get("uri"): reply_parent_uri_str = parent["uri"]

        # Embed
        embed = record_data.get("embed", {})
        if isinstance(embed, dict):
            embed_type_str = embed.get("$type", "")

            match embed_type_str:
                case "app.bsky.embed.images":
                    imgs = embed.get("images")
                    embed_image_count = len(imgs) if isinstance(imgs, list) else 0

                case "app.bsky.embed.external":
                    ext = embed.get("external", {})
                    if isinstance(ext, dict):
                        embed_external_uri = ext.get("uri", "")

                case "app.bsky.embed.record":
                    ref = embed.get("record", {})
                    if isinstance(ref, dict):
                        embed_record_uri = ref.get("uri", "")

                case "app.bsky.embed.recordWithMedia":
                    rec_part = embed.get("record", {})
                    if isinstance(rec_part, dict):
                        inner_rec = rec_part.get("record", {})
                        if isinstance(inner_rec, dict):
                            embed_record_uri = inner_rec.get("uri", "")
                    media_part = embed.get("media", {})
                    if isinstance(media_part, dict) and media_part.get("$type") == "app.bsky.embed.images":
                        imgs = media_part.get("images")
                        embed_image_count = len(imgs) if isinstance(imgs, list) else 0

    # ------------------------------------------------------------------ PyValues
    attributes = [
        uri,
        commit_cid,
        repo,
        seq,
        commit_time,
        pycore_module.PyStringValue(text),
        record_created_at,
        pycore_module.PyStringValue(langs_str),
        pycore_module.PyStringValue(reply_root_uri_str),
        pycore_module.PyStringValue(reply_parent_uri_str),
        pycore_module.PyStringValue(embed_type_str),
        pycore_module.PyIntValue(embed_image_count),
        pycore_module.PyStringValue(embed_external_uri),
        pycore_module.PyStringValue(embed_record_uri),
    ]
    return attributes


def create_bluesky_like_attributes(
    op_details: dict[str, Any],
    record_data: dict[str, Any],
    commit_payload: dict[str, Any],
    pycore_module,
) -> list:
    """
    Crea atributos para BlueskyLikes.CreateLike
    """
    path = op_details.get("path", "")

    uri         = pycore_module.PyStringValue(f"at://{commit_payload.get('repo', '')}/{path}")
    commit_cid  = pycore_module.PyStringValue(op_details.get("cid_str", ""))
    repo        = pycore_module.PyStringValue(commit_payload.get("repo", ""))
    seq         = pycore_module.PyIntValue(commit_payload.get("seq", -1))

    rca_str = record_data.get("createdAt") if isinstance(record_data, dict) else None
    c_time_str = commit_payload.get("time")

    record_created_at = pycore_module.PyDateValue(
        get_robust_nanosecond_timestamp_as_int(rca_str, c_time_str, path)
    )

    commit_time_ns = get_robust_nanosecond_timestamp_as_float(c_time_str, None, path)
    if commit_time_ns == 0.0 and c_time_str is None:
        commit_time_ns = time.time() * 1e9
    commit_time = pycore_module.PyDoubleValue(commit_time_ns)

    subject_uri_str = ""
    subject_cid_str = ""
    if isinstance(record_data, dict):
        if (sub := record_data.get("subject")) and isinstance(sub, dict):
            subject_uri_str = sub.get("uri", "")
            subject_cid_str = sub.get("cid", "")

    attributes = [
        uri,
        commit_cid,
        repo,
        seq,
        commit_time,
        record_created_at,
        pycore_module.PyStringValue(subject_uri_str),
        pycore_module.PyStringValue(subject_cid_str),
    ]
    return attributes


def create_bluesky_repost_attributes(
    op_details: dict[str, Any],
    record_data: dict[str, Any],
    commit_payload: dict[str, Any],
    pycore_module,
) -> list:
    """
    Crea atributos para BlueskyReposts.CreateRepost
    """
    path = op_details.get("path", "")

    uri         = pycore_module.PyStringValue(f"at://{commit_payload.get('repo', '')}/{path}")
    commit_cid  = pycore_module.PyStringValue(op_details.get("cid_str", ""))
    repo        = pycore_module.PyStringValue(commit_payload.get("repo", ""))
    seq         = pycore_module.PyIntValue(commit_payload.get("seq", -1))

    rca_str = record_data.get("createdAt") if isinstance(record_data, dict) else None
    c_time_str = commit_payload.get("time")

    record_created_at = pycore_module.PyDateValue(
        get_robust_nanosecond_timestamp_as_int(rca_str, c_time_str, path)
    )

    commit_time_ns = get_robust_nanosecond_timestamp_as_float(c_time_str, None, path)
    if commit_time_ns == 0.0 and c_time_str is None:
        commit_time_ns = time.time() * 1e9
    commit_time = pycore_module.PyDoubleValue(commit_time_ns)

    subject_uri_str = ""
    subject_cid_str = ""
    if isinstance(record_data, dict):
        if (sub := record_data.get("subject")) and isinstance(sub, dict):
            subject_uri_str = sub.get("uri", "")
            subject_cid_str = sub.get("cid", "")

    attributes = [
        uri,
        commit_cid,
        repo,
        seq,
        commit_time,
        record_created_at,
        pycore_module.PyStringValue(subject_uri_str),
        pycore_module.PyStringValue(subject_cid_str),
    ]
    return attributes

def create_bluesky_profile_attributes(
    op_details: dict[str, Any],
    record_data: dict[str, Any],
    commit_payload: dict[str, Any],
    pycore_module,
) -> list:
    """
    Crea atributos para BlueskyEvents.UpdateProfile.
    Une displayName y description en un solo campo para evitar ambigüedad.
    """
    repo         = pycore_module.PyStringValue(commit_payload.get("repo", ""))
    commit_cid   = pycore_module.PyStringValue(op_details.get("cid_str", ""))
    seq          = pycore_module.PyIntValue(commit_payload.get("seq", -1))
    
    c_time_str = commit_payload.get("time")
    commit_time_ns_float = get_robust_nanosecond_timestamp_as_float(c_time_str, None, "profile")
    if commit_time_ns_float == 0.0 and c_time_str is None:
        commit_time_ns_float = time.time() * 1e9
    commit_time = pycore_module.PyDoubleValue(commit_time_ns_float)

    display_name_str = ""
    description_str = ""
    if isinstance(record_data, dict):
        display_name_str = record_data.get("displayName", "")
        description_str = record_data.get("description", "")

    # Unimos los campos con un separador fiable
    profile_text_str = f"{display_name_str}|||{description_str}"
    profile_text = pycore_module.PyStringValue(profile_text_str)

    attributes = [
        repo,
        commit_cid,
        seq,
        commit_time,
        profile_text,
    ]
    return attributes

def create_bluesky_follow_attributes(
    op_details: dict[str, Any],
    record_data: dict[str, Any],
    commit_payload: dict[str, Any],
    pycore_module,
) -> list:
    """
    Crea atributos para BlueskyEvents.CreateFollow
    """
    path = op_details.get("path", "")
    repo = pycore_module.PyStringValue(commit_payload.get("repo", ""))
    commit_cid = pycore_module.PyStringValue(op_details.get("cid_str", ""))
    seq = pycore_module.PyIntValue(commit_payload.get("seq", -1))

    rca_str = record_data.get("createdAt") if isinstance(record_data, dict) else None
    c_time_str = commit_payload.get("time")
    record_created_at = pycore_module.PyDateValue(
        get_robust_nanosecond_timestamp_as_int(rca_str, c_time_str, path)
    )
    commit_time_ns = get_robust_nanosecond_timestamp_as_float(c_time_str, None, path)
    if commit_time_ns == 0.0 and c_time_str is None:
        commit_time_ns = time.time() * 1e9
    commit_time = pycore_module.PyDoubleValue(commit_time_ns)

    subject_did_str = ""
    if isinstance(record_data, dict):
        subject_did_str = record_data.get("subject", "")

    attributes = [
        repo,
        commit_cid,
        seq,
        commit_time,
        record_created_at,
        pycore_module.PyStringValue(subject_did_str),
    ]
    return attributes

def create_bluesky_block_attributes(
    op_details: dict[str, Any],
    record_data: dict[str, Any],
    commit_payload: dict[str, Any],
    pycore_module,
) -> list:
    """
    Crea atributos para BlueskyEvents.CreateBlock
    (Idéntico a Follow, pero semánticamente distinto)
    """
    return create_bluesky_follow_attributes(op_details, record_data, commit_payload, pycore_module)