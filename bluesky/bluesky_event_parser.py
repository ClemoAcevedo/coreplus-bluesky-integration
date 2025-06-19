"""
bluesky_event_parser.py
=======================

Convierte las líneas de texto crudo que publica el *mirror* de Bluesky
(Firehose → proc. C++ → texto) en diccionarios de atributos listos para
inyectar en CORE+.

• Cada *stream.event* se describe en `STREAM_ATTRIBUTE_DEFINITIONS` con
  su nombre, tipo lógico y regex de captura.

• `parse_event_attributes()` recorre el esquema secuencialmente: extrae,
  castea y devuelve `(attributes_dict, errors_list)`.
"""

from __future__ import annotations

import json
import re
from typing import Any

# ──────────────────────────────────────────────────────────
# Regex bases (nombres PEP-8 friendly)
# ──────────────────────────────────────────────────────────
PAT_AT_URI           = r"at:\/\/(?:did:[a-z0-9]+:[a-zA-Z0-9._:%-]+|[a-zA-Z0-9.-]+)\/[a-zA-Z0-9.-]+\/[a-zA-Z0-9._~-]+"
PAT_CID              = r"(?:z[a-zA-Z0-9]{48}|b[a-z2-7]{50,})"
PAT_REPO             = r"did:[a-z0-9]+:[a-zA-Z0-9._:%-]+"
PAT_LANGS_ANCHORED   = r"[a-z]{2,3}(?:-[A-Z0-9]{1,4})?(?:,[a-z]{2,3}(?:-[A-Z0-9]{1,4})?)*(?=\s|$)"
PAT_EMBED_TYPE       = r"app\.bsky\.embed\.[a-zA-Z0-9]+(?=\s|$)"
PAT_INT              = r"-?\d+"
PAT_FLOAT            = r"-?\d+\.\d{6}"
PAT_NS_INT           = r"\d{18,19}"
PAT_NONSPACE         = r"[^\s]+"

# Para la heurística “texto vacío vs timestamp”:  un timestamp al inicio de línea
_RE_PRIMARY_TIME_WHOLE = re.compile(r"^\d{18,19}(?:\s|$)")

# ──────────────────────────────────────────────────────────
# Esquema de streams / events (DDL ←→ parser)
# ──────────────────────────────────────────────────────────
STREAM_ATTRIBUTE_DEFINITIONS: dict[str, list[dict[str, Any]]] = {
    "BlueskyEvents.CreatePost": [
        {"name": "uri",               "type": "string",        "regex_pattern": PAT_AT_URI},
        {"name": "commit_cid",        "type": "string",        "regex_pattern": PAT_CID},
        {"name": "repo",              "type": "string",        "regex_pattern": PAT_REPO},
        {"name": "seq",               "type": "int",           "regex_pattern": PAT_INT},
        {"name": "commit_time",       "type": "double",        "regex_pattern": PAT_FLOAT},
        {"name": "record_text",       "type": "string_multitoken"},
        {"name": "record_created_at", "type": "primary_time",  "regex_pattern": PAT_NS_INT},
        {"name": "langs",             "type": "string",        "regex_pattern": PAT_LANGS_ANCHORED,
         "is_optional_empty": True,   "default_empty": ""},
        {"name": "reply_root_uri",    "type": "string",        "regex_pattern": PAT_AT_URI,
         "is_optional_empty": True,   "default_empty": ""},
        {"name": "reply_parent_uri",  "type": "string",        "regex_pattern": PAT_AT_URI,
         "is_optional_empty": True,   "default_empty": ""},
        {"name": "embed_type",        "type": "string",        "regex_pattern": PAT_EMBED_TYPE,
         "is_optional_empty": True,   "default_empty": ""},
        {"name": "embed_image_count", "type": "int",           "regex_pattern": PAT_INT,
         "default_empty": 0},
        {"name": "embed_external_uri","type": "string",        "regex_pattern": PAT_NONSPACE,
         "is_optional_empty": True,   "default_empty": ""},
        {"name": "embed_record_uri",  "type": "string",        "regex_pattern": PAT_AT_URI,
         "is_optional_empty": True,   "default_empty": ""},
    ],

    "BlueskyEvents.CreateLike": [
        {"name": "uri",               "type": "string",        "regex_pattern": PAT_AT_URI},
        {"name": "commit_cid",        "type": "string",        "regex_pattern": PAT_CID},
        {"name": "repo",              "type": "string",        "regex_pattern": PAT_REPO},
        {"name": "seq",               "type": "int",           "regex_pattern": PAT_INT},
        {"name": "commit_time",       "type": "double",        "regex_pattern": PAT_FLOAT},
        {"name": "record_created_at", "type": "primary_time",  "regex_pattern": PAT_NS_INT},
        {"name": "subject_uri",       "type": "string",        "regex_pattern": PAT_AT_URI},
        {"name": "subject_cid",       "type": "string",        "regex_pattern": PAT_CID,
         "is_optional_empty": True,   "default_empty": ""},
    ],

    "BlueskyEvents.CreateRepost": [
        {"name": "uri",               "type": "string",        "regex_pattern": PAT_AT_URI},
        {"name": "commit_cid",        "type": "string",        "regex_pattern": PAT_CID},
        {"name": "repo",              "type": "string",        "regex_pattern": PAT_REPO},
        {"name": "seq",               "type": "int",           "regex_pattern": PAT_INT},
        {"name": "commit_time",       "type": "double",        "regex_pattern": PAT_FLOAT},
        {"name": "record_created_at", "type": "primary_time",  "regex_pattern": PAT_NS_INT},
        {"name": "subject_uri",       "type": "string",        "regex_pattern": PAT_AT_URI},
        {"name": "subject_cid",       "type": "string",        "regex_pattern": PAT_CID,
         "is_optional_empty": True, "default_empty": ""}, 
    ],
    
    "BlueskyEvents.UpdateProfile": [
        {"name": "repo",              "type": "string", "regex_pattern": PAT_REPO},
        {"name": "commit_cid",        "type": "string", "regex_pattern": PAT_CID},
        {"name": "seq",               "type": "int",    "regex_pattern": PAT_INT},
        {"name": "commit_time",       "type": "double", "regex_pattern": PAT_FLOAT},
        # Este campo ahora contiene displayName y description unidos.
        # Es el último, por lo que puede ser multitoken y consumir el resto de la línea.
        {"name": "profile_text",      "type": "string_multitoken", "is_optional_empty": True, "default_empty": "|||"},
    ],

    "BlueskyEvents.CreateFollow": [
        {"name": "repo",              "type": "string",        "regex_pattern": PAT_REPO},
        {"name": "commit_cid",        "type": "string",        "regex_pattern": PAT_CID},
        {"name": "seq",               "type": "int",           "regex_pattern": PAT_INT},
        {"name": "commit_time",       "type": "double",        "regex_pattern": PAT_FLOAT},
        {"name": "record_created_at", "type": "primary_time",  "regex_pattern": PAT_NS_INT},
        {"name": "subject_did",       "type": "string",        "regex_pattern": PAT_REPO}, # Un DID tiene formato de repo
    ],

    "BlueskyEvents.CreateBlock": [
        {"name": "repo",              "type": "string",        "regex_pattern": PAT_REPO},
        {"name": "commit_cid",        "type": "string",        "regex_pattern": PAT_CID},
        {"name": "seq",               "type": "int",           "regex_pattern": PAT_INT},
        {"name": "commit_time",       "type": "double",        "regex_pattern": PAT_FLOAT},
        {"name": "record_created_at", "type": "primary_time",  "regex_pattern": PAT_NS_INT},
        {"name": "subject_did",       "type": "string",        "regex_pattern": PAT_REPO},
    ]
}

# Pre-compilamos todas las regex
for _ev, _attrs in STREAM_ATTRIBUTE_DEFINITIONS.items():
    for _a in _attrs:
        if isinstance(_a.get("regex_pattern"), str):
            _a["_compiled_regex"] = re.compile(_a["regex_pattern"])


# ──────────────────────────────────────────────────────────
# Función pública
# ──────────────────────────────────────────────────────────
def parse_event_attributes(event_type_name: str, raw_line: str) -> tuple[dict[str, Any], list[str]]:
    """
    Transforma una línea `raw_line` → (dict atributos, lista errores).

    · Soporta “multitoken” (texto con espacios) para *record_text* detectando
      dónde empieza el siguiente campo (record_created_at).

    · Maneja campos opcionales con `default_empty`.
    """
    DEBUG_THIS_FUNCTION = False
    def _dbg(msg: str) -> None:
        if DEBUG_THIS_FUNCTION: print(msg)
    
    normalized = re.sub(r"\s+", " ", raw_line.replace("\xa0", " ")).strip()
    _dbg(f"\n[PARSER] {event_type_name}  original={repr(raw_line)}\n          norm = {repr(normalized)}")

    schema = STREAM_ATTRIBUTE_DEFINITIONS.get(event_type_name)
    if not schema:
        return {"_RAW_CONTENT_": normalized}, [f"No schema for event '{event_type_name}'"]

    attributes: dict[str, Any] = {}
    errors: list[str] = []
    remaining = normalized

    def _opt_default(a_def: dict) -> Any:
        if "default_empty" in a_def: return a_def["default_empty"]
        t = a_def["type"]
        return 0 if t in ("int", "primary_time") else (0.0 if t == "double" else "")

    for idx, a_def in enumerate(schema):
        name = a_def["name"]; a_type = a_def["type"]; cregex = a_def.get("_compiled_regex")
        opt_empty = a_def.get("is_optional_empty", False); default = _opt_default(a_def)
        _dbg(f"  • {name:>18}  | remaining='{remaining[:70]}'")
        current_val_str: str | None = None

        if a_type == "string_multitoken":
            if name == "record_text" and _RE_PRIMARY_TIME_WHOLE.match(remaining.lstrip()):
                current_val_str = ""
            else:
                next_regex = None
                for next_idx in range(idx + 1, len(schema)):
                    if schema[next_idx].get("_compiled_regex"):
                        next_regex = schema[next_idx]["_compiled_regex"]; break
                if next_regex:
                    m = re.search(rf"\s+({next_regex.pattern})(?=\s|$)", remaining)
                    if m:
                        current_val_str = remaining[:m.start()].strip()
                        remaining = remaining[m.start():].strip()
                    else:
                        current_val_str = remaining.strip(); remaining = ""
                else:
                    current_val_str = remaining.strip(); remaining = ""
        elif cregex:
            m = cregex.match(remaining.lstrip())
            if m:
                token = m.group(0)
                cut_start_index = remaining.find(token)
                if cut_start_index == -1: cut_start_index = len(remaining) - len(remaining.lstrip())
                cut_end_index = cut_start_index + len(token)
                current_val_str = token
                remaining = remaining[cut_end_index:]
            elif opt_empty:
                current_val_str = str(default)
            else:
                errors.append(f"Pattern for '{name}' did not match near «{remaining[:40]}…»")
                attributes[name] = default; continue
        else:
            current_val_str = "" if opt_empty else None

        if current_val_str is not None:
            try:
                match a_type:
                    case "int" | "primary_time": attributes[name] = int(current_val_str)
                    case "double": attributes[name] = float(current_val_str)
                    case _: attributes[name] = str(current_val_str).strip()
            except (ValueError, TypeError) as ve:
                errors.append(f"Conversion error for '{name}' from value '{current_val_str}': {ve}")
                attributes[name] = default
        elif opt_empty:
            attributes[name] = default
        _dbg(f"      → {attributes.get(name)}")

    if remaining.strip():
        errors.append(f"Extra unparsed content: '{remaining.strip()}'")
        attributes["_UNPARSED_REMAINDER_"] = remaining.strip()

    # --- PASO DE POST-PROCESAMIENTO ---
    if event_type_name == "BlueskyEvents.UpdateProfile" and "profile_text" in attributes:
        profile_text = attributes.pop("profile_text") # Extraemos el campo combinado
        parts = profile_text.split("|||", 1)
        attributes["display_name"] = parts[0]
        attributes["description"] = parts[1] if len(parts) > 1 else ""
    
    if DEBUG_THIS_FUNCTION:
        print(json.dumps(attributes, indent=2), "\nErrors:", errors)

    return attributes, errors