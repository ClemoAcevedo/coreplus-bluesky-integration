"""
Integración Bluesky → CORE+ — VERSIÓN MEJORADA (STREAM UNIFICADO)

• Declara un stream único / opciones / queries en el servidor CORE C++.
• Lanza el listener WebSocket que consume el fire-hose de Bluesky,
  crea atributos (handlers) y re-emite PyEvents a CORE.
• Recibe resultados de queries y vuelve a parsearlos con Python
  para la demo, mostrando cada primitive "bonito".

❗ IMPORTANTE:
    Si cambias el DDL o el orden de streams / eventos, reinicia
     el servidor CORE C++ para evitar desajustes de IDs.
    Variables de conexión al inicio.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import copy
import re
import json
from datetime import datetime
from typing import Dict, List, Any, Tuple
import textwrap

# ---------------------------------------------------------------------
#  Pathing – añadimos carpeta raíz (…/CORE) a PYTHONPATH
# ---------------------------------------------------------------------
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

# pylint: disable=wrong-import-position
import _pycore # type: ignore
from bluesky.streams import (
    BLUESKY_EVENTS_STREAM_DECLARATION,
)
from bluesky.handlers import BLUESKY_EVENT_HANDLERS_CONFIG
from bluesky.listener import bluesky_websocket_listener
from bluesky.bluesky_event_parser import parse_event_attributes

# ---------------------------------------------------------------------
#  Configuración (host/puertos, opciones CORE+, etc.)
# ---------------------------------------------------------------------
CORE_HOST = "tcp://localhost"
CORE_SERVER_PORT = 5000
CORE_STREAMER_PORT = 5001
CORE_QUERY_INITIAL_PORT = 5002

QUARANTINE_TEMPLATE = """CREATE QUARANTINE {
        FIXED_TIME 60 seconds { {S} }
    }"""

# ---------------------------------------------------------------------
#  UI Helpers - Colores y Emojis
# ---------------------------------------------------------------------
_COLOR = os.getenv("TERM", "") and os.getenv("NO_COLOR", "0") not in ("1", "true", "TRUE")

def color(text: str, code: str) -> str:
    """Aplica color ANSI al texto si está habilitado."""
    return f"\033[{code}m{text}\033[0m" if _COLOR else text

EMOJI = {
    "BlueskyEvents.CreatePost": "📝",
    "BlueskyEvents.CreateLike": "❤️",
    "BlueskyEvents.CreateRepost": "🔁",
    "BlueskyEvents.UpdateProfile": "👤",
    "BlueskyEvents.CreateFollow": "🤝",
    "BlueskyEvents.CreateBlock": "🚫",
}

def _now_str() -> str:
    """Timestamp en formato ISO-like (sin microsegundos) para logs."""
    return time.strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str, *, level: str = "INFO") -> None:
    """Genera logs homogéneos con colores."""
    colors = {
        "INFO": "92",
        "WARN": "93",
        "ERROR": "91",
        "DEBUG": "94",
    }
    level_colored = color(level, colors.get(level, "0"))
    print(f"[{_now_str()}] {level_colored}: {msg}")

_PRETTY = os.getenv("CORE_DEMO_PRETTY", "1") not in ("0", "false", "FALSE")
INDENT = " " * 4

def _pp(label: str, obj: dict) -> None:
    """Pretty-print mejorado para diccionarios de atributos."""
    if not _PRETTY:
        return
    emoji = EMOJI.get(label, "•")
    print(f"{INDENT}{emoji} {color(label, '96')}:")
    display_obj = obj.copy()
    json_str = json.dumps(display_obj, ensure_ascii=False, indent=2)
    print(textwrap.indent(json_str, INDENT * 2))

# ---------------------------------------------------------------------
#  Variables Globales
# ---------------------------------------------------------------------
EVENT_ID_TO_NAME: Dict[int, str] = {}
SUBSCRIBED_QUERY_MAP: Dict[int, str] = {}
QUERY_EVENT_COUNT = 0

# ---------------------------------------------------------------------
#  Handler de Resultados de Queries
# ---------------------------------------------------------------------
def create_query_handler(alias_for_closure: str):
    def query_handler(enumerator: _pycore.PyEnumerator) -> None:
        global QUERY_EVENT_COUNT
        effective_alias = alias_for_closure

        complex_events_list = list(enumerator)
        if not complex_events_list:
            return

        print(f"🎯 HANDLER INVOKED (effective alias: '{effective_alias}') - {len(complex_events_list)} ComplexEvent(s) recibido(s) 🎯")

        batch_results: List[Dict[str, Any]] = []
        for complex_event in complex_events_list:
            QUERY_EVENT_COUNT += 1
            raw_ce = complex_event.to_string().strip()
            m_ce = re.match(r"^\[(.+?)\],\s*\[(.*)\]$", raw_ce, re.DOTALL)
            if not m_ce:
                log(f"Formato de complex-event inesperado para alias '{effective_alias}': {raw_ce}", level="WARN")
                continue

            ce_ts, primitives_blob = m_ce.groups()
            ce_info: Dict[str, Any] = {"n": QUERY_EVENT_COUNT, "alias": effective_alias, "core_ts": ce_ts, "primitives": [], "errors": []}

            for pe_id, pe_attrs in re.findall(r"\(id:\s*(\d+)\s*attributes:\s*\[(.*?)\]\)", primitives_blob, re.DOTALL):
                pe_id_int = int(pe_id)
                fqn = EVENT_ID_TO_NAME.get(pe_id_int, f"UnknownEventId.{pe_id_int}")
                parsed, err = parse_event_attributes(fqn, pe_attrs)

                if "commit_time" in parsed and isinstance(parsed["commit_time"], float):
                    try:
                        commit_time_seconds = parsed["commit_time"] / 1e9
                        dt_object = datetime.fromtimestamp(commit_time_seconds)
                        parsed["commit_time_readable"] = dt_object.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"
                    except Exception as e_time:
                        parsed["commit_time_readable"] = f"Error formateando tiempo: {e_time}"

                ce_info["primitives"].append({"id": pe_id_int, "type": fqn, "attr": parsed})
                if err:
                    ce_info["errors"].extend(err)
            batch_results.append(ce_info)

        for ce_data in batch_results:
            header = (color(f"🎯 {ce_data['alias']}", "95") + " │ " + color(f"📦 CE#{ce_data['n']:>4}", "93") +
                      " │ prims=" + color(str(len(ce_data['primitives'])), "96") + " │ ts=" + ce_data['core_ts'].split(",")[0])
            print(header)
            for prim in ce_data["primitives"]:
                _pp(prim["type"], prim["attr"])
            if ce_data["errors"]:
                log(f"⚠️  ComplexEvent #{ce_data['n']} (alias: {ce_data['alias']}) tuvo errores de parseo ({len(ce_data['errors'])})", level="WARN")
                for e_idx, e_msg in enumerate(ce_data["errors"]):
                    log(f"   – Error {e_idx+1}: {e_msg}", level="WARN")
            print()
    return query_handler

# ---------------------------------------------------------------------
#  Helpers para Interacción con CORE C++
# ---------------------------------------------------------------------
def init_core_client(host: str, port: int) -> _pycore.PyClient:
    log(f"Inicializando PyClient → {host}:{port} …")
    client = _pycore.PyClient(host, port)
    log("Cliente CORE listo ✅")
    return client

def declare_bluesky_streams(client: _pycore.PyClient) -> Dict[str, _pycore.PyStreamInfo]:
    global EVENT_ID_TO_NAME
    EVENT_ID_TO_NAME.clear()
    stream_name = "BlueskyEvents"
    ddl = BLUESKY_EVENTS_STREAM_DECLARATION
    declared_streams_info: Dict[str, _pycore.PyStreamInfo] = {}
    log("Declarando stream unificado Bluesky …")
    try:
        info = client.declare_stream(ddl)
        declared_streams_info[info.name] = info
        log(f"  • Stream «{info.name}» ID={info.id} declarado exitosamente.")
        for ev in info.events_info:
            EVENT_ID_TO_NAME[ev.id] = f"{info.name}.{ev.name}"
            log(f"      – Evento «{ev.name}» ID={ev.id}")
    except _pycore.PyStreamNameAlreadyDeclaredException:
        log(f"  • Stream «{stream_name}» ya existía. Intentando fallback para IDs...", level="WARN")
        stream_id = 0
        event_details = [
            {"name": "CreatePost", "id": 0}, {"name": "CreateLike", "id": 1}, {"name": "CreateRepost", "id": 2},
            {"name": "UpdateProfile", "id": 3}, {"name": "CreateFollow", "id": 4}, {"name": "CreateBlock", "id": 5},
        ]
        mock_event_infos = []
        log(f"       Fallback: Asumiendo Stream «{stream_name}» con ID={stream_id}")
        for ev_detail in event_details:
            event_id_global, event_name = ev_detail["id"], ev_detail["name"]
            EVENT_ID_TO_NAME[event_id_global] = f"{stream_name}.{event_name}"
            log(f"       Fallback: Asumiendo Evento «{event_name}» ID global={event_id_global}")
            mock_event_infos.append(_pycore.PyEventInfo(event_id_global, event_name, []))
        declared_streams_info[stream_name] = _pycore.PyStreamInfo(stream_id, stream_name, mock_event_infos)
    except Exception as exc:
        log(f"Fallo declarando/obteniendo stream {stream_name}: {type(exc).__name__}: {exc}", level="ERROR")
    return declared_streams_info

def map_handler_ids(handler_template: Dict[str, Any], stream_infos_map: Dict[str, _pycore.PyStreamInfo]) -> Dict[str, Any]:
    populated_handlers = copy.deepcopy(handler_template)
    all_mapped_ok = True
    log("Mapeando IDs de $type de Bluesky a IDs de Stream/Evento de CORE+...")
    for bsky_type, handler_cfg in populated_handlers.items():
        stream_name_needed = handler_cfg["stream_name"]
        event_name_needed = handler_cfg["event_name"]
        found_event_id_in_core = None
        fqn_needed = f"{stream_name_needed}.{event_name_needed}"
        for ev_id, fqn in EVENT_ID_TO_NAME.items():
            if fqn == fqn_needed:
                found_event_id_in_core = ev_id
                break
        if found_event_id_in_core is not None and stream_infos_map.get(stream_name_needed):
            handler_cfg["stream_id"] = stream_infos_map[stream_name_needed].id
            handler_cfg["event_id"] = found_event_id_in_core
        else:
            log(f"Error mapeando: Evento «{event_name_needed}» no encontrado para $type {bsky_type}", level="ERROR")
            all_mapped_ok = False
    status_msg = "✅ Todos los handlers mapeados exitosamente." if all_mapped_ok else "❌ Falló el mapeo para algunos handlers."
    log(f"Mapeo de handlers completado. Estado: {status_msg}")
    for bsky_type, cfg in populated_handlers.items():
        is_ok = cfg.get('stream_id') is not None and cfg.get('event_id') is not None
        log(f"   {bsky_type:<26} → stream_id={cfg.get('stream_id', 'N/A')}, event_id={cfg.get('event_id', 'N/A')}, creator={cfg['attribute_creator'].__name__}",
            level="DEBUG" if is_ok else "WARN")
    return populated_handlers

def add_queries_and_subscribe(client: _pycore.PyClient, queries_to_add: List[Tuple[str, str]], query_base_port: int) -> List[_pycore.PyCallbckHandler]:
    global SUBSCRIBED_QUERY_MAP
    SUBSCRIBED_QUERY_MAP.clear()
    active_queries = [(alias, sql.strip()) for alias, sql in queries_to_add if sql.strip() and not sql.lstrip().startswith("--")]
    if not active_queries:
        log("No hay queries activas para añadir/suscribir.", level="INFO")
        return []
    successful_aliases = []
    for alias, sql_query in active_queries:
        try:
            client.add_query(sql_query)
            log(f"Query «{alias}» añadida al cliente CORE+: {sql_query.splitlines()[0][:70]}…")
            successful_aliases.append(alias)
        except Exception as e:
            log(f"Error añadiendo query «{alias}» al cliente CORE+: {type(e).__name__}: {e}", level="ERROR")
    
    active_queries = [q for q in active_queries if q[0] in successful_aliases]
    if not active_queries:
        log("Ninguna query fue añadida exitosamente.", level="WARN")
        return []

    num_queries = len(active_queries)
    final_port = query_base_port + num_queries
    log(f"Intentando suscribir {num_queries} handler(s) en el rango de puertos {query_base_port} a {final_port - 1}.")
    try:
        handlers = _pycore.subscribe_to_queries(client, query_base_port, final_port)
        if len(handlers) != num_queries:
            log(f"Error de suscripción: Se esperaban {num_queries} handlers pero C++ devolvió {len(handlers)}.", level="ERROR")
            return []
        log(f"Handlers C++ ({len(handlers)}) obtenidos, asignando callbacks Python...")
        for idx, (alias, _) in enumerate(active_queries):
            port = query_base_port + idx
            handlers[idx].set_event_handler(create_query_handler(alias))
            SUBSCRIBED_QUERY_MAP[port] = alias
            log(f"Suscrito handler para «{alias}» en puerto {port}.")
        if num_queries > 1:
            log(f"NOTA: Debido al handler estático en C++, todos los eventos de query probablemente usarán el callback Python configurado para el ÚLTIMO alias («{active_queries[-1][0]}»).", level="WARN")
        return handlers
    except Exception as e:
        log(f"Error crítico durante la suscripción de queries: {type(e).__name__}: {e}", level="ERROR")
        return []

def init_core_streamer(host: str, port: int) -> _pycore.PyStreamer:
    log(f"Inicializando PyStreamer → {host}:{port}")
    streamer = _pycore.PyStreamer(host, port)
    log("Streamer CORE listo ✅")
    return streamer

# ---------------------------------------------------------------------
#  MAIN
# ---------------------------------------------------------------------
async def main() -> None:
    banner = ("\n" + "*" * 80 + "\n" + color("🚀  MAIN_BLUESKY.PY iniciado – demo integración CORE+", "92") + "\n" + "*" * 80)
    print(banner)

    client = init_core_client(CORE_HOST, CORE_SERVER_PORT)
    stream_infos_map = declare_bluesky_streams(client)

    if not EVENT_ID_TO_NAME:
        log("FATAL: No se pudieron declarar/obtener los IDs de los eventos.", level="ERROR")
        return

    handlers_config = map_handler_ids(BLUESKY_EVENT_HANDLERS_CONFIG, stream_infos_map)
    if not all(cfg.get("stream_id") is not None and cfg.get("event_id") is not None for cfg in handlers_config.values()):
        log("FATAL: Mapeo de handlers incompleto.", level="ERROR")
        return

    # --- QUERIES DE DEMOSTRACIÓN (ESTABLES Y FUNCIONALES) ---
    # Este conjunto de queries utiliza únicamente la sintaxis que hemos
    # confirmado que funciona, y demuestra la ingesta de todos los tipos de eventos.
    queries_demo = [
        # ("POSTS_CON_IMAGENES",
        #  # Filtro numérico (sabemos que funciona)
        #  "SELECT P FROM BlueskyEvents WHERE CreatePost AS P FILTER P[embed_image_count > 0]"
        # ),
        # ("LIKES_SIN_FILTRO",
        #  # Selección simple de un tipo de evento (sabemos que funciona)
        #  "SELECT L FROM BlueskyEvents WHERE CreateLike AS L"
        # ),
        # ("REPOSTS_SIN_FILTRO",
        #  "SELECT R FROM BlueskyEvents WHERE CreateRepost AS R"
        # ),
        # ("PROFILES_SIN_FILTRO",
        #  "SELECT Pr FROM BlueskyEvents WHERE UpdateProfile AS Pr"
        # ),
        # ("FOLLOWS_SIN_FILTRO",
        #  "SELECT F FROM BlueskyEvents WHERE CreateFollow AS F"
        # ),
        # ("BLOCKS_SIN_FILTRO",
        #  "SELECT B FROM BlueskyEvents WHERE CreateBlock AS B"
        # ),
        ("POSTS_EN_ESPANOL",
         # Este es el único filtro de string que confirmamos que funciona.
         "SELECT P FROM BlueskyEvents WHERE CreatePost AS P FILTER P[langs = 'es']"
        ),
    ]

    add_queries_and_subscribe(client, queries_demo, CORE_QUERY_INITIAL_PORT)

    try:
        quarantine_option_str = QUARANTINE_TEMPLATE.replace("{S}", "BlueskyEvents")
        client.declare_option(quarantine_option_str)
        log(f"Opción de cuarentena aplicada a BlueskyEvents: {quarantine_option_str.strip()}")
    except Exception as e:
        log(f"Error aplicando opción de cuarentena: {type(e).__name__}: {e}", level="ERROR")

    streamer = init_core_streamer(CORE_HOST, CORE_STREAMER_PORT)

    log("Iniciando listener de WebSocket para Bluesky Firehose …")
    try:
        await bluesky_websocket_listener(streamer, handlers_config, _pycore)
    except asyncio.CancelledError:
        log("Listener de Bluesky cancelado.", level="INFO")
    except Exception as exc:
        log(f"Error crítico en el listener de Bluesky: {type(exc).__name__}: {exc}", level="ERROR")
        raise
    finally:
        log("Listener de Bluesky finalizado.")

    log(f"Fin de ejecución. Total complex events recibidos: {QUERY_EVENT_COUNT}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Interrupción por teclado (Ctrl+C) recibida abortando.", level="INFO")
    except Exception as exc:
        log(f"Error fatal no capturado en __main__: {type(exc).__name__}: {exc}", level="ERROR")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        log("Script principal finalizado.")