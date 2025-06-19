"""
Integración Bluesky → CORE+ — VERSIÓN MEJORADA (STREAM UNIFICADO)

• Declara un stream único / opciones / queries en el servidor CORE C++.
• Lanza el listener WebSocket que consume el fire-hose de Bluesky,
  crea atributos (handlers) y re-emite PyEvents a CORE.
• Recibe resultados de queries y vuelve a parsearlos con Python
  para la demo, mostrando cada primitive "bonito".

❗ IMPORTANTE:
   – Si cambias el DDL o el orden de streams / eventos, reinicia
     el servidor CORE C++ para evitar desajustes de IDs.
   – Variables de conexión al inicio.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import copy
import re
import json
from datetime import datetime # Importado para formatear timestamps
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
# Importar la nueva declaración de stream único
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
CORE_QUERY_INITIAL_PORT = 5002 # Puerto inicial por defecto para los handlers de query

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

# Actualizar EMOJI para reflejar el stream unificado
EMOJI = {
    "BlueskyEvents.CreatePost": "📝",
    "BlueskyEvents.CreateLike": "❤️",
    "BlueskyEvents.CreateRepost": "🔁",
}

def _now_str() -> str:
    """Timestamp en formato ISO-like (sin microsegundos) para logs."""
    return time.strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str, *, level: str = "INFO") -> None:
    """Genera logs homogéneos con colores."""
    colors = {
        "INFO": "92",   # verde
        "WARN": "93",   # amarillo
        "ERROR": "91",  # rojo
        "DEBUG": "94",  # azul
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
EVENT_ID_TO_NAME: Dict[int, str] = {} # Mapea ID de evento numérico a su nombre completo (Stream.Evento)
SUBSCRIBED_QUERY_MAP: Dict[int, str] = {} # Mapea puerto del handler al alias de la query
QUERY_EVENT_COUNT = 0 # Contador global de ComplexEvents recibidos

# ---------------------------------------------------------------------
#  Handler de Resultados de Queries (Modificado para Limpieza)
# ---------------------------------------------------------------------
def create_query_handler(alias_for_closure: str):
    """
    Factory para crear handlers de query.
    Cada handler captura el 'alias_for_closure' para identificar la query en los logs.
    NOTA IMPORTANTE: Debido a que CallbackHandler::event_handler en C++ es estático,
    solo el callback Python establecido por la ÚLTIMA llamada a set_event_handler
    estará activo para TODOS los puertos de query suscritos por este proceso.
    El 'effective_alias' será, por tanto, el de esa última query.
    """
    def query_handler(enumerator: _pycore.PyEventEnumerator) -> None:
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
            ce_info: Dict[str, Any] = {
                "n": QUERY_EVENT_COUNT,
                "alias": effective_alias,
                "core_ts": ce_ts,
                "raw": raw_ce,
                "primitives": [],
                "errors": [],
            }

            for pe_id, pe_attrs in re.findall(
                r"\(id:\s*(\d+)\s*attributes:\s*\[(.*?)\]\)", primitives_blob, re.DOTALL
            ):
                pe_id_int = int(pe_id)
                fqn = EVENT_ID_TO_NAME.get(pe_id_int, f"UnknownEventId.{pe_id_int}")
                parsed, err = parse_event_attributes(fqn, pe_attrs)

                if "commit_time" in parsed and isinstance(parsed["commit_time"], float):
                    try:
                        commit_time_seconds = parsed["commit_time"] / 1e9 # Convertir ns a s
                        dt_object = datetime.fromtimestamp(commit_time_seconds)
                        parsed["commit_time_readable"] = dt_object.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"
                    except Exception as e_time:
                        parsed["commit_time_readable"] = f"Error formateando tiempo: {e_time}"

                ce_info["primitives"].append(
                    {"id": pe_id_int, "type": fqn, "attr": parsed}
                )
                if err:
                    ce_info["errors"].extend(err)
            batch_results.append(ce_info)

        for ce_data in batch_results:
            header = (
                color(f"🎯 {ce_data['alias']}", "95")
                + " │ "
                + color(f"📦 CE#{ce_data['n']:>4}", "93")
                + " │ prims="
                + color(str(len(ce_data['primitives'])), "96")
                + " │ ts="
                + ce_data['core_ts'].split(",")[0]
            )
            print(header)
            for prim in ce_data["primitives"]:
                _pp(prim["type"], prim["attr"])
            if ce_data["errors"]:
                log(
                    f"⚠️  ComplexEvent #{ce_data['n']} (alias: {ce_data['alias']}) tuvo errores de parseo "
                    f"({len(ce_data['errors'])})",
                    level="WARN",
                )
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
    """
    Declara el stream único de Bluesky en el servidor CORE+.
    Si el stream ya existe, intenta un fallback para popular EVENT_ID_TO_NAME
    asumiendo que los IDs de los eventos son estables (0, 1, 2...).
    Es crucial reiniciar el servidor CORE+ para asegurar IDs consistentes si se modifica el DDL.
    Retorna un mapeo de nombre de stream a PyStreamInfo.
    """
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
    except _pycore.PyStreamNameAlreadyDeclaredException: # type: ignore
        log(f"  • Stream «{stream_name}» ya existía. Intentando fallback para IDs...", level="WARN")
        # Fallback si el stream ya existe (asume IDs de evento estables 0, 1, 2)
        stream_id = 0 # Asumimos que el primer stream declarado tiene ID 0
        event_details = [
            {"name": "CreatePost", "id": 0},
            {"name": "CreateLike", "id": 1},
            {"name": "CreateRepost", "id": 2}
        ]
        mock_event_infos = []
        log(f"      – Fallback: Asumiendo Stream «{stream_name}» con ID={stream_id}")
        for ev_detail in event_details:
            event_id_global = ev_detail["id"]
            event_name = ev_detail["name"]
            EVENT_ID_TO_NAME[event_id_global] = f"{stream_name}.{event_name}"
            log(f"      – Fallback: Asumiendo Evento «{event_name}» ID global={event_id_global}")
            mock_event_infos.append(_pycore.PyEventInfo(event_id_global, event_name, []))

        declared_streams_info[stream_name] = _pycore.PyStreamInfo(stream_id, stream_name, mock_event_infos)

    except Exception as exc:
        log(f"Fallo declarando/obteniendo stream {stream_name}: {type(exc).__name__}: {exc}", level="ERROR")

    return declared_streams_info


def map_handler_ids(
    handler_template: Dict[str, Any],
    stream_infos_map: Dict[str, _pycore.PyStreamInfo],
) -> Dict[str, Any]:
    """
    Mapea los $type de Bluesky a los stream_id y event_id de CORE+
    basándose en la información de los streams declarados/obtenidos.
    """
    populated_handlers = copy.deepcopy(handler_template)
    all_mapped_ok = True
    log("Mapeando IDs de $type de Bluesky a IDs de Stream/Evento de CORE+...")

    for bsky_type, handler_cfg in populated_handlers.items():
        stream_name_needed = handler_cfg["stream_name"]
        event_name_needed = handler_cfg["event_name"]

        core_stream_info = stream_infos_map.get(stream_name_needed)

        if not core_stream_info:
            log(f"Error mapeando: No se encontró StreamInfo para «{stream_name_needed}» (necesario para $type {bsky_type})", level="ERROR")
            all_mapped_ok = False
            continue

        found_event_id_in_core = None
        core_event_infos = core_stream_info.events_info if hasattr(core_stream_info, 'events_info') and core_stream_info.events_info is not None else []

        for core_event_info in core_event_infos:
            if core_event_info.name == event_name_needed:
                found_event_id_in_core = core_event_info.id
                break

        if found_event_id_in_core is not None:
            handler_cfg["stream_id"] = core_stream_info.id
            handler_cfg["event_id"] = found_event_id_in_core
        else:
            log(f"Error mapeando: Evento «{event_name_needed}» no encontrado en Stream «{stream_name_needed}» (para $type {bsky_type})", level="ERROR")
            all_mapped_ok = False

    status_msg = "✅ Todos los handlers mapeados exitosamente." if all_mapped_ok else "❌ Falló el mapeo para algunos handlers."
    log(f"Mapeo de handlers completado. Estado: {status_msg}")
    for bsky_type, cfg in populated_handlers.items():
        log(
            f"   {bsky_type:<24} → stream_id={cfg.get('stream_id', 'N/A')}, "
            f"event_id={cfg.get('event_id', 'N/A')}, creator={cfg['attribute_creator'].__name__}",
            level="DEBUG" if cfg.get('stream_id') is not None and cfg.get('event_id') is not None else "WARN",
        )
    return populated_handlers

def add_queries_and_subscribe(
    client: _pycore.PyClient,
    queries_to_add: List[Tuple[str, str]], # Lista de (alias, sql_string)
    query_base_port: int, # Puerto inicial para el primer handler de query
) -> List[_pycore.PyQueryHandler]:
    """
    Añade queries al servidor CORE+ y suscribe handlers Python para sus resultados.
    Debido a la naturaleza estática del event_handler en C++, todas las queries
    terminarán usando el callback Python establecido para la última query en la lista.
    """
    global SUBSCRIBED_QUERY_MAP
    SUBSCRIBED_QUERY_MAP.clear()

    if not queries_to_add:
        log("No hay queries para añadir/suscribir.", level="INFO")
        return []

    active_queries = [(alias, sql.strip()) for alias, sql in queries_to_add
                      if sql.strip() and not sql.lstrip().startswith("--")]
    if not active_queries:
        log("No hay queries activas después del filtrado.", level="INFO")
        return []

    successfully_added_queries_aliases: List[str] = []
    for alias, sql_query in active_queries:
        try:
            client.add_query(sql_query)
            log(f"Query «{alias}» añadida al cliente CORE+: {sql_query.splitlines()[0][:60]} …")
            successfully_added_queries_aliases.append(alias)
        except Exception as e:
            log(f"Error añadiendo query «{alias}» al cliente CORE+: {type(e).__name__}: {e}", level="ERROR")

    if not successfully_added_queries_aliases:
        log("Ninguna query fue añadida exitosamente. No se suscribirán handlers.", level="WARN")
        return []

    ordered_queries_for_subscription = [
        (alias, sql) for alias, sql in active_queries
        if alias in successfully_added_queries_aliases
    ]

    num_queries_to_subscribe = len(ordered_queries_for_subscription)
    exclusive_final_port_cpp = query_base_port + num_queries_to_subscribe

    log(f"Intentando suscribir {num_queries_to_subscribe} handler(s). "
        f"Rango de puertos en Python (inclusive): {query_base_port} a {exclusive_final_port_cpp - 1}. "
        f"Argumento final_port para C++ (exclusive): {exclusive_final_port_cpp}")

    python_query_handlers_list: List[_pycore.PyQueryHandler] = []
    if num_queries_to_subscribe > 0:
        try:
            python_query_handlers_list = _pycore.subscribe_to_queries(client, query_base_port, exclusive_final_port_cpp)
            log(f"Llamada a _pycore.subscribe_to_queries devolvió {len(python_query_handlers_list)} objeto(s) handler.")
        except Exception as e:
            log(f"Error crítico llamando a _pycore.subscribe_to_queries: {type(e).__name__}: {e}", level="ERROR")
            return []

    if len(python_query_handlers_list) != num_queries_to_subscribe:
        log(f"Error de suscripción: Se esperaban {num_queries_to_subscribe} handlers pero se obtuvieron {len(python_query_handlers_list)}.", level="ERROR")
        return []

    log(f"Handlers obtenidos ({len(python_query_handlers_list)}), asignando callbacks Python...")
    for idx, (alias, _) in enumerate(ordered_queries_for_subscription):
        current_handler_port = query_base_port + idx
        try:
            python_query_handlers_list[idx].set_event_handler(create_query_handler(alias))
            SUBSCRIBED_QUERY_MAP[current_handler_port] = alias
            log(f"Suscrito handler para «{alias}» en puerto {current_handler_port}. "
                f"(Callback Python para alias '{alias}' establecido, podría ser sobrescrito).")
        except Exception as e:
            log(f"Error configurando el callback para «{alias}» en puerto {current_handler_port}: {type(e).__name__}: {e}", level="ERROR")

    if num_queries_to_subscribe > 0 :
        log("NOTA: Debido al handler estático en C++, todos los eventos de query probablemente usarán el callback Python "
            f"configurado para el ÚLTIMO alias («{ordered_queries_for_subscription[-1][0]}»).", level="WARN")
    return python_query_handlers_list

def init_core_streamer(host: str, port: int) -> _pycore.PyStreamer:
    log(f"Inicializando PyStreamer → {host}:{port}")
    streamer = _pycore.PyStreamer(host, port)
    log("Streamer CORE listo ✅")
    return streamer

# ---------------------------------------------------------------------
#  MAIN
# ---------------------------------------------------------------------
async def main() -> None:
    """
    Función principal: Inicializa cliente CORE+, declara streams, configura queries,
    e inicia el listener de Bluesky para enviar eventos a CORE+.
    """
    banner = (
        "\n" + "*" * 80
        + "\n" + color("🚀  MAIN_BLUESKY.PY iniciado – demo integración CORE+", "92")
        + "\n" + "*" * 80
    )
    print(banner)

    client = init_core_client(CORE_HOST, CORE_SERVER_PORT)

    stream_infos_map = declare_bluesky_streams(client)

    if not EVENT_ID_TO_NAME and not stream_infos_map:
        log("FATAL: EVENT_ID_TO_NAME está vacío y no se pudo obtener/declarar StreamInfo. "
            "Asegúrate de que el servidor CORE+ esté limpio (reiniciado).",
            level="ERROR")
        return
    if not stream_infos_map and EVENT_ID_TO_NAME: # Si solo tenemos el fallback
        log("Advertencia: Usando información de stream/evento de fallback. "
            "Asegúrate de que los IDs (0,1,2) son correctos para tus eventos.", level="WARN")

    handlers_config = map_handler_ids(BLUESKY_EVENT_HANDLERS_CONFIG, stream_infos_map)
    if not all(cfg.get("stream_id") is not None and cfg.get("event_id") is not None
               for cfg in handlers_config.values()):
        log("FATAL: Mapeo de handlers incompleto. Algunos stream_id o event_id son None.", level="ERROR")
        return

    # --- QUERIES DE DEMOSTRACIÓN (Actualizadas para el stream único) ---
    queries_demo = [
        ("POSTS_EN_ESPANOL",
         # Seleccionamos el evento CreatePost del stream unificado
         "SELECT P FROM BlueskyEvents WHERE CreatePost AS P FILTER P[langs = 'es']"
        ),

        #("POSTS_CON_IMAGENES",
        # "SELECT P FROM BlueskyEvents WHERE CreatePost AS P FILTER P[embed_image_count > 0]"
        #),

        #("LIKES_SIN_FILTRO",
        # "SELECT L FROM BlueskyEvents WHERE CreateLike AS L"
        #),

        # ("REPOSTS_CON_LIMIT",
        #  "SELECT R FROM BlueskyEvents WHERE CreateRepost AS R LIMIT 5"
        #  # NOTA: LIMIT sigue siendo anómalo en CORE+, pero la query es válida.
        # ),
    ]

    query_python_handlers = add_queries_and_subscribe(client, queries_demo, CORE_QUERY_INITIAL_PORT)
    if not query_python_handlers and queries_demo:
        log("Fallo crítico: No se suscribieron handlers para las queries.", level="ERROR")

    # Aplicar la opción de cuarentena al stream único
    try:
        quarantine_option_str = QUARANTINE_TEMPLATE.replace("{S}", "BlueskyEvents")
        client.declare_option(quarantine_option_str)
        log("Opción de cuarentena aplicada a: BlueskyEvents")
        log(f"DDL de Cuarentena: {quarantine_option_str.strip()}")
    except Exception as e:
        log(f"Error aplicando opción de cuarentena: {type(e).__name__}: {e}", level="ERROR")

    streamer = init_core_streamer(CORE_HOST, CORE_STREAMER_PORT)

    log("Iniciando listener de WebSocket para Bluesky Firehose …")
    try:
        await bluesky_websocket_listener(streamer, handlers_config, _pycore)
    except asyncio.CancelledError:
        log("Listener de Bluesky cancelado gracefully.", level="INFO")
    except Exception as exc:
        log(f"Error crítico en el listener de Bluesky: {type(exc).__name__}: {exc}", level="ERROR")
        raise
    finally:
        log("Listener de Bluesky finalizado o interrumpido.")


    log(f"Fin de ejecución principal. Total complex events recibidos y procesados por Python: {QUERY_EVENT_COUNT}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Interrupción por teclado (Ctrl+C) recibida – abortando script.", level="INFO")
    except Exception as exc:
        log(f"Error fatal no capturado en __main__: {type(exc).__name__}: {exc}", level="ERROR")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        log("Script principal finalizado (bloque __main__ finally).")