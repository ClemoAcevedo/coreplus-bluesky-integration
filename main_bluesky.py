"""
Integración Bluesky → CORE+ — VERSIÓN MEJORADA

• Declara streams / opciones / queries en el servidor CORE C++.
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
from bluesky.streams import (
    BLUESKY_POST_STREAM_DECLARATION,
    BLUESKY_LIKE_STREAM_DECLARATION,
    BLUESKY_REPOST_STREAM_DECLARATION,
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

EMOJI = {
    "BlueskyPosts.CreatePost": "📝",
    "BlueskyLikes.CreateLike": "❤️",
    "BlueskyReposts.CreateRepost": "🔁",
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
    # Copia para no modificar el original si se añade commit_time_readable aquí
    # Aunque ahora se hace en create_query_handler antes de llamar a _pp
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
        effective_alias = alias_for_closure # Será el alias de la última query para la que se llamó set_event_handler

        # Materializa el enumerador para verificar si está vacío y evitar imprimir si no hay datos.
        # Esto también permite saber cuántos CEs llegaron en esta invocación.
        complex_events_list = list(enumerator)

        if not complex_events_list:
            # Si no hay ComplexEvents, no imprimimos el "HANDLER INVOKED" para mantener el output limpio.
            # Se podría añadir un log de DEBUG aquí si se desea rastrear estas invocaciones vacías.
            # log(f"Handler para alias '{effective_alias}' invocado, pero sin ComplexEvents.", level="DEBUG")
            return

        # Imprime el encabezado del handler solo si hay CEs.
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
                
                # Formatear commit_time si existe
                if "commit_time" in parsed and isinstance(parsed["commit_time"], float):
                    try:
                        commit_time_seconds = parsed["commit_time"] / 1e9 # Convertir ns a s
                        dt_object = datetime.fromtimestamp(commit_time_seconds)
                        # Formato con milisegundos y UTC
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
                + ce_data['core_ts'].split(",")[0] # Mostrar solo el primer timestamp del CE
            )
            print(header)
            for prim in ce_data["primitives"]:
                _pp(prim["type"], prim["attr"]) # _pp ahora mostrará commit_time_readable
            if ce_data["errors"]:
                log(
                    f"⚠️  ComplexEvent #{ce_data['n']} (alias: {ce_data['alias']}) tuvo errores de parseo "
                    f"({len(ce_data['errors'])})",
                    level="WARN",
                )
                for e_idx, e_msg in enumerate(ce_data["errors"]):
                    log(f"   – Error {e_idx+1}: {e_msg}", level="WARN")
            print() # Separador visual
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
    Declara los streams de Bluesky en el servidor CORE+.
    Si un stream ya existe (PyStreamNameAlreadyDeclaredException), intenta un fallback
    para popular EVENT_ID_TO_NAME asumiendo IDs estables (0, 1, 2).
    Esto es un workaround debido a la ausencia de un método 'get_stream_info' en PyClient.
    Es crucial reiniciar el servidor CORE+ para asegurar IDs consistentes si se modifica el DDL.
    Retorna un mapeo de nombre de stream a PyStreamInfo para los streams exitosamente declarados/recuperados.
    """
    global EVENT_ID_TO_NAME
    EVENT_ID_TO_NAME.clear()
    ddl_map = {
        "BlueskyPosts": BLUESKY_POST_STREAM_DECLARATION,
        "BlueskyLikes": BLUESKY_LIKE_STREAM_DECLARATION,
        "BlueskyReposts": BLUESKY_REPOST_STREAM_DECLARATION,
    }
    declared_streams_info: Dict[str, _pycore.PyStreamInfo] = {}
    log("Declarando streams Bluesky …")
    for s_name, ddl in ddl_map.items():
        try:
            info = client.declare_stream(ddl)
            declared_streams_info[info.name] = info
            log(f"  • Stream «{info.name}» ID={info.id} declarado exitosamente.")
            for ev in info.events_info:
                EVENT_ID_TO_NAME[ev.id] = f"{info.name}.{ev.name}"
                log(f"      – Evento «{ev.name}» ID={ev.id}")
        except _pycore.PyStreamNameAlreadyDeclaredException: # type: ignore
            log(f"  • Stream «{s_name}» ya existía. Intentando fallback para IDs...", level="WARN")
            # Fallback si el stream ya existe (asume IDs estables 0,1,2 y nombres de evento fijos)
            # Esto es frágil y depende de que el servidor CORE+ no se haya reiniciado con un catálogo diferente.
            stream_id_map = {"BlueskyPosts": 0, "BlueskyLikes": 1, "BlueskyReposts": 2}
            event_details_map = {
                "BlueskyPosts": {"name": "CreatePost", "id_offset": 0}, # Asumimos que el ID del evento es el mismo que el del stream
                "BlueskyLikes": {"name": "CreateLike", "id_offset": 0}, # si solo hay un tipo de evento por stream.
                "BlueskyReposts": {"name": "CreateRepost", "id_offset": 0}
            }
            if s_name in stream_id_map:
                s_id = stream_id_map[s_name]
                e_name = event_details_map[s_name]["name"]
                # Asumiendo que el event_id para estos streams de un solo evento es el mismo que el stream_id
                # o un offset predecible (aquí, 0 para el primer/único evento).
                # En _pycore, el EventID es único globalmente, no relativo al stream.
                # Los IDs 0, 1, 2 son para los eventos según el orden de declaración de streams.
                event_id_global = s_id # Esta es una suposición fuerte basada en los logs anteriores.
                EVENT_ID_TO_NAME[event_id_global] = f"{s_name}.{e_name}"
                log(f"      – Fallback: Asumiendo Evento «{e_name}» ID global={event_id_global} para Stream «{s_name}» ID={s_id}")
                
                # Crear un PyStreamInfo mock para que map_handler_ids pueda funcionar.
                # Atributos de EventInfo no son cruciales para map_handler_ids, solo nombre e id.
                mock_event_info = _pycore.PyEventInfo(event_id_global, e_name, []) 
                declared_streams_info[s_name] = _pycore.PyStreamInfo(s_id, s_name, [mock_event_info])
            else:
                log(f"      – Fallback: No se conoce la estructura de ID para «{s_name}»", level="ERROR")
        except Exception as exc:
            log(f"Fallo declarando/obteniendo stream {s_name}: {type(exc).__name__}: {exc}", level="ERROR")
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
        # Asegurarse de que core_stream_info.events_info es iterable
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
) -> List[_pycore.PyQueryHandler]: # El tipo real de _pycore.PyQueryHandler puede variar
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

    # Filtrar queries comentadas o vacías
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

    # Reconstruir la lista de queries que realmente se añadieron, manteniendo el orden
    ordered_queries_for_subscription = [
        (alias, sql) for alias, sql in active_queries 
        if alias in successfully_added_queries_aliases
    ]

    num_queries_to_subscribe = len(ordered_queries_for_subscription)
    # El argumento final_port para la función C++ subscribe_to_queries es exclusivo.
    exclusive_final_port_cpp = query_base_port + num_queries_to_subscribe

    log(f"Intentando suscribir {num_queries_to_subscribe} handler(s). "
        f"Rango de puertos en Python (inclusive): {query_base_port} a {exclusive_final_port_cpp - 1}. "
        f"Argumento final_port para C++ (exclusive): {exclusive_final_port_cpp}")

    python_query_handlers_list: List[_pycore.PyQueryHandler] = []
    if num_queries_to_subscribe > 0:
        try:
            # Llamada corregida, usando el puerto final exclusivo para la función C++
            python_query_handlers_list = _pycore.subscribe_to_queries(client, query_base_port, exclusive_final_port_cpp)
            log(f"Llamada a _pycore.subscribe_to_queries devolvió {len(python_query_handlers_list)} objeto(s) handler.")
        except Exception as e:
            log(f"Error crítico llamando a _pycore.subscribe_to_queries: {type(e).__name__}: {e}", level="ERROR")
            return [] 

    if len(python_query_handlers_list) != num_queries_to_subscribe:
        log(f"Error de suscripción: Se esperaban {num_queries_to_subscribe} handlers pero se obtuvieron {len(python_query_handlers_list)}.", level="ERROR")
        return [] # No continuar si la cantidad de handlers no coincide

    log(f"Handlers obtenidos ({len(python_query_handlers_list)}), asignando callbacks Python...")
    for idx, (alias, _) in enumerate(ordered_queries_for_subscription):
        current_handler_port = query_base_port + idx
        try:
            # IMPORTANTE: Esto establece CallbackHandler::event_handler (estático en C++).
            # Cada llamada sobrescribe la anterior. El 'alias' capturado por create_query_handler
            # que finalmente se usará para TODOS los eventos será el de la ÚLTIMA query en este bucle.
            python_query_handlers_list[idx].set_event_handler(create_query_handler(alias))
            SUBSCRIBED_QUERY_MAP[current_handler_port] = alias # Guardamos para referencia, aunque el handler sea "global"
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

    # Inicializar cliente CORE+
    # Asegúrate de que el servidor CORE+ C++ esté corriendo en CORE_HOST:CORE_SERVER_PORT
    client = init_core_client(CORE_HOST, CORE_SERVER_PORT)

    # Declarar streams. Esto también poblará EVENT_ID_TO_NAME.
    # Es crucial que esto funcione correctamente. Si los streams ya existen y el servidor
    # no fue reiniciado, el fallback en declare_bluesky_streams intentará manejarlo.
    stream_infos_map = declare_bluesky_streams(client) 
    
    # Verificar si la información esencial de streams/eventos se pudo cargar/declarar
    if not EVENT_ID_TO_NAME and not stream_infos_map:
        log("FATAL: EVENT_ID_TO_NAME está vacío y no se pudo obtener/declarar StreamInfo. "
            "Esto puede ocurrir si los streams ya existían y el fallback falló, o si hubo un error de declaración. "
            "Asegúrate de que el servidor CORE+ esté limpio (reiniciado) o que los IDs sean consistentes.", 
            level="ERROR")
        return
    if not stream_infos_map and EVENT_ID_TO_NAME: # Si solo tenemos el fallback
        log("Advertencia: Usando información de stream/evento de fallback. "
            "Asegúrate de que los IDs (0,1,2) son correctos para tus streams.", level="WARN")


    # Mapear $types de Bluesky a IDs de stream/evento de CORE+ para el listener
    handlers_config = map_handler_ids(BLUESKY_EVENT_HANDLERS_CONFIG, stream_infos_map)
    if not all(cfg.get("stream_id") is not None and cfg.get("event_id") is not None 
               for cfg in handlers_config.values()):
        log("FATAL: Mapeo de handlers incompleto. Algunos stream_id o event_id son None. "
            "Verifica la declaración de streams y la lógica de fallback.", level="ERROR")
        return

    # --- QUERIES DE DEMOSTRACIÓN EXTENDIDA ---
    # Descomenta las queries que quieras probar. Para ver el alias correcto en el output del handler,
    # prueba una query a la vez debido al handler estático en C++.
    # Si pruebas múltiples, el alias mostrado será el de la última query en esta lista.
    queries_demo = [
        # === QUERIES QUE FUNCIONAN CORRECTAMENTE (Según tests aislados) ===
        ("POSTS_EN_ESPANOL",
         "SELECT P FROM BlueskyPosts WHERE CreatePost AS P FILTER P[langs = 'es']"
         # Objetivo: Detectar posts cuyo único idioma declarado sea 'es' (español).
         # Funcionalidad: CORRECTA. El filtro de igualdad de string P[langs = 'es'] funciona.
        ),

        # ("POSTS_EN_INGLES",
        #  "SELECT P FROM BlueskyPosts WHERE CreatePost AS P FILTER P[langs = 'en']"
        #  # Objetivo: Detectar posts cuyo único idioma declarado sea 'en' (inglés).
        #  # Funcionalidad: CORRECTA. El filtro de igualdad de string P[langs = 'en'] funciona.
        # ),

        # ("POSTS_CON_IMAGENES",
        #  "SELECT P FROM BlueskyPosts WHERE CreatePost AS P FILTER P[embed_image_count > 0]"
        #  # Objetivo: Encontrar posts que contengan al menos una imagen.
        #  #           Utiliza el atributo 'embed_image_count' (int).
        #  # Funcionalidad: CORRECTA. El filtro numérico P[embed_image_count > 0] funciona para BlueskyPosts.
        # ),

        # ("LIKES_SIN_FILTRO_NI_LIMIT",
        #  "SELECT L FROM BlueskyLikes WHERE CreateLike AS L"
        #  # Objetivo: Obtener todos los eventos de 'CreateLike' que lleguen al stream 'BlueskyLikes'.
        #  #           Sirve como base para verificar la selección de este tipo de evento.
        #  # Funcionalidad: CORRECTA. La selección base de CreateLike desde BlueskyLikes funciona.
        # ),

        # === QUERIES CON COMPORTAMIENTO ANÓMALO O BUGS EN CORE+ ===
        # ("PING_POST_CON_LIMIT",
        #  "SELECT P FROM BlueskyPosts WHERE CreatePost AS P LIMIT 1"
        #  # Objetivo: Detectar el PRIMER evento 'CreatePost' que llegue.
        #  # Funcionalidad: INCORRECTA/ANÓMALA. La query funciona y selecciona CreatePost,
        #  #                pero la cláusula `LIMIT 1` no restringe la salida a un solo
        #  #                ComplexEvent. Sigue emitiendo resultados. Bug en CORE+.
        # ),
        # ("LIKES_CON_LIMIT_SOLAMENTE",
        #  "SELECT L FROM BlueskyLikes WHERE CreateLike AS L LIMIT 1"
        #  # Objetivo: Detectar el PRIMER evento 'CreateLike' que llegue.
        #  # Funcionalidad: INCORRECTA/ANÓMALA. `LIMIT 1` no restringe la salida. Bug en CORE+.
        # ),
        # ("LIKES_CON_FILTRO_SEQ_POSITIVO",
        #  "SELECT L FROM BlueskyLikes WHERE CreateLike AS L FILTER L[seq > 0]"
        #  # Objetivo: Detectar 'CreateLike' con 'seq' > 0.
        #  # Funcionalidad: INCORRECTA/ANÓMALA. El handler Python es invocado pero sin ComplexEvents.
        #  #                Indica un problema en CORE+ con este filtro numérico específico o la finalización del CE.
        # ),

        # === QUERIES QUE CAUSAN CRASH EN EL SERVIDOR CORE+ ===
        # ("LIKES_CON_FILTRO_SEQ_NEGATIVO_CRASH",
        #  "SELECT L FROM BlueskyLikes WHERE CreateLike AS L FILTER L[seq > -100]"
        #  # Objetivo: (TEST FALLIDO) Demostrar filtro con número negativo.
        #  # Funcionalidad: CRASH DEL SERVIDOR CORE+ ("visit Negation not implemented"). Imposible de usar.
        # ),
    ]
    # --- FIN QUERIES DEMOSTRACIÓN ---

    query_python_handlers = add_queries_and_subscribe(client, queries_demo, CORE_QUERY_INITIAL_PORT)
    if not query_python_handlers and queries_demo: 
        log("Fallo crítico: No se suscribieron handlers para las queries. "
            "El procesamiento de resultados de queries no funcionará.", level="ERROR")
        # Considerar si se debe continuar si las queries son esenciales para la demo.
        # return 

    # Aplicar la opción de cuarentena a todos los streams definidos
    all_stream_names_for_quarantine = ["BlueskyPosts", "BlueskyLikes", "BlueskyReposts"]
    try:
        quarantine_option_str = QUARANTINE_TEMPLATE.replace("{S}", " , ".join(all_stream_names_for_quarantine))
        client.declare_option(quarantine_option_str)
        log(f"Opción de cuarentena aplicada a: {', '.join(all_stream_names_for_quarantine)}")
        log(f"DDL de Cuarentena: {quarantine_option_str.strip()}")
    except Exception as e:
        log(f"Error aplicando opción de cuarentena: {type(e).__name__}: {e}", level="ERROR")

    # Inicializar el streamer para enviar datos a CORE+
    streamer = init_core_streamer(CORE_HOST, CORE_STREAMER_PORT)

    log("Iniciando listener de WebSocket para Bluesky Firehose …")
    try:
        # El listener consumirá eventos de Bluesky y los enviará a CORE+ vía el streamer.
        await bluesky_websocket_listener(streamer, handlers_config, _pycore)
    except asyncio.CancelledError:
        log("Listener de Bluesky cancelado gracefully.", level="INFO")
    except Exception as exc:
        log(f"Error crítico en el listener de Bluesky: {type(exc).__name__}: {exc}", level="ERROR")
        raise 
    finally:
        log("Listener de Bluesky finalizado o interrumpido.")
        # Aquí se podrían añadir llamadas de cierre para streamer y client si _pycore las proveyera
        # y si fueran necesarias/seguras de llamar en un contexto async/finally.
        # Ejemplo conceptual:
        # if 'streamer' in locals() and hasattr(streamer, 'close'): streamer.close()
        # if 'client' in locals() and hasattr(client, 'close'): client.close()
        # Esto es importante para intentar evitar los errores de ZMQ al salir.

    log(f"Fin de ejecución principal. Total complex events recibidos y procesados por Python: {QUERY_EVENT_COUNT}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Interrupción por teclado (Ctrl+C) recibida – abortando script.", level="INFO")
    except Exception as exc: # Captura otras excepciones inesperadas en el nivel más alto.
        log(f"Error fatal no capturado en __main__: {type(exc).__name__}: {exc}", level="ERROR")
        import traceback
        traceback.print_exc()
        sys.exit(1) # Salir con código de error si hay una falla fatal.
    finally:
        # Este log se imprimirá siempre, incluso después de Ctrl+C o excepciones.
        log("Script principal finalizado (bloque __main__ finally).")