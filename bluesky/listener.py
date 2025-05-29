"""
Módulo de escucha (“listener”) para el *firehose* de Bluesky.

Se conecta a `wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos`,
decodifica los frames CBOR y reenvía los eventos *post / like / repost*
a CORE+ mediante el *streamer* Python.

►  Dependencias externas:
    websockets, cbor2, libipld, multiformats           (pip)
    PyCORE ( bindings de tu motor CORE+ )               (importado por main)

El código evita bufferizar bloques innecesariamente mediante una caché
ligera basada en CID y mantiene varias métricas de ejecución para
mostrar en la consola el poder de CORE en la demo.
"""
from __future__ import annotations

import asyncio
from io import BytesIO
from typing import Any

import cbor2
import websockets
from multiformats import CID
import libipld

from .attributes import get_robust_nanosecond_timestamp_as_int

# ──────────────────────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────────────────────
BLUESKY_FIREHOSE_URI = (
    "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
)

# Caché ligera:  <cid:str>  →  <dict record_decoded>
DECODED_BLOCKS_CACHE: dict[str, dict[str, Any]] = {}

# Métricas   (se actualizan en tiempo de ejecución)
EVENTS_SENT_TO_CORE_COUNTER = 0
EVENTS_SENT_SUMMARY_THRESHOLD = 100  # «ping» cada N eventos

# ──────────────────────────────────────────────────────────
# Funciones auxiliares
# ──────────────────────────────────────────────────────────


def decode_cbor_message_frame(raw_bytes: bytes) -> tuple[dict | None, dict | None]:
    """
    Decodifica un *frame* CBOR: primero el encabezado, después el payload.

    Retorna:
        (header_dict | None, payload_dict | None)

        Si ocurre un error se encapsula en el dict con la clave
        ``_decode_error_*`` para que el llamador lo descarte elegantemente.
    """
    if not raw_bytes:
        return None, None

    stream = BytesIO(raw_bytes)
    try:
        header = cbor2.load(stream)  # primer objeto
    except Exception as exc:
        return None, {"_decode_error_header_": str(exc), "_raw_": raw_bytes}

    payload: dict | None = None
    if header.get("op") == 1 and header.get("t") == "#commit":
        try:
            payload = cbor2.load(stream)  # segundo objeto
        except Exception as exc:
            payload = {"_decode_error_payload_": str(exc)}

    return header, payload


def cid_obj_to_str(cid_obj) -> str | None:
    """
    Convierte un objeto CID con *tag* 42 a su representación multibase.

    Bluesky incluye el CID como «CBOR tag 42».

    Devuelve *None* si la conversión falla.
    """
    if (
        cid_obj
        and getattr(cid_obj, "tag", None) == 42
        and hasattr(cid_obj, "value")
    ):
        # Algunos registros llevan un byte 0x00 de relleno
        raw = cid_obj.value[1:] if cid_obj.value.startswith(b"\x00") else cid_obj.value
        try:
            return str(CID.decode(raw))
        except Exception:
            return None
    return None


# ──────────────────────────────────────────────────────────
# Listener principal
# ──────────────────────────────────────────────────────────
async def bluesky_websocket_listener(streamer, handlers, pycore) -> None:
    """
    Bucle principal:

    1. Conecta al *firehose* de Bluesky.
    2. Procesa sólo `op == "create"` de los tipos *post/like/repost*.
    3. Llama a la función *attribute_creator* correspondiente y envía el
       evento a CORE+ con ``streamer.send_stream``.
    4. Muestra métricas periódicas para la demo.
    """
    global EVENTS_SENT_TO_CORE_COUNTER, DECODED_BLOCKS_CACHE

    print("[Listener] ⏳ Conectando a Bluesky Firehose…")

    # Métricas vivas
    ws_frame_count = 0
    commit_count = 0
    post_count = like_count = repost_count = 0

    while True:
        try:
            async with websockets.connect(
                BLUESKY_FIREHOSE_URI,
                ping_interval=20,
                ping_timeout=30,
                max_size=2**22,  # 4 MiB
            ) as ws:
                print(f"[Listener] ✅ Conectado a {BLUESKY_FIREHOSE_URI}")

                async for raw in ws:
                    ws_frame_count += 1
                    if ws_frame_count % 1_000 == 0:
                        print(
                            f"[Listener] WS frames: {ws_frame_count:,}   commits: {commit_count:,}"
                        )

                    if not isinstance(raw, bytes):
                        continue

                    header, payload = decode_cbor_message_frame(raw)
                    if (
                        not header
                        or header.get("op") != 1
                        or header.get("t") != "#commit"
                        or not payload
                        or payload.get("_decode_error_header_")
                        or payload.get("_decode_error_payload_")
                    ):
                        continue

                    commit_count += 1

                    # ── 1.  Desempaquetar los bloques CAR
                    if car_bytes := payload.get("blocks"):
                        try:
                            _car_hdr, blocks = libipld.decode_car(car_bytes)
                            for cid_bytes, decoded in blocks.items():
                                try:
                                    DECODED_BLOCKS_CACHE[str(CID.decode(cid_bytes))] = decoded
                                except Exception:
                                    pass
                        except Exception:
                            pass

                    repo_did = payload.get("repo")
                    ops = payload.get("ops", [])
                    seq = payload.get("seq")

                    # ── 2.  Procesar cada operación «create»
                    for op in ops:
                        if op.get("action") != "create":
                            continue

                        cid_text = cid_obj_to_str(op.get("cid"))
                        record = DECODED_BLOCKS_CACHE.get(cid_text or "")
                        if not record:
                            continue

                        rtype = record.get("$type")
                        if rtype not in handlers:
                            continue

                        cfg = handlers[rtype]
                        attr_creator = cfg["attribute_creator"]
                        stream_id = cfg["stream_id"]
                        event_id = cfg["event_id"]

                        op_meta = dict(
                            action="create",
                            path=op.get("path", ""),
                            cid_str=cid_text,
                            repo=repo_did,
                            seq=seq,
                        )

                        try:
                            attrs = attr_creator(op_meta, record, payload, pycore)
                            if attrs is None:
                                continue

                            # Log suave para la demo
                            if EVENTS_SENT_TO_CORE_COUNTER % EVENTS_SENT_SUMMARY_THRESHOLD == 0:
                                ts_ns = get_robust_nanosecond_timestamp_as_int(
                                    record.get("createdAt"),
                                    payload.get("time"),
                                    op_meta["path"],
                                )
                                print(
                                    f"[Listener → CORE+] {cfg['stream_name']}.{cfg['event_name']:12s} "
                                    f"path={op_meta['path']:<40s} ts={ts_ns}"
                                )

                            # Enviar a CORE+
                            event = pycore.PyEvent(event_id, attrs)
                            streamer.send_stream(stream_id, event)
                            EVENTS_SENT_TO_CORE_COUNTER += 1

                            # Métricas por tipo
                            if rtype == "app.bsky.feed.post":
                                post_count += 1
                            elif rtype == "app.bsky.feed.like":
                                like_count += 1
                            elif rtype == "app.bsky.feed.repost":
                                repost_count += 1

                            # Resumen global
                            if EVENTS_SENT_TO_CORE_COUNTER % EVENTS_SENT_SUMMARY_THRESHOLD == 0:
                                print(
                                    f"[Listener] ▶︎ enviados={EVENTS_SENT_TO_CORE_COUNTER:,}  "
                                    f"posts={post_count:,} likes={like_count:,} reposts={repost_count:,}"
                                )

                        except Exception as exc:
                            print(
                                f"[Listener] ⚠️  Error procesando op create "
                                f"({rtype}) repo={repo_did}: {type(exc).__name__}: {exc}"
                            )

                    # ── 3.  Evictar caché si crece demasiado
                    if len(DECODED_BLOCKS_CACHE) > 15_000:
                        for key in list(DECODED_BLOCKS_CACHE)[:7_500]:
                            DECODED_BLOCKS_CACHE.pop(key, None)

        # ───────  Gestión de reconexiones / errores  ───────
        except KeyboardInterrupt:
            print("[Listener] ⏹  Interrumpido por el usuario.")
            break
        except (
            websockets.exceptions.ConnectionClosed,
            websockets.exceptions.ConnectionClosedError,
            websockets.exceptions.ConnectionClosedOK,
            asyncio.TimeoutError,
        ) as exc:
            print(
                f"[Listener] 🔌 WebSocket cerrado ({type(exc).__name__}: {exc}). "
                "Reintentando en 5 s…"
            )
            DECODED_BLOCKS_CACHE.clear()
            await asyncio.sleep(5)
        except Exception as exc:
            print(
                f"[Listener] ❌ Error inesperado ({type(exc).__name__}: {exc}). "
                "Reintentando en 15 s…"
            )
            import traceback

            traceback.print_exc()
            DECODED_BLOCKS_CACHE.clear()
            await asyncio.sleep(15)

    print("[Listener] ▶️  Listener finalizado.")
