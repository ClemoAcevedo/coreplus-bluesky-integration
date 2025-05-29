# 🔵 coreplus-bluesky-integration

Este repositorio contiene los scripts y módulos necesarios para integrar eventos en tiempo real desde la red social [Bluesky](https://bsky.app) con el motor de Reconocimiento de Eventos Complejos [CORE+]([https://core.ing.uc.cl](https://github.com/CORE-cer/CORE)).

## 📂 Estructura del Proyecto

```
.
├── bluesky/
│   ├── __init__.py
│   ├── attributes.py
│   ├── bluesky_event_parser.py
│   ├── handlers.py
│   ├── listener.py
│   └── streams.py
├── main_bluesky.py
└── test_bluesky_event_parser.py
```

## 🚀 Funcionalidad

- Conexión al *firehose* de Bluesky mediante WebSocket.
- Decodificación de eventos CBOR.
- Transformación a `PyEvent` y envío al motor CORE+ vía `_pycore`.
- Declaración automática de streams y queries CEQL.
- Visualización de `ComplexEvents` en consola.

## 📦 Requisitos

- Proyecto CORE+ compilado (con módulo `_pycore` accesible).
- Python 3.8+ con los siguientes paquetes:
  - `websockets`
  - `cbor2`
  - `libipld`
  - `multiformats`
  - `python-dateutil`

## 🧪 Scripts de Ejecución

- `main_bluesky.py`: Script principal, conecta Bluesky → CORE+.
- `test_bluesky_event_parser.py`: Utilidad para parsear eventos complejos simulados.

## 🔧 Uso

```bash
# Desde la raíz del proyecto CORE+:
./build/Debug/online_server -d "" -c "" -q "" -w 9000
python3 ./build/Debug/main_bluesky.py
```
