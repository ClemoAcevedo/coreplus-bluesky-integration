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
└── jetstream_rate_test.py
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
- `jetstream_rate_test.py`: Script autónomo que mide la tasa de eventos recibidos desde el firehose de Bluesky. Útil para estimar la carga de datos esperada y verificar conectividad básica.

### 🔍 Medición de tasa de eventos

Este script se puede ejecutar directamente desde cualquier entorno con Python 3 y `websockets` instalado, sin necesidad de CORE+:

```bash
python3 jetstream_rate_test.py
```
## 🔧 Uso

```bash
# Desde la raíz del proyecto CORE+, main_bluesky.py y test_bluesky_event_parser.py deben estar en CORE/build/Debug:
./build/Debug/online_server -d "" -c "" -q "" -w 9000
python3 ./build/Debug/main_bluesky.py
```
