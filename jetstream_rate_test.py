import asyncio
import websockets
import time

# usamos el URI del Firehose de Bluesky
URI = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"

# definimos un intervalo para reportar la tasa (en segs)
REPORT_INTERVAL = 60  # por ahora, reportamos cada minuto

async def measure_event_rate():
    """
    Se conecta a Bluesky Jetstream y mide la tasa de eventos recibidos.
    """
    reconnection_attempts = 0
    max_reconnection_attempts = 5 # limitamos los intentos de reconexion
    
    while reconnection_attempts < max_reconnection_attempts:
        try:
            async with websockets.connect(URI, ping_interval=20, ping_timeout=30) as websocket:
                print(f"✅ Conectado a {URI}. Midiendo tasa de eventos...")
                reconnection_attempts = 0 # reseteamos los intentos tras conexión exitosa

                event_count_interval = 0
                session_total_events = 0
                
                interval_start_time = time.monotonic()
                session_start_time = time.monotonic()

                while True:
                    try:
                        # en comparacion con el script original, solo necesitamos recibir el mensaje, no decodificarlo para este test
                        # el timeout en recv() ayuda a que el bucle no se bloquee indefinidamente si es que
                        # la conexión está viva pero no llegan mensajes (poco probable en firehose, pero mejor prevenir q lamentar)
                        # Tambien permite que el chequeo de REPORT_INTERVAL ocurra mejor
                        raw_message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        if raw_message:
                            event_count_interval += 1
                            session_total_events += 1
                    
                    except asyncio.TimeoutError:
                        # esto entra si no se recibieron mensajes en el ultimo segundo, continuamos para chequear el intervalo de reporte
                        pass
                    except websockets.exceptions.ConnectionClosed as e:
                        print(f"🔴 Conexión cerrada durante la recepción: {e}")
                        raise # propagamos para que el bucle de reconexión exterior lo maneje
                    except Exception as e:
                        print(f"🚨 Error inesperado durante la recepción: {e}")
                        # considerar si continuar o propagar, por ahora da igual
                        await asyncio.sleep(0.1) 

                    current_time = time.monotonic()
                    elapsed_in_interval = current_time - interval_start_time

                    if elapsed_in_interval >= REPORT_INTERVAL:
                        rate_this_interval = event_count_interval / elapsed_in_interval if elapsed_in_interval > 0 else 0
                        
                        session_elapsed_time = current_time - session_start_time
                        overall_session_rate = session_total_events / session_elapsed_time if session_elapsed_time > 0 else 0

                        print(f"\n--- Reporte de Tasa ({time.strftime('%Y-%m-%d %H:%M:%S')}) ---")
                        print(f"Eventos en los últimos {elapsed_in_interval:.2f} seg: {event_count_interval}")
                        print(f"Tasa promedio en este intervalo: {rate_this_interval:.2f} eventos/segundo")
                        print(f"Total eventos recibidos en sesión: {session_total_events}")
                        print(f"Tiempo total de sesión: {session_elapsed_time:.2f} segundos")
                        print(f"Tasa promedio general de sesión: {overall_session_rate:.2f} eventos/segundo")
                        print("---------------------------------------------------\n")

                        event_count_interval = 0
                        interval_start_time = current_time
        
        except websockets.exceptions.ConnectionClosed as e:
            reconnection_attempts += 1
            print(f"🔴 Conexión cerrada. Intento {reconnection_attempts}/{max_reconnection_attempts}. Reintentando en {5 * reconnection_attempts} segundos... (Error: {e})")
            if reconnection_attempts >= max_reconnection_attempts:
                print("🚨 Se alcanzó el máximo de intentos de reconexión. Saliendo.")
                break
            await asyncio.sleep(5 * reconnection_attempts) # esperamos un tiempo exponencial entre intentos de reconexión (conocido como backoff exponencial simple)
        except Exception as e:
            reconnection_attempts += 1
            print(f"🚨 Error al conectar o en el bucle principal. Intento {reconnection_attempts}/{max_reconnection_attempts}. Reintentando en {5 * reconnection_attempts} segundos... (Error: {e})")
            if reconnection_attempts >= max_reconnection_attempts:
                print("🚨 Se alcanzó el máximo de intentos de reconexión debido a errores. Saliendo.")
                break
            await asyncio.sleep(5 * reconnection_attempts)

if __name__ == "__main__":
    try:
        asyncio.run(measure_event_rate())
    except KeyboardInterrupt:
        print("\n📉 Medición de tasa interrumpida por el usuario.")
    finally:
        print("ℹ️ Script de medición de tasa finalizado.")