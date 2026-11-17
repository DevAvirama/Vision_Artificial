"""
Daemon de Monitoreo Autónomo y Pesaje Continuo por RTSP.
Se conecta al stream de la cámara del corral en vivo, detecta automáticamente
cuando un cerdo entra y está centrado bajo la cámara (validar_bordes=True),
estima su peso con XGBoost/ArUco, guarda evidencia fotográfica y registra
los pesajes en un archivo CSV cronológico ('pesajes_diarios.csv').

Uso:
    python worker_rtsp_watcher.py --rtsp "rtsp://admin:admin123@192.168.1.108:554/cam/realmonitor?channel=1&subtype=0"

Registro opcional en porcitech-backend (POST /api/v1/pesajes/manual, requiere JWT):
    python worker_rtsp_watcher.py --rtsp "..." \
        --webhook http://localhost:8000/api/v1/pesajes/manual \
        --token "$PORCITECH_TOKEN" --id-cerdo <UUID> --corral-id <UUID>
"""

import os
import re
import sys
import time
import uuid
import base64
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import httpx

# Carpeta donde se almacenan las fotos anotadas de cada pesaje
CARPETA_EVIDENCIAS = Path("evidencias_pesajes")
ARCHIVO_HISTORICO_CSV = Path("pesajes_diarios.csv")


def mask_rtsp_url(url: str) -> str:
    """Oculta usuario y contraseña de la URL RTSP en los mensajes de consola."""
    return re.sub(r"(rtsp://)[^/@]+@", r"\1***:***@", url or "")


def rotar_evidencias(max_archivos: int) -> None:
    """Conserva solo las `max_archivos` fotos más recientes en la carpeta de evidencias."""
    if max_archivos <= 0:
        return
    fotos = sorted(CARPETA_EVIDENCIAS.glob("pesaje_*.jpg"), key=lambda f: f.stat().st_mtime)
    for antigua in fotos[:-max_archivos]:
        try:
            antigua.unlink()
        except OSError:
            pass


def enviar_pesaje_backend(
    client: httpx.Client,
    webhook_url: str,
    token: str,
    id_cerdo: str,
    corral_id: str,
    peso: float,
    conf: float,
    algoritmo: str,
    fecha_iso: str,
) -> None:
    """Registra el pesaje con el contrato PesajeManualCreate del backend (metodo='vision_ai')."""
    payload = {
        "id_cerdo": id_cerdo,
        "corral_id": corral_id,
        "peso_kg": round(float(peso), 2),
        "metodo": "vision_ai",
        "tiempo": fecha_iso,
        "observaciones": f"Watcher RTSP | conf {conf * 100:.1f}% | {algoritmo}",
        "id_local": str(uuid.uuid4()),
    }
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = client.post(webhook_url, json=payload, headers=headers, timeout=8.0)
    except Exception as e:
        print(f"[Aviso] No se pudo contactar el backend: {e}")
        return
    if resp.status_code in (200, 201):
        print("         ↳ Registrado en porcitech-backend.")
    elif resp.status_code == 401:
        print("[Error] Backend respondió 401: token JWT inválido o expirado (--token / PORCITECH_TOKEN).")
    else:
        print(f"[Error] Backend respondió HTTP {resp.status_code}: {resp.text[:300]}")


def iniciar_vigilancia_rtsp(
    rtsp_url: str,
    intervalo_segundos: float = 3.0,
    umbral_confianza: float = 0.50,
    cooldown_segundos: float = 15.0,
    servidor_url: str = "http://localhost:8001/estimar-peso-rtsp",
    backend_webhook_url: Optional[str] = None,
    token: Optional[str] = None,
    id_cerdo: Optional[str] = None,
    corral_id: Optional[str] = None,
    max_evidencias: int = 500,
):
    CARPETA_EVIDENCIAS.mkdir(exist_ok=True)

    if backend_webhook_url and not (token and id_cerdo and corral_id):
        print(
            "[Aviso] --webhook requiere --token (o PORCITECH_TOKEN), --id-cerdo y --corral-id: "
            "el backend exige JWT y no puede identificar al animal solo con la cámara. "
            "El envío al backend queda DESACTIVADO; se seguirá guardando el CSV local."
        )
        backend_webhook_url = None

    # Inicializar CSV si no existe
    if not ARCHIVO_HISTORICO_CSV.exists():
        with open(ARCHIVO_HISTORICO_CSV, "w", encoding="utf-8") as f:
            f.write(
                "fecha_utc,peso_kg,confianza_ia,area_cm2,largo_cm,ancho_torax_cm,algoritmo,escala_ppc,foto_evidencia\n"
            )

    print("=========================================================")
    print("👁️  PORCITECH VISION: VIGILANCIA AUTÓNOMA RTSP ACTIVA")
    print("=========================================================")
    print(f" • Cámara RTSP:         {mask_rtsp_url(rtsp_url)}")
    print(f" • Envío a backend:     {backend_webhook_url or 'desactivado'}")
    print(f" • Microservicio:       {servidor_url}")
    print(f" • Intervalo muestreo:  Cada {intervalo_segundos}s")
    print(f" • Cooldown entre cerdos: {cooldown_segundos}s")
    print(f" • Registro histórico:  {ARCHIVO_HISTORICO_CSV.resolve()}")
    print(f" • Carpeta evidencias:  {CARPETA_EVIDENCIAS.resolve()}")
    print("=========================================================\n")
    print("Esperando que un ejemplar ingrese al corral...")

    client = httpx.Client(timeout=15.0)
    ultimo_pesaje_tiempo = 0.0

    try:
        while True:
            ahora = time.time()

            # Evitar registrar múltiples veces al mismo cerdo que se queda parado
            if (ahora - ultimo_pesaje_tiempo) < cooldown_segundos:
                time.sleep(intervalo_segundos)
                continue

            payload = {
                "rtsp_url": rtsp_url,
                "conf_threshold": umbral_confianza,
                "validar_bordes": True  # Excluye capturas donde el cerdo toque el borde
            }

            try:
                resp = client.post(servidor_url, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    peso = data["peso_estimado_kg"]
                    conf = data["confianza"]
                    area = data["area_cm2"]
                    largo = data["largo_cm"]
                    torax = data["ancho_toracico_cm"]
                    algoritmo = data["algoritmo_peso"]
                    ppc = data["pixels_per_cm_utilizado"]
                    fecha_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                    fecha_iso = datetime.now(timezone.utc).isoformat()

                    # Guardar imagen anotada
                    nombre_foto = f"pesaje_{fecha_str}_{peso:.1f}kg.jpg"
                    ruta_foto = CARPETA_EVIDENCIAS / nombre_foto
                    b64_str = data["imagen_anotada_base64"].split(",", 1)[1]
                    with open(ruta_foto, "wb") as f_img:
                        f_img.write(base64.b64decode(b64_str))
                    rotar_evidencias(max_evidencias)

                    # Guardar en histórico CSV
                    with open(ARCHIVO_HISTORICO_CSV, "a", encoding="utf-8") as f_csv:
                        f_csv.write(
                            f"{fecha_iso},{peso},{conf:.3f},{area},{largo},{torax},{algoritmo},{ppc},{nombre_foto}\n"
                        )

                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] 🐷 ¡NUEVO PESAJE REGISTRADO! "
                        f"Peso: {peso:.2f} kg | Conf: {conf*100:.1f}% | Área: {area:.1f} cm² | "
                        f"Foto: {nombre_foto}"
                    )

                    # Enviar a backend central si está configurado
                    if backend_webhook_url:
                        enviar_pesaje_backend(
                            client, backend_webhook_url, token, id_cerdo, corral_id,
                            peso, conf, algoritmo, fecha_iso,
                        )

                    ultimo_pesaje_tiempo = time.time()
                elif resp.status_code == 422:
                    # 422 indica simplemente que no hay cerdo o está cortado en el borde
                    pass
                else:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Aviso: HTTP {resp.status_code}")
            except Exception as e:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Reconectando stream... ({e})")

            time.sleep(intervalo_segundos)
    except KeyboardInterrupt:
        print("\n\n[INFO] Vigilancia RTSP detenida por el operador.")
    finally:
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daemon de Vigilancia y Pesaje Continuo RTSP")
    parser.add_argument("--rtsp", type=str, required=True, help="URL RTSP de la cámara (ej. rtsp://user:pass@IP:554/live)")
    parser.add_argument("--intervalo", type=float, default=2.5, help="Intervalo en segundos entre chequeos (default: 2.5s)")
    parser.add_argument("--cooldown", type=float, default=15.0, help="Segundos de espera para no re-pesar al mismo cerdo (default: 15s)")
    parser.add_argument("--conf", type=float, default=0.50, help="Umbral de confianza mínima YOLO (default: 0.50)")
    parser.add_argument("--servidor", type=str, default="http://localhost:8001/estimar-peso-rtsp", help="URL del endpoint de pesaje RTSP")
    parser.add_argument("--webhook", type=str, default=None, help="URL de POST /api/v1/pesajes/manual en porcitech-backend")
    parser.add_argument("--token", type=str, default=os.getenv("PORCITECH_TOKEN"), help="JWT Bearer del backend (o variable PORCITECH_TOKEN)")
    parser.add_argument("--id-cerdo", type=str, default=None, help="UUID del cerdo bajo observación")
    parser.add_argument("--corral-id", type=str, default=None, help="UUID del corral de la cámara")
    parser.add_argument("--max-evidencias", type=int, default=500, help="Fotos de evidencia a conservar (0 = sin límite)")
    args = parser.parse_args()

    iniciar_vigilancia_rtsp(
        rtsp_url=args.rtsp,
        intervalo_segundos=args.intervalo,
        umbral_confianza=args.conf,
        cooldown_segundos=args.cooldown,
        servidor_url=args.servidor,
        backend_webhook_url=args.webhook,
        token=args.token,
        id_cerdo=args.id_cerdo,
        corral_id=args.corral_id,
        max_evidencias=args.max_evidencias,
    )
