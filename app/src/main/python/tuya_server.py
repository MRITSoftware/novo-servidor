import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from flask import Flask, jsonify, request
import tinytuya

APP_HOST = "0.0.0.0"
APP_PORT = 8000

DISCOVERY_TIMEOUT_SEC = 12
DISCOVERY_RETRY_COUNT = 2
DISCOVERY_RETRY_BACKOFF_SEC = 2
COMMAND_TIMEOUT_SEC = 8
DISCOVERY_CACHE_TTL_SEC = 60 * 10
HEARTBEAT_INTERVAL_SEC = 60
WATCHDOG_INTERVAL_SEC = 10
DISCOVERY_REFRESH_INTERVAL_SEC = 60 * 3
THREAD_RESTART_INTERVAL_SEC = 5

SUPABASE_URL = "https://seu-projeto.supabase.co"
SUPABASE_ANON_KEY = "sua_anon_key_aqui"
SUPABASE_REST_URL = SUPABASE_URL.rstrip("/") + "/rest/v1"
SUPABASE_TABLE = "tuya_devices"

DEFAULT_TUYA_ACCOUNTS = [
    {
        "access_id": "td7tp3cvq3nrc35emwg3",
        "access_key": "bbcdaa3dfe9545fca4326fcfa1cf3e2c",
        "endpoint": "https://openapi.tuyaus.com",
        "uid": "az1715569264750N2mUr",
    },
    {
        "access_id": "wwxsqj37wnfdnp98wu54",
        "access_key": "d7a140221f3b4e8f916601af4fbd6816",
        "endpoint": "https://openapi.tuyaus.com",
        "uid": "az1759235287550HcJRz",
    },
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

app = Flask(__name__)

_discovery_cache_lock = threading.Lock()
_discovery_cache: Dict[str, Dict[str, Any]] = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _supabase_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        "Content-Type": "application/json",
    }


def _supabase_get_device(tuya_device_id: str) -> Optional[Dict[str, Any]]:
    url = f"{SUPABASE_REST_URL}/{SUPABASE_TABLE}"
    params = {"tuya_device_id": f"eq.{tuya_device_id}", "limit": "1"}
    try:
        response = requests.get(url, headers=_supabase_headers(), params=params, timeout=8)
        response.raise_for_status()
        data = response.json()
        return data[0] if data else None
    except requests.RequestException as exc:
        logging.warning("Supabase GET falhou: %s", exc)
        return None


def _supabase_create_device(payload: Dict[str, Any]) -> bool:
    url = f"{SUPABASE_REST_URL}/{SUPABASE_TABLE}"
    try:
        response = requests.post(url, headers=_supabase_headers(), data=json.dumps(payload), timeout=8)
        response.raise_for_status()
        return True
    except requests.RequestException as exc:
        logging.warning("Supabase POST falhou: %s", exc)
        return False


def _supabase_update_device(tuya_device_id: str, updates: Dict[str, Any]) -> bool:
    url = f"{SUPABASE_REST_URL}/{SUPABASE_TABLE}"
    params = {"tuya_device_id": f"eq.{tuya_device_id}"}
    try:
        response = requests.patch(url, headers=_supabase_headers(), params=params, data=json.dumps(updates), timeout=8)
        response.raise_for_status()
        return True
    except requests.RequestException as exc:
        logging.warning("Supabase PATCH falhou: %s", exc)
        return False


def _scan_worker(result_holder: Dict[str, Any], timeout_sec: int) -> None:
    try:
        try:
            devices = tinytuya.deviceScan(timeout=timeout_sec)
        except TypeError:
            devices = tinytuya.deviceScan()
        result_holder["devices"] = devices or {}
    except Exception as exc:
        logging.warning("Scan falhou: %s", exc)
        result_holder["devices"] = {}


def discover_devices(timeout_sec: int = DISCOVERY_TIMEOUT_SEC) -> Dict[str, Any]:
    result_holder: Dict[str, Any] = {}
    worker = threading.Thread(target=_scan_worker, args=(result_holder, timeout_sec), daemon=True)
    worker.start()
    worker.join(timeout=timeout_sec + 2)
    if worker.is_alive():
        logging.warning("Scan excedeu o timeout, retornando vazio.")
        return {}
    return result_holder.get("devices", {})


def _update_discovery_cache(scan_results: Dict[str, Any]) -> None:
    with _discovery_cache_lock:
        for dev_id, info in scan_results.items():
            ip = info.get("ip")
            if not ip:
                continue
            _discovery_cache[dev_id] = {
                "tuya_device_id": dev_id,
                "ip": ip,
                "version": info.get("ver") or info.get("version"),
                "product_id": info.get("product_id"),
                "name": info.get("name"),
                "last_seen": time.time(),
                "raw": info,
            }


def get_device_ip(
    tuya_device_id: str,
    force_scan: bool = False,
    retries: int = DISCOVERY_RETRY_COUNT,
) -> Optional[Tuple[str, Optional[str]]]:
    with _discovery_cache_lock:
        cached = _discovery_cache.get(tuya_device_id)
        if not force_scan and cached and (time.time() - cached.get("last_seen", 0)) < DISCOVERY_CACHE_TTL_SEC:
            return cached.get("ip"), cached.get("version")

    attempts = max(1, retries)
    for attempt in range(attempts):
        scan_results = discover_devices()
        _update_discovery_cache(scan_results)
        with _discovery_cache_lock:
            cached = _discovery_cache.get(tuya_device_id)
            if cached:
                return cached.get("ip"), cached.get("version")
        if attempt < attempts - 1:
            time.sleep(DISCOVERY_RETRY_BACKOFF_SEC)
    return None


def _ensure_device_record(
    tuya_device_id: str,
    lan_ip: Optional[str],
    protocol_version: Optional[str],
    local_key: Optional[str],
) -> Optional[Dict[str, Any]]:
    existing = _supabase_get_device(tuya_device_id)
    if existing:
        updates: Dict[str, Any] = {
            "lan_ip": lan_ip or existing.get("lan_ip"),
            "protocol_version": protocol_version or existing.get("protocol_version"),
            "server_status": "online",
            "heartbeat_at": _now_iso(),
        }
        _supabase_update_device(tuya_device_id, {k: v for k, v in updates.items() if v})
        return {**existing, **updates}

    payload = {
        "tuya_device_id": tuya_device_id,
        "lan_ip": lan_ip,
        "protocol_version": protocol_version,
        "local_key": local_key,
        "server_status": "online",
        "heartbeat_at": _now_iso(),
    }
    _supabase_create_device(payload)
    return payload


def _resolve_device_credentials(
    tuya_device_id: str,
    lan_ip: Optional[str],
    protocol_version: Optional[str],
) -> Optional[Dict[str, Any]]:
    device_record = _supabase_get_device(tuya_device_id)
    if not device_record:
        return None

    resolved_ip = lan_ip or device_record.get("lan_ip")
    resolved_version = protocol_version or device_record.get("protocol_version")
    if not resolved_ip:
        ip_info = get_device_ip(tuya_device_id, force_scan=True)
        if ip_info:
            resolved_ip, resolved_version = ip_info[0], ip_info[1] or resolved_version

    if resolved_ip:
        _supabase_update_device(
            tuya_device_id,
            {
                "lan_ip": resolved_ip,
                "protocol_version": resolved_version,
                "server_status": "online",
                "heartbeat_at": _now_iso(),
            },
        )

    return {
        "tuya_device_id": tuya_device_id,
        "lan_ip": resolved_ip,
        "protocol_version": resolved_version,
        "local_key": device_record.get("local_key"),
        "device_name": device_record.get("device_name"),
        "site_id": device_record.get("site_id"),
    }


def _execute_command(
    tuya_device_id: str,
    command: str,
    lan_ip: Optional[str],
    local_key: str,
    protocol_version: Optional[str],
) -> Tuple[bool, str]:
    result_holder: Dict[str, Any] = {"ok": False, "message": "timeout"}

    def _command_worker() -> None:
        try:
            device = tinytuya.OutletDevice(tuya_device_id, lan_ip, local_key)
            if protocol_version:
                device.set_version(float(protocol_version))
            command_lower = command.lower()
            if command_lower in ("on", "ligar", "turn_on"):
                device.turn_on()
            elif command_lower in ("off", "desligar", "turn_off"):
                device.turn_off()
            else:
                result_holder["ok"] = False
                result_holder["message"] = f"comando desconhecido: {command}"
                return
            result_holder["ok"] = True
            result_holder["message"] = "ok"
        except Exception as exc:
            result_holder["ok"] = False
            result_holder["message"] = f"falha ao enviar comando: {exc}"

    worker = threading.Thread(target=_command_worker, daemon=True)
    worker.start()
    worker.join(timeout=COMMAND_TIMEOUT_SEC)
    if worker.is_alive():
        return False, "timeout ao enviar comando"
    return bool(result_holder["ok"]), str(result_holder["message"])


@app.route("/tuya/command", methods=["POST"])
def tuya_command() -> Any:
    payload = request.get_json(silent=True) or {}
    tuya_device_id = payload.get("tuya_device_id")
    command = payload.get("command")
    lan_ip = payload.get("lan_ip")
    protocol_version = payload.get("protocol_version")

    if not tuya_device_id or not command:
        return jsonify({"error": "tuya_device_id e command são obrigatórios"}), 400

    device_data = _resolve_device_credentials(tuya_device_id, lan_ip, protocol_version)
    if not device_data:
        ip_info = get_device_ip(tuya_device_id, force_scan=True)
        if ip_info:
            resolved_ip, resolved_version = ip_info[0], ip_info[1]
            _ensure_device_record(tuya_device_id, resolved_ip, resolved_version, local_key=None)
            device_data = {
                "tuya_device_id": tuya_device_id,
                "lan_ip": resolved_ip,
                "protocol_version": resolved_version,
                "local_key": None,
            }
        else:
            return jsonify({"error": "dispositivo não encontrado na rede"}), 404

    resolved_ip = device_data.get("lan_ip")
    local_key = device_data.get("local_key")
    resolved_version = device_data.get("protocol_version") or protocol_version

    if not resolved_ip:
        ip_info = get_device_ip(tuya_device_id, force_scan=True)
        if ip_info:
            resolved_ip, resolved_version = ip_info[0], ip_info[1] or resolved_version

    if not resolved_ip:
        return jsonify({"error": "lan_ip não encontrado"}), 404

    if not local_key:
        return jsonify({"error": "local_key não encontrada", "lan_ip": resolved_ip}), 400

    ok, message = _execute_command(
        tuya_device_id=tuya_device_id,
        command=command,
        lan_ip=resolved_ip,
        local_key=local_key,
        protocol_version=resolved_version,
    )

    _ensure_device_record(tuya_device_id, resolved_ip, resolved_version, local_key)
    _supabase_update_device(
        tuya_device_id,
        {
            "lan_ip": resolved_ip,
            "protocol_version": resolved_version,
            "last_command": command,
            "last_command_at": _now_iso(),
            "server_status": "online",
            "heartbeat_at": _now_iso(),
        },
    )

    status_code = 200 if ok else 502
    return jsonify({"ok": ok, "message": message, "lan_ip": resolved_ip}), status_code


@app.route("/tuya/health", methods=["GET"])
def health() -> Any:
    return jsonify({"status": "ok", "time": _now_iso()}), 200


@app.route("/tuya/discover", methods=["GET"])
def tuya_discover() -> Any:
    force_scan = request.args.get("force", "false").lower() in ("1", "true", "yes")
    if force_scan:
        scan_results = discover_devices()
        _update_discovery_cache(scan_results)
        return jsonify({"devices": scan_results, "cached": False}), 200

    with _discovery_cache_lock:
        cached = {dev_id: info.get("raw") for dev_id, info in _discovery_cache.items()}
    if cached:
        return jsonify({"devices": cached, "cached": True}), 200

    scan_results = discover_devices()
    _update_discovery_cache(scan_results)
    return jsonify({"devices": scan_results, "cached": False}), 200


def _heartbeat_loop(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            with _discovery_cache_lock:
                cached_ids = list(_discovery_cache.keys())
            for device_id in cached_ids:
                _supabase_update_device(
                    device_id,
                    {
                        "server_status": "online",
                        "heartbeat_at": _now_iso(),
                    },
                )
        except Exception as exc:
            logging.warning("Heartbeat falhou: %s", exc)
        stop_event.wait(HEARTBEAT_INTERVAL_SEC)


def _discovery_refresh_loop(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            scan_results = discover_devices()
            if scan_results:
                previous_cache: Dict[str, Dict[str, Any]] = {}
                with _discovery_cache_lock:
                    previous_cache = dict(_discovery_cache)
                _update_discovery_cache(scan_results)
                with _discovery_cache_lock:
                    for dev_id, info in _discovery_cache.items():
                        previous = previous_cache.get(dev_id, {})
                        if info.get("ip") != previous.get("ip") or info.get("version") != previous.get("version"):
                            _supabase_update_device(
                                dev_id,
                                {
                                    "lan_ip": info.get("ip"),
                                    "protocol_version": info.get("version"),
                                    "server_status": "online",
                                    "heartbeat_at": _now_iso(),
                                },
                            )
        except Exception as exc:
            logging.warning("Refresh de discovery falhou: %s", exc)
        stop_event.wait(DISCOVERY_REFRESH_INTERVAL_SEC)


def _start_flask_server() -> threading.Thread:
    server_thread = threading.Thread(
        target=lambda: app.run(host=APP_HOST, port=APP_PORT, debug=False, use_reloader=False),
        daemon=True,
    )
    server_thread.start()
    return server_thread


def _watchdog_loop(stop_event: threading.Event) -> None:
    server_thread = _start_flask_server()
    while not stop_event.is_set():
        if not server_thread.is_alive():
            logging.warning("Servidor HTTP caiu. Reiniciando.")
            server_thread = _start_flask_server()
        stop_event.wait(WATCHDOG_INTERVAL_SEC)


def _supervisor_loop(stop_event: threading.Event, threads: Dict[str, threading.Thread]) -> None:
    while not stop_event.is_set():
        for name, thread in list(threads.items()):
            if not thread.is_alive():
                logging.warning("Thread %s caiu. Reiniciando.", name)
                if name == "heartbeat":
                    threads[name] = threading.Thread(target=_heartbeat_loop, args=(stop_event,), daemon=True)
                elif name == "discovery_refresh":
                    threads[name] = threading.Thread(target=_discovery_refresh_loop, args=(stop_event,), daemon=True)
                elif name == "watchdog":
                    threads[name] = threading.Thread(target=_watchdog_loop, args=(stop_event,), daemon=True)
                else:
                    continue
                threads[name].start()
        stop_event.wait(THREAD_RESTART_INTERVAL_SEC)


def main() -> None:
    logging.info("Iniciando gateway Tuya local.")
    logging.info("Executando scan inicial para diagnóstico.")
    scan_results = discover_devices()
    _update_discovery_cache(scan_results)
    logging.info("Dispositivos encontrados: %s", list(scan_results.keys()))

    stop_event = threading.Event()
    threads: Dict[str, threading.Thread] = {
        "heartbeat": threading.Thread(target=_heartbeat_loop, args=(stop_event,), daemon=True),
        "watchdog": threading.Thread(target=_watchdog_loop, args=(stop_event,), daemon=True),
        "discovery_refresh": threading.Thread(target=_discovery_refresh_loop, args=(stop_event,), daemon=True),
    }
    for thread in threads.values():
        thread.start()
    supervisor_thread = threading.Thread(target=_supervisor_loop, args=(stop_event, threads), daemon=True)
    supervisor_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Encerrando servidor.")
        stop_event.set()


if __name__ == "__main__":
    main()
