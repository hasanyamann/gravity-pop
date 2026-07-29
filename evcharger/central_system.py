"""Ev tipi Vestel EV sarj cihazi icin OCPP 1.6J merkezi sistem + telefon API'si.

Iki sunucuyu ayni olay dongusunde calistirir:

  * :9000 -> OCPP WebSocket. Sarj cihazi (gercek Vestel ya da simulator)
    buraya baglanir.
  * :8080 -> HTTP. Telefon arayuzunu servis eder, REST komutlarini alir ve
    canli durumu WebSocket ile iter.

Telefonda gormek istedigimiz uc sey de sarj cihazinin gonderdigi OCPP
mesajlarindan turetilir:

  anlik kW      <- MeterValues / Power.Active.Import
  toplam kWh    <- MeterValues / Energy.Active.Import.Register eksi
                   StartTransaction'daki meterStart
  gecen sure    <- StartTransaction ile simdiki an arasindaki fark

Uzaktan baslatma ve durdurma ise OCPP'nin RemoteStartTransaction ve
RemoteStopTransaction cagrilariyla yapilir.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import time
from collections import deque
from pathlib import Path
from typing import Any

from aiohttp import WSMsgType, web
from websockets.asyncio.server import serve as ws_serve

from ocpp_core import OcppConnection, OcppError, utc_now_iso

log = logging.getLogger("cs")

# ---------------------------------------------------------------------------
# Olcum degeri ayristirma
# ---------------------------------------------------------------------------

# OCPP birimleri serbest metin gelir; hepsini tek bir tabana cekiyoruz ki
# arayuz tarafinda birim donusumu dusunmek zorunda kalmayalim.
_ENERGY_TO_WH = {"wh": 1.0, "kwh": 1000.0}
_POWER_TO_W = {"w": 1.0, "kw": 1000.0, "va": 1.0, "kva": 1000.0}

# Olcum adi verilmediginde OCPP 1.6 bu olcumu varsayar (spesifikasyon 7.29).
_DEFAULT_MEASURAND = "Energy.Active.Import.Register"


def _to_float(value: Any) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _normalize_phase(phase: str | None) -> str | None:
    """"L1-N" gibi degerleri "L1"e indirger."""
    if not phase:
        return None
    phase = phase.strip().upper()
    if phase.startswith("L") and len(phase) >= 2 and phase[1].isdigit():
        return phase[:2]
    return phase


def parse_meter_values(meter_value_list: list) -> dict:
    """MeterValues yukunu tek bir olcum sozlugune cevirir.

    Bir MeterValues mesaji birden fazla zaman damgasi tasiyabilir; en yeni
    degerin kazanmasi icin listeyi sirayla gezip uzerine yaziyoruz.
    """
    out: dict[str, Any] = {
        "timestamp": None,
        "energy_wh": None,
        "power_w": None,
        "voltage_v": None,
        "soc": None,
        "current_a": {},
    }

    for entry in meter_value_list or []:
        if not isinstance(entry, dict):
            continue
        out["timestamp"] = entry.get("timestamp") or out["timestamp"]

        for sample in entry.get("sampledValue") or []:
            if not isinstance(sample, dict):
                continue
            value = _to_float(sample.get("value"))
            if value is None:
                continue

            measurand = sample.get("measurand") or _DEFAULT_MEASURAND
            unit = (sample.get("unit") or "").strip().lower()
            phase = _normalize_phase(sample.get("phase"))

            if measurand == "Energy.Active.Import.Register":
                # Birim yoksa OCPP varsayilani Wh.
                out["energy_wh"] = value * _ENERGY_TO_WH.get(unit, 1.0)

            elif measurand == "Power.Active.Import":
                # Fazli guc ornekleri toplam degildir; toplami tasiyan
                # fazsiz ornegi tercih ediyoruz.
                if phase is None:
                    out["power_w"] = value * _POWER_TO_W.get(unit, 1.0)

            elif measurand == "Current.Import":
                out["current_a"][phase or "L1"] = value

            elif measurand == "Voltage":
                if phase is None or phase == "L1":
                    out["voltage_v"] = value

            elif measurand == "SoC":
                out["soc"] = value

    # Bazi cihazlar (ve eski firmware'ler) anlik gucu hic gondermez.
    # Elimizde akim ve gerilim varsa gucu kendimiz hesaplayabiliriz;
    # aksi halde arayuzde kW alani bos kalirdi.
    if out["power_w"] is None and out["current_a"] and out["voltage_v"]:
        total_current = sum(out["current_a"].values())
        out["power_w"] = total_current * out["voltage_v"]

    return out


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


# ---------------------------------------------------------------------------
# Sarj oturumu
# ---------------------------------------------------------------------------

class Session:
    """Tek bir sarj oturumu (OCPP terimiyle bir "transaction")."""

    def __init__(self, transaction_id: int, connector_id: int, id_tag: str, meter_start_wh: float):
        self.transaction_id = transaction_id
        self.connector_id = connector_id
        self.id_tag = id_tag
        self.meter_start_wh = meter_start_wh
        self.meter_now_wh = meter_start_wh

        self.started_at = time.time()
        # Sure icin duvar saati yerine monotonik saat kullaniyoruz; NTP
        # duzeltmesi saati geri alsa bile gecen sure geri sarmasin.
        self._started_mono: float | None = time.monotonic()

        self.power_w = 0.0
        self.voltage_v: float | None = None
        self.current_a: dict[str, float] = {}
        self.soc: float | None = None
        self.last_meter_at: float | None = None

        self.stopped_at: float | None = None
        self.stop_reason: str | None = None
        # Grafik icin (zaman, kW) halka tamponu.
        self.power_series: deque = deque(maxlen=360)

    # -- turetilen degerler ------------------------------------------------
    @property
    def duration_s(self) -> float:
        if self.stopped_at is not None:
            return max(0.0, self.stopped_at - self.started_at)
        if self._started_mono is not None:
            return max(0.0, time.monotonic() - self._started_mono)
        # Sunucu oturum sirasinda yeniden baslatilmissa monotonik referans
        # kaybolur; o durumda duvar saatine duseriz.
        return max(0.0, time.time() - self.started_at)

    @property
    def energy_wh(self) -> float:
        # Sayac toplam (omur boyu) degerdir; oturumun enerjisi farktir.
        # Sayac sifirlanirsa fark negatife duser, bunu kirpiyoruz.
        return max(0.0, self.meter_now_wh - self.meter_start_wh)

    @property
    def energy_kwh(self) -> float:
        return self.energy_wh / 1000.0

    def apply_meter(self, reading: dict) -> None:
        if reading.get("energy_wh") is not None:
            self.meter_now_wh = reading["energy_wh"]
        if reading.get("power_w") is not None:
            self.power_w = reading["power_w"]
        if reading.get("voltage_v") is not None:
            self.voltage_v = reading["voltage_v"]
        if reading.get("soc") is not None:
            self.soc = reading["soc"]
        if reading.get("current_a"):
            self.current_a = reading["current_a"]
        self.last_meter_at = time.time()
        self.power_series.append([round(self.duration_s, 1), round(self.power_w / 1000.0, 3)])

    def to_dict(self, price_per_kwh: float, currency: str) -> dict:
        duration = self.duration_s
        energy_kwh = self.energy_kwh
        return {
            "transaction_id": self.transaction_id,
            "connector_id": self.connector_id,
            "id_tag": self.id_tag,
            "started_at": self.started_at,
            "stopped_at": self.stopped_at,
            "stop_reason": self.stop_reason,
            "duration_s": round(duration, 1),
            "duration_text": format_duration(duration),
            "duration_min": round(duration / 60.0, 1),
            "energy_kwh": round(energy_kwh, 3),
            "power_kw": round(self.power_w / 1000.0, 3),
            "voltage_v": self.voltage_v,
            "current_a": self.current_a,
            "soc": self.soc,
            "avg_power_kw": round(energy_kwh / (duration / 3600.0), 2) if duration >= 5 else 0.0,
            "cost": round(energy_kwh * price_per_kwh, 2),
            "currency": currency,
            "meter_start_wh": self.meter_start_wh,
            "meter_now_wh": self.meter_now_wh,
            "power_series": list(self.power_series),
        }


# ---------------------------------------------------------------------------
# Sarj cihazi
# ---------------------------------------------------------------------------

class ChargePoint:
    """Baglanan bir sarj cihazinin durumu ve OCPP isleyicileri."""

    def __init__(self, cp_id: str, cs: "CentralSystem"):
        self.cp_id = cp_id
        self.cs = cs
        self.conn: OcppConnection | None = None

        self.vendor: str | None = None
        self.model: str | None = None
        self.firmware: str | None = None
        self.serial: str | None = None

        # Konnektor 0 tum istasyonu temsil eder; ev tipi cihazda tek
        # konnektor (1) vardir.
        self.status = "Unavailable"
        self.error_code = "NoError"
        self.last_seen: float | None = None
        self.last_heartbeat: float | None = None

        self.session: Session | None = None
        # Uzaktan komut gonderdik ama cihaz henuz sarja baslamadi durumu.
        self.pending: dict | None = None

    @property
    def online(self) -> bool:
        return self.conn is not None

    # -- OCPP: cihazdan gelenler ------------------------------------------
    def handlers(self) -> dict:
        return {
            "BootNotification": self.on_boot_notification,
            "Heartbeat": self.on_heartbeat,
            "StatusNotification": self.on_status_notification,
            "Authorize": self.on_authorize,
            "StartTransaction": self.on_start_transaction,
            "MeterValues": self.on_meter_values,
            "StopTransaction": self.on_stop_transaction,
            "DataTransfer": self.on_data_transfer,
            "FirmwareStatusNotification": self.on_ack,
            "DiagnosticsStatusNotification": self.on_ack,
        }

    def _touch(self) -> None:
        self.last_seen = time.time()

    async def on_boot_notification(self, payload: dict) -> dict:
        self._touch()
        self.vendor = payload.get("chargePointVendor")
        self.model = payload.get("chargePointModel")
        self.firmware = payload.get("firmwareVersion")
        self.serial = payload.get("chargePointSerialNumber")
        log.info("%s acildi: %s %s (fw %s)", self.cp_id, self.vendor, self.model, self.firmware)
        self.cs.publish()
        return {
            "status": "Accepted",
            "currentTime": utc_now_iso(),
            "interval": self.cs.heartbeat_interval,
        }

    async def on_heartbeat(self, payload: dict) -> dict:
        self._touch()
        self.last_heartbeat = time.time()
        self.cs.publish()
        return {"currentTime": utc_now_iso()}

    async def on_status_notification(self, payload: dict) -> dict:
        self._touch()
        connector = payload.get("connectorId", 1)
        status = payload.get("status", "Unavailable")
        # Konnektor 0 istasyonun kendisidir; konnektor durumunu ezmemeli.
        if connector != 0:
            self.status = status
            self.error_code = payload.get("errorCode", "NoError")
        log.info("%s konnektor %s -> %s (%s)", self.cp_id, connector, status, payload.get("errorCode"))

        # Cihaz sarja gectiyse bekleyen uzaktan komut tamamlanmis demektir.
        if self.pending and self.pending.get("action") == "start" and status == "Charging":
            self.pending = None
        if self.pending and self.pending.get("action") == "stop" and status in ("Available", "Preparing", "Finishing"):
            self.pending = None

        self.cs.publish()
        return {}

    async def on_authorize(self, payload: dict) -> dict:
        self._touch()
        id_tag = payload.get("idTag", "")
        accepted = self.cs.is_tag_allowed(id_tag)
        log.info("%s yetkilendirme %s -> %s", self.cp_id, id_tag, "kabul" if accepted else "red")
        return {"idTagInfo": {"status": "Accepted" if accepted else "Invalid"}}

    async def on_start_transaction(self, payload: dict) -> dict:
        self._touch()
        id_tag = payload.get("idTag", "")
        if not self.cs.is_tag_allowed(id_tag):
            # Reddederken transactionId yine de zorunlu alan.
            return {"transactionId": 0, "idTagInfo": {"status": "Invalid"}}

        # Cihaz yeniden baslamis ve eski oturumu kapatmamis olabilir.
        # Yeni bir islem geldiyse eskisi artik gecerli degildir.
        if self.session is not None:
            log.warning("%s zaten acik olan #%s oturumunu kapatiyorum", self.cp_id, self.session.transaction_id)
            self._close_session("Other")

        meter_start = _to_float(payload.get("meterStart")) or 0.0
        transaction_id = self.cs.next_transaction_id()
        self.session = Session(
            transaction_id=transaction_id,
            connector_id=payload.get("connectorId", 1),
            id_tag=id_tag,
            meter_start_wh=meter_start,
        )
        self.pending = None
        log.info("%s sarj basladi: islem #%s, sayac %.0f Wh", self.cp_id, transaction_id, meter_start)
        self.cs.publish()
        return {"transactionId": transaction_id, "idTagInfo": {"status": "Accepted"}}

    async def on_meter_values(self, payload: dict) -> dict:
        self._touch()
        reading = parse_meter_values(payload.get("meterValue") or [])
        if self.session is not None:
            self.session.apply_meter(reading)
            self.cs.publish()
        else:
            # Oturum disi olcumler normaldir (bekleme halindeki cihaz da
            # periyodik olcum gonderebilir); sadece not dusuyoruz.
            log.debug("%s oturum disi olcum: %s", self.cp_id, reading)
        return {}

    async def on_stop_transaction(self, payload: dict) -> dict:
        self._touch()
        transaction_id = payload.get("transactionId")
        meter_stop = _to_float(payload.get("meterStop"))
        reason = payload.get("reason", "Local")

        if self.session is None:
            log.warning("%s bilinmeyen islem #%s icin durdurma geldi", self.cp_id, transaction_id)
        else:
            if meter_stop is not None:
                self.session.meter_now_wh = meter_stop
            self._close_session(reason)

        self.pending = None
        self.cs.publish()
        # OCPP durdurmayi her zaman kabul etmeyi bekler.
        return {"idTagInfo": {"status": "Accepted"}}

    async def on_data_transfer(self, payload: dict) -> dict:
        self._touch()
        log.debug("%s DataTransfer: %s", self.cp_id, payload)
        return {"status": "UnknownVendorId"}

    async def on_ack(self, payload: dict) -> dict:
        self._touch()
        return {}

    def _close_session(self, reason: str) -> None:
        session = self.session
        if session is None:
            return
        session.stopped_at = time.time()
        session.stop_reason = reason
        session.power_w = 0.0
        log.info(
            "%s sarj bitti: islem #%s, %.3f kWh, %s, sebep %s",
            self.cp_id, session.transaction_id, session.energy_kwh,
            format_duration(session.duration_s), reason,
        )
        self.cs.record_history(self.cp_id, session)
        self.session = None

    # -- OCPP: cihaza gonderilenler ---------------------------------------
    async def remote_start(self, id_tag: str, connector_id: int = 1) -> dict:
        if self.conn is None:
            raise OcppError("GenericError", "Sarj cihazi cevrimdisi")
        if self.session is not None:
            raise OcppError("GenericError", "Zaten devam eden bir sarj var")

        result = await self.conn.call(
            "RemoteStartTransaction",
            {"idTag": id_tag, "connectorId": connector_id},
        )
        status = result.get("status", "Rejected")
        if status == "Accepted":
            # Kabul edildi, ama sarjin gercekten basladigini ancak
            # StartTransaction gelince anlariz: kablo takili degilse ya da
            # arac hazir degilse cihaz komutu kabul edip bekleyebilir.
            self.pending = {"action": "start", "since": time.time(), "deadline": time.time() + 60}
        self.cs.publish()
        return {"status": status}

    async def remote_stop(self) -> dict:
        if self.conn is None:
            raise OcppError("GenericError", "Sarj cihazi cevrimdisi")
        if self.session is None:
            raise OcppError("GenericError", "Durdurulacak aktif sarj yok")

        result = await self.conn.call(
            "RemoteStopTransaction",
            {"transactionId": self.session.transaction_id},
        )
        status = result.get("status", "Rejected")
        if status == "Accepted":
            self.pending = {"action": "stop", "since": time.time(), "deadline": time.time() + 60}
        self.cs.publish()
        return {"status": status}

    async def trigger_meter_values(self) -> dict:
        """Cihazdan hemen bir olcum iste (periyodik araligi beklemeden)."""
        if self.conn is None:
            raise OcppError("GenericError", "Sarj cihazi cevrimdisi")
        result = await self.conn.call(
            "TriggerMessage",
            {"requestedMessage": "MeterValues", "connectorId": 1},
        )
        return {"status": result.get("status", "NotImplemented")}

    def expire_pending(self) -> bool:
        """Suresi gecen bekleyen komutu temizler. Degisiklik olduysa True."""
        if self.pending and time.time() > self.pending["deadline"]:
            log.info("%s bekleyen '%s' komutu zaman asimina ugradi", self.cp_id, self.pending["action"])
            self.pending = None
            return True
        return False

    def to_dict(self, price_per_kwh: float, currency: str) -> dict:
        return {
            "cp_id": self.cp_id,
            "online": self.online,
            "vendor": self.vendor,
            "model": self.model,
            "firmware": self.firmware,
            "serial": self.serial,
            "status": self.status if self.online else "Offline",
            "error_code": self.error_code,
            "last_seen": self.last_seen,
            "last_heartbeat": self.last_heartbeat,
            "charging": self.session is not None,
            "pending": self.pending,
            "session": self.session.to_dict(price_per_kwh, currency) if self.session else None,
        }


# ---------------------------------------------------------------------------
# Merkezi sistem
# ---------------------------------------------------------------------------

class CentralSystem:
    def __init__(
        self,
        data_dir: Path,
        price_per_kwh: float = 3.5,
        currency: str = "TL",
        heartbeat_interval: int = 60,
        allowed_tags: set[str] | None = None,
        api_token: str | None = None,
    ):
        self.data_dir = data_dir
        self.price_per_kwh = price_per_kwh
        self.currency = currency
        self.heartbeat_interval = heartbeat_interval
        self.allowed_tags = allowed_tags
        self.api_token = api_token

        self.charge_points: dict[str, ChargePoint] = {}
        self.history: list[dict] = []
        self._transaction_seq = 1
        self._subscribers: set[asyncio.Queue] = set()

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.data_dir / "state.json"
        self._load()

    # -- kalicilik ---------------------------------------------------------
    def _load(self) -> None:
        if not self.state_file.exists():
            return
        try:
            data = json.loads(self.state_file.read_text("utf-8"))
        except (OSError, ValueError) as exc:
            log.warning("Durum dosyasi okunamadi (%s), sifirdan basliyoruz", exc)
            return
        self.history = data.get("history", [])
        # Islem numaralari yeniden baslatma sonrasi tekrar etmemeli; cihaz
        # eski bir numarayla gelen yaniti karistirabilir.
        self._transaction_seq = max(1, int(data.get("transaction_seq", 1)))
        log.info("%d gecmis oturum yuklendi", len(self.history))

    def _save(self) -> None:
        payload = {"history": self.history[-200:], "transaction_seq": self._transaction_seq}
        tmp = self.state_file.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), "utf-8")
            tmp.replace(self.state_file)
        except OSError as exc:
            log.warning("Durum kaydedilemedi: %s", exc)

    def next_transaction_id(self) -> int:
        transaction_id = self._transaction_seq
        self._transaction_seq += 1
        self._save()
        return transaction_id

    def record_history(self, cp_id: str, session: Session) -> None:
        entry = session.to_dict(self.price_per_kwh, self.currency)
        entry.pop("power_series", None)  # gecmiste grafik tutmuyoruz
        entry["cp_id"] = cp_id
        self.history.append(entry)
        self.history = self.history[-200:]
        self._save()

    # -- yetkilendirme -----------------------------------------------------
    def is_tag_allowed(self, id_tag: str) -> bool:
        # Liste tanimlanmadiysa ev kullanimi icin her etiketi kabul ediyoruz.
        if self.allowed_tags is None:
            return True
        return id_tag in self.allowed_tags

    # -- abonelik / yayin --------------------------------------------------
    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=8)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._subscribers.discard(queue)

    def publish(self) -> None:
        """Tum bagli telefonlara guncel durumu it."""
        if not self._subscribers:
            return
        snapshot = self.snapshot()
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(snapshot)
            except asyncio.QueueFull:
                # Yavas istemci yuzunden bellek sismesin; en eskiyi at.
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
                with contextlib.suppress(asyncio.QueueFull):
                    queue.put_nowait(snapshot)

    def snapshot(self) -> dict:
        return {
            "type": "state",
            "server_time": time.time(),
            "tariff": {"price_per_kwh": self.price_per_kwh, "currency": self.currency},
            "charge_points": [
                cp.to_dict(self.price_per_kwh, self.currency)
                for cp in sorted(self.charge_points.values(), key=lambda c: c.cp_id)
            ],
            "history": list(reversed(self.history[-20:])),
        }

    def get_charge_point(self, cp_id: str | None) -> ChargePoint:
        if cp_id:
            cp = self.charge_points.get(cp_id)
            if cp is None:
                raise OcppError("GenericError", f"Bilinmeyen sarj cihazi: {cp_id}")
            return cp
        # Ev kurulumunda tek cihaz olur; belirtilmediyse onu sec.
        online = [cp for cp in self.charge_points.values() if cp.online]
        pool = online or list(self.charge_points.values())
        if not pool:
            raise OcppError("GenericError", "Hic sarj cihazi baglanmadi")
        if len(pool) > 1:
            raise OcppError("GenericError", "Birden fazla cihaz var, cp_id belirtin")
        return pool[0]

    # -- OCPP baglanti yonetimi -------------------------------------------
    async def handle_charger(self, ws) -> None:
        path = getattr(getattr(ws, "request", None), "path", "") or ""
        # Cihaz genelde ws://sunucu:9000/<cihazAdi> seklinde baglanir.
        segments = [s for s in path.split("/") if s]
        cp_id = segments[-1] if segments else "unknown"

        cp = self.charge_points.get(cp_id)
        if cp is None:
            cp = ChargePoint(cp_id, self)
            self.charge_points[cp_id] = cp

        if cp.conn is not None:
            # Cihaz eski baglantiyi duzgun kapatmadan yeniden baglanmis
            # olabilir (guc kesintisi, wifi kopmasi). Yenisi kazanir.
            log.warning("%s yeniden baglandi, eski baglanti dusuruluyor", cp_id)
            with contextlib.suppress(Exception):
                await cp.conn.ws.close()

        conn = OcppConnection(ws, cp_id, handlers=cp.handlers(), log=logging.getLogger(f"ocpp.{cp_id}"))
        cp.conn = conn
        cp.last_seen = time.time()
        log.info("%s baglandi (%s)", cp_id, path)
        self.publish()

        try:
            await conn.run()
        finally:
            if cp.conn is conn:
                cp.conn = None
                cp.status = "Offline"
                cp.pending = None
                # Oturumu SILMIYORUZ: wifi koptu diye arac sarj olmayi
                # birakmaz. Cihaz geri gelince ayni islemi surdurur.
                log.info("%s baglantisi kesildi", cp_id)
                self.publish()

    async def watchdog(self) -> None:
        """Bekleyen komutlarin zaman asimini ve saniyelik yayini yurutur."""
        while True:
            await asyncio.sleep(1)
            changed = False
            for cp in self.charge_points.values():
                if cp.expire_pending():
                    changed = True
            # Sarj sirasinda saniyede bir yayin, sayacin akici gorunmesi
            # icin degil (arayuz zamani kendi sayar) ama gucun ve enerjinin
            # gecikmesiz gorunmesi icin.
            if changed or any(cp.session for cp in self.charge_points.values()):
                self.publish()


# ---------------------------------------------------------------------------
# Telefon icin HTTP API
# ---------------------------------------------------------------------------

def build_app(cs: CentralSystem, web_dir: Path) -> web.Application:
    app = web.Application(middlewares=[auth_middleware(cs)])

    async def index(request: web.Request) -> web.StreamResponse:
        return web.FileResponse(web_dir / "index.html")

    async def api_state(request: web.Request) -> web.StreamResponse:
        return web.json_response(cs.snapshot())

    async def api_start(request: web.Request) -> web.StreamResponse:
        body = await _json_body(request)
        cp = cs.get_charge_point(body.get("cp_id"))
        result = await cp.remote_start(
            id_tag=body.get("id_tag") or "PHONE",
            connector_id=int(body.get("connector_id") or 1),
        )
        accepted = result["status"] == "Accepted"
        return web.json_response({
            "ok": accepted,
            "status": result["status"],
            "message": "Baslatma komutu gonderildi" if accepted else "Cihaz komutu reddetti",
        })

    async def api_stop(request: web.Request) -> web.StreamResponse:
        body = await _json_body(request)
        cp = cs.get_charge_point(body.get("cp_id"))
        result = await cp.remote_stop()
        accepted = result["status"] == "Accepted"
        return web.json_response({
            "ok": accepted,
            "status": result["status"],
            "message": "Durdurma komutu gonderildi" if accepted else "Cihaz komutu reddetti",
        })

    async def api_refresh(request: web.Request) -> web.StreamResponse:
        body = await _json_body(request)
        cp = cs.get_charge_point(body.get("cp_id"))
        result = await cp.trigger_meter_values()
        return web.json_response({"ok": result["status"] == "Accepted", **result})

    async def api_history(request: web.Request) -> web.StreamResponse:
        return web.json_response({"history": list(reversed(cs.history))})

    async def api_ws(request: web.Request) -> web.StreamResponse:
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        queue = cs.subscribe()
        await ws.send_json(cs.snapshot())

        async def pump() -> None:
            while True:
                snapshot = await queue.get()
                await ws.send_json(snapshot)

        pump_task = asyncio.create_task(pump())
        try:
            async for msg in ws:
                # Istemci mesajlarini beklemiyoruz; sadece baglanti
                # kapanisini yakalamak icin donguyu tutuyoruz.
                if msg.type == WSMsgType.ERROR:
                    break
        finally:
            pump_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pump_task
            cs.unsubscribe(queue)
        return ws

    app.router.add_get("/", index)
    app.router.add_get("/api/state", api_state)
    app.router.add_post("/api/start", api_start)
    app.router.add_post("/api/stop", api_stop)
    app.router.add_post("/api/refresh", api_refresh)
    app.router.add_get("/api/history", api_history)
    app.router.add_get("/api/ws", api_ws)
    app.router.add_static("/static", web_dir)
    for filename in ("manifest.json", "sw.js", "icon.svg"):
        app.router.add_get(f"/{filename}", _file_route(web_dir / filename))
    return app


def _file_route(path: Path):
    async def handler(request: web.Request) -> web.StreamResponse:
        if not path.exists():
            raise web.HTTPNotFound()
        return web.FileResponse(path)
    return handler


async def _json_body(request: web.Request) -> dict:
    if not request.can_read_body:
        return {}
    try:
        body = await request.json()
    except (ValueError, TypeError):
        return {}
    return body if isinstance(body, dict) else {}


def auth_middleware(cs: CentralSystem):
    @web.middleware
    async def middleware(request: web.Request, handler):
        if cs.api_token and request.path.startswith("/api/"):
            supplied = request.headers.get("X-Auth-Token") or request.query.get("token")
            if supplied != cs.api_token:
                return web.json_response({"ok": False, "message": "Yetkisiz"}, status=401)
        try:
            return await handler(request)
        except OcppError as exc:
            # Cihaz cevrimdisi, aktif oturum yok gibi beklenen durumlar;
            # arayuzde okunabilir bir mesaja donusmeli.
            return web.json_response({"ok": False, "message": exc.description or str(exc)}, status=409)
    return middleware


# ---------------------------------------------------------------------------
# Giris noktasi
# ---------------------------------------------------------------------------

def _select_subprotocol(connection, subprotocols):
    """OCPP alt protokolunu sec, ama sart kosma.

    Bazi sarj cihazi firmware'leri Sec-WebSocket-Protocol basligini hic
    gondermez ya da yanlis gonderir. Kutuphanenin varsayilani boyle bir
    istemciyi reddeder; ev kurulumunda cihazi disarida birakmaktansa
    baglantiyi kabul etmeyi tercih ediyoruz.
    """
    for candidate in ("ocpp1.6", "ocpp1.6j"):
        if candidate in (subprotocols or []):
            return candidate
    return None


async def main_async(args: argparse.Namespace) -> None:
    cs = CentralSystem(
        data_dir=Path(args.data_dir),
        price_per_kwh=args.price,
        currency=args.currency,
        heartbeat_interval=args.heartbeat,
        allowed_tags=set(args.allow_tag) if args.allow_tag else None,
        api_token=args.token,
    )

    web_dir = Path(__file__).parent / "web"
    app = build_app(cs, web_dir)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, args.host, args.http_port)
    await site.start()

    watchdog = asyncio.create_task(cs.watchdog(), name="watchdog")

    async with ws_serve(
        cs.handle_charger,
        args.host,
        args.ocpp_port,
        select_subprotocol=_select_subprotocol,
        # Sarj cihazlari wifi uzerinden konusur ve bazilari ping'e yavas
        # yanit verir; varsayilan 20s/20s bu cihazlari bosuna dusurur.
        ping_interval=args.ping_interval,
        ping_timeout=args.ping_timeout,
        max_size=2 ** 20,
    ):
        log.info("OCPP sunucusu: ws://%s:%s/<CihazAdi>", args.host, args.ocpp_port)
        log.info("Telefon arayuzu: http://%s:%s/", args.host, args.http_port)
        if cs.api_token:
            log.info("API belirteci etkin (X-Auth-Token ya da ?token=)")
        try:
            await asyncio.Future()  # sonsuza kadar calis
        finally:
            watchdog.cancel()
            await runner.cleanup()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Vestel EV sarj cihazi icin OCPP 1.6J merkezi sistem")
    parser.add_argument("--host", default=os.getenv("EV_HOST", "0.0.0.0"))
    parser.add_argument("--ocpp-port", type=int, default=int(os.getenv("EV_OCPP_PORT", "9000")))
    parser.add_argument("--http-port", type=int, default=int(os.getenv("EV_HTTP_PORT", "8080")))
    parser.add_argument("--data-dir", default=os.getenv("EV_DATA_DIR", str(Path(__file__).parent / "data")))
    parser.add_argument("--price", type=float, default=float(os.getenv("EV_PRICE_PER_KWH", "3.50")),
                        help="kWh basina birim fiyat")
    parser.add_argument("--currency", default=os.getenv("EV_CURRENCY", "TL"))
    parser.add_argument("--heartbeat", type=int, default=int(os.getenv("EV_HEARTBEAT", "60")),
                        help="cihaza soylenecek heartbeat araligi (saniye)")
    parser.add_argument("--allow-tag", action="append", default=None,
                        help="kabul edilecek RFID etiketi (birden fazla kez verilebilir; "
                             "hic verilmezse tum etiketler kabul edilir)")
    parser.add_argument("--token", default=os.getenv("EV_API_TOKEN") or None,
                        help="telefon API'si icin paylasilan gizli anahtar")
    parser.add_argument("--ping-interval", type=float, default=45.0)
    parser.add_argument("--ping-timeout", type=float, default=90.0)
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)-14s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("websockets").setLevel(logging.WARNING)
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        log.info("Kapatiliyor")


if __name__ == "__main__":
    main()
