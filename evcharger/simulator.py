"""Sanal Vestel EV sarj cihazi (OCPP 1.6J istemcisi).

Gercek cihaz gibi davranir: merkezi sisteme baglanir, BootNotification
gonderir, periyodik olcum yollar, uzaktan baslatma/durdurma komutlarina
cevap verir. Boylece evdeki cihaza dokunmadan tum akisi test edebiliriz.

Sarj egrisi kabaca gercek hayattaki gibidir: kontaktor kapandiktan sonra
guc rampa ile yukselir, batarya %80'i gectikten sonra kademeli olarak
kisilir ve %100'de oturum kendiliginden biter.

Ornek:
    python3 simulator.py --url ws://localhost:9000 --id VESTEL-EV-01
    python3 simulator.py --speed 60      # 1 saniye = 1 dakika, hizli test
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import time

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from ocpp_core import OcppConnection, utc_now_iso

log = logging.getLogger("sim")

# Kontaktor kapandiktan sonra gucun tam degere ulasmasi icin gecen sure.
RAMP_SECONDS = 20.0
# Bu doluluk oraninin uzerinde arac akimi kismaya baslar.
TAPER_START_SOC = 80.0
# %100'de kalan guc orani.
TAPER_MIN_FACTOR = 0.15


class VirtualCharger:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.cp_id = args.id
        self.conn: OcppConnection | None = None

        # Elektriksel yapilandirma
        self.voltage = args.voltage
        self.phases = args.phases
        self.max_current = args.max_current
        self.max_power_w = self.voltage * self.max_current * self.phases

        # Batarya durumu
        self.battery_kwh = args.battery
        self.soc = args.soc

        # Sayac omur boyu toplamdir, sifirlanmaz.
        self.meter_wh = args.meter_start

        self.plugged = not args.unplugged
        self.status = "Preparing" if self.plugged else "Available"
        self.transaction_id: int | None = None
        self.id_tag: str | None = None
        self.session_start_meter = 0.0
        self.charging_since: float | None = None
        self.power_w = 0.0
        self._stop_requested = False

    # ------------------------------------------------------------------
    # OCPP: merkezi sistemden gelen komutlar
    # ------------------------------------------------------------------
    def handlers(self) -> dict:
        return {
            "RemoteStartTransaction": self.on_remote_start,
            "RemoteStopTransaction": self.on_remote_stop,
            "TriggerMessage": self.on_trigger_message,
            "GetConfiguration": self.on_get_configuration,
            "ChangeConfiguration": self.on_change_configuration,
            "ChangeAvailability": self.on_change_availability,
            "Reset": self.on_reset,
            "UnlockConnector": self.on_unlock_connector,
        }

    async def on_remote_start(self, payload: dict) -> dict:
        if self.transaction_id is not None:
            log.info("Uzaktan baslatma reddedildi: zaten sarj var")
            return {"status": "Rejected"}

        self.id_tag = payload.get("idTag", "REMOTE")
        log.info("Uzaktan baslatma komutu alindi (idTag=%s)", self.id_tag)
        # Once komutu kabul ettigimizi bildiriyoruz, StartTransaction'i
        # ayri bir gorevde yolluyoruz. OCPP'de bunlar iki ayri istektir ve
        # gercek cihazda da once yanit doner, sonra kontaktor kapanir.
        asyncio.create_task(self._begin_transaction())
        return {"status": "Accepted"}

    async def on_remote_stop(self, payload: dict) -> dict:
        if self.transaction_id != payload.get("transactionId"):
            log.info("Uzaktan durdurma reddedildi: islem numarasi eslesmiyor")
            return {"status": "Rejected"}
        log.info("Uzaktan durdurma komutu alindi")
        self._stop_requested = True
        return {"status": "Accepted"}

    async def on_trigger_message(self, payload: dict) -> dict:
        requested = payload.get("requestedMessage")
        if requested == "MeterValues":
            asyncio.create_task(self.send_meter_values("Trigger"))
            return {"status": "Accepted"}
        if requested == "StatusNotification":
            asyncio.create_task(self.send_status())
            return {"status": "Accepted"}
        if requested == "Heartbeat":
            asyncio.create_task(self._send_heartbeat())
            return {"status": "Accepted"}
        return {"status": "NotImplemented"}

    async def on_get_configuration(self, payload: dict) -> dict:
        return {
            "configurationKey": [
                {"key": "MeterValueSampleInterval", "readonly": False, "value": str(self.args.meter_interval)},
                {"key": "HeartbeatInterval", "readonly": False, "value": str(self.args.heartbeat)},
                {"key": "NumberOfConnectors", "readonly": True, "value": "1"},
            ],
            "unknownKey": [],
        }

    async def on_change_configuration(self, payload: dict) -> dict:
        key, value = payload.get("key"), payload.get("value")
        if key == "MeterValueSampleInterval":
            self.args.meter_interval = max(1, int(value))
            return {"status": "Accepted"}
        if key == "HeartbeatInterval":
            self.args.heartbeat = max(1, int(value))
            return {"status": "Accepted"}
        return {"status": "NotSupported"}

    async def on_change_availability(self, payload: dict) -> dict:
        return {"status": "Accepted"}

    async def on_reset(self, payload: dict) -> dict:
        log.info("Reset komutu alindi (%s)", payload.get("type"))
        self._stop_requested = True
        return {"status": "Accepted"}

    async def on_unlock_connector(self, payload: dict) -> dict:
        return {"status": "Unlocked"}

    # ------------------------------------------------------------------
    # OCPP: cihazdan giden mesajlar
    # ------------------------------------------------------------------
    async def _send_heartbeat(self) -> None:
        if self.conn:
            await self.conn.call("Heartbeat", {})

    async def send_status(self, status: str | None = None) -> None:
        if status:
            self.status = status
        if self.conn:
            await self.conn.call("StatusNotification", {
                "connectorId": 1,
                "errorCode": "NoError",
                "status": self.status,
                "timestamp": utc_now_iso(),
            })

    async def send_meter_values(self, context: str = "Sample.Periodic") -> None:
        if not self.conn or self.transaction_id is None:
            return

        current_per_phase = round(self.power_w / (self.voltage * self.phases), 1) if self.power_w > 0 else 0.0
        samples = [
            {
                "value": f"{self.meter_wh:.0f}",
                "measurand": "Energy.Active.Import.Register",
                "unit": "Wh",
                "context": context,
                "location": "Outlet",
                "format": "Raw",
            },
            {
                "value": f"{self.power_w:.1f}",
                "measurand": "Power.Active.Import",
                "unit": "W",
                "context": context,
            },
            {
                "value": f"{self.soc:.0f}",
                "measurand": "SoC",
                "unit": "Percent",
                "context": context,
            },
        ]
        for phase_index in range(self.phases):
            phase = f"L{phase_index + 1}"
            samples.append({
                "value": f"{current_per_phase:.1f}",
                "measurand": "Current.Import",
                "unit": "A",
                "phase": phase,
                "context": context,
            })
            samples.append({
                "value": f"{self.voltage + random.uniform(-2, 2):.1f}",
                "measurand": "Voltage",
                "unit": "V",
                "phase": f"{phase}-N",
                "context": context,
            })

        await self.conn.call("MeterValues", {
            "connectorId": 1,
            "transactionId": self.transaction_id,
            "meterValue": [{"timestamp": utc_now_iso(), "sampledValue": samples}],
        })

    async def _begin_transaction(self) -> None:
        """Kontaktoru kapat ve sarj oturumunu ac."""
        await asyncio.sleep(self.args.start_delay)

        if not self.plugged:
            # Gercek cihaz da komutu kabul edip kablo takilana kadar
            # bekler; burada bekleme halinde kaliyoruz.
            log.info("Kablo takili degil, sarj baslatilamiyor")
            return
        if self.transaction_id is not None or self.conn is None:
            return

        self.session_start_meter = self.meter_wh
        result = await self.conn.call("StartTransaction", {
            "connectorId": 1,
            "idTag": self.id_tag or "LOCAL",
            "meterStart": int(self.meter_wh),
            "timestamp": utc_now_iso(),
        })

        info = result.get("idTagInfo", {})
        if info.get("status") != "Accepted":
            log.warning("Merkezi sistem sarji reddetti: %s", info.get("status"))
            return

        self.transaction_id = result.get("transactionId")
        self.charging_since = time.monotonic()
        self._stop_requested = False
        log.info("Sarj basladi, islem #%s", self.transaction_id)
        await self.send_status("Charging")

    async def _end_transaction(self, reason: str) -> None:
        if self.transaction_id is None or self.conn is None:
            return
        transaction_id = self.transaction_id
        # Once yerel durumu temizliyoruz ki devam eden olcum dongusu
        # kapanmis bir islem icin mesaj yollamasin.
        self.transaction_id = None
        self.charging_since = None
        self.power_w = 0.0
        self._stop_requested = False

        await self.send_status("Finishing")
        await self.conn.call("StopTransaction", {
            "transactionId": transaction_id,
            "meterStop": int(self.meter_wh),
            "timestamp": utc_now_iso(),
            "reason": reason,
        })
        log.info(
            "Sarj bitti (%s): %.3f kWh verildi, doluluk %%%.0f",
            reason, (self.meter_wh - self.session_start_meter) / 1000.0, self.soc,
        )
        await self.send_status("Preparing" if self.plugged else "Available")

    # ------------------------------------------------------------------
    # Fizik
    # ------------------------------------------------------------------
    def _target_power_w(self) -> float:
        """Anlik hedef guc: rampa ve batarya kismasi dahil."""
        if self.charging_since is None:
            return 0.0

        power = self.max_power_w

        # Batarya doldukca arac akimi kisar (CV bolgesi).
        if self.soc > TAPER_START_SOC:
            span = 100.0 - TAPER_START_SOC
            progress = min(1.0, (self.soc - TAPER_START_SOC) / span)
            power *= 1.0 - (1.0 - TAPER_MIN_FACTOR) * progress

        # Kontaktor kapandiktan sonraki yumusak yukselis.
        elapsed_sim = (time.monotonic() - self.charging_since) * self.args.speed
        if elapsed_sim < RAMP_SECONDS:
            power *= elapsed_sim / RAMP_SECONDS

        # Sebeke dalgalanmasini taklit eden kucuk gurultu.
        return max(0.0, power * random.uniform(0.98, 1.02))

    def _advance(self, dt_sim: float) -> None:
        """Simulasyonu dt_sim saniye ilerlet."""
        if self.transaction_id is None:
            self.power_w = 0.0
            return

        self.power_w = self._target_power_w()
        wh = self.power_w * dt_sim / 3600.0
        self.meter_wh += wh
        if self.battery_kwh > 0:
            self.soc = min(100.0, self.soc + (wh / (self.battery_kwh * 1000.0)) * 100.0)

    # ------------------------------------------------------------------
    # Ana dongu
    # ------------------------------------------------------------------
    async def _run_session(self) -> None:
        """Tek bir WebSocket baglantisi boyunca cihazi calistir."""
        # Okuma dongusu ilk sirada baslamali: BootNotification'in yanitini
        # okuyacak olan odur. Once el sikismayi bekleseydik, yanit hic
        # okunmayacagi icin acilis zaman asimina duserdi.
        reader = asyncio.create_task(self.conn.run(), name="ocpp-reader")
        tasks: list[asyncio.Task] = []
        try:
            await self.conn.call("BootNotification", {
                "chargePointVendor": self.args.vendor,
                "chargePointModel": self.args.model,
                "chargePointSerialNumber": self.args.serial,
                "firmwareVersion": self.args.firmware,
            })
            await self.send_status()

            tasks = [
                asyncio.create_task(self._heartbeat_loop(), name="heartbeat"),
                asyncio.create_task(self._physics_loop(), name="physics"),
                asyncio.create_task(self._meter_loop(), name="meter"),
            ]
            if self.args.auto_start:
                tasks.append(asyncio.create_task(self._auto_start(), name="auto-start"))

            await reader
        finally:
            reader.cancel()
            for task in tasks:
                task.cancel()
            await asyncio.gather(reader, *tasks, return_exceptions=True)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.args.heartbeat)
            try:
                await self._send_heartbeat()
            except Exception:  # noqa: BLE001 - baglanti kopmasi normaldir
                return

    async def _physics_loop(self) -> None:
        tick = 0.5
        while True:
            await asyncio.sleep(tick)
            self._advance(tick * self.args.speed)

            if self.transaction_id is None:
                continue
            if self._stop_requested:
                await self._end_transaction("Remote")
            elif self.soc >= 100.0:
                await self._end_transaction("Local")

    async def _meter_loop(self) -> None:
        while True:
            # Olcum araligi simulasyon zamanindadir; hizlandirilmis
            # calismada gercek zamanda daha sik gonderilir.
            await asyncio.sleep(max(0.25, self.args.meter_interval / self.args.speed))
            if self.transaction_id is not None:
                try:
                    await self.send_meter_values()
                except Exception:  # noqa: BLE001
                    return

    async def _auto_start(self) -> None:
        """Kullanici RFID okutmus ya da tak-sarj ol gibi yerel baslatma."""
        await asyncio.sleep(self.args.auto_start)
        if self.transaction_id is None and self.plugged:
            log.info("Yerel baslatma (arac takildi)")
            self.id_tag = "LOCAL-RFID"
            await self._begin_transaction()

    async def run_forever(self) -> None:
        url = f"{self.args.url.rstrip('/')}/{self.cp_id}"
        backoff = 1.0
        while True:
            try:
                log.info("Baglaniliyor: %s", url)
                async with connect(url, subprotocols=["ocpp1.6"], max_size=2 ** 20) as ws:
                    log.info("Baglandi")
                    backoff = 1.0
                    self.conn = OcppConnection(ws, self.cp_id, handlers=self.handlers(),
                                               log=logging.getLogger("ocpp.sim"))
                    await self._run_session()
            except (OSError, ConnectionClosed) as exc:
                log.warning("Baglanti sorunu: %s", exc)
            except asyncio.CancelledError:
                raise
            finally:
                self.conn = None

            if self.args.once:
                return
            # Gercek cihaz da kopunca artan araliklarla yeniden dener.
            log.info("%.0f saniye sonra yeniden denenecek", backoff)
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 2)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sanal Vestel EV sarj cihazi (OCPP 1.6J)")
    parser.add_argument("--url", default="ws://localhost:9000", help="merkezi sistem adresi")
    parser.add_argument("--id", default="VESTEL-EV-01", help="sarj cihazi adi")

    parser.add_argument("--vendor", default="Vestel")
    parser.add_argument("--model", default="EVC04-AC22")
    parser.add_argument("--serial", default="SIM0000001")
    parser.add_argument("--firmware", default="v3.20-sim")

    parser.add_argument("--voltage", type=float, default=230.0)
    parser.add_argument("--phases", type=int, default=1, choices=[1, 3],
                        help="1 faz (7.4 kW) ya da 3 faz (22 kW)")
    parser.add_argument("--max-current", type=float, default=32.0, help="faz basina amper")

    parser.add_argument("--battery", type=float, default=60.0, help="arac batarya kapasitesi (kWh)")
    parser.add_argument("--soc", type=float, default=35.0, help="baslangic doluluk yuzdesi")
    parser.add_argument("--meter-start", type=float, default=125_400.0,
                        help="cihazin omur boyu sayac degeri (Wh)")

    parser.add_argument("--unplugged", action="store_true", help="kablo takili olmadan basla")
    parser.add_argument("--auto-start", type=float, default=0,
                        help="bu kadar saniye sonra kendiliginden sarja basla (0 = kapali)")
    parser.add_argument("--start-delay", type=float, default=1.5,
                        help="komut ile kontaktorun kapanmasi arasindaki gecikme")

    parser.add_argument("--meter-interval", type=float, default=10.0,
                        help="olcum gonderme araligi (simulasyon saniyesi)")
    parser.add_argument("--heartbeat", type=float, default=30.0)
    parser.add_argument("--speed", type=float, default=1.0,
                        help="zaman carpani; 60 girersen 1 gercek saniye 1 simulasyon dakikasi olur")
    parser.add_argument("--once", action="store_true", help="baglanti kopunca yeniden deneme")
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)-10s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("websockets").setLevel(logging.WARNING)
    charger = VirtualCharger(args)
    try:
        asyncio.run(charger.run_forever())
    except KeyboardInterrupt:
        log.info("Simulator kapatildi")


if __name__ == "__main__":
    main()
