"""Merkezi sistem + simulator ucdan uca testleri.

Calistirmak icin:  python3 test_e2e.py

Gercek cihaz olmadan su akisi dogruluyoruz: cihaz baglanir, telefondan
baslatma komutu gider, olcumler akar, kWh ve sure artar, telefondan
durdurma komutu oturumu kapatir ve gecmise yazar.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import tempfile
import unittest
from pathlib import Path

import aiohttp
from aiohttp import web
from websockets.asyncio.server import serve as ws_serve

import simulator
from central_system import (
    CentralSystem,
    _select_subprotocol,
    build_app,
    format_duration,
    parse_meter_values,
)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def wait_for(predicate, timeout: float = 10.0, interval: float = 0.05):
    """Kosul saglanana kadar bekle; saglanmazsa testi anlamli bir mesajla dusur."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        value = predicate()
        if value:
            return value
        await asyncio.sleep(interval)
    raise AssertionError(f"{timeout} saniyede kosul saglanmadi")


class MeterParsingTests(unittest.TestCase):
    """Farkli firmware'lerin gonderdigi bicimler dogru okunmali."""

    def test_reads_watt_hours_and_watts(self):
        reading = parse_meter_values([{
            "timestamp": "2026-07-29T10:00:00Z",
            "sampledValue": [
                {"value": "125400", "measurand": "Energy.Active.Import.Register", "unit": "Wh"},
                {"value": "7350.5", "measurand": "Power.Active.Import", "unit": "W"},
                {"value": "56", "measurand": "SoC", "unit": "Percent"},
            ],
        }])
        self.assertAlmostEqual(reading["energy_wh"], 125400)
        self.assertAlmostEqual(reading["power_w"], 7350.5)
        self.assertEqual(reading["soc"], 56)

    def test_converts_kilo_units(self):
        reading = parse_meter_values([{
            "sampledValue": [
                {"value": "125.4", "measurand": "Energy.Active.Import.Register", "unit": "kWh"},
                {"value": "7.35", "measurand": "Power.Active.Import", "unit": "kW"},
            ],
        }])
        self.assertAlmostEqual(reading["energy_wh"], 125400)
        self.assertAlmostEqual(reading["power_w"], 7350)

    def test_measurand_defaults_to_energy_register(self):
        # OCPP 1.6, olcum adi verilmezse bunu enerji sayaci sayar.
        reading = parse_meter_values([{"sampledValue": [{"value": "8000"}]}])
        self.assertAlmostEqual(reading["energy_wh"], 8000)

    def test_derives_power_when_only_current_and_voltage_sent(self):
        reading = parse_meter_values([{
            "sampledValue": [
                {"value": "16", "measurand": "Current.Import", "unit": "A", "phase": "L1"},
                {"value": "16", "measurand": "Current.Import", "unit": "A", "phase": "L2"},
                {"value": "16", "measurand": "Current.Import", "unit": "A", "phase": "L3"},
                {"value": "230", "measurand": "Voltage", "unit": "V", "phase": "L1-N"},
            ],
        }])
        self.assertAlmostEqual(reading["power_w"], 48 * 230)
        self.assertEqual(sorted(reading["current_a"]), ["L1", "L2", "L3"])

    def test_phase_power_does_not_overwrite_total(self):
        # Fazli guc ornekleri toplam degildir; toplami tasiyan fazsiz
        # ornek kazanmali.
        reading = parse_meter_values([{
            "sampledValue": [
                {"value": "6900", "measurand": "Power.Active.Import", "unit": "W"},
                {"value": "2300", "measurand": "Power.Active.Import", "unit": "W", "phase": "L1"},
            ],
        }])
        self.assertAlmostEqual(reading["power_w"], 6900)

    def test_ignores_unparsable_values(self):
        reading = parse_meter_values([{
            "sampledValue": [
                {"value": "bozuk", "measurand": "Power.Active.Import", "unit": "W"},
                {"value": "1000", "measurand": "Energy.Active.Import.Register", "unit": "Wh"},
            ],
        }])
        self.assertIsNone(reading["power_w"])
        self.assertAlmostEqual(reading["energy_wh"], 1000)

    def test_survives_garbage_payload(self):
        self.assertIsNone(parse_meter_values([])["energy_wh"])
        self.assertIsNone(parse_meter_values([{"sampledValue": None}])["energy_wh"])

    def test_duration_format(self):
        self.assertEqual(format_duration(65), "01:05")
        self.assertEqual(format_duration(3725), "01:02:05")
        self.assertEqual(format_duration(-5), "00:00")


class ChargingFlowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ocpp_port = free_port()
        self.http_port = free_port()

        self.cs = CentralSystem(data_dir=Path(self.tmp.name), price_per_kwh=4.0, currency="TL")

        self.runner = web.AppRunner(build_app(self.cs, Path(__file__).parent / "web"), access_log=None)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", self.http_port).start()

        self.ws_server = await ws_serve(
            self.cs.handle_charger, "127.0.0.1", self.ocpp_port,
            select_subprotocol=_select_subprotocol,
        )
        self.watchdog = asyncio.create_task(self.cs.watchdog())
        self.session = aiohttp.ClientSession(f"http://127.0.0.1:{self.http_port}")
        self.sim_task: asyncio.Task | None = None

    async def asyncTearDown(self):
        if self.sim_task:
            self.sim_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.sim_task
        self.watchdog.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.watchdog
        await self.session.close()
        self.ws_server.close()
        await self.ws_server.wait_closed()
        await self.runner.cleanup()
        self.tmp.cleanup()

    # -- yardimcilar -------------------------------------------------------
    async def start_simulator(self, *extra: str) -> simulator.VirtualCharger:
        args = simulator.parse_args([
            "--url", f"ws://127.0.0.1:{self.ocpp_port}",
            "--id", "TEST-EV",
            "--speed", "60",
            "--meter-interval", "10",
            "--start-delay", "0.2",
            *extra,
        ])
        charger = simulator.VirtualCharger(args)
        self.sim_task = asyncio.create_task(charger.run_forever())
        return charger

    def cp(self) -> dict | None:
        points = self.cs.snapshot()["charge_points"]
        return points[0] if points else None

    async def api(self, path: str, method: str = "POST") -> dict:
        async with self.session.request(method, path, json={}) as resp:
            return await resp.json()

    # -- testler -----------------------------------------------------------
    async def test_full_charge_cycle(self):
        await self.start_simulator()

        cp = await wait_for(lambda: self.cp() if self.cp() and self.cp()["online"] else None)
        self.assertEqual(cp["vendor"], "Vestel")
        await wait_for(lambda: self.cp()["status"] == "Preparing")

        # Telefondan baslat
        result = await self.api("/api/start")
        self.assertTrue(result["ok"], result)

        session = await wait_for(lambda: self.cp()["session"])
        self.assertEqual(session["id_tag"], "PHONE")
        await wait_for(lambda: self.cp()["status"] == "Charging")

        # Guc, enerji ve sure gercekten artmali
        await wait_for(lambda: self.cp()["session"]["power_kw"] > 1.0)
        first = self.cp()["session"]
        await wait_for(lambda: self.cp()["session"]["energy_kwh"] > first["energy_kwh"])

        later = self.cp()["session"]
        self.assertGreater(later["duration_s"], 0)
        self.assertGreater(later["energy_kwh"], 0)
        self.assertAlmostEqual(later["cost"], round(later["energy_kwh"] * 4.0, 2), places=2)
        self.assertGreater(len(later["power_series"]), 1)
        # Sayac omur boyu toplam; oturum enerjisi baslangictan olan fark.
        self.assertAlmostEqual(
            later["energy_kwh"],
            (later["meter_now_wh"] - later["meter_start_wh"]) / 1000.0,
            places=3,
        )

        # Telefondan durdur
        result = await self.api("/api/stop")
        self.assertTrue(result["ok"], result)

        await wait_for(lambda: self.cp()["session"] is None)
        history = await wait_for(lambda: self.cs.snapshot()["history"] or None)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["stop_reason"], "Remote")
        self.assertGreater(history[0]["energy_kwh"], 0)
        self.assertEqual(history[0]["cp_id"], "TEST-EV")

    async def test_stops_by_itself_when_battery_is_full(self):
        # Kucuk batarya + yuksek baslangic doluluguyla hizlica dolar.
        await self.start_simulator("--battery", "1", "--soc", "97")
        await wait_for(lambda: self.cp() and self.cp()["online"])
        await self.api("/api/start")
        await wait_for(lambda: self.cp()["session"])

        await wait_for(lambda: self.cp()["session"] is None, timeout=25)
        history = self.cs.snapshot()["history"]
        self.assertEqual(history[0]["stop_reason"], "Local")

    async def test_local_start_is_visible_on_phone(self):
        # Kullanici RFID okutup baslattiginda da telefon oturumu gormeli.
        await self.start_simulator("--auto-start", "0.3")
        await wait_for(lambda: self.cp() and self.cp()["online"])
        session = await wait_for(lambda: self.cp()["session"])
        self.assertEqual(session["id_tag"], "LOCAL-RFID")

    async def test_start_rejected_when_charger_offline(self):
        # Hic cihaz baglanmamisken baslatma anlamli bir hata donmeli.
        async with self.session.post("/api/start", json={}) as resp:
            self.assertEqual(resp.status, 409)
            body = await resp.json()
        self.assertFalse(body["ok"])
        self.assertIn("sarj cihazi", body["message"].lower())

    async def test_stop_rejected_when_nothing_is_charging(self):
        await self.start_simulator()
        await wait_for(lambda: self.cp() and self.cp()["online"])
        async with self.session.post("/api/stop", json={}) as resp:
            self.assertEqual(resp.status, 409)
            body = await resp.json()
        self.assertIn("aktif sarj yok", body["message"].lower())

    async def test_session_survives_charger_reconnect(self):
        # Wifi kopmasi sarji bitirmez; cihaz geri gelince oturum durmali.
        charger = await self.start_simulator()
        await wait_for(lambda: self.cp() and self.cp()["online"])
        await self.api("/api/start")
        session = await wait_for(lambda: self.cp()["session"])
        transaction_id = session["transaction_id"]

        await charger.conn.ws.close()
        await wait_for(lambda: not self.cp()["online"])
        # Oturum silinmemeli, sadece cihaz cevrimdisi gorunmeli.
        self.assertIsNotNone(self.cp()["session"])

        await wait_for(lambda: self.cp()["online"], timeout=15)
        self.assertEqual(self.cp()["session"]["transaction_id"], transaction_id)

    async def test_websocket_pushes_live_updates(self):
        await self.start_simulator()
        await wait_for(lambda: self.cp() and self.cp()["online"])

        async with self.session.ws_connect("/api/ws") as ws:
            first = await asyncio.wait_for(ws.receive_json(), timeout=5)
            self.assertEqual(first["type"], "state")

            await self.api("/api/start")
            # Sarj basladigina dair bir guncelleme itilmeli.
            async def charging_message():
                while True:
                    message = await ws.receive_json()
                    points = message.get("charge_points") or []
                    if points and points[0].get("session"):
                        return points[0]["session"]
            session = await asyncio.wait_for(charging_message(), timeout=10)
            self.assertGreater(session["transaction_id"], 0)

    async def test_api_token_is_enforced(self):
        self.cs.api_token = "gizli"
        try:
            async with self.session.get("/api/state") as resp:
                self.assertEqual(resp.status, 401)
            async with self.session.get("/api/state", headers={"X-Auth-Token": "gizli"}) as resp:
                self.assertEqual(resp.status, 200)
            async with self.session.get("/api/state?token=gizli") as resp:
                self.assertEqual(resp.status, 200)
        finally:
            self.cs.api_token = None


if __name__ == "__main__":
    unittest.main(verbosity=2)
