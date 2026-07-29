"""OCPP 1.6J mesaj katmani.

Vestel EVC serisi ev tipi sarj cihazlari, OCPP 1.6 JSON-over-WebSocket
protokolunu konusur. Bu dosya o protokolun tasima katmanini uygular:
cerceveleme, istek/yanit eslestirme ve hata yonetimi. Hem merkezi sistem
(central_system.py) hem de simulator (simulator.py) ayni sinifi kullanir,
boylece simulatorde calisan bir sey gercek cihazda da ayni yoldan gecer.

Tel uzerindeki format (OCPP 1.6 spesifikasyonu, bolum 4):

    CALL        [2, "<benzersizId>", "<Eylem>", {<yuk>}]
    CALLRESULT  [3, "<benzersizId>", {<yuk>}]
    CALLERROR   [4, "<benzersizId>", "<hataKodu>", "<aciklama>", {<detay>}]
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

CALL = 2
CALLRESULT = 3
CALLERROR = 4

# OCPP 1.6 tarih formati: UTC, ISO 8601.
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class OcppError(Exception):
    """Karsi tarafa CALLERROR olarak donecek hata.

    Bir isleyici (handler) bu istisnayi firlatirsa, protokole uygun bir
    CALLERROR cercevesi uretilir. Diger tum istisnalar InternalError olur.
    """

    def __init__(self, code: str = "GenericError", description: str = "", details: dict | None = None):
        super().__init__(f"{code}: {description}")
        self.code = code
        self.description = description
        self.details = details or {}


class CallTimeout(OcppError):
    def __init__(self, action: str, timeout: float):
        super().__init__("GenericError", f"{action} icin {timeout}s icinde yanit gelmedi")
        self.action = action


Handler = Callable[[dict], Awaitable[dict]]


class OcppConnection:
    """Tek bir WebSocket baglantisi uzerinde OCPP RPC.

    Okuma dongusu ile isleyici dongusu bilerek ayrildi. Gelen bir CALL'i
    isleyen kod, kendisi de disari bir CALL gonderip yanit bekleyebilsin
    istiyoruz (ornegin: StartTransaction geldi, biz de cihaza bir
    ChangeConfiguration yollayalim). Eger isleyicileri okuma dongusunun
    icinde calistirsaydik, okuma dongusu bloke olur ve beklenen yanit hic
    okunamayacagi icin kilitlenme olurdu. Ayri bir kuyruk isleyicisi bunu
    onler; kuyruk tek tuketicili oldugu icin de OCPP'nin gerektirdigi
    mesaj sirasi korunur.
    """

    def __init__(
        self,
        ws: Any,
        peer_id: str,
        handlers: dict[str, Handler] | None = None,
        response_timeout: float = 30.0,
        log: logging.Logger | None = None,
    ):
        self.ws = ws
        self.peer_id = peer_id
        self.handlers: dict[str, Handler] = dict(handlers or {})
        self.response_timeout = response_timeout
        self.log = log or logging.getLogger("ocpp")

        self._pending: dict[str, asyncio.Future] = {}
        self._inbox: asyncio.Queue = asyncio.Queue()
        self._closed = False

    # ------------------------------------------------------------------
    # Giden istekler
    # ------------------------------------------------------------------
    async def call(self, action: str, payload: dict | None = None, timeout: float | None = None) -> dict:
        """Karsi tarafa bir CALL gonder ve CALLRESULT yukunu dondur."""
        if self._closed:
            raise OcppError("GenericError", f"{self.peer_id} baglantisi kapali")

        timeout = self.response_timeout if timeout is None else timeout
        unique_id = uuid.uuid4().hex
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[unique_id] = future

        frame = json.dumps([CALL, unique_id, action, payload or {}])
        self.log.debug("-> %s %s %s", self.peer_id, action, frame)
        try:
            await self.ws.send(frame)
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            raise CallTimeout(action, timeout) from None
        finally:
            self._pending.pop(unique_id, None)

    # ------------------------------------------------------------------
    # Dongular
    # ------------------------------------------------------------------
    async def run(self) -> None:
        """Baglanti kapanana kadar oku ve isle."""
        worker = asyncio.create_task(self._process_inbox(), name=f"ocpp-inbox-{self.peer_id}")
        try:
            async for raw in self.ws:
                self._on_frame(raw)
        finally:
            self._closed = True
            worker.cancel()
            # Bekleyen tum istekleri serbest birak, yoksa cagiranlar
            # zaman asimina kadar bosuna bekler.
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(OcppError("GenericError", "baglanti kapandi"))
            self._pending.clear()
            try:
                await worker
            except asyncio.CancelledError:
                pass

    def _on_frame(self, raw: Any) -> None:
        """Cerceveyi ayristir; yanitlari aninda coz, istekleri kuyruga at."""
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "replace")
        try:
            message = json.loads(raw)
            if not isinstance(message, list) or not message:
                raise ValueError("mesaj bir dizi degil")
            kind = message[0]
        except (ValueError, TypeError) as exc:
            self.log.warning("%s bicimsiz cerceve gonderdi (%s): %.200s", self.peer_id, exc, raw)
            return

        self.log.debug("<- %s %.400s", self.peer_id, raw)

        if kind == CALL:
            # [2, id, action, payload]
            if len(message) < 4:
                self.log.warning("%s eksik CALL gonderdi: %.200s", self.peer_id, raw)
                return
            self._inbox.put_nowait((message[1], message[2], message[3] or {}))

        elif kind == CALLRESULT:
            # [3, id, payload]
            future = self._pending.get(message[1])
            if future and not future.done():
                future.set_result(message[2] if len(message) > 2 else {})
            else:
                self.log.debug("%s eslesmeyen CALLRESULT: %s", self.peer_id, message[1])

        elif kind == CALLERROR:
            # [4, id, code, description, details]
            future = self._pending.get(message[1])
            if future and not future.done():
                code = message[2] if len(message) > 2 else "GenericError"
                desc = message[3] if len(message) > 3 else ""
                details = message[4] if len(message) > 4 else {}
                future.set_exception(OcppError(code, desc, details))
            else:
                self.log.debug("%s eslesmeyen CALLERROR: %s", self.peer_id, message[1])

        else:
            self.log.warning("%s bilinmeyen mesaj tipi %r", self.peer_id, kind)

    async def _process_inbox(self) -> None:
        """Gelen CALL'leri sirayla isle ve yanitla."""
        while True:
            unique_id, action, payload = await self._inbox.get()
            try:
                reply = await self._dispatch(action, payload)
                frame = json.dumps([CALLRESULT, unique_id, reply])
            except OcppError as exc:
                self.log.warning("%s %s reddedildi: %s", self.peer_id, action, exc)
                frame = json.dumps([CALLERROR, unique_id, exc.code, exc.description, exc.details])
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - isleyici hatasi baglantiyi dusurmemeli
                self.log.exception("%s %s islenirken hata", self.peer_id, action)
                frame = json.dumps([CALLERROR, unique_id, "InternalError", str(exc), {}])

            try:
                await self.ws.send(frame)
            except Exception:  # noqa: BLE001 - baglanti gitmis olabilir
                self.log.debug("%s icin yanit gonderilemedi (baglanti kapali)", self.peer_id)
                return

    async def _dispatch(self, action: str, payload: dict) -> dict:
        handler = self.handlers.get(action)
        if handler is None:
            # OCPP 1.6, desteklenmeyen eylem icin bu kodu bekler. Cihazin
            # gonderdigi her seyi desteklemek zorunda degiliz; standart
            # cevap vermek cihazin baglantiyi kesmesini onler.
            raise OcppError("NotImplemented", f"{action} desteklenmiyor")
        result = await handler(payload)
        return result if result is not None else {}
