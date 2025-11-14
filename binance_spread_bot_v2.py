#!/usr/bin/env python3
# ============================================================================
# BINANCE SPREAD BOT V2 - BTCFDUSD SPOT (FIXED EVENT LOOP)
# WebSocket-only, POST-ONLY LIMIT orders, Real-time stats
# ============================================================================

import os, sys, json, time, asyncio, logging, threading, hmac, hashlib, uuid
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from collections import OrderedDict, Counter
from typing import Optional
from dotenv import load_dotenv

import websockets, requests
from binance.client import Client
from binance.enums import SIDE_BUY, SIDE_SELL
from binance.exceptions import BinanceAPIException

# ============================================================================
# LOGGING SETUP
# ============================================================================
LOG_FILE = "/opt/spred/bot_v2.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def ts_ns():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def log(msg):
    logger.info(f"{ts_ns()} | {msg}")

# ============================================================================
# CONFIG
# ============================================================================
load_dotenv()
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

if not API_KEY or not API_SECRET:
    raise RuntimeError("❌ Faltan BINANCE_API_KEY / BINANCE_API_SECRET")

SYMBOL = "BTCFDUSD"
QTY = Decimal("0.00006")
SPREAD_TARGET = Decimal("1.5")
PRICE_OFFSET = Decimal("1")
MAX_RETRIES = 3
POSTONLY_STEPS = [10, 20, 40]
WAIT_FILL_TIMEOUT = 5.0
WAIT_SPREAD_TIMEOUT = 10.0
STALE_ORDER_TIME = 14400

client = Client(API_KEY, API_SECRET)
sym_info = client.get_symbol_info(SYMBOL)

_lot = next(f for f in sym_info["filters"] if f["filterType"] == "LOT_SIZE")
STEP_SIZE = Decimal(_lot["stepSize"])
MIN_QTY = Decimal(_lot["minQty"])

_price = next(f for f in sym_info["filters"] if f["filterType"] == "PRICE_FILTER")
TICK_SIZE = Decimal(_price["tickSize"])
PRICE_DECIMALS = max(0, -TICK_SIZE.as_tuple().exponent)

_notional = next(f for f in sym_info["filters"] if f["filterType"] in ("MIN_NOTIONAL", "NOTIONAL"))
MIN_NOTIONAL = Decimal(_notional.get("minNotional") or _notional.get("notional"))

# ============================================================================
# HELPERS
# ============================================================================
def round_price_down(p: Decimal) -> Decimal:
    p = Decimal(str(p))
    return (p / TICK_SIZE).to_integral_value(rounding=ROUND_DOWN) * TICK_SIZE

def round_price_up(p: Decimal) -> Decimal:
    p = Decimal(str(p))
    return (p / TICK_SIZE).to_integral_value(rounding=ROUND_UP) * TICK_SIZE

def fmt_price(p) -> str:
    return f"{float(p):.{PRICE_DECIMALS}f}"

def round_qty_down(q: Decimal) -> Decimal:
    q = Decimal(str(q))
    return (q / STEP_SIZE).to_integral_value(rounding=ROUND_DOWN) * STEP_SIZE

def fmt_qty(q: Decimal) -> str:
    q = round_qty_down(q)
    s = f"{q:.{max(0, -STEP_SIZE.as_tuple().exponent)}f}"
    return s.rstrip('0').rstrip('.') if '.' in s else s

def is_post_only_reject(err_msg: str) -> bool:
    s = str(err_msg).lower()
    return any(x in s for x in ["post only", "would immediately match", "would be immediately matched"])

# ============================================================================
# GLOBAL STATE
# ============================================================================
bid_price = Decimal("0")
ask_price = Decimal("0")
book_lock = threading.Lock()
book_updated = threading.Event()

active_orders = {}
orders_lock = threading.Lock()

trades_completed = 0
trades_incomplete = 0
pnl_realized = Decimal("0")
max_pending = 0
stats_lock = threading.Lock()

_BUCKETS = [
    (0, 1, "0–1s"), (1, 2, "1–2s"), (2, 3, "2–3s"), (3, 4, "3–4s"),
    (4, 5, "4–5s"), (5, 6, "5–6s"), (6, 7, "6–7s"), (7, 20, "7–20s"),
    (20, 40, "20–40s"), (40, 80, "40–80s"), (80, 160, "80–160s"),
    (160, 300, "160–300s"), (300, 600, "300–600s"), (600, 1200, "600–1200s"),
    (1200, 3000, "1200–3000s"), (3000, 6000, "3000–6000s"),
    (6000, 14400, "6000–14400s"), (14400, 30000, "14400–30000s"),
    (30000, 60000, "30000–60000s"), (60000, 172800, "60000–172800s"),
]
time_buckets = OrderedDict((lab, 0) for _, _, lab in _BUCKETS)

_DELTA_BINS = [
    (0, 1, "Δ0–1"), (1, 3, "Δ1–3"), (3, 6, "Δ3–6"),
    (6, 12, "Δ6–12"), (12, 24, "Δ12–24"), (24, float("inf"), "Δ24+"),
]
DELTA_EMOJI = {
    "Δ0–1": "🟩", "Δ1–3": "💚", "Δ3–6": "🟨",
    "Δ6–12": "🟧", "Δ12–24": "🟥", "Δ24+": "🟥",
}
delta_buckets = OrderedDict((lab, Counter()) for _, _, lab in _DELTA_BINS)

stale_orders = []
stale_lock = threading.Lock()

# ============================================================================
# WEBSOCKET CLIENTS (CON EVENT LOOP COMPARTIDO)
# ============================================================================
class BookWS:
    def __init__(self, loop):
        self.uri = f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@bookTicker"
        self.loop = loop

    def start(self):
        asyncio.run_coroutine_threadsafe(self._connect(), self.loop)
        log("📖 BookWS iniciado")

    async def _connect(self):
        while True:
            try:
                async with websockets.connect(
                    self.uri, ping_interval=20, ping_timeout=10, close_timeout=2, compression=None
                ) as ws:
                    log(f"✅ BookWS conectado")
                    async for msg in ws:
                        try:
                            data = json.loads(msg)
                            global bid_price, ask_price
                            with book_lock:
                                bid_price = Decimal(data["b"])
                                ask_price = Decimal(data["a"])
                            book_updated.set()
                        except:
                            pass
            except Exception as e:
                log(f"⚠️ BookWS error: {e}")
                await asyncio.sleep(2)


class UserStreamWS:
    def __init__(self, loop):
        self.api_key = API_KEY
        self.api_secret = API_SECRET
        self.rest_url = "https://api.binance.com"
        self.ws_uri_base = "wss://stream.binance.com:9443/ws"
        self.listen_key = None
        self.loop = loop

    def start(self):
        asyncio.run_coroutine_threadsafe(self._connect(), self.loop)
        log("👂 UserStreamWS iniciado")

    async def _get_listen_key(self) -> Optional[str]:
        try:
            headers = {"X-MBX-APIKEY": self.api_key}
            resp = requests.post(f"{self.rest_url}/api/v3/userDataStream", headers=headers, timeout=5)
            if resp.status_code == 200:
                lk = resp.json()["listenKey"]
                log(f"🔑 listenKey: {lk[:8]}...{lk[-8:]}")
                return lk
        except:
            pass
        return None

    async def _keepalive(self, lk: str) -> bool:
        try:
            headers = {"X-MBX-APIKEY": self.api_key}
            resp = requests.put(f"{self.rest_url}/api/v3/userDataStream", params={"listenKey": lk}, headers=headers, timeout=5)
            return resp.status_code == 200
        except:
            return False

    async def _connect(self):
        while True:
            try:
                self.listen_key = await self._get_listen_key()
                if not self.listen_key:
                    await asyncio.sleep(5)
                    continue

                uri = f"{self.ws_uri_base}/{self.listen_key}"
                last_keep = time.time()

                async with websockets.connect(uri, ping_interval=20, ping_timeout=10, close_timeout=2, compression=None) as ws:
                    log(f"✅ UserStreamWS conectado")
                    while True:
                        if time.time() - last_keep > 1500:
                            if not await self._keepalive(self.listen_key):
                                break
                            last_keep = time.time()

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=5)
                            data = json.loads(msg)
                            await self._on_message(data)
                        except asyncio.TimeoutError:
                            pass
            except:
                await asyncio.sleep(2)

    async def _on_message(self, msg: dict):
        if msg.get("e") != "executionReport":
            return

        oid = int(msg.get("i", 0))
        status = msg.get("X")
        side = msg.get("S")
        L = msg.get("L")
        z = msg.get("z")

        log(f"📨 #{oid} {side} → {status}")

        with orders_lock:
            if oid in active_orders:
                if status == "FILLED":
                    active_orders[oid]["filled"] = True
                    active_orders[oid]["filled_price"] = Decimal(L) if L else active_orders[oid]["price"]


class OrderWS:
    def __init__(self, loop):
        self.uri = "wss://ws-api.binance.com/ws-api/v3"
        self.loop = loop
        self.pending = {}
        self.send_lock = asyncio.Lock()
        self.ws = None

    def start(self):
        asyncio.run_coroutine_threadsafe(self._connect(), self.loop)
        log("🔗 OrderWS iniciado")

    async def _connect(self):
        while True:
            try:
                async with websockets.connect(self.uri, ping_interval=None, close_timeout=3) as ws:
                    self.ws = ws
                    log(f"✅ OrderWS conectado")
                    asyncio.create_task(self._keep_alive(ws))
                    asyncio.create_task(self._reader(ws))
                    await asyncio.sleep(3600)
            except:
                self.ws = None
                await asyncio.sleep(2)

    async def _keep_alive(self, ws):
        while True:
            try:
                await ws.ping()
                await asyncio.sleep(35)
            except:
                break

    async def _reader(self, ws):
        try:
            async for msg in ws:
                data = json.loads(msg)
                rid = data.get("id")
                fut = self.pending.pop(rid, None)
                if fut and not fut.done():
                    fut.set_result(data)
        except:
            pass

    async def place_order(self, side: str, price: str, qty: str, timeout: float = 5.0) -> Optional[int]:
        if not self.ws:
            return None

        req_id = str(uuid.uuid4())
        ts_ms = int(time.time() * 1000)

        params = {
            "apiKey": API_KEY,
            "symbol": SYMBOL,
            "side": side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": qty,
            "price": price,
            "timestamp": ts_ms,
            "recvWindow": 15000,
        }

        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
        params["signature"] = sig

        payload = {"id": req_id, "method": "order.place", "params": params}

        try:
            fut = self.loop.create_future()
            self.pending[req_id] = fut

            async with self.send_lock:
                await self.ws.send(json.dumps(payload))

            resp = await asyncio.wait_for(fut, timeout=timeout)

            if resp.get("code") == 0 and resp.get("result"):
                oid = resp["result"].get("orderId")
                log(f"✅ {side} orden #{oid} @ {price}")
                return oid
            else:
                err_msg = resp.get("error", {}).get("msg", str(resp))
                if is_post_only_reject(err_msg):
                    log(f"⚠️ {side} POST-ONLY: {err_msg}")
                    return None
                else:
                    log(f"❌ {side}: {err_msg}")
                    return None
        except asyncio.TimeoutError:
            log(f"⏳ {side} timeout")
            return None
        except Exception as e:
            log(f"❌ {side} exception: {e}")
            return None


# ============================================================================
# MAIN LOGIC
# ============================================================================
async def place_order_with_retry(order_ws: OrderWS, side: str, initial_price: Decimal) -> Optional[int]:
    price = initial_price

    for attempt in range(MAX_RETRIES):
        oid = await order_ws.place_order(side, fmt_price(price), fmt_qty(QTY))

        if oid is not None:
            with orders_lock:
                active_orders[oid] = {
                    "side": side,
                    "price": price,
                    "qty": QTY,
                    "created_at": time.time(),
                    "filled": False
                }
            return oid

        if attempt < MAX_RETRIES - 1:
            with book_lock:
                fresh_bid = bid_price
                fresh_ask = ask_price

            adjustment = Decimal(POSTONLY_STEPS[attempt])
            if side == SIDE_BUY:
                price = round_price_down(fresh_bid - adjustment)
                log(f"🔄 Reintento BUY #{attempt + 1}: {fmt_price(price)}")
            else:
                price = round_price_up(fresh_ask + adjustment)
                log(f"🔄 Reintento SELL #{attempt + 1}: {fmt_price(price)}")

            await asyncio.sleep(0.05)

    log(f"❌ {side} falló tras {MAX_RETRIES} intentos")
    return None


async def execute_spread_operation(order_ws: OrderWS, bid: Decimal, ask: Decimal, spread: Decimal):
    global trades_completed, trades_incomplete, pnl_realized, max_pending

    log(f"🚀 Spread: {spread} USD | bid={bid:.2f} | ask={ask:.2f}")

    buy_price = round_price_down(bid - PRICE_OFFSET)
    sell_price = round_price_up(ask + PRICE_OFFSET)

    t0 = time.time()
    buy_task = asyncio.create_task(place_order_with_retry(order_ws, SIDE_BUY, buy_price))
    sell_task = asyncio.create_task(place_order_with_retry(order_ws, SIDE_SELL, sell_price))

    buy_id, sell_id = await asyncio.gather(buy_task, sell_task)

    if buy_id is None and sell_id is None:
        log(f"⚠️ Ambas fallaron, esperando nuevo spread...")
        await asyncio.sleep(WAIT_SPREAD_TIMEOUT)
        return

    deadline = time.time() + WAIT_FILL_TIMEOUT
    buy_filled = False
    sell_filled = False

    while time.time() < deadline:
        with orders_lock:
            if buy_id and buy_id in active_orders and active_orders[buy_id]["filled"]:
                buy_filled = True
            if sell_id and sell_id in active_orders and active_orders[sell_id]["filled"]:
                sell_filled = True

        if buy_filled and sell_filled:
            break

        await asyncio.sleep(0.1)

    elapsed = time.time() - t0

    with stats_lock:
        bucket_label = next((lab for lo, hi, lab in _BUCKETS if lo <= elapsed < hi), _BUCKETS[-1][2])
        time_buckets[bucket_label] += 1

        if buy_filled and sell_filled:
            trades_completed += 1
            with orders_lock:
                buy_px = active_orders[buy_id]["filled_price"]
                sell_px = active_orders[sell_id]["filled_price"]
            spread_realized = sell_px - buy_px
            pnl_realized += spread_realized * QTY

            delta_label = next((lab for lo, hi, lab in _DELTA_BINS if lo <= spread_realized < hi), _DELTA_BINS[-1][2])
            delta_buckets[bucket_label][delta_label] += 1

            log(f"✅ TRADE #{trades_completed}: BUY={fmt_price(buy_px)} | SELL={fmt_price(sell_px)} | Δ={fmt_price(spread_realized)} | PnL={float(spread_realized * QTY):.8f}")
        else:
            trades_incomplete += 1
            log(f"⏳ PENDIENTE: BUY={buy_filled} | SELL={sell_filled}")

        pending = len([o for o in active_orders.values() if not o["filled"]])
        if pending > max_pending:
            max_pending = pending
        log(f"📊 Pendientes: {pending} | Max: {max_pending}")


async def monitor_stale_orders():
    while True:
        await asyncio.sleep(10)
        current_time = time.time()
        with orders_lock:
            for oid, order in list(active_orders.items()):
                if not order["filled"]:
                    age = current_time - order["created_at"]
                    if age >= STALE_ORDER_TIME and oid not in [o["oid"] for o in stale_orders]:
                        with book_lock:
                            ref_price = ask_price if order["side"] == SIDE_BUY else bid_price
                        loss = (order["price"] - ref_price) * order["qty"]
                        if order["side"] == SIDE_SELL:
                            loss = (ref_price - order["price"]) * order["qty"]

                        with stale_lock:
                            stale_orders.append({
                                "oid": oid,
                                "side": order["side"],
                                "price": order["price"],
                                "current_price": ref_price,
                                "loss": loss,
                                "age": age
                            })
                        log(f"⚠️ STALE: #{oid} {order['side']} {fmt_price(order['price'])} → {fmt_price(ref_price)} | Pérdida: {float(loss):.8f}")


def print_stats():
    log("=" * 100)
    log("📈 ESTADÍSTICAS FINALES")
    log(f"✅ Trades completados: {trades_completed}")
    log(f"⏳ Trades incompletos: {trades_incomplete}")
    log(f"💰 PnL realizado: {float(pnl_realized):.8f} USD")
    log(f"📊 Máximo pendientes: {max_pending}")

    log("\n🕐 Distribución por tiempo:")
    for lab, count in time_buckets.items():
        if count > 0:
            deltas = delta_buckets[lab]
            delta_str = " | ".join([f"{DELTA_EMOJI[d]}{d}:{c}" for d, c in deltas.items()])
            log(f"   {lab}: {count} | {delta_str}")

    if stale_orders:
        with stale_lock:
            loss_sim = sum(o["loss"] for o in stale_orders)
        log(f"\n⚠️ Pérdida simulada (>14400s): {float(loss_sim):.8f} USD")
        log(f"📉 Total: {float(pnl_realized + loss_sim):.8f} USD")

    log("=" * 100)


async def main_loop(order_ws: OrderWS):
    log("▶️ Bot iniciado")

    while True:
        book_updated.wait()
        book_updated.clear()

        with book_lock:
            bid = bid_price
            ask = ask_price

        spread = ask - bid

        if spread >= SPREAD_TARGET:
            await execute_spread_operation(order_ws, bid, ask, spread)
        else:
            await asyncio.sleep(0.5)


async def async_main(loop):
    """Función que corre en el event loop principal"""
    book_ws = BookWS(loop)
    user_ws = UserStreamWS(loop)
    order_ws = OrderWS(loop)

    book_ws.start()
    user_ws.start()
    order_ws.start()

    await asyncio.sleep(5)

    await asyncio.gather(
        main_loop(order_ws),
        monitor_stale_orders()
    )


if __name__ == "__main__":
    try:
        log("=" * 100)
        log("🟢 INICIANDO BINANCE SPREAD BOT V2")
        log("=" * 100)
        log(f"📊 Símbolo: {SYMBOL} | Spread: {SPREAD_TARGET} USD | Qty: {QTY}")
        log(f"🔗 Log: {LOG_FILE}")
        log("=" * 100)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(async_main(loop))
        except KeyboardInterrupt:
            log("\n🛑 Bot detenido por usuario")
            print_stats()

    except Exception as e:
        log(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        print_stats()
