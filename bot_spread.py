# === LOGGING ===
import os, logging
from logging.handlers import RotatingFileHandler
import random
import asyncio, json, hmac, hashlib, uuid, websockets, time

LOG_PATH = "/opt/spred/bot_log.log"
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

class FSyncRotatingHandler(RotatingFileHandler):
    def __init__(self, *a, fsync_every=1, **kw):
        super().__init__(*a, **kw)
        self._fsync_every = max(1, int(fsync_every))
        self._counter = 0
    def emit(self, record):
        super().emit(record)
        try:
            if self.stream and hasattr(self.stream, "fileno"):
                self._counter += 1
                if self._counter >= self._fsync_every:
                    os.fsync(self.stream.fileno())
                    self._counter = 0
        except Exception:
            pass

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.propagate = False

handler = FSyncRotatingHandler(
    LOG_PATH,
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8",
    delay=False,
    fsync_every=200,
)
handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)

import io, sys, threading, atexit, signal

class _StreamToLogger(io.TextIOBase):
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
    def write(self, buf):
        if not buf:
            return 0
        for line in str(buf).splitlines():
            line = line.strip()
            if line:
                self.logger.log(self.level, line)
        return len(buf)
    def flush(self):
        pass

def _safe_print(*args, **kwargs):
    sep = kwargs.get("sep", " ")
    end = kwargs.get("end", "")
    msg = sep.join(map(str, args)) + end
    logger.info(msg)

print = _safe_print

sys.stdout = _StreamToLogger(logger, logging.INFO)
sys.stderr = _StreamToLogger(logger, logging.ERROR)

def _uncaught(exc_type, exc, tb):
    if issubclass(exc_type, KeyboardInterrupt):
        logger.info("KeyboardInterrupt (Ctrl+C/SIGHUP). Cerrando limpio…")
        logging.shutdown()
        return
    logger.error("Uncaught exception", exc_info=(exc_type, exc, tb))
sys.excepthook = _uncaught

def _thread_hook(args):
    logger.error("Uncaught thread exception", exc_info=(args.exc_type, args.exc_value, args.exc_traceback))
threading.excepthook = _thread_hook

def _shutdown_ws_client():
    try:
        if ws_client and ws_client.ws:
            asyncio.run_coroutine_threadsafe(ws_client.ws.close(), ws_client.loop)
    except: pass
    try:
        if ws_client and ws_client.loop:
            ws_client.loop.call_soon_threadsafe(ws_client.loop.stop)
    except: pass

def _graceful_shutdown(*_):
    global twm
    logger.info("SIGTERM/SIGHUP recibido; cerrando…")
    try: twm.stop()
    except Exception: pass
    try: _shutdown_ws_client()
    except Exception: pass
    logging.shutdown()
    try: sys.exit(0)
    except SystemExit: pass

for _sig in (signal.SIGTERM, signal.SIGHUP, getattr(signal, "SIGQUIT", None)):
    if _sig is None:
        continue
    try:
        signal.signal(_sig, _graceful_shutdown)
    except Exception:
        pass

atexit.register(logging.shutdown)

from decimal import Decimal
from datetime import datetime
def timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

def _signal_name(sig):
    try:
        import signal as _s
        for n in dir(_s):
            if n.startswith("SIG") and getattr(_s, n) == sig:
                return n
    except Exception:
        pass
    return str(sig)

def _on_sigint(sig, frame):
    logger.info(f"{timestamp()} | 🛑 Ctrl+C (SIGINT) recibido — cerrando conexiones y deteniendo bot...")
    try:
        if twm:
            twm.stop()
            logger.info("🔌 ThreadedWebsocketManager detenido.")
    except Exception as e:
        logger.warning(f"⚠️ Error al detener TWM: {e}")

    try:
        if ws_client and ws_client.ws:
            asyncio.run_coroutine_threadsafe(ws_client.ws.close(), ws_client.loop)
            logger.info("🔌 WSClient cerrado.")
    except Exception as e:
        logger.warning(f"⚠️ Error al cerrar WSClient: {e}")

    try:
        logging.shutdown()
    except Exception:
        pass

    sys.exit(0)

try:
    signal.signal(signal.SIGINT, _on_sigint)
    signal.siginterrupt(signal.SIGINT, False)
except Exception:
    pass

import os, time, threading

from collections import OrderedDict
from dotenv import load_dotenv
from binance.client import Client
from binance import ThreadedWebsocketManager
from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_LIMIT_MAKER
from binance.exceptions import BinanceAPIException

import threading
STATS_LOCK = threading.Lock()
ws_ready = False
import queue
SPREAD_QUEUE = queue.Queue(maxsize=100)

load_dotenv()
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

if not API_KEY or not API_SECRET:
    raise RuntimeError("Faltan BINANCE_API_KEY / BINANCE_API_SECRET en el .env")

SIMBOLO = "BTCFDUSD"
QTY = Decimal("0.00006")
SPREAD_OBJETIVO = 1.5
COOLDOWN_S = 0.1
OFFSET_USD = 1
MAX_WAIT_S = 10
ordenes_activas = set()
pnl_total = 0.0
delta_total = 0.0
pnl_porcentaje = 0.0
CAPITAL_INICIAL_USD = 10
MIN_GAP_BETWEEN_LAUNCHES = 0.2
_last_launch = 0.0

OPERACIONES_ACTIVAS = 0
MAX_OPERACIONES_SIMULTANEAS = 1
OPERACIONES_LOCK = threading.Lock()

ORDENES_LOCK = threading.Lock()
REACT_LOCK = threading.Lock()
CREACION_LOCK = threading.Lock()
BOOK_READY = threading.Event()

POSTONLY_STEP_USD = 10.0
POSTONLY_MAX_TRIES = 5
ws_book_task = None

BOT_PAUSE = threading.Event()
BOT_STOP = threading.Event()

client = Client(API_KEY, API_SECRET)

ultimo_bid = 0.0
ultimo_ask = 0.0

ORDER_STATE = {}
ORDER_EVENT = {}
ORDER_LOCK = threading.Lock()

def _ensure_event(oid: int) -> threading.Event:
    with ORDER_LOCK:
        ev = ORDER_EVENT.get(oid)
        if ev is None:
            ev = threading.Event()
            ORDER_EVENT[oid] = ev
        return ev

from binance.exceptions import BinanceAPIException
import time
from decimal import Decimal, ROUND_DOWN

_sym = client.get_symbol_info(SIMBOLO)
if not _sym or "filters" not in _sym:
    raise RuntimeError(f"Símbolo inválido/no disponible en spot: {SIMBOLO}")

_lot = next(f for f in _sym["filters"] if f["filterType"] == "LOT_SIZE")
STEP_SIZE = Decimal(_lot["stepSize"])
MIN_QTY = Decimal(_lot["minQty"])
MAX_QTY = Decimal(_lot["maxQty"])
QTY_DECIMALS = max(0, -STEP_SIZE.as_tuple().exponent)

_notional = next(f for f in _sym["filters"] if f["filterType"] in ("MIN_NOTIONAL","NOTIONAL"))
MIN_NOTIONAL = Decimal(_notional.get("minNotional") or _notional.get("notional"))

_price_filter = next(f for f in _sym["filters"] if f["filterType"] == "PRICE_FILTER")
TICK_SIZE = Decimal(_price_filter["tickSize"])

def _round_qty_down(q: Decimal) -> Decimal:
    q = Decimal(str(q))
    return (q / STEP_SIZE).to_integral_value(rounding=ROUND_DOWN) * STEP_SIZE

def fmt_qty(q: Decimal) -> str:
    q = _round_qty_down(q)
    s = f"{q:.{QTY_DECIMALS}f}"
    return s.rstrip('0').rstrip('.') if '.' in s else s

def _min_qty_for_price(price: float) -> Decimal:
    p = Decimal(str(price))
    need = (MIN_NOTIONAL / p)
    need = _round_qty_down(need) if need > MIN_QTY else MIN_QTY
    return need

def fix_qty_for(side: str, limit_price: float, desired: Decimal) -> Decimal | None:
    q = _round_qty_down(desired)
    if q < MIN_QTY:
        q = MIN_QTY

    notional = Decimal(str(limit_price)) * q
    if notional < MIN_NOTIONAL:
        q = _min_qty_for_price(limit_price)

    if q > MAX_QTY:
        return None
    return q

def _t_now():
    try:    return time.perf_counter()
    except: return time.time()

def _sleep(s):
    try:    time.sleep(s)
    except: pass

def _round_price_buy(p):
    p = Decimal(str(p))
    q = (p / TICK_SIZE).to_integral_value(rounding=ROUND_DOWN) * TICK_SIZE
    return float(q)

def _round_price_sell(p):
    p = Decimal(str(p))
    q = (p / TICK_SIZE).to_integral_value(rounding=ROUND_DOWN) * TICK_SIZE
    return float(q)

PRICE_DECIMALS = max(0, -TICK_SIZE.as_tuple().exponent)

def fmt_price(p: float) -> str:
    return f"{p:.{PRICE_DECIMALS}f}"

def fmt_price_safe(p) -> str:
    if p is None:
        return "None"
    try:
        return fmt_price(float(p))
    except Exception:
        try:
            return f"{float(p):.{PRICE_DECIMALS}f}"
        except Exception:
            return str(p)

def sanitize_maker_prices(buy_target: float, sell_target: float):
    if not _book_is_fresh():
        return None, None
    bid_now, ask_now = _book_now(max_age_s=MAX_BOOK_AGE_S)
    if bid_now <= 0 or ask_now <= 0:
        return None, None

    guard = float(TICK_SIZE)
    safe_buy = _round_price_buy(min(buy_target, ask_now - guard))
    safe_sell = _round_price_sell(max(sell_target, bid_now + guard))
    if safe_buy >= safe_sell:
        return None, None
    return safe_buy, safe_sell

def pause_new_ops(reason=""):
    BOT_PAUSE.set()
    print(f"{timestamp()} | ⏸️ Pausa nuevas operaciones: {reason}")

def resume_new_ops():
    if BOT_PAUSE.is_set():
        BOT_PAUSE.clear()
        print(f"{timestamp()} | ▶️ Reanudadas nuevas operaciones")

def hard_stop(reason=""):
    BOT_STOP.set()
    print(f"{timestamp()} | ⛔ Bot detenido por seguridad: {reason}")

def is_post_only_reject(err: Exception) -> bool:
    s = str(err).lower()
    return (
        "post only" in s
        or "would immediately match" in s
        or "would be immediately matched" in s
        or "order would be immediately" in s
    )

def oa_add(oid):
    with ORDENES_LOCK:
        ordenes_activas.add(oid)

def oa_discard(oid):
    with ORDENES_LOCK:
        ordenes_activas.discard(oid)

def oa_len():
    with ORDENES_LOCK:
        return len(ordenes_activas)

def oa_snapshot():
    with ORDENES_LOCK:
        return set(ordenes_activas)

def oa_replace(new_ids: set):
    with ORDENES_LOCK:
        ordenes_activas.clear()
        ordenes_activas.update(new_ids)

import collections
import threading as _th
from uuid import uuid4 as _uuid4

PNL_LOCK = _th.Lock()
PAIR_REG = {}
PAIR_DATA = {}

def _avg_fill_price(order_obj) -> float:
    try:
        exq = float(order_obj.get("executedQty", "0"))
        cq = float(order_obj.get("cummulativeQuoteQty", "0"))
        if exq > 0 and cq > 0:
            return cq / exq
    except Exception:
        pass
    try:
        return float(order_obj.get("price", 0.0))
    except Exception:
        return 0.0

def _mark_pair_on_send(buy_id, sell_id):
    pid = str(_uuid4())
    if buy_id: PAIR_REG[buy_id] = pid
    if sell_id: PAIR_REG[sell_id] = pid
    PAIR_DATA[pid] = {
        "buy_id": buy_id, "sell_id": sell_id,
        "buy_px": None, "sell_px": None,
        "qty": float(QTY),
        "accounted": False
    }
    return pid

def _get_executed_qty(order_obj) -> float:
    try: return float(order_obj.get("executedQty", "0") or 0.0)
    except: return 0.0

def _account_pnl_if_pair_ready(oid) -> bool:
    pid = PAIR_REG.get(oid)
    if not pid: return False
    pd = PAIR_DATA.get(pid)
    if not pd or pd.get("accounted"): return False

    try:
        ob = client.get_order(symbol=SIMBOLO, orderId=pd["buy_id"]) if pd["buy_id"] else None
        os_ = client.get_order(symbol=SIMBOLO, orderId=pd["sell_id"]) if pd["sell_id"] else None

        if ob and ob.get("status") == "FILLED" and pd["buy_px"] is None:
            pd["buy_px"] = _avg_fill_price(ob)
        if os_ and os_.get("status") == "FILLED" and pd["sell_px"] is None:
            pd["sell_px"] = _avg_fill_price(os_)

        if pd["buy_px"] is not None and pd["sell_px"] is not None:
            q_buy = _get_executed_qty(ob) if ob else 0.0
            q_sell = _get_executed_qty(os_) if os_ else 0.0
            q_eff = min(q_buy, q_sell)
            if q_eff > 0:
                pnl_local = (pd["sell_px"] - pd["buy_px"]) * q_eff
                with PNL_LOCK:
                    global pnl_total, delta_total, pnl_porcentaje
                    pnl_total += pnl_local
                    delta_total += (pd["sell_px"] - pd["buy_px"])
                    pnl_porcentaje = (pnl_total / CAPITAL_INICIAL_USD) * 100.0
                pd["accounted"] = True
                return True
    except Exception:
        pass
    return False

ORDEN_TIMESTAMPS = collections.deque(maxlen=2000)
ORDENES_TOTAL = 0
ORDENES_MINUTO = collections.deque(maxlen=60)
MAX_ORDENES_SEG = 0
FECHA_MAX = "-"

def registrar_orden():
    global ORDENES_TOTAL, MAX_ORDENES_SEG, FECHA_MAX
    ahora = time.time()
    with STATS_LOCK:
        ORDEN_TIMESTAMPS.append(ahora)
        ORDENES_TOTAL += 1
        recientes = [t for t in ORDEN_TIMESTAMPS if ahora - t <= 1]
        por_segundo = len(recientes)
        if por_segundo > MAX_ORDENES_SEG:
            MAX_ORDENES_SEG = por_segundo
            FECHA_MAX = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ORDENES_MINUTO.append(por_segundo)
        prom_minuto = sum(ORDENES_MINUTO) / len(ORDENES_MINUTO)
        max_seg = MAX_ORDENES_SEG
        fecha = FECHA_MAX
        total = ORDENES_TOTAL
    return por_segundo, prom_minuto, max_seg, fecha, total

def print_metricas():
    por_seg, prom_min, max_seg, fecha, total = registrar_orden()
    fecha_txt = fecha or "-"
    msg = (f"{timestamp()} | ⚙️ {por_seg}/s | ⏱ Prom (último min): {prom_min:.2f}/s "
           f"≈ {prom_min * 60:.0f}/min | 🧭 Máx: {max_seg} ({fecha_txt}) | 🧮 Total: {total}")
    print(msg)

_BUCKETS = [
    (0, 1, "0–1s"),
    (1, 2, "1–2s"),
    (2, 3, "2–3s"),
    (3, 4, "3–4s"),
    (4, 5, "4–5s"),
    (5, 6, "5–6s"),
    (6, 7, "6–7s"),
    (7, 20, "7–20s"),
    (20, 40, "20–40s"),
    (40, 80, "40–80s"),
    (80, 160, "80–160s"),
    (160, 300, "160–300s"),
    (300, 600, "300–600s"),
    (600, 1200, "600–1200s"),
    (1200, 3000, "1200–3000s"),
    (3000, 6000, "3000–6000s"),
    (6000, 14400, "6000–14400s"),
    (14400, 30000, "14400–30000s"),
    (30000, 60000, "30000–60000s"),
    (60000, 172800, "60000–172800s"),
]
hist_fills = OrderedDict((lab, 0) for _, _, lab in _BUCKETS)
timeouts_600p = 0
max_pendientes = 0

_DELTA_BINS = [
    (0, 1, "Δ0–1"),
    (1, 3, "Δ1–3"),
    (3, 6, "Δ3–6"),
    (6, 12, "Δ6–12"),
    (12, 24, "Δ12–24"),
    (24, float("inf"), "Δ24+"),
]
from collections import Counter
delta_fills_by_bucket = OrderedDict((lab, Counter()) for _, _, lab in _BUCKETS)

DELTA_EMOJI = {
    "Δ0–1": "🟩",
    "Δ1–3": "💚",
    "Δ3–6": "🟨",
    "Δ6–12": "🟧",
    "Δ12–24": "🟥",
    "Δ24+": "🟥",
}

def _delta_label(d: float) -> str:
    for lo, hi, lab in _DELTA_BINS:
        if lo <= d < hi:
            return lab
    return "Δ24+"

def _format_top_deltas(time_bucket: str, top_n: int = 4) -> str:
    cnt = delta_fills_by_bucket.get(time_bucket, Counter())
    if not cnt:
        return ""
    items = sorted(cnt.items(), key=lambda kv: (-kv[1], kv[0]))[:top_n]
    medals = ["🥇", "🥈", "🥉", "4️⃣"]
    parts = []
    for i, (lab, n) in enumerate(items):
        em = DELTA_EMOJI.get(lab, "")
        parts.append(f"{medals[i]} {em}{lab}: {n}")
    return " (" + " | ".join(parts) + ")"

def _bucket_label(s: float) -> str:
    for lo, hi, lab in _BUCKETS:
        if lo <= s < hi:
            return lab
    return _BUCKETS[-1][2]

def ts_ms() -> int:
    return int(time.time() * 1000)

def best_bid() -> float:
    b, _ = _book_now(max_age_s=MAX_BOOK_AGE_S)
    return b

def best_ask() -> float:
    _, a = _book_now(max_age_s=MAX_BOOK_AGE_S)
    return a

class ExchangeWouldMatch(Exception):
    pass

def is_post_only_reject(err: Exception) -> bool:
    s = str(err).lower()
    return (
        "post only" in s
        or "would immediately match" in s
        or "would be immediately matched" in s
        or "order would be immediately" in s
    )

def _would_immediately_match(side, price, bid_now, ask_now):
    if side == SIDE_BUY:
        return price >= ask_now
    else:
        return price <= bid_now

def log_book_context(tag, side=None, price=None):
    bid_now, ask_now = _book_now()
    extra = ""
    if side is not None and price is not None and bid_now and ask_now:
        wm = _would_immediately_match(side, float(price), bid_now, ask_now)
        extra = f" | side={side} | px={float(price):.2f} | would_match={wm}"
    print(f"{timestamp()} | 🔎 BOOK[{tag}] bid_now={bid_now:.2f} ask_now={ask_now:.2f}{extra}")

def _print_hist():
    global max_pendientes, FECHA_MAX
    limpiar_set_ordenes()
    actuales = oa_len()

    with STATS_LOCK:
        if actuales > max_pendientes:
            max_pendientes = actuales
            FECHA_MAX = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        hist_copy = list(hist_fills.items())
        max_pend_copy = max_pendientes
        fecha_max_copy = FECHA_MAX or "-"

    partes = []
    for lab, cnt in hist_copy:
        if cnt > 0:
            tops = _format_top_deltas(lab, top_n=4)
            partes.append(f"{lab}: {cnt}{tops}")

    partes.append(f"🕒 Pendientes activas: {actuales}")
    if max_pend_copy > 0:
        partes.append(f"🏔️ Máx pendientes históricas: {max_pend_copy} ({fecha_max_copy})")

    print(f"{timestamp()} | 📊 Distribución fills ↓\n" + "\n".join("   " + p for p in partes))

def limpiar_set_ordenes():
    snapshot = oa_snapshot()
    ids_verificados = set()
    for oid in snapshot:
        try:
            o = client.get_order(symbol=SIMBOLO, orderId=oid)
            if o["status"] in ("NEW", "PARTIALLY_FILLED"):
                ids_verificados.add(oid)
        except BinanceAPIException:
            pass
    oa_replace(ids_verificados)

def _ws_is_open(ws) -> bool:
    if ws is None:
        return False
    val_open = getattr(ws, "open", None)
    if val_open is not None:
        try: return bool(val_open)
        except: pass
    val_closed = getattr(ws, "closed", None)
    if val_closed is not None:
        try: return not bool(val_closed)
        except: pass
    state = getattr(ws, "state", None)
    try:
        from websockets.protocol import State
        return state == State.OPEN
    except Exception:
        return False

class WSClient:
    def __init__(self):
        self.uri = "wss://ws-api.binance.com/ws-api/v3"
        self.ws = None
        self.ws_ready = asyncio.Event()
        self.connecting = False
        self.last_connect_time = 0.0
        self.pending = {}
        self.send_lock = asyncio.Lock()
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self.loop.run_forever, daemon=True).start()
        asyncio.run_coroutine_threadsafe(self.keep_alive(), self.loop)
        asyncio.run_coroutine_threadsafe(self.reader(), self.loop)
        self.last_order_time = 0.0

    async def connect(self):
        if self.connecting:
            return
        self.connecting = True
        try:
            if _ws_is_open(self.ws):
                self.ws_ready.set(); return

            now = time.time()
            if now - self.last_connect_time < 1.0:
                await asyncio.sleep(1.0 - (now - self.last_connect_time))

            self.ws = await websockets.connect(self.uri, ping_interval=None, close_timeout=3)
            self.last_connect_time = time.time()
            if _ws_is_open(self.ws):
                self.ws_ready.set()
                print(f"{timestamp()} | ✅ WS conectado a Binance API v3")
            else:
                self.ws_ready.clear()
        except Exception as e:
            self.ws_ready.clear(); self.ws = None
            print(f"{timestamp()} | ⚠️ Error conectando WS: {e}")
        finally:
            self.connecting = False

    async def keep_alive(self):
        while True:
            try:
                if not _ws_is_open(self.ws):
                    self.ws_ready.clear()
                    print(f"{timestamp()} | 🔁 Reintentando conexión WS...")
                    await self.connect()
                else:
                    try:
                        pong = await self.ws.ping()
                        await asyncio.wait_for(pong, timeout=10)
                    except Exception as pe:
                        print(f"{timestamp()} | ⚠️ WS ping falló: {pe}")
                        try:
                            if self.ws: await self.ws.close()
                        except: pass
                        self.ws = None
                        self.ws_ready.clear()
                await asyncio.sleep(35)
            except Exception as e:
                print(f"{timestamp()} | ⚠️ WS KeepAlive: {e}")
                try:
                    if self.ws: await self.ws.close()
                except: pass
                self.ws = None
                self.ws_ready.clear()
                await asyncio.sleep(10)

    async def reader(self):
        while True:
            try:
                await self.ws_ready.wait()
                msg = await self.ws.recv()
                data = json.loads(msg)
                rid = data.get("id")
                fut = self.pending.pop(rid, None)
                if fut and not fut.done():
                    fut.set_result(data)
            except Exception as e:
                print(f"{timestamp()} | ⚠️ WS reader: {e}")
                try:
                    if self.ws: await self.ws.close()
                except: pass
                self.ws = None
                self.ws_ready.clear()
                await asyncio.sleep(1.0)

    def _order_params(self, side, price, ts_ms=None):
        adj_qty = fix_qty_for(side, price, QTY)
        if adj_qty is None:
            return None
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)
        p = {
            "apiKey": API_KEY,
            "symbol": SIMBOLO,
            "side": side,
            "type": "LIMIT",
            "timeInForce": "IOC",  # Immediate-or-Cancel: ejecuta ahora o cancela
            "quantity": fmt_qty(adj_qty),
            "price": fmt_price(price),
            "timestamp": ts_ms,
            "recvWindow": 15000,
        }
        query = "&".join(f"{k}={p[k]}" for k in sorted(p))
        sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
        p["signature"] = sig
        return p

    async def _send_json(self, payload: dict):
        async with self.send_lock:
            await self.ws.send(json.dumps(payload))

    async def send_order(self, side, price):
        try:
            await asyncio.wait_for(self.ws_ready.wait(), timeout=3)
        except asyncio.TimeoutError:
            return None
        if not _ws_is_open(self.ws):
            await self.connect()
            try: await asyncio.wait_for(self.ws_ready.wait(), timeout=2)
            except asyncio.TimeoutError: return None
            if not _ws_is_open(self.ws): return None

        now = time.perf_counter()
        if now - self.last_order_time < 0.02:
            await asyncio.sleep(0.02 - (now - self.last_order_time))
        self.last_order_time = time.perf_counter()

        req_id = str(uuid.uuid4())
        params = self._order_params(side, price)
        if params is None:
            print(f"{timestamp()} | ⏭️ Skip WS {side}: qty no cumple filtros (MIN_NOTIONAL/LOT_SIZE).")
            return None
        payload = {"id": req_id, "method": "order.place", "params": params}

        fut = self.loop.create_future()
        self.pending[req_id] = fut

        try:
            t0 = time.perf_counter()
            await self._send_json(payload)
            resp = await asyncio.wait_for(fut, timeout=5)
            t1 = time.perf_counter()
            print(f"{timestamp()} | ⚡ WS orden {side} enviada ({(t1 - t0)*1000:.2f} ms)")
            if "result" in resp and resp["result"]:
                return resp["result"].get("orderId")
            else:
                err = resp.get("error", {})
                print(f"{timestamp()} | ❌ WS Rechazada ({err.get('code')}) → {err.get('msg')}")
                return None
        except Exception as e:
            print(f"{timestamp()} | ⚠️ WS Error al enviar {side}: {e}")
            return None
        finally:
            self.pending.pop(req_id, None)

    async def cancel_fast(self, order_id: int) -> bool:
        try:
            await asyncio.wait_for(self.ws_ready.wait(), timeout=0.3)
        except asyncio.TimeoutError:
            return False
        if not _ws_is_open(self.ws):
            return False
        ts_ms = int(time.time() * 1000)
        params = {
            "apiKey": API_KEY,
            "symbol": SIMBOLO,
            "orderId": order_id,
            "timestamp": ts_ms
        }
        query = "&".join(f"{k}={params[k]}" for k in sorted(params))
        sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
        params["signature"] = sig
        try:
            await self._send_json({"id": str(uuid.uuid4()), "method": "order.cancel", "params": params})
            return True
        except Exception:
            return False

    async def place_two(self, buy_price, sell_price, timeout=5.0):
        sb, ss = sanitize_maker_prices(buy_price, sell_price)
        if sb is None:
            print(f"{timestamp()} | ⏭️ Abort place_two: sanitize maker.")
            return None, None, None
        buy_price, sell_price = sb, ss
        try:
            await asyncio.wait_for(self.ws_ready.wait(), timeout=3)
        except asyncio.TimeoutError:
            return None, None, None
        if not _ws_is_open(self.ws):
            await self.connect()
            try: await asyncio.wait_for(self.ws_ready.wait(), timeout=2)
            except asyncio.TimeoutError: return None, None, None
            if not _ws_is_open(self.ws): return None, None, None

        ts_ms = int(time.time() * 1000)
        bid_id = str(uuid.uuid4())
        ask_id = str(uuid.uuid4())

        params_buy = self._order_params(SIDE_BUY, buy_price, ts_ms)
        params_sell = self._order_params(SIDE_SELL, sell_price, ts_ms)
        if params_buy is None or params_sell is None:
            print(f"{timestamp()} | ⏭️ Abort place_two: qty/minNotional fuera de rango.")
            return None, None, None

        buy_payload = {"id": bid_id, "method": "order.place", "params": params_buy}
        sell_payload = {"id": ask_id, "method": "order.place", "params": params_sell}

        fut_buy = self.loop.create_future()
        fut_sell = self.loop.create_future()
        self.pending[bid_id] = fut_buy
        self.pending[ask_id] = fut_sell

        now = time.perf_counter()
        if now - self.last_order_time < 0.02:
            await asyncio.sleep(0.02 - (now - self.last_order_time))

        t0 = time.perf_counter()
        await self._send_json(buy_payload)
        t1 = time.perf_counter()
        await self._send_json(sell_payload)
        t2 = time.perf_counter()
        self.last_order_time = time.perf_counter()
        delta_ms = (t2 - t1) * 1000.0

        try:
            resp_buy, resp_sell = await asyncio.wait_for(
                asyncio.gather(fut_buy, fut_sell), timeout=timeout
            )
        except Exception as e:
            print(f"{timestamp()} | ⚠️ Espera dual falló: {e}")
            for rid in (bid_id, ask_id):
                fut = self.pending.pop(rid, None)
                if fut and not fut.done():
                    fut.cancel()
            return None, None, delta_ms
        finally:
            self.pending.pop(bid_id, None)
            self.pending.pop(ask_id, None)

        buy_oid = resp_buy.get("result", {}).get("orderId") if isinstance(resp_buy, dict) else None
        sell_oid = resp_sell.get("result", {}).get("orderId") if isinstance(resp_sell, dict) else None
        log_book_context("dual-place/BUY", SIDE_BUY, buy_price)
        log_book_context("dual-place/SELL", SIDE_SELL, sell_price)
        print(f"{timestamp()} | 📨 dual-send Δenvío≈{delta_ms:.3f} ms | BUY_px={buy_price:.2f} | SELL_px={sell_price:.2f}")
        return buy_oid, sell_oid, delta_ms

ws_client = WSClient()

def cancelar_ya(order_id: int):
    try:
        asyncio.run_coroutine_threadsafe(ws_client.cancel_fast(order_id), ws_client.loop)
    except Exception:
        pass

def colocar_ordenes_casi_atomicas(precio_buy: float, precio_sell: float, timeout=5.0):
    fut = asyncio.run_coroutine_threadsafe(
        ws_client.place_two(precio_buy, precio_sell, timeout=timeout),
        ws_client.loop
    )
    try:
        return fut.result(timeout=timeout + 1.0)
    except Exception as e:
        print(f"{timestamp()} | ❌ dual-place timeout/error: {e}")
        return None, None, None

def colocar_orden_post(side, precio):
    try:
        future = asyncio.run_coroutine_threadsafe(ws_client.send_order(side, precio), ws_client.loop)
        orden_id = future.result(timeout=5)

        if orden_id:
            registrar_creacion(orden_id)
            oa_add(orden_id)
            cur = oa_len()
            with STATS_LOCK:
                global max_pendientes, FECHA_MAX
                if cur > max_pendientes:
                    max_pendientes = cur
                    FECHA_MAX = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            print_metricas()
            print(f"{timestamp()} | 🟢 {side} colocada a {precio:.2f} USD (vía WS persistente)")
        return orden_id
    except Exception as e:
        print(f"{timestamp()} | ❌ Error WS en {side}: {e}")
        return None

def esperar_ambos_con_timeout(id_buy, id_sell, t_start, timeout_s=MAX_WAIT_S):
    """
    ✅ MEJORADO: Detecta fills individuales y sigue operación normal
    - Si UNA orden se llena: procésala y cancela la contraparte si no se llena
    - Si AMBAS se llenan: retorna True con ambos precios
    - Si timeout: retorna False pero contabiliza lo que sí se llenó
    """
    buy_price = None
    sell_price = None
    deadline = time.perf_counter() + timeout_s
    ids = [oid for oid in (id_buy, id_sell) if oid]

    for oid in ids:
        _ensure_event(oid)

    # === CHECK INMEDIATO ===
    for oid in list(ids):
        with ORDER_LOCK:
            st = ORDER_STATE.get(oid, {}).get("status")
            px = ORDER_STATE.get(oid, {}).get("last_px")

        if st == "FILLED" and px and px > 0:
            if oid == id_buy: buy_price = px
            if oid == id_sell: sell_price = px
            try: _account_pnl_if_pair_ready(oid)
            except: pass
            ids.remove(oid)

    if buy_price is not None and sell_price is not None:
        elapsed = time.perf_counter() - t_start
        return True, buy_price, sell_price, elapsed

    # === LOOP DE ESPERA ===
    while time.perf_counter() < deadline:
        for oid in list(ids):
            with ORDER_LOCK:
                st = ORDER_STATE.get(oid, {}).get("status")
                px = ORDER_STATE.get(oid, {}).get("last_px")

            if st == "FILLED" and px and px > 0:
                if oid == id_buy: buy_price = px
                if oid == id_sell: sell_price = px
                try: _account_pnl_if_pair_ready(oid)
                except: pass
                ids.remove(oid)

                # ✅ SI UNA SE LLENA, CANCELA LA CONTRAPARTE
                if buy_price is not None and id_sell and id_sell in ids:
                    print(f"{timestamp()} | 🎯 BUY FILLED @ {buy_price:.2f} — Cancelando SELL {id_sell}...")
                    try:
                        st_cancel, _ = cancelar_con_confirmacion(id_sell, SIMBOLO)
                        oa_discard(id_sell)
                        print(f"{timestamp()} | ✅ SELL cancelada → {st_cancel}")
                    except Exception as e:
                        print(f"{timestamp()} | ⚠️ Error cancelando SELL: {e}")
                    ids.remove(id_sell) if id_sell in ids else None

                elif sell_price is not None and id_buy and id_buy in ids:
                    print(f"{timestamp()} | 🎯 SELL FILLED @ {sell_price:.2f} — Cancelando BUY {id_buy}...")
                    try:
                        st_cancel, _ = cancelar_con_confirmacion(id_buy, SIMBOLO)
                        oa_discard(id_buy)
                        print(f"{timestamp()} | ✅ BUY cancelada → {st_cancel}")
                    except Exception as e:
                        print(f"{timestamp()} | ⚠️ Error cancelando BUY: {e}")
                    ids.remove(id_buy) if id_buy in ids else None

        if buy_price is not None and sell_price is not None:
            elapsed = time.perf_counter() - t_start
            return True, buy_price, sell_price, elapsed

        wait_left = max(0.02, min(0.2, deadline - time.perf_counter()))
        any_set = False
        for oid in ids:
            ev = _ensure_event(oid)
            if ev.wait(timeout=wait_left):
                any_set = True
        if any_set:
            continue

        # Fallback REST después de 2s
        if (time.perf_counter() - t_start) > 2.0:
            for oid in list(ids):
                try:
                    o = client.get_order(symbol=SIMBOLO, orderId=oid)
                    st = o.get("status")
                    if st == "FILLED":
                        px = _avg_fill_price(o)
                        if px and px > 0:
                            if oid == id_buy: buy_price = px
                            if oid == id_sell: sell_price = px
                            oa_discard(oid)
                            with CREACION_LOCK:
                                ordenes_creadas.pop(oid, None)
                            try: _account_pnl_if_pair_ready(oid)
                            except: pass
                            ids.remove(oid)
                    elif st in ("CANCELED", "REJECTED", "EXPIRED"):
                        oa_discard(oid)
                        with CREACION_LOCK:
                            ordenes_creadas.pop(oid, None)
                        ids.remove(oid)
                except BinanceAPIException:
                    pass

        if buy_price is not None and sell_price is not None:
            elapsed = time.perf_counter() - t_start
            return True, buy_price, sell_price, elapsed

    elapsed = time.perf_counter() - t_start

    # ✅ LOG MEJORADO: Muestra qué se llenó incluso con timeout
    if buy_price is not None or sell_price is not None:
        print(f"{timestamp()} | ⏳ TIMEOUT ({timeout_s}s) pero PARCIAL FILL: BUY={buy_price}, SELL={sell_price}")
        return False, buy_price, sell_price, elapsed

    print(f"{timestamp()} | ⏳ TIMEOUT ({timeout_s}s) — Sin fills. Sigo escuchando userDataStream...")
    return False, buy_price, sell_price, elapsed


LAST_BOOK_TS = 0.0
MAX_BOOK_AGE_S = 0.30

def _now_mono():
    try: return time.perf_counter()
    except: return time.time()

def _book_is_fresh(max_age_s=MAX_BOOK_AGE_S):
    return (_now_mono() - LAST_BOOK_TS) <= max_age_s

def _book_now(max_age_s=2.0):
    if ultimo_bid and ultimo_ask and (_now_mono() - LAST_BOOK_TS) <= max_age_s:
        return float(ultimo_bid), float(ultimo_ask)
    try:
        bk = client.get_orderbook_ticker(symbol=SIMBOLO)
        return float(bk["bidPrice"]), float(bk["askPrice"])
    except Exception:
        return float(ultimo_bid or 0.0), float(ultimo_ask or 0.0)

def on_book(msg):
    global ultimo_bid, ultimo_ask, ws_ready, LAST_BOOK_TS

    if not BOOK_READY.is_set():
        BOOK_READY.set()
        print(f"{timestamp()} | 🎯 Primer frame del BOOK recibido")

    if not ws_ready or not isinstance(msg, dict):
        return

    bid_key = 'b' if 'b' in msg else 'bidPrice'
    ask_key = 'a' if 'a' in msg else 'askPrice'

    if bid_key not in msg or ask_key not in msg:
        return

    try:
        bid = float(msg[bid_key])
        ask = float(msg[ask_key])

        if bid <= 0 or ask <= 0:
            return

        ultimo_bid, ultimo_ask = bid, ask
        LAST_BOOK_TS = _now_mono()

        spread = ask - bid
        if spread >= SPREAD_OBJETIVO:
            try:
                SPREAD_QUEUE.put_nowait((bid, ask, spread))
            except queue.Full:
                pass

    except (ValueError, KeyError) as e:
        print(f"{timestamp()} | ⚠️ Error parseando book: {e}, msg={msg}")
    except Exception as e:
        print(f"{timestamp()} | ⚠️ Error inesperado en on_book: {e}")

BOOK_WS_URI = f"wss://stream.binance.com:9443/ws/{SIMBOLO.lower()}@bookTicker"
ws_book_lock = threading.Lock()
ws_book_loop = asyncio.new_event_loop()
ws_book_task = None
ws_book_thread = None

async def _book_loop():
    global ws_ready, LAST_BOOK_TS
    uri = BOOK_WS_URI
    reconnect_delay = 1.0

    while True:
        ws = None
        try:
            ws = await websockets.connect(
                uri,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=2,
                max_size=2**20,
                compression=None
            )
            ws_ready = True
            reconnect_delay = 1.0
            print(f"{timestamp()} | ✅ BOOK conectado: {uri}")

            msg_count = 0
            last_msg_time = time.time()

            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    on_book(msg)

                    msg_count += 1
                    now = time.time()

                    if msg_count % 500 == 0:
                        elapsed = now - last_msg_time
                        rate = 500 / elapsed if elapsed > 0 else 0
                        print(f"{timestamp()} | 📊 BOOK: {msg_count} msgs recibidos (~{rate:.1f} msg/s)")
                        last_msg_time = now

                except json.JSONDecodeError as e:
                    print(f"{timestamp()} | ⚠️ BOOK JSON inválido: {e}")
                except Exception as e:
                    print(f"{timestamp()} | ⚠️ BOOK parse error: {e}")

        except asyncio.CancelledError:
            print(f"{timestamp()} | 🔁 BOOK task cancelada (restart interno)")
            if ws:
                try:
                    await ws.close()
                except:
                    pass
            raise

        except websockets.exceptions.ConnectionClosed as e:
            ws_ready = False
            print(f"{timestamp()} | ⚠️ BOOK conexión cerrada por servidor: code={e.code}, reason={e.reason}")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 1.5, 10.0)

        except Exception as e:
            ws_ready = False
            print(f"{timestamp()} | ⚠️ BOOK error inesperado: {type(e).__name__}: {e}")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 1.5, 10.0)

        finally:
            if ws:
                try:
                    await ws.close()
                except:
                    pass

def start_ws_book():
    global ws_book_thread, ws_book_task

    def _runner():
        asyncio.set_event_loop(ws_book_loop)
        ws_book_task = ws_book_loop.create_task(_book_loop())
        try:
            ws_book_loop.run_forever()
        except KeyboardInterrupt:
            pass
        finally:
            print(f"{timestamp()} | 🔻 Loop del BOOK detenido limpiamente.")
            pending = asyncio.all_tasks()
            for t in pending:
                t.cancel()
            try:
                ws_book_loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            ws_book_loop.close()

    ws_book_thread = threading.Thread(target=_runner, daemon=True)
    ws_book_thread.start()
    print(f"{timestamp()} | 🧩 Loop del BOOK iniciado (modo persistente).")

def restart_ws_book():
    global ws_book_task

    def _restart_task():
        global ws_book_task
        try:
            if ws_book_task and not ws_book_task.done():
                ws_book_task.cancel()
        except Exception:
            pass
        ws_book_task = ws_book_loop.create_task(_book_loop())
        print(f"{timestamp()} | ♻️ BOOK reiniciado internamente dentro del loop.")

    if ws_book_loop and ws_book_loop.is_running():
        ws_book_loop.call_soon_threadsafe(_restart_task)
    else:
        start_ws_book()

BOOK_BOOT_T0 = time.time()
LAST_RESTART_TS = 0.0

def monitor_book_freshness():
    global LAST_RESTART_TS
    DEBOUNCE = 2.0
    while True:
        try:
            if time.time() - BOOK_BOOT_T0 < 3.0:
                time.sleep(0.5); continue
            if not (ws_book_loop and ws_book_loop.is_running()):
                time.sleep(0.5); continue

            age = _now_mono() - LAST_BOOK_TS
            now = time.time()
            if age > 0.5 and (now - LAST_RESTART_TS) > DEBOUNCE:
                print(f"{timestamp()} | ⚠️ BOOK stale {age:.3f}s → restart (debounced)")
                LAST_RESTART_TS = now
                restart_ws_book()
                time.sleep(2)
            else:
                time.sleep(0.1)
        except Exception as e:
            print(f"{timestamp()} | ⚠️ monitor_book_freshness: {e}")
            time.sleep(1)

def on_user_data(msg):
    """✅ MEJORADO: Parsing robusto del USER_SOCKET"""
    try:
        if not isinstance(msg, dict):
            return

        event_type = msg.get('e', 'unknown')
        if event_type != 'executionReport':
            return

        required = ['i', 'X', 's', 'S']
        if not all(k in msg for k in required):
            return

        oid = int(msg['i'])
        st = msg.get('X')
        sym = msg.get('s')
        side = msg.get('S')
        L = msg.get('L')
        z = msg.get('z')
        Z = msg.get('Z')

        # ✅ PARSING SEGURO: maneja '0', None, strings vacíos
        try:
            last_px = float(L) if (L and L != '0' and L != '0.0') else None
        except (ValueError, TypeError):
            last_px = None

        try:
            exec_qty = float(z or 0.0)
        except (ValueError, TypeError):
            exec_qty = 0.0

        try:
            cum_quote = float(Z or 0.0)
        except (ValueError, TypeError):
            cum_quote = 0.0

        with ORDER_LOCK:
            ORDER_STATE[oid] = {
                "status": st, "last_px": last_px, "execQty": exec_qty, "cumQuote": cum_quote,
                "side": side, "symbol": sym
            }
            ev = _ensure_event(oid)
            if st in ("FILLED","CANCELED","EXPIRED","REJECTED","PENDING_CANCEL"):
                ev.set()

        status_emoji = {
            "FILLED": "✅", "CANCELED": "❌", "EXPIRED": "⏱️",
            "REJECTED": "🚫", "NEW": "🆕", "PARTIALLY_FILLED": "🔄"
        }
        emoji = status_emoji.get(st, "📨")
        price_str = f" @ {last_px:.2f}" if last_px else ""
        qty_str = f" qty={exec_qty:.6f}" if exec_qty > 0 else ""

        print(f"{timestamp()} | {emoji} USER {sym} {side} #{oid} → {st}{price_str}{qty_str}")

        if st in ("FILLED","CANCELED","EXPIRED","REJECTED","PENDING_CANCEL"):
            try: oa_discard(oid)
            except: pass
            with CREACION_LOCK:
                try: ordenes_creadas.pop(oid, None)
                except: pass
            try: _account_pnl_if_pair_ready(oid)
            except: pass

    except Exception as e:
        print(f"{timestamp()} | ⚠️ Error en on_user_data: {e}")

def resolver_estado_orden(order_id: int, symbol: str, max_wait_s: float = 1.5):
    """Determina si la orden FILLED o CANCELED"""
    fin = _t_now() + max_wait_s
    last_err = None

    while _t_now() < fin:
        try:
            o = client.get_order(symbol=symbol, orderId=order_id)
            st = o.get("status")
            if st in ("FILLED", "CANCELED", "EXPIRED", "REJECTED", "PENDING_CANCEL"):
                return st, o
            if st in ("NEW", "PARTIALLY_FILLED"):
                return "OPEN", o
        except BinanceAPIException as e:
            last_err = e
            if e.code not in (-2011,):
                _sleep(0.05)

        try:
            trades = client.get_my_trades(symbol=symbol, limit=500)
            for t in trades:
                if int(t.get("orderId", -1)) == int(order_id):
                    return "FILLED", {"trades": [t]}
        except Exception:
            pass

        try:
            all_orders = client.get_all_orders(symbol=symbol, orderId=int(order_id), limit=10)
            for o2 in all_orders:
                if int(o2.get("orderId", -1)) == int(order_id):
                    st2 = o2.get("status")
                    if st2:
                        return st2, o2
        except Exception:
            try:
                all_orders = client.get_all_orders(symbol=symbol, limit=50)
                for o2 in all_orders:
                    if int(o2.get("orderId", -1)) == int(order_id):
                        st2 = o2.get("status")
                        if st2:
                            return st2, o2
            except Exception:
                pass

        try:
            opens = client.get_open_orders(symbol=symbol)
            if any(int(o3.get("orderId", -1)) == int(order_id) for o3 in opens):
                return "OPEN", None
        except Exception:
            pass

        _sleep(0.05)

    if last_err and getattr(last_err, "code", None) == -2011:
        return "CLOSED_DESCONOCIDO", None
    return "UNKNOWN", None

def cancelar_con_confirmacion(order_id: int, symbol: str):
    """Intenta cancelar; si sale -2011, resuelve el estado"""
    try:
        resp = client.cancel_order(symbol=symbol, orderId=order_id)
        st = resp.get("status") or "CANCELED"
        return st, resp
    except BinanceAPIException as e:
        if e.code == -2011:
            st, data = resolver_estado_orden(order_id, symbol)
            return st, data
        raise

def ejecutar_operacion(bid, ask, spread):
    global pnl_total, delta_total, pnl_porcentaje
    global max_pendientes, FECHA_MAX

    if BOT_STOP.is_set():
        print(f"{timestamp()} | ⛔ STOP global activo — abortando ejecución.")
        return

    if not _book_is_fresh():
        return
    bid_now, ask_now = _book_now(max_age_s=MAX_BOOK_AGE_S)
    if not bid_now or not ask_now:
        return
    spread_now = ask_now - bid_now

    print(f"{timestamp()} | 🚀 Spread detectado (fresh): {spread_now:.2f} USD — bid={bid_now:.2f} | ask={ask_now:.2f}")

    precio_buy_target = bid_now - OFFSET_USD
    precio_sell_target = ask_now + OFFSET_USD

    san_buy, san_sell = sanitize_maker_prices(precio_buy_target, precio_sell_target)
    if san_buy is None:
        print(f"{timestamp()} | ⏭️ Skip: sanitize colisionó.")
        return

    t_send0 = time.perf_counter()
    id_buy, id_sell, delta_ms = colocar_ordenes_casi_atomicas(san_buy, san_sell, timeout=5.0)
    t_send1 = time.perf_counter()
    print(f"{timestamp()} | ⏱️ Dual-place total={(t_send1 - t_send0)*1000:.2f} ms")

    try:
        _mark_pair_on_send(id_buy, id_sell)
    except Exception:
        pass

    if id_buy:
        registrar_creacion(id_buy); oa_add(id_buy)
    if id_sell:
        registrar_creacion(id_sell); oa_add(id_sell)

    print_metricas()

    if not id_buy and not id_sell:
        print(f"{timestamp()} | ❌ Ambas órdenes rechazadas.")
        return

    # === Espera ambas órdenes ===
    t0 = time.perf_counter()
    completed, price_buy, price_sell, elapsed = esperar_ambos_con_timeout(id_buy, id_sell, t0, MAX_WAIT_S)

    if not completed:
        limpiar_set_ordenes()
        actuales = oa_len()
        with STATS_LOCK:
            if actuales > max_pendientes:
                max_pendientes = actuales
                FECHA_MAX = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"{timestamp()} | ⏳ Operación incompleta. Pendientes: {actuales}")
        _print_hist()
        return

    pnl = (price_sell - price_buy) * float(QTY)
    with PNL_LOCK:
        pnl_total += pnl
        delta_total += (price_sell - price_buy)
        pnl_porcentaje = (pnl_total / CAPITAL_INICIAL_USD) * 100

    label = _bucket_label(elapsed)
    with STATS_LOCK:
        hist_fills[label] = hist_fills.get(label, 0) + 1

    print(f"{timestamp()} | ✅ BUY={price_buy:.2f} | SELL={price_sell:.2f} | Δ={price_sell - price_buy:.2f} | "
          f"PnL={pnl:.8f} | Total={pnl_total:.8f} | PnL%={pnl_porcentaje:.4f}% | ⏳ {elapsed:.3f}s")

    limpiar_set_ordenes()
    _print_hist()
    time.sleep(COOLDOWN_S)

def procesar_spreads():
    global OPERACIONES_ACTIVAS, _last_launch
    while True:
        bid, ask, spread = SPREAD_QUEUE.get()

        try:
            try:
                while True:
                    bid, ask, spread = SPREAD_QUEUE.get_nowait()
                    SPREAD_QUEUE.task_done()
            except queue.Empty:
                pass

            if BOT_STOP.is_set():
                return

            while BOT_PAUSE.is_set() and not BOT_STOP.is_set():
                time.sleep(0.05)

            if not _book_is_fresh():
                continue

            now = time.perf_counter()
            gap = now - _last_launch
            if gap < MIN_GAP_BETWEEN_LAUNCHES:
                time.sleep(MIN_GAP_BETWEEN_LAUNCHES - gap)

            while True:
                with OPERACIONES_LOCK:
                    if OPERACIONES_ACTIVAS < MAX_OPERACIONES_SIMULTANEAS:
                        OPERACIONES_ACTIVAS += 1
                        _last_launch = time.perf_counter()
                        break
                time.sleep(0.01)

            threading.Thread(
                target=_operacion_wrapper,
                args=(bid, ask, spread),
                daemon=True
            ).start()

        finally:
            SPREAD_QUEUE.task_done()

def _operacion_wrapper(bid, ask, spread):
    global OPERACIONES_ACTIVAS
    try:
        ejecutar_operacion(bid, ask, spread)
    finally:
        with OPERACIONES_LOCK:
            OPERACIONES_ACTIVAS -= 1

ordenes_creadas = {}

def registrar_creacion(oid, grupo_id=None):
    with CREACION_LOCK:
        ordenes_creadas[oid] = (time.time(), grupo_id)

def limpiar_periodicamente():
    while True:
        try:
            limpiar_set_ordenes()
            with ORDENES_LOCK:
                ordenes_activas.difference_update({None, "", 0})
            time.sleep(10)
        except Exception as e:
            print(f"{timestamp()} | ⚠️ Error en limpieza: {e}")
            time.sleep(10)

twm = None
USER_CONN_KEY = None

if __name__ == "__main__":
    print(f"\n{timestamp()} | 🟢 Iniciando bot SPREAD para {SIMBOLO}")
    print(f"{'='*100}")
    print(f"🎯 Spread objetivo: {SPREAD_OBJETIVO} USD | ⚙️ Offset: {OFFSET_USD} USD | 💰 Qty: {QTY}")
    print(f"{'='*100}\n")

    LAST_BOOK_TS = _now_mono()
    start_ws_book()
    threading.Thread(target=monitor_book_freshness, daemon=True).start()

    try:
        from user_stream_manager import SpotUserStream
        uds = SpotUserStream(API_KEY, API_SECRET, domain="com", callback=on_user_data)
        uds.start()
        print(f"{timestamp()} | 👂 USER_SOCKET (manual) activo")
    except ModuleNotFoundError:
        print(f"{timestamp()} | ℹ️ user_stream_manager no disponible → fallback TWM")
        twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
        twm.start()
        USER_CONN_KEY = twm.start_user_socket(callback=on_user_data)

    print(f"{timestamp()} | ⏳ Esperando estabilización WS...")
    time.sleep(10)
    ws_ready = True

    print(f"{timestamp()} | ⏳ Esperando primer frame del BOOK…")
    BOOK_READY.wait(timeout=15)
    ws_ready = True
    print(f"{timestamp()} | ✅ BOOK vivo; iniciando operaciones…")

    threading.Thread(target=limpiar_periodicamente, daemon=True).start()
    threading.Thread(target=procesar_spreads, daemon=True).start()

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"{timestamp()} | 🛑 Detenido por usuario.")
        try:
            if twm: twm.stop()
        except Exception:
            pass
        try:
            if ws_client and ws_client.ws:
                asyncio.run_coroutine_threadsafe(ws_client.ws.close(), ws_client.loop)
        except Exception:
            pass
        sys.exit(0)
