"""Electronics store event simulator -> Kafka.

Topics (key in parentheses):
  orders (user_id), page_views (user_id), cart_events (user_id),
  price_changes (product_id), inventory_updates (product_id)

All configuration comes from environment variables (see .env.example).
"""
import json
import os
import random
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

from confluent_kafka import KafkaException, Producer
from dotenv import find_dotenv, load_dotenv

from catalog import CATEGORIES, build_catalog

load_dotenv(find_dotenv())

# ---------- configuration ----------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
STEPS_PER_SECOND = float(os.getenv("SIM_STEPS_PER_SECOND") or 20)
ACTIVE_SESSIONS = int(os.getenv("SIM_ACTIVE_SESSIONS") or 50)
USER_POOL = int(os.getenv("SIM_USER_POOL") or 2000)
LATE_RATE = float(os.getenv("SIM_LATE_EVENT_RATE") or 0.02)
DUP_RATE = float(os.getenv("SIM_DUPLICATE_RATE") or 0.01)
MAX_EVENTS = int(os.getenv("SIM_MAX_EVENTS") or 0)
SEED = os.getenv("SIM_SEED")

CURRENCY = "USD"
ADD_TO_CART_PROB = 0.10      # after viewing a product page
PRICE_CHANGE_PROB = 0.01     # per step
RESTOCK_THRESHOLD = 10
STATS_EVERY_SECONDS = 5

TOPIC_ORDERS = "orders"
TOPIC_PAGE_VIEWS = "page_views"
TOPIC_CART = "cart_events"
TOPIC_PRICES = "price_changes"
TOPIC_INVENTORY = "inventory_updates"
TOPICS = [TOPIC_ORDERS, TOPIC_PAGE_VIEWS, TOPIC_CART, TOPIC_PRICES, TOPIC_INVENTORY]

PAGE_TYPES = ["home", "category", "search", "product"]
PAGE_WEIGHTS = [10, 25, 15, 50]
DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [55, 35, 10]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "gift_card", "bank_transfer"]
PAYMENT_WEIGHTS = [45, 25, 20, 5, 5]
SEARCH_TERMS = [
    "laptop", "iphone", "wireless earbuds", "4k tv", "gaming console",
    "smartwatch", "usb-c charger", "mirrorless camera", "tablet",
    "noise cancelling headphones",
]
CITIES = [
    ("New York", "US"), ("Chicago", "US"), ("Austin", "US"), ("Toronto", "CA"),
    ("London", "GB"), ("Berlin", "DE"), ("Paris", "FR"), ("Sydney", "AU"),
    ("Singapore", "SG"), ("Dubai", "AE"),
]


def log(message: str) -> None:
    print(f"{datetime.now().strftime('%H:%M:%S')}  {message}", flush=True)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(ts: datetime) -> str:
    return ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Session:
    __slots__ = ("session_id", "user_id", "device", "cart", "steps_left")

    def __init__(self) -> None:
        self.session_id = str(uuid.uuid4())
        self.user_id = f"U{random.randint(1, USER_POOL):05d}"
        self.device = random.choices(DEVICES, DEVICE_WEIGHTS)[0]
        self.cart: dict[str, dict] = {}
        self.steps_left = random.randint(3, 25)


class Simulator:
    def __init__(self) -> None:
        self.products = build_catalog()
        self.weights = [p["popularity"] for p in self.products]
        self.prices = {p["product_id"]: p["base_price"] for p in self.products}
        self.stock = {p["product_id"]: p["initial_stock"] for p in self.products}
        self.sessions = [Session() for _ in range(ACTIVE_SESSIONS)]
        self.counts: Counter = Counter()
        self.delivery_errors = 0
        self.producer = Producer(
            {
                "bootstrap.servers": BOOTSTRAP,
                "client.id": "electronics-simulator",
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 50,
                "compression.type": "lz4",
            }
        )

    # ---------- kafka plumbing ----------
    def check_kafka(self) -> None:
        try:
            metadata = self.producer.list_topics(timeout=10)
        except KafkaException as exc:
            log(f"ERROR: cannot reach Kafka at {BOOTSTRAP}: {exc}")
            log("Is the stack running? Try: docker compose ps -a")
            sys.exit(1)
        missing = [t for t in TOPICS if t not in metadata.topics]
        if missing:
            log(f"ERROR: missing topics {missing}. Run: docker compose up -d")
            sys.exit(1)

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.delivery_errors += 1

    def _send(self, topic: str, key: str, value: bytes) -> None:
        while True:
            try:
                self.producer.produce(
                    topic, key=key.encode(), value=value, on_delivery=self._on_delivery
                )
                break
            except BufferError:
                self.producer.poll(1)  # local queue full: wait for deliveries
        self.producer.poll(0)
        self.counts[topic] += 1

    def emit(self, topic: str, key: str, event_type: str, payload: dict) -> None:
        event_time = utc_now()
        if random.random() < LATE_RATE:
            event_time -= timedelta(seconds=random.randint(60, 600))
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_time": iso(event_time),
            **payload,
        }
        value = json.dumps(event).encode()
        self._send(topic, key, value)
        if random.random() < DUP_RATE:
            self._send(topic, key, value)  # same event_id: a duplicate delivery

    # ---------- shopper behaviour ----------
    def pick_product(self) -> dict:
        return random.choices(self.products, self.weights)[0]

    def browse(self, s: Session) -> None:
        page_type = random.choices(PAGE_TYPES, PAGE_WEIGHTS)[0]
        product, category, query = None, None, None
        if page_type == "product":
            product = self.pick_product()
            category = product["category"]
        elif page_type == "category":
            category = random.choice(list(CATEGORIES))
        elif page_type == "search":
            query = random.choice(SEARCH_TERMS)

        self.emit(
            TOPIC_PAGE_VIEWS, s.user_id, "page_view",
            {
                "user_id": s.user_id,
                "session_id": s.session_id,
                "page_type": page_type,
                "product_id": product["product_id"] if product else None,
                "category": category,
                "search_query": query,
                "device": s.device,
            },
        )
        if product and random.random() < ADD_TO_CART_PROB:
            self.add_to_cart(s, product)

    def add_to_cart(self, s: Session, product: dict) -> None:
        pid = product["product_id"]
        qty = random.choices([1, 2, 3], [85, 12, 3])[0]
        price = self.prices[pid]
        entry = s.cart.setdefault(pid, {"quantity": 0, "unit_price": price})
        entry["quantity"] += qty
        self.emit(
            TOPIC_CART, s.user_id, "cart_add",
            {
                "user_id": s.user_id, "session_id": s.session_id,
                "product_id": pid, "quantity": qty, "unit_price": price,
            },
        )

    def remove_from_cart(self, s: Session) -> None:
        pid = random.choice(list(s.cart))
        entry = s.cart.pop(pid)
        self.emit(
            TOPIC_CART, s.user_id, "cart_remove",
            {
                "user_id": s.user_id, "session_id": s.session_id,
                "product_id": pid, "quantity": entry["quantity"],
                "unit_price": entry["unit_price"],
            },
        )

    def checkout(self, s: Session) -> None:
        items = []
        for pid, entry in s.cart.items():
            qty = min(entry["quantity"], self.stock[pid])
            if qty > 0:
                items.append(
                    {"product_id": pid, "quantity": qty, "unit_price": entry["unit_price"]}
                )
        if not items:
            return  # everything out of stock
        city, country = random.choice(CITIES)
        self.emit(
            TOPIC_ORDERS, s.user_id, "order_placed",
            {
                "order_id": str(uuid.uuid4()),
                "user_id": s.user_id,
                "session_id": s.session_id,
                "items": items,
                "total_amount": round(sum(i["quantity"] * i["unit_price"] for i in items), 2),
                "currency": CURRENCY,
                "payment_method": random.choices(PAYMENT_METHODS, PAYMENT_WEIGHTS)[0],
                "shipping_city": city,
                "shipping_country": country,
            },
        )
        for item in items:
            self.adjust_stock(item["product_id"], -item["quantity"], "sale")

    def adjust_stock(self, pid: str, delta: int, reason: str) -> None:
        self.stock[pid] += delta
        self.emit(
            TOPIC_INVENTORY, pid, "inventory_update",
            {"product_id": pid, "change": delta, "reason": reason, "stock_after": self.stock[pid]},
        )
        if self.stock[pid] < RESTOCK_THRESHOLD:
            restock = random.randint(50, 200)
            self.stock[pid] += restock
            self.emit(
                TOPIC_INVENTORY, pid, "inventory_update",
                {"product_id": pid, "change": restock, "reason": "restock", "stock_after": self.stock[pid]},
            )

    def change_price(self) -> None:
        product = random.choice(self.products)
        pid = product["product_id"]
        old = self.prices[pid]
        factor = 1 + random.uniform(-0.20, 0.10)
        new = round(max(old * factor, 4.99), 2)
        if new == old:
            return
        self.prices[pid] = new
        self.emit(
            TOPIC_PRICES, pid, "price_change",
            {
                "product_id": pid, "old_price": old, "new_price": new,
                "reason": "promotion" if new < old else "market_adjustment",
            },
        )

    def advance(self, s: Session) -> bool:
        """Advance one session by one step. Returns False when the session ends."""
        s.steps_left -= 1
        r = random.random()
        if s.cart:
            if r < 0.15:
                self.checkout(s)
                return False
            if r < 0.22:
                self.remove_from_cart(s)
                return s.steps_left > 0
            if r < 0.28:
                return False  # abandoned with items in cart
        self.browse(s)
        return s.steps_left > 0

    def step(self) -> None:
        idx = random.randrange(len(self.sessions))
        if not self.advance(self.sessions[idx]):
            self.sessions[idx] = Session()  # keep concurrency constant
        if random.random() < PRICE_CHANGE_PROB:
            self.change_price()

    # ---------- main loop ----------
    def total(self) -> int:
        return sum(self.counts.values())

    def stats_line(self, elapsed: float) -> str:
        parts = " ".join(f"{t}={self.counts[t]}" for t in TOPICS)
        return f"[{elapsed:6.0f}s] total={self.total()} {parts} errors={self.delivery_errors}"

    def run(self) -> None:
        self.check_kafka()
        log(f"Connected to {BOOTSTRAP}. ~{STEPS_PER_SECOND:g} steps/s, "
            f"{ACTIVE_SESSIONS} sessions. Ctrl+C to stop.")
        interval = 1.0 / STEPS_PER_SECOND
        start = time.monotonic()
        next_tick = start
        next_stats = start + STATS_EVERY_SECONDS
        try:
            while True:
                self.step()
                if MAX_EVENTS and self.total() >= MAX_EVENTS:
                    break
                now = time.monotonic()
                if now >= next_stats:
                    log(self.stats_line(now - start))
                    next_stats = now + STATS_EVERY_SECONDS
                next_tick += interval
                delay = next_tick - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                else:
                    next_tick = time.monotonic()  # fell behind: do not burst
        except KeyboardInterrupt:
            log("Stopping...")
        finally:
            unsent = self.producer.flush(15)
            log(f"FINAL {self.stats_line(time.monotonic() - start)} unsent={unsent}")


def main() -> None:
    if SEED:
        random.seed(int(SEED))
    Simulator().run()


if __name__ == "__main__":
    main()