"""Static product catalog for the simulated electronics store.

Generated with a fixed seed so product IDs, names and base prices are
identical on every run, regardless of SIM_SEED.
"""
import random

CATEGORIES = {
    "smartphones": {"line": "Phone", "brands": ["Apple", "Samsung", "Google", "OnePlus", "Xiaomi"], "price": (180, 1400)},
    "laptops": {"line": "Book", "brands": ["Apple", "Dell", "HP", "Lenovo", "ASUS"], "price": (350, 2600)},
    "headphones": {"line": "Sound", "brands": ["Sony", "Bose", "JBL", "Sennheiser", "Anker"], "price": (25, 450)},
    "tablets": {"line": "Tab", "brands": ["Apple", "Samsung", "Lenovo", "Xiaomi"], "price": (120, 1300)},
    "smartwatches": {"line": "Watch", "brands": ["Apple", "Samsung", "Garmin", "Fitbit"], "price": (60, 800)},
    "tvs": {"line": "TV", "brands": ["Samsung", "LG", "Sony", "TCL", "Hisense"], "price": (200, 2800)},
    "cameras": {"line": "Cam", "brands": ["Canon", "Nikon", "Sony", "Fujifilm", "GoPro"], "price": (150, 2400)},
    "gaming_consoles": {"line": "Console", "brands": ["Sony", "Microsoft", "Nintendo", "Valve"], "price": (250, 650)},
    "accessories": {"line": "Accessory", "brands": ["Anker", "Logitech", "Belkin", "Razer", "SanDisk"], "price": (8, 180)},
}


def build_catalog(per_category: int = 8) -> list[dict]:
    rng = random.Random(42)
    products = []
    idx = 1
    for category, spec in CATEGORIES.items():
        lo, hi = spec["price"]
        for n in range(1, per_category + 1):
            brand = rng.choice(spec["brands"])
            suffix = rng.choice(["", " Pro", " Max", " Lite", " Plus"])
            price = round(rng.uniform(lo, hi) / 10) * 10 - 0.01
            products.append(
                {
                    "product_id": f"P{idx:04d}",
                    "name": f"{brand} {spec['line']} {n}{suffix}",
                    "brand": brand,
                    "category": category,
                    "base_price": round(max(price, 4.99), 2),
                    "initial_stock": rng.randint(40, 300),
                }
            )
            idx += 1

    # Popularity: a few bestsellers and a long tail (Zipf-like)
    ranks = list(range(1, len(products) + 1))
    rng.shuffle(ranks)
    for product, rank in zip(products, ranks):
        product["popularity"] = round(1 / rank**0.8, 4)
    return products