#!/usr/bin/env python3
"""
FastAPI mock business-event API for the ecommerce CDC source schema.

The API exposes three workflow levels:
  - Primitive actions: one small business event per request.
  - Scenarios: composed workflows made from primitive actions.
  - Scheduled scenarios: in-memory jobs that run scenarios on an interval.

Run:
    uvicorn scripts.mock_business_api:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import os
import random
import re
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field


app = FastAPI(
    title="Ecommerce Mock Business API",
    description="Business-event API for creating realistic CDC changes.",
    version="0.2.0",
)


FIRST_NAMES = [
    "An", "Binh", "Chau", "Dung", "Giang", "Hoa", "Khang", "Lan",
    "Minh", "Nga", "Phuc", "Quyen", "Son", "Thu", "Vy", "Bao",
    "Cuong", "Diem", "Hien", "Khanh", "Linh", "Nam", "Nhu", "Phuong",
    "Anh", "Dat", "Duc", "Ha", "Hai", "Hanh", "Hieu", "Huong",
    "Lam", "Long", "Mai", "My", "Ngan", "Nghia", "Phong", "Quang",
    "Tam", "Thao", "Thanh", "Thien", "Trang", "Tuan", "Tung", "Yen",
    "Gia", "Han", "Hau", "Huy", "Lien", "Loan", "Loc", "Luan",
    "Nhi", "Nhat", "Oanh", "Phat", "Tien", "Tram", "Truc", "Vinh",
]

LAST_NAMES = [
    "Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Do", "Bui",
    "Dang", "Ngo", "Duong", "Ly", "Mai", "Phan", "Trinh", "Dinh",
    "Cao", "Chau", "Ha", "Huynh", "Lam", "Luong", "Luu", "Mac",
    "Ngoc", "Phung", "Ta", "Thai", "Ton", "Truong", "Vo", "Vuong",
    "Chu", "Diep", "Giang", "Hua", "Kieu", "La", "Lai", "Tang",
]

PHONE_PREFIXES = [
    "032", "033", "034", "035", "036", "037", "038", "039",
    "086", "096", "097", "098", "070", "076", "077", "078",
    "079", "089", "090", "093",
]

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "mail.com", "outlook.com", "vnn.vn"]

STREET_NAMES = [
    "Ba Trieu", "Bach Mai", "Cau Giay", "Chua Boc",
    "Dai Co Viet", "Dao Tan", "Dich Vong Hau", "Dinh Tien Hoang",
    "Doc Ngu", "Doi Can", "Giang Vo", "Giai Phong",
    "Hang Bai", "Hang Bong", "Hang Buom", "Hang Dao",
    "Hang Gai", "Hang Ma", "Hoang Cau", "Hoang Dieu",
    "Hoang Hoa Tham", "Hoang Mai", "Ho Tung Mau", "Huynh Thuc Khang",
    "Kim Ma", "Khuat Duy Tien", "Lac Long Quan", "Lang Ha",
    "Le Duan", "Le Thai To", "Le Thanh Nghi", "Le Van Luong",
    "Lieu Giai", "Lo Duc", "Luong Ngoc Quyen", "Ly Nam De",
    "Ly Thuong Kiet", "Minh Khai", "Ngoc Ha", "Nguyen Chi Thanh",
    "Nguyen Du", "Nguyen Hoang", "Nguyen Khang", "Nguyen Khuyen",
    "Nguyen Phong Sac", "Nguyen Trai", "Nguyen Van Cu", "Nguyen Van Huyen",
    "O Cho Dua", "Pham Hung", "Pham Ngoc Thach", "Pham Van Dong",
    "Phan Chu Trinh", "Quan Thanh", "Tay Son", "To Hieu",
    "Ton Duc Thang", "Tran Cung", "Tran Duy Hung", "Tran Hung Dao",
    "Tran Khat Chan", "Tran Nhat Duat", "Tran Phu", "Tran Thai Tong",
    "Trang Thi", "Trieu Viet Vuong", "Truong Chinh", "Van Cao",
    "Vong Thi", "Xa Dan", "Xuan Dieu", "Yen Phu",
]

CITY_REGIONS = [
    ("Ha Noi", "Ba Dinh"), ("Ha Noi", "Bac Tu Liem"), ("Ha Noi", "Cau Giay"),
    ("Ha Noi", "Dong Da"), ("Ha Noi", "Ha Dong"), ("Ha Noi", "Hai Ba Trung"),
    ("Ha Noi", "Hoan Kiem"), ("Ha Noi", "Hoang Mai"), ("Ha Noi", "Long Bien"),
    ("Ha Noi", "Nam Tu Liem"), ("Ha Noi", "Tay Ho"), ("Ha Noi", "Thanh Xuan"),
    ("Ha Noi", "Thanh Tri"), ("Ha Noi", "Gia Lam"), ("Ha Noi", "Dong Anh"),
    ("Ha Noi", "Soc Son"), ("Ha Noi", "Hoai Duc"), ("Ha Noi", "Dan Phuong"),
    ("Ha Noi", "Thanh Oai"), ("Ha Noi", "Thuong Tin"), ("Ha Noi", "Me Linh"),
    ("Ha Noi", "Son Tay"), ("Ha Noi", "Quoc Oai"), ("Ha Noi", "Thach That"),
    ("Ha Noi", "Phu Xuyen"), ("Ha Noi", "Phuc Tho"), ("Ha Noi", "Chuong My"),
    ("Ha Noi", "Ba Vi"), ("Ha Noi", "My Duc"), ("Ha Noi", "Ung Hoa"),
]

BRANDS = [
    ("AMD", "United States"), ("Intel", "United States"), ("NVIDIA", "United States"),
    ("ASUS", "Taiwan"), ("MSI", "Taiwan"), ("Gigabyte", "Taiwan"),
    ("Corsair", "United States"), ("Samsung", "South Korea"), ("Logitech", "Switzerland"),
    ("Kingston", "United States"), ("Western Digital", "United States"),
    ("Seagate", "United States"), ("Crucial", "United States"), ("G.Skill", "Taiwan"),
    ("Cooler Master", "Taiwan"), ("Seasonic", "Taiwan"), ("EVGA", "United States"),
    ("NZXT", "United States"), ("Razer", "United States"), ("SteelSeries", "Denmark"),
    ("Dell", "United States"), ("LG", "South Korea"), ("BenQ", "Taiwan"),
    ("Acer", "Taiwan"), ("Lenovo", "China"), ("HP", "United States"),
    ("Apple", "United States"), ("TP-Link", "China"), ("Ubiquiti", "United States"),
    ("Epson", "Japan"),
]

PRODUCT_BLUEPRINTS = [
    ("CPU-AMD-7800X3D", "AMD Ryzen 7 7800X3D", "CPU", "AMD", Decimal("449.00")),
    ("CPU-INT-14700K", "Intel Core i7-14700K", "CPU", "Intel", Decimal("409.00")),
    ("CPU-AMD-7600", "AMD Ryzen 5 7600", "CPU", "AMD", Decimal("229.00")),
    ("CPU-AMD-7950X", "AMD Ryzen 9 7950X", "CPU", "AMD", Decimal("549.00")),
    ("CPU-INT-14600K", "Intel Core i5-14600K", "CPU", "Intel", Decimal("319.00")),
    ("CPU-INT-14900K", "Intel Core i9-14900K", "CPU", "Intel", Decimal("589.00")),
    ("GPU-NV-4070S", "NVIDIA GeForce RTX 4070 SUPER", "GPU", "NVIDIA", Decimal("599.00")),
    ("GPU-NV-4060", "NVIDIA GeForce RTX 4060", "GPU", "NVIDIA", Decimal("299.00")),
    ("GPU-NV-4080S", "NVIDIA GeForce RTX 4080 SUPER", "GPU", "NVIDIA", Decimal("999.00")),
    ("GPU-AMD-7800XT", "AMD Radeon RX 7800 XT", "GPU", "AMD", Decimal("499.00")),
    ("GPU-AMD-7900XTX", "AMD Radeon RX 7900 XTX", "GPU", "AMD", Decimal("899.00")),
    ("MB-ASUS-B650", "ASUS TUF Gaming B650-PLUS WIFI", "Mainboard", "ASUS", Decimal("199.00")),
    ("MB-MSI-Z790", "MSI MAG Z790 Tomahawk WIFI", "Mainboard", "MSI", Decimal("259.00")),
    ("MB-GB-B760M", "Gigabyte B760M AORUS Elite AX", "Mainboard", "Gigabyte", Decimal("169.00")),
    ("MB-ASUS-X670E", "ASUS ROG Strix X670E-E Gaming WIFI", "Mainboard", "ASUS", Decimal("469.00")),
    ("MB-MSI-B650M", "MSI PRO B650M-A WIFI", "Mainboard", "MSI", Decimal("159.00")),
    ("RAM-COR-32-6000", "Corsair Vengeance 32GB DDR5-6000", "RAM", "Corsair", Decimal("109.00")),
    ("RAM-GSK-32-6400", "G.Skill Trident Z5 RGB 32GB DDR5-6400", "RAM", "G.Skill", Decimal("129.00")),
    ("RAM-KIN-16-5600", "Kingston Fury Beast 16GB DDR5-5600", "RAM", "Kingston", Decimal("59.00")),
    ("RAM-CRU-64-5600", "Crucial Pro 64GB DDR5-5600", "RAM", "Crucial", Decimal("189.00")),
    ("SSD-SAM-990-2T", "Samsung 990 PRO 2TB", "Storage", "Samsung", Decimal("169.00")),
    ("SSD-WD-SN850X-1T", "Western Digital Black SN850X 1TB", "Storage", "Western Digital", Decimal("99.00")),
    ("SSD-KIN-KC3000-2T", "Kingston KC3000 2TB NVMe", "Storage", "Kingston", Decimal("149.00")),
    ("HDD-SEA-BAR-4T", "Seagate BarraCuda 4TB", "Storage", "Seagate", Decimal("89.00")),
    ("PSU-SEA-850GX", "Seasonic Focus GX-850 Gold", "Power Supply", "Seasonic", Decimal("139.00")),
    ("PSU-COR-RM750E", "Corsair RM750e 750W Gold", "Power Supply", "Corsair", Decimal("119.00")),
    ("CASE-NZXT-H5", "NZXT H5 Flow", "Case", "NZXT", Decimal("94.00")),
    ("CASE-CM-NR200P", "Cooler Master NR200P", "Case", "Cooler Master", Decimal("109.00")),
    ("COOL-CM-ML240", "Cooler Master MasterLiquid ML240L", "Cooling", "Cooler Master", Decimal("89.00")),
    ("COOL-COR-H100I", "Corsair iCUE H100i Elite", "Cooling", "Corsair", Decimal("149.00")),
    ("MON-LG-27GP850", "LG UltraGear 27GP850-B 27 Inch", "Monitor", "LG", Decimal("329.00")),
    ("MON-DELL-U2723QE", "Dell UltraSharp U2723QE 27 Inch", "Monitor", "Dell", Decimal("549.00")),
    ("MON-BENQ-EX240", "BenQ MOBIUZ EX240 24 Inch", "Monitor", "BenQ", Decimal("189.00")),
    ("MOU-LOG-G502", "Logitech G502 X Lightspeed", "Peripheral", "Logitech", Decimal("139.00")),
    ("MOU-RAZ-BASILISK", "Razer Basilisk V3", "Peripheral", "Razer", Decimal("69.00")),
    ("KEY-LOG-MXMECH", "Logitech MX Mechanical", "Peripheral", "Logitech", Decimal("149.00")),
    ("KEY-STE-APEXPRO", "SteelSeries Apex Pro TKL", "Peripheral", "SteelSeries", Decimal("189.00")),
    ("HEAD-RAZ-BLACKV2", "Razer BlackShark V2", "Peripheral", "Razer", Decimal("99.00")),
    ("LAP-LEN-LEGION5", "Lenovo Legion 5 16IRX9", "Laptop", "Lenovo", Decimal("1399.00")),
    ("LAP-HP-OMEN16", "HP OMEN 16", "Laptop", "HP", Decimal("1299.00")),
    ("LAP-ACER-NITROV", "Acer Nitro V 15", "Laptop", "Acer", Decimal("899.00")),
    ("LAP-APPLE-MBA13", "Apple MacBook Air 13 M3", "Laptop", "Apple", Decimal("1099.00")),
    ("NET-TPL-AX73", "TP-Link Archer AX73", "Networking", "TP-Link", Decimal("149.00")),
    ("NET-UBI-U6PLUS", "Ubiquiti UniFi U6 Plus", "Networking", "Ubiquiti", Decimal("129.00")),
    ("PRN-EPS-L3250", "Epson EcoTank L3250", "Printer", "Epson", Decimal("199.00")),
]

WAREHOUSE_BLUEPRINTS = [
    ("WAREHOUSE-HN-01", "Long Bien Fulfillment Center", "Ha Noi", "Vietnam"),
    ("WAREHOUSE-HN-02", "Gia Lam Logistics Hub", "Ha Noi", "Vietnam"),
    ("WAREHOUSE-HN-03", "Hoang Mai Fulfillment Center", "Ha Noi", "Vietnam"),
    ("WAREHOUSE-HN-04", "Nam Tu Liem Distribution Hub", "Ha Noi", "Vietnam"),
    ("WAREHOUSE-HN-05", "Ha Dong Fulfillment Center", "Ha Noi", "Vietnam"),
    ("WAREHOUSE-HN-06", "Dong Anh Cross-Dock", "Ha Noi", "Vietnam"),
    ("WAREHOUSE-HN-07", "Soc Son Logistics Hub", "Ha Noi", "Vietnam"),
    ("WAREHOUSE-HN-08", "Thanh Tri Distribution Center", "Ha Noi", "Vietnam"),
    ("WAREHOUSE-HN-09", "Bac Tu Liem Fulfillment Center", "Ha Noi", "Vietnam"),
    ("WAREHOUSE-HN-10", "Hoai Duc Sorting Center", "Ha Noi", "Vietnam"),
]

TIER_THRESHOLDS = [
    (Decimal("10000"), "PLATINUM"),
    (Decimal("5000"), "GOLD"),
    (Decimal("1000"), "SILVER"),
    (Decimal("0"), "BRONZE"),
]

ORDER_TRANSITIONS = {
    "pending_payment": "paid",
    "paid": "processing",
    "processing": "shipped",
    "shipped": "delivered",
}

SHIPMENT_TRANSITIONS = {
    "created": "packed",
    "packed": "shipped",
    "shipped": "in_transit",
    "in_transit": "delivered",
}


class RegisterCustomerRequest(BaseModel):
    full_name: str | None = Field(default=None, max_length=120)
    email: str | None = Field(default=None, max_length=255)
    phone: str | None = Field(default=None, max_length=32)


class CustomerAddressRequest(BaseModel):
    address_type: str = "shipping"
    recipient_name: str | None = Field(default=None, max_length=120)
    phone: str | None = Field(default=None, max_length=32)
    line1: str | None = Field(default=None, max_length=255)
    line2: str | None = Field(default=None, max_length=255)
    city: str | None = Field(default=None, max_length=80)
    region: str | None = Field(default=None, max_length=80)
    postal_code: str | None = Field(default=None, max_length=32)
    country: str = "Vietnam"
    is_default: bool = True


class UpdateCustomerRequest(BaseModel):
    full_name: str | None = Field(default=None, max_length=120)
    phone: str | None = Field(default=None, max_length=32)
    loyalty_tier: str | None = None
    status: str | None = None


class BrandRequest(BaseModel):
    brand_name: str | None = Field(default=None, max_length=120)
    country: str | None = Field(default=None, max_length=80)


class CategoryRequest(BaseModel):
    category_name: str | None = Field(default=None, max_length=120)
    parent_category_id: uuid.UUID | None = None
    is_active: bool = True


class ProductRequest(BaseModel):
    brand_id: uuid.UUID | None = None
    sku: str | None = Field(default=None, max_length=64)
    product_name: str | None = Field(default=None, max_length=180)
    product_type: str | None = Field(default=None, max_length=80)
    list_price: Decimal | None = Field(default=None, gt=0)
    status: str = "active"
    specs: dict[str, Any] = Field(default_factory=dict)
    category_id: uuid.UUID | None = None


class UpdateProductRequest(BaseModel):
    list_price: Decimal | None = Field(default=None, gt=0)
    status: str | None = None
    specs: dict[str, Any] | None = None


class WarehouseRequest(BaseModel):
    warehouse_code: str | None = Field(default=None, max_length=40)
    warehouse_name: str | None = Field(default=None, max_length=160)
    city: str | None = Field(default=None, max_length=80)
    country: str = "Vietnam"


class InventoryRequest(BaseModel):
    product_id: uuid.UUID | None = None
    warehouse_id: uuid.UUID | None = None
    quantity_on_hand: int = Field(default=100, ge=0)
    quantity_reserved: int = Field(default=0, ge=0)
    reorder_level: int = Field(default=20, ge=0)


class InventoryAdjustRequest(BaseModel):
    product_id: uuid.UUID | None = None
    warehouse_id: uuid.UUID | None = None
    quantity_change: int = Field(default=0)
    movement_type: str = "stock_adjustment"
    reason: str = "manual adjustment"


class CreateCartRequest(BaseModel):
    customer_id: uuid.UUID | None = None


class AddCartItemRequest(BaseModel):
    product_id: uuid.UUID | None = None
    quantity: int = Field(default=1, ge=1, le=10)


class CheckoutRequest(BaseModel):
    shipping_address_id: uuid.UUID | None = None
    billing_address_id: uuid.UUID | None = None
    promotion_id: uuid.UUID | None = None


class PaymentRequest(BaseModel):
    payment_method: str = "card"
    payment_status: str = "paid"
    amount: Decimal | None = Field(default=None, ge=0)


class ShipmentRequest(BaseModel):
    carrier: str | None = None
    shipment_status: str = "created"


class StatusRequest(BaseModel):
    status: str | None = None


class BootstrapRequest(BaseModel):
    products: int = Field(default=5, ge=1, le=50)
    warehouses: int = Field(default=2, ge=1, le=len(WAREHOUSE_BLUEPRINTS))


class SalesBurstRequest(BaseModel):
    orders: int = Field(default=5, ge=1, le=50)
    item_count: int = Field(default=2, ge=1, le=5)


class ScheduleRequest(BaseModel):
    scenario: str = "random_activity"
    interval_seconds: float = Field(default=10.0, ge=1.0)
    max_runs: int | None = Field(default=None, ge=1)
    payload: dict[str, Any] = Field(default_factory=dict)


class ScheduleState(BaseModel):
    schedule_id: str
    scenario: str
    interval_seconds: float
    max_runs: int | None
    run_count: int
    started_at: str
    last_run_at: str | None
    active: bool
    last_result: dict[str, Any] | None
    last_error: str | None


@dataclass
class ScheduleJob:
    schedule_id: str
    scenario: str
    interval_seconds: float
    payload: dict[str, Any]
    max_runs: int | None
    stop_event: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None
    run_count: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_run_at: datetime | None = None
    last_result: dict[str, Any] | None = None
    last_error: str | None = None


SCHEDULES: dict[str, ScheduleJob] = {}
SCHEDULE_LOCK = threading.Lock()


def db_config() -> dict[str, Any]:
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "user": os.getenv("DB_USER", "admin"),
        "password": os.getenv("DB_PASSWORD", "admin"),
        "dbname": os.getenv("DB_NAME", "enterprise_db"),
    }


@contextmanager
def db_cursor():
    conn = psycopg2.connect(**db_config())
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def make_phone() -> str:
    return random.choice(PHONE_PREFIXES) + str(random.randint(1_000_000, 9_999_999))


def make_customer_name() -> str:
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def random_email(full_name: str) -> str:
    parts = full_name.lower().split()
    suffix = random.randint(10, 99999)
    domain = random.choice(EMAIL_DOMAINS)
    return f"{parts[0]}.{parts[-1]}{suffix}@{domain}"


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or f"category-{random.randint(1000, 9999)}"


def normalize_record(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if record is None:
        return None
    normalized = {}
    for key, value in record.items():
        if isinstance(value, Decimal):
            normalized[key] = float(value)
        elif isinstance(value, (datetime, uuid.UUID)):
            normalized[key] = str(value)
        else:
            normalized[key] = value
    return normalized


def fetch_one(cur, query: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    cur.execute(query, params)
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Requested record was not found")
    return row


def calc_tier(total_spent: Decimal) -> str:
    for threshold, tier in TIER_THRESHOLDS:
        if total_spent >= threshold:
            return tier
    return "BRONZE"


def choose_address() -> tuple[str, str, str]:
    city, region = random.choice(CITY_REGIONS)
    line1 = f"{random.randint(1, 999)} {random.choice(STREET_NAMES)}"
    return line1, city, region


def register_customer_event(cur, request: RegisterCustomerRequest | None = None) -> dict[str, Any]:
    request = request or RegisterCustomerRequest()
    full_name = request.full_name or make_customer_name()
    email = request.email or random_email(full_name)
    phone = request.phone or make_phone()

    try:
        cur.execute(
            """INSERT INTO ecommerce.customers (email, full_name, phone, loyalty_tier, status)
               VALUES (%s, %s, %s, 'BRONZE', 'active')
               RETURNING *""",
            (email, full_name, phone),
        )
    except psycopg2.errors.UniqueViolation as exc:
        raise HTTPException(status_code=409, detail=f"Customer email already exists: {email}") from exc

    return {"event": "register_customer", "customer": normalize_record(cur.fetchone())}


def create_customer_address_event(
    cur,
    customer_id: uuid.UUID,
    request: CustomerAddressRequest | None = None,
) -> dict[str, Any]:
    request = request or CustomerAddressRequest()
    customer = fetch_one(
        cur,
        "SELECT customer_id, full_name, phone FROM ecommerce.customers WHERE customer_id = %s",
        (str(customer_id),),
    )
    line1, city, region = choose_address()
    recipient_name = request.recipient_name or customer["full_name"]
    phone = request.phone or customer["phone"] or make_phone()

    if request.is_default:
        cur.execute(
            """UPDATE ecommerce.customer_addresses
               SET is_default = FALSE
               WHERE customer_id = %s AND address_type = %s""",
            (str(customer_id), request.address_type),
        )

    cur.execute(
        """INSERT INTO ecommerce.customer_addresses
           (customer_id, address_type, recipient_name, phone, line1, line2, city,
            region, postal_code, country, is_default)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING *""",
        (
            str(customer_id),
            request.address_type,
            recipient_name,
            phone,
            request.line1 or line1,
            request.line2,
            request.city or city,
            request.region or region,
            request.postal_code or str(random.randint(70000, 79999)),
            request.country,
            request.is_default,
        ),
    )
    return {"event": "create_customer_address", "address": normalize_record(cur.fetchone())}


def update_customer_event(cur, customer_id: uuid.UUID, request: UpdateCustomerRequest) -> dict[str, Any]:
    fields = []
    values: list[Any] = []
    for field_name in ["full_name", "phone", "loyalty_tier", "status"]:
        value = getattr(request, field_name)
        if value is not None:
            fields.append(f"{field_name} = %s")
            values.append(value)
    if not fields:
        raise HTTPException(status_code=422, detail="At least one customer field is required")

    values.append(str(customer_id))
    cur.execute(
        f"""UPDATE ecommerce.customers
            SET {", ".join(fields)}
            WHERE customer_id = %s
            RETURNING *""",
        values,
    )
    customer = cur.fetchone()
    if not customer:
        raise HTTPException(status_code=404, detail="Customer was not found")
    return {"event": "patch_customer", "customer": normalize_record(customer)}


def create_brand_event(cur, request: BrandRequest | None = None) -> dict[str, Any]:
    request = request or BrandRequest()
    brand_name, country = random.choice(BRANDS)
    brand_name = request.brand_name or brand_name
    country = request.country or country

    cur.execute(
        """INSERT INTO ecommerce.brands (brand_name, country)
           VALUES (%s, %s)
           ON CONFLICT (brand_name) DO UPDATE SET country = EXCLUDED.country
           RETURNING *""",
        (brand_name, country),
    )
    return {"event": "create_brand", "brand": normalize_record(cur.fetchone())}


def create_category_event(cur, request: CategoryRequest | None = None) -> dict[str, Any]:
    request = request or CategoryRequest()
    category_name = request.category_name or random.choice(["CPU", "GPU", "RAM", "Storage", "Mainboard", "Peripheral"])
    category_slug = slugify(category_name)
    cur.execute(
        """INSERT INTO ecommerce.categories
           (parent_category_id, category_name, category_slug, is_active)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT (category_slug) DO UPDATE
           SET category_name = EXCLUDED.category_name,
               is_active = EXCLUDED.is_active
           RETURNING *""",
        (str(request.parent_category_id) if request.parent_category_id else None, category_name, category_slug, request.is_active),
    )
    return {"event": "create_category", "category": normalize_record(cur.fetchone())}


def get_or_create_category(cur, category_name: str) -> dict[str, Any]:
    slug = slugify(category_name)
    cur.execute("SELECT * FROM ecommerce.categories WHERE category_slug = %s", (slug,))
    category = cur.fetchone()
    if category:
        return category
    cur.execute(
        """INSERT INTO ecommerce.categories (category_name, category_slug, is_active)
           VALUES (%s, %s, TRUE)
           RETURNING *""",
        (category_name, slug),
    )
    return cur.fetchone()


def get_or_create_brand(cur, brand_name: str) -> dict[str, Any]:
    country = next((country for name, country in BRANDS if name == brand_name), None)
    cur.execute(
        """INSERT INTO ecommerce.brands (brand_name, country)
           VALUES (%s, %s)
           ON CONFLICT (brand_name) DO UPDATE SET country = COALESCE(ecommerce.brands.country, EXCLUDED.country)
           RETURNING *""",
        (brand_name, country),
    )
    return cur.fetchone()


def create_product_event(cur, request: ProductRequest | None = None) -> dict[str, Any]:
    request = request or ProductRequest()
    sku, product_name, product_type, brand_name, price = random.choice(PRODUCT_BLUEPRINTS)
    unique_suffix = random.randint(1000, 9999)
    sku = request.sku or f"{sku}-{unique_suffix}"
    product_name = request.product_name or product_name
    product_type = request.product_type or product_type
    list_price = request.list_price or price

    if request.brand_id:
        brand_id = str(request.brand_id)
    else:
        brand_id = str(get_or_create_brand(cur, brand_name)["brand_id"])

    cur.execute(
        """INSERT INTO ecommerce.products
           (brand_id, sku, product_name, product_type, list_price, status, specs)
           VALUES (%s, %s, %s, %s, %s, %s, %s)
           RETURNING *""",
        (
            brand_id,
            sku,
            product_name,
            product_type,
            list_price,
            request.status,
            psycopg2.extras.Json(request.specs or {"source": "mock_api"}),
        ),
    )
    product = cur.fetchone()

    category = None
    if request.category_id:
        category_id = str(request.category_id)
    else:
        category = get_or_create_category(cur, product_type)
        category_id = str(category["category_id"])
    cur.execute(
        """INSERT INTO ecommerce.product_categories (product_id, category_id, is_primary)
           VALUES (%s, %s, TRUE)
           ON CONFLICT (product_id, category_id) DO UPDATE SET is_primary = TRUE
           RETURNING *""",
        (str(product["product_id"]), category_id),
    )

    return {
        "event": "create_product",
        "product": normalize_record(product),
        "category": normalize_record(category) if category else None,
    }


def update_product_event(cur, product_id: uuid.UUID | None, request: UpdateProductRequest | None = None) -> dict[str, Any]:
    request = request or UpdateProductRequest()
    if product_id:
        product = fetch_one(cur, "SELECT * FROM ecommerce.products WHERE product_id = %s", (str(product_id),))
    else:
        product = fetch_one(
            cur,
            "SELECT * FROM ecommerce.products WHERE status = 'active' ORDER BY RANDOM() LIMIT 1",
        )

    fields = []
    values: list[Any] = []
    if request.list_price is not None:
        fields.append("list_price = %s")
        values.append(request.list_price)
    elif request.status is None and request.specs is None:
        multiplier = Decimal(str(random.uniform(0.9, 1.15)))
        fields.append("list_price = %s")
        values.append((product["list_price"] * multiplier).quantize(Decimal("0.01")))
    if request.status is not None:
        fields.append("status = %s")
        values.append(request.status)
    if request.specs is not None:
        fields.append("specs = %s")
        values.append(psycopg2.extras.Json(request.specs))

    values.append(str(product["product_id"]))
    cur.execute(
        f"""UPDATE ecommerce.products
            SET {", ".join(fields)}
            WHERE product_id = %s
            RETURNING *""",
        values,
    )
    return {
        "event": "patch_product",
        "old_product": normalize_record(product),
        "product": normalize_record(cur.fetchone()),
    }


def create_warehouse_event(cur, request: WarehouseRequest | None = None) -> dict[str, Any]:
    request = request or WarehouseRequest()
    code, name, city, country = random.choice(WAREHOUSE_BLUEPRINTS)
    code = request.warehouse_code or code
    name = request.warehouse_name or name
    city = request.city or city
    country = request.country or country

    cur.execute(
        """INSERT INTO ecommerce.warehouses (warehouse_code, warehouse_name, city, country, is_active)
           VALUES (%s, %s, %s, %s, TRUE)
           ON CONFLICT (warehouse_code) DO UPDATE
           SET warehouse_name = EXCLUDED.warehouse_name,
               city = EXCLUDED.city,
               country = EXCLUDED.country,
               is_active = TRUE
           RETURNING *""",
        (code, name, city, country),
    )
    return {"event": "create_warehouse", "warehouse": normalize_record(cur.fetchone())}


def random_active_product(cur) -> dict[str, Any]:
    return fetch_one(
        cur,
        "SELECT * FROM ecommerce.products WHERE status = 'active' ORDER BY RANDOM() LIMIT 1",
    )


def random_active_warehouse(cur) -> dict[str, Any]:
    return fetch_one(
        cur,
        "SELECT * FROM ecommerce.warehouses WHERE is_active = TRUE ORDER BY RANDOM() LIMIT 1",
    )


def create_inventory_event(cur, request: InventoryRequest | None = None) -> dict[str, Any]:
    request = request or InventoryRequest()
    product = fetch_one(cur, "SELECT * FROM ecommerce.products WHERE product_id = %s", (str(request.product_id),)) if request.product_id else random_active_product(cur)
    warehouse = fetch_one(cur, "SELECT * FROM ecommerce.warehouses WHERE warehouse_id = %s", (str(request.warehouse_id),)) if request.warehouse_id else random_active_warehouse(cur)

    if request.quantity_reserved > request.quantity_on_hand:
        raise HTTPException(status_code=422, detail="quantity_reserved cannot exceed quantity_on_hand")

    cur.execute(
        """INSERT INTO ecommerce.inventory
           (product_id, warehouse_id, quantity_on_hand, quantity_reserved, reorder_level)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (product_id, warehouse_id) DO UPDATE
           SET quantity_on_hand = EXCLUDED.quantity_on_hand,
               quantity_reserved = EXCLUDED.quantity_reserved,
               reorder_level = EXCLUDED.reorder_level
           RETURNING *""",
        (
            str(product["product_id"]),
            str(warehouse["warehouse_id"]),
            request.quantity_on_hand,
            request.quantity_reserved,
            request.reorder_level,
        ),
    )
    inventory = cur.fetchone()
    cur.execute(
        """INSERT INTO ecommerce.inventory_movements
           (product_id, warehouse_id, movement_type, quantity_change, quantity_after, reason)
           VALUES (%s, %s, 'initial_stock', %s, %s, 'initial inventory holder created')
           RETURNING *""",
        (
            str(product["product_id"]),
            str(warehouse["warehouse_id"]),
            request.quantity_on_hand,
            request.quantity_on_hand,
        ),
    )
    return {
        "event": "create_initial_inventory",
        "inventory": normalize_record(inventory),
        "movement": normalize_record(cur.fetchone()),
    }


def random_active_customer(cur) -> dict[str, Any]:
    return fetch_one(
        cur,
        "SELECT * FROM ecommerce.customers WHERE status = 'active' ORDER BY RANDOM() LIMIT 1",
    )


def create_cart_event(cur, request: CreateCartRequest | None = None) -> dict[str, Any]:
    request = request or CreateCartRequest()
    customer = fetch_one(cur, "SELECT * FROM ecommerce.customers WHERE customer_id = %s", (str(request.customer_id),)) if request.customer_id else random_active_customer(cur)

    cur.execute(
        "SELECT * FROM ecommerce.carts WHERE customer_id = %s AND status = 'active' LIMIT 1",
        (str(customer["customer_id"]),),
    )
    cart = cur.fetchone()
    if cart:
        return {"event": "resume_cart", "cart": normalize_record(cart)}

    cur.execute(
        """INSERT INTO ecommerce.carts (customer_id, status)
           VALUES (%s, 'active')
           RETURNING *""",
        (str(customer["customer_id"]),),
    )
    return {"event": "create_cart", "cart": normalize_record(cur.fetchone())}


def product_with_available_inventory(cur, product_id: uuid.UUID | None = None) -> dict[str, Any]:
    params: tuple[Any, ...] = ()
    product_filter = ""
    if product_id:
        product_filter = "AND p.product_id = %s"
        params = (str(product_id),)
    return fetch_one(
        cur,
        f"""SELECT p.product_id, p.list_price, p.product_name, SUM(i.quantity_on_hand - i.quantity_reserved) AS available_quantity
            FROM ecommerce.products p
            JOIN ecommerce.inventory i ON i.product_id = p.product_id
            WHERE p.status = 'active'
              AND i.quantity_on_hand > i.quantity_reserved
              {product_filter}
            GROUP BY p.product_id, p.list_price, p.product_name
            ORDER BY RANDOM()
            LIMIT 1""",
        params,
    )


def add_cart_item_event(cur, cart_id: uuid.UUID, request: AddCartItemRequest | None = None) -> dict[str, Any]:
    request = request or AddCartItemRequest()
    cart = fetch_one(cur, "SELECT * FROM ecommerce.carts WHERE cart_id = %s AND status = 'active'", (str(cart_id),))
    product = product_with_available_inventory(cur, request.product_id)
    if int(product["available_quantity"]) < request.quantity:
        raise HTTPException(status_code=409, detail="Not enough available inventory for requested quantity")

    cur.execute(
        """INSERT INTO ecommerce.cart_items
           (cart_id, product_id, quantity, unit_price_snapshot)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT ON CONSTRAINT ux_cart_items_cart_product DO UPDATE
           SET quantity = ecommerce.cart_items.quantity + EXCLUDED.quantity,
               unit_price_snapshot = EXCLUDED.unit_price_snapshot,
               updated_at = CURRENT_TIMESTAMP
           RETURNING *""",
        (str(cart["cart_id"]), str(product["product_id"]), request.quantity, product["list_price"]),
    )
    return {"event": "add_item_to_cart", "cart_item": normalize_record(cur.fetchone())}


def default_address_for_customer(cur, customer_id: str, address_type: str) -> dict[str, Any]:
    cur.execute(
        """SELECT * FROM ecommerce.customer_addresses
           WHERE customer_id = %s
             AND address_type = %s
           ORDER BY is_default DESC, created_at DESC
           LIMIT 1""",
        (customer_id, address_type),
    )
    address = cur.fetchone()
    if address:
        return address
    request = CustomerAddressRequest(address_type=address_type, is_default=True)
    return create_customer_address_event(cur, uuid.UUID(customer_id), request)["address"]


def reserve_inventory_for_order(cur, order_id: str, product_id: str, quantity: int) -> list[dict[str, Any]]:
    remaining = quantity
    movements = []
    cur.execute(
        """SELECT * FROM ecommerce.inventory
           WHERE product_id = %s
             AND quantity_on_hand > quantity_reserved
           ORDER BY quantity_on_hand - quantity_reserved DESC
           FOR UPDATE""",
        (product_id,),
    )
    rows = cur.fetchall()
    for row in rows:
        if remaining <= 0:
            break
        available = int(row["quantity_on_hand"]) - int(row["quantity_reserved"])
        reserve_qty = min(remaining, available)
        cur.execute(
            """UPDATE ecommerce.inventory
               SET quantity_reserved = quantity_reserved + %s
               WHERE inventory_id = %s
               RETURNING *""",
            (reserve_qty, str(row["inventory_id"])),
        )
        inventory = cur.fetchone()
        cur.execute(
            """INSERT INTO ecommerce.inventory_movements
               (product_id, warehouse_id, order_id, movement_type, quantity_change, quantity_after, reason)
               VALUES (%s, %s, %s, 'stock_reserved', %s, %s, 'checkout reservation')
               RETURNING *""",
            (
                product_id,
                str(row["warehouse_id"]),
                order_id,
                -reserve_qty,
                inventory["quantity_on_hand"],
            ),
        )
        movements.append(normalize_record(cur.fetchone()))
        remaining -= reserve_qty

    if remaining:
        raise HTTPException(status_code=409, detail="Not enough inventory to reserve cart")
    return movements


def checkout_cart_event(cur, cart_id: uuid.UUID, request: CheckoutRequest | None = None) -> dict[str, Any]:
    request = request or CheckoutRequest()
    cart = fetch_one(
        cur,
        "SELECT * FROM ecommerce.carts WHERE cart_id = %s AND status = 'active'",
        (str(cart_id),),
    )
    cur.execute("SELECT * FROM ecommerce.cart_items WHERE cart_id = %s ORDER BY added_at", (str(cart_id),))
    cart_items = cur.fetchall()
    if not cart_items:
        raise HTTPException(status_code=409, detail="Cannot checkout an empty cart")

    customer_id = str(cart["customer_id"])
    shipping_address = (
        fetch_one(cur, "SELECT * FROM ecommerce.customer_addresses WHERE address_id = %s", (str(request.shipping_address_id),))
        if request.shipping_address_id else default_address_for_customer(cur, customer_id, "shipping")
    )
    billing_address = (
        fetch_one(cur, "SELECT * FROM ecommerce.customer_addresses WHERE address_id = %s", (str(request.billing_address_id),))
        if request.billing_address_id else default_address_for_customer(cur, customer_id, "billing")
    )

    subtotal = sum(item["unit_price_snapshot"] * item["quantity"] for item in cart_items)
    discount = Decimal("0.00")
    shipping = Decimal(random.randint(1500, 5000)) / Decimal("100")
    tax = (subtotal * Decimal("0.10")).quantize(Decimal("0.01"))
    total = subtotal - discount + shipping + tax

    cur.execute(
        """INSERT INTO ecommerce.orders
           (customer_id, shipping_address_id, billing_address_id, promotion_id, order_number,
            order_status, subtotal_amount, discount_amount, shipping_amount, tax_amount, total_amount)
           VALUES (%s, %s, %s, %s, '', 'pending_payment', %s, %s, %s, %s, %s)
           RETURNING *""",
        (
            customer_id,
            shipping_address["address_id"],
            billing_address["address_id"],
            str(request.promotion_id) if request.promotion_id else None,
            subtotal,
            discount,
            shipping,
            tax,
            total,
        ),
    )
    order = cur.fetchone()
    order_items = []
    movements = []

    for item in cart_items:
        line_total = item["unit_price_snapshot"] * item["quantity"]
        cur.execute(
            """INSERT INTO ecommerce.order_items
               (order_id, product_id, quantity, unit_price, discount_amount, line_total)
               VALUES (%s, %s, %s, %s, 0, %s)
               RETURNING *""",
            (
                str(order["order_id"]),
                str(item["product_id"]),
                item["quantity"],
                item["unit_price_snapshot"],
                line_total,
            ),
        )
        order_items.append(normalize_record(cur.fetchone()))
        movements.extend(reserve_inventory_for_order(cur, str(order["order_id"]), str(item["product_id"]), item["quantity"]))

    cur.execute("UPDATE ecommerce.carts SET status = 'checked_out' WHERE cart_id = %s RETURNING *", (str(cart_id),))

    return {
        "event": "checkout_cart",
        "order": normalize_record(order),
        "order_items": order_items,
        "inventory_movements": movements,
    }


def create_payment_event(cur, order_id: uuid.UUID, request: PaymentRequest | None = None) -> dict[str, Any]:
    request = request or PaymentRequest()
    order = fetch_one(cur, "SELECT * FROM ecommerce.orders WHERE order_id = %s", (str(order_id),))
    amount = request.amount if request.amount is not None else order["total_amount"]
    transaction_reference = f"TXN-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{random.randint(100000, 999999)}"
    paid_at = datetime.now(timezone.utc) if request.payment_status in {"paid", "authorized"} else None

    cur.execute(
        """INSERT INTO ecommerce.payments
           (order_id, payment_method, payment_status, amount, transaction_reference, paid_at)
           VALUES (%s, %s, %s, %s, %s, %s)
           RETURNING *""",
        (str(order_id), request.payment_method, request.payment_status, amount, transaction_reference, paid_at),
    )
    payment = cur.fetchone()

    new_status = "paid" if request.payment_status in {"paid", "authorized"} else "payment_failed"
    cur.execute(
        "UPDATE ecommerce.orders SET order_status = %s WHERE order_id = %s RETURNING *",
        (new_status, str(order_id)),
    )
    return {
        "event": "payment_attempt",
        "payment": normalize_record(payment),
        "order": normalize_record(cur.fetchone()),
    }


def create_shipment_event(cur, order_id: uuid.UUID, request: ShipmentRequest | None = None) -> dict[str, Any]:
    request = request or ShipmentRequest()
    order = fetch_one(
        cur,
        "SELECT * FROM ecommerce.orders WHERE order_id = %s AND order_status IN ('paid', 'processing', 'shipped')",
        (str(order_id),),
    )
    carrier = request.carrier or random.choice(["GHTK", "GHN", "Viettel Post", "DHL"])
    tracking_number = f"TRK-{random.randint(100000000, 999999999)}"
    shipped_at = datetime.now(timezone.utc) if request.shipment_status in {"shipped", "in_transit", "delivered"} else None
    delivered_at = datetime.now(timezone.utc) if request.shipment_status == "delivered" else None

    cur.execute(
        """INSERT INTO ecommerce.shipments
           (order_id, carrier, tracking_number, shipment_status, shipped_at, delivered_at)
           VALUES (%s, %s, %s, %s, %s, %s)
           RETURNING *""",
        (str(order_id), carrier, tracking_number, request.shipment_status, shipped_at, delivered_at),
    )
    shipment = cur.fetchone()
    order_status = "delivered" if request.shipment_status == "delivered" else "processing"
    cur.execute(
        "UPDATE ecommerce.orders SET order_status = %s WHERE order_id = %s RETURNING *",
        (order_status, str(order["order_id"])),
    )
    return {
        "event": "create_shipment",
        "shipment": normalize_record(shipment),
        "order": normalize_record(cur.fetchone()),
    }


def ship_reserved_stock(cur, order_id: str) -> list[dict[str, Any]]:
    movements = []
    cur.execute(
        """SELECT product_id, warehouse_id, -SUM(quantity_change) AS reserved_qty
           FROM ecommerce.inventory_movements
           WHERE order_id = %s
             AND movement_type = 'stock_reserved'
           GROUP BY product_id, warehouse_id""",
        (order_id,),
    )
    reservations = cur.fetchall()
    for reservation in reservations:
        qty = int(reservation["reserved_qty"])
        cur.execute(
            """UPDATE ecommerce.inventory
               SET quantity_on_hand = quantity_on_hand - %s,
                   quantity_reserved = quantity_reserved - %s
               WHERE product_id = %s
                 AND warehouse_id = %s
                 AND quantity_reserved >= %s
               RETURNING *""",
            (
                qty,
                qty,
                str(reservation["product_id"]),
                str(reservation["warehouse_id"]),
                qty,
            ),
        )
        inventory = cur.fetchone()
        if inventory:
            cur.execute(
                """INSERT INTO ecommerce.inventory_movements
                   (product_id, warehouse_id, order_id, movement_type, quantity_change, quantity_after, reason)
                   VALUES (%s, %s, %s, 'stock_sold', %s, %s, 'shipment delivered')
                   RETURNING *""",
                (
                    str(reservation["product_id"]),
                    str(reservation["warehouse_id"]),
                    order_id,
                    -qty,
                    inventory["quantity_on_hand"],
                ),
            )
            movements.append(normalize_record(cur.fetchone()))
    return movements


def update_shipment_status_event(cur, shipment_id: uuid.UUID | None = None, request: StatusRequest | None = None) -> dict[str, Any]:
    if shipment_id:
        shipment = fetch_one(cur, "SELECT * FROM ecommerce.shipments WHERE shipment_id = %s", (str(shipment_id),))
    else:
        shipment = fetch_one(
            cur,
            """SELECT * FROM ecommerce.shipments
               WHERE shipment_status IN ('created', 'packed', 'shipped', 'in_transit')
               ORDER BY RANDOM()
               LIMIT 1""",
        )
    old_status = shipment["shipment_status"]
    new_status = request.status if request and request.status else SHIPMENT_TRANSITIONS.get(old_status)
    if not new_status:
        raise HTTPException(status_code=409, detail=f"Shipment cannot advance from {old_status}")

    shipped_at_sql = ", shipped_at = COALESCE(shipped_at, CURRENT_TIMESTAMP)" if new_status in {"shipped", "in_transit", "delivered"} else ""
    delivered_at_sql = ", delivered_at = CURRENT_TIMESTAMP" if new_status == "delivered" else ""
    cur.execute(
        f"""UPDATE ecommerce.shipments
            SET shipment_status = %s{shipped_at_sql}{delivered_at_sql}
            WHERE shipment_id = %s
            RETURNING *""",
        (new_status, str(shipment["shipment_id"])),
    )
    updated_shipment = cur.fetchone()
    movements = []
    if new_status == "delivered":
        movements = ship_reserved_stock(cur, str(updated_shipment["order_id"]))
        cur.execute(
            "UPDATE ecommerce.orders SET order_status = 'delivered' WHERE order_id = %s RETURNING *",
            (str(updated_shipment["order_id"]),),
        )
        order = cur.fetchone()
        update_customer_tier_from_orders(cur, str(order["customer_id"]))
    else:
        order_status = "shipped" if new_status in {"shipped", "in_transit"} else "processing"
        cur.execute(
            "UPDATE ecommerce.orders SET order_status = %s WHERE order_id = %s RETURNING *",
            (order_status, str(updated_shipment["order_id"])),
        )
        order = cur.fetchone()

    return {
        "event": "advance_shipment_status",
        "old_status": old_status,
        "new_status": new_status,
        "shipment": normalize_record(updated_shipment),
        "order": normalize_record(order),
        "inventory_movements": movements,
    }


def update_customer_tier_from_orders(cur, customer_id: str) -> dict[str, Any] | None:
    cur.execute(
        """SELECT COALESCE(SUM(total_amount), 0) AS total_spent
           FROM ecommerce.orders
           WHERE customer_id = %s
             AND order_status IN ('delivered', 'partially_returned', 'returned', 'refunded')""",
        (customer_id,),
    )
    total_spent = cur.fetchone()["total_spent"]
    tier = calc_tier(total_spent)
    cur.execute(
        "UPDATE ecommerce.customers SET loyalty_tier = %s WHERE customer_id = %s RETURNING *",
        (tier, customer_id),
    )
    return normalize_record(cur.fetchone())


def adjust_inventory_event(cur, request: InventoryAdjustRequest | None = None) -> dict[str, Any]:
    request = request or InventoryAdjustRequest(quantity_change=random.randint(-5, 25))
    if request.quantity_change == 0:
        raise HTTPException(status_code=422, detail="quantity_change cannot be 0")
    if request.product_id and request.warehouse_id:
        inventory = fetch_one(
            cur,
            "SELECT * FROM ecommerce.inventory WHERE product_id = %s AND warehouse_id = %s",
            (str(request.product_id), str(request.warehouse_id)),
        )
    else:
        inventory = fetch_one(cur, "SELECT * FROM ecommerce.inventory ORDER BY RANDOM() LIMIT 1")

    cur.execute(
        """UPDATE ecommerce.inventory
           SET quantity_on_hand = quantity_on_hand + %s
           WHERE inventory_id = %s
             AND quantity_on_hand + %s >= quantity_reserved
           RETURNING *""",
        (request.quantity_change, str(inventory["inventory_id"]), request.quantity_change),
    )
    updated_inventory = cur.fetchone()
    if not updated_inventory:
        raise HTTPException(status_code=409, detail="Inventory adjustment would make stock invalid")

    cur.execute(
        """INSERT INTO ecommerce.inventory_movements
           (product_id, warehouse_id, movement_type, quantity_change, quantity_after, reason)
           VALUES (%s, %s, %s, %s, %s, %s)
           RETURNING *""",
        (
            str(updated_inventory["product_id"]),
            str(updated_inventory["warehouse_id"]),
            request.movement_type,
            request.quantity_change,
            updated_inventory["quantity_on_hand"],
            request.reason,
        ),
    )
    return {
        "event": "adjust_inventory",
        "inventory": normalize_record(updated_inventory),
        "movement": normalize_record(cur.fetchone()),
    }


def restock_lowest_inventory_event(cur) -> dict[str, Any]:
    inventory = fetch_one(
        cur,
        """SELECT * FROM ecommerce.inventory
           ORDER BY quantity_on_hand - quantity_reserved ASC
           LIMIT 1""",
    )
    qty = random.randint(30, 100)
    return adjust_inventory_event(
        cur,
        InventoryAdjustRequest(
            product_id=inventory["product_id"],
            warehouse_id=inventory["warehouse_id"],
            quantity_change=qty,
            movement_type="stock_in",
            reason="supplier restock received",
        ),
    )


def deactivate_dormant_customer_event(cur) -> dict[str, Any]:
    cur.execute(
        """UPDATE ecommerce.customers
           SET status = 'inactive'
           WHERE customer_id = (
               SELECT c.customer_id
               FROM ecommerce.customers c
               WHERE c.status = 'active'
                 AND c.loyalty_tier = 'BRONZE'
                 AND NOT EXISTS (
                     SELECT 1 FROM ecommerce.orders o WHERE o.customer_id = c.customer_id
                 )
               ORDER BY RANDOM()
               LIMIT 1
           )
           RETURNING *""",
    )
    customer = cur.fetchone()
    if not customer:
        raise HTTPException(status_code=404, detail="No dormant active BRONZE customer exists")
    return {"event": "deactivate_dormant_customer", "customer": normalize_record(customer)}


def heartbeat_event(cur) -> dict[str, Any]:
    cur.execute(
        """INSERT INTO ecommerce.cdc_heartbeat (id, ts)
           VALUES (1, NOW())
           ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts
           RETURNING *""",
    )
    return {"event": "cdc_heartbeat", "heartbeat": normalize_record(cur.fetchone())}


def bootstrap_catalog_event(cur, request: BootstrapRequest | None = None) -> dict[str, Any]:
    request = request or BootstrapRequest()
    steps = []
    warehouses = []
    for blueprint in WAREHOUSE_BLUEPRINTS[:request.warehouses]:
        warehouse = create_warehouse_event(cur, WarehouseRequest(
            warehouse_code=blueprint[0],
            warehouse_name=blueprint[1],
            city=blueprint[2],
            country=blueprint[3],
        ))
        warehouses.append(warehouse["warehouse"])
        steps.append(warehouse)

    for idx in range(request.products):
        blueprint = PRODUCT_BLUEPRINTS[idx % len(PRODUCT_BLUEPRINTS)]
        product = create_product_event(cur, ProductRequest(
            sku=f"{blueprint[0]}-{random.randint(1000, 9999)}",
            product_name=blueprint[1],
            product_type=blueprint[2],
            list_price=blueprint[4],
            status="active",
            specs={"mock": True, "product_type": blueprint[2]},
        ))
        steps.append(product)
        warehouse = random.choice(warehouses)
        inventory = create_inventory_event(cur, InventoryRequest(
            product_id=product["product"]["product_id"],
            warehouse_id=warehouse["warehouse_id"],
            quantity_on_hand=random.randint(50, 200),
            reorder_level=random.randint(10, 30),
        ))
        steps.append(inventory)
    return {"event": "bootstrap_catalog", "steps": steps}


def ensure_catalog_ready(cur) -> None:
    cur.execute("SELECT COUNT(*) AS count FROM ecommerce.inventory")
    if int(cur.fetchone()["count"]) == 0:
        bootstrap_catalog_event(cur, BootstrapRequest(products=5, warehouses=2))


def run_customer_cart_checkout(cur, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    ensure_catalog_ready(cur)
    customer = register_customer_event(cur, RegisterCustomerRequest(**payload.get("customer", {})))
    customer_id = uuid.UUID(customer["customer"]["customer_id"])
    shipping = create_customer_address_event(cur, customer_id, CustomerAddressRequest(address_type="shipping", is_default=True))
    billing = create_customer_address_event(cur, customer_id, CustomerAddressRequest(address_type="billing", is_default=True))
    cart = create_cart_event(cur, CreateCartRequest(customer_id=customer_id))
    item_count = payload.get("item_count", 2)
    cart_items = [
        add_cart_item_event(cur, uuid.UUID(cart["cart"]["cart_id"]), AddCartItemRequest(quantity=random.randint(1, 3)))
        for _ in range(item_count)
    ]
    order = checkout_cart_event(cur, uuid.UUID(cart["cart"]["cart_id"]), CheckoutRequest(
        shipping_address_id=shipping["address"]["address_id"],
        billing_address_id=billing["address"]["address_id"],
    ))
    return {
        "scenario": "customer_cart_checkout",
        "steps": [customer, shipping, billing, cart, *cart_items, order],
    }


def run_paid_shipped_order(cur, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    checkout = run_customer_cart_checkout(cur, payload)
    order_id = checkout["steps"][-1]["order"]["order_id"]
    payment = create_payment_event(cur, uuid.UUID(order_id), PaymentRequest(payment_status="paid"))
    shipment = create_shipment_event(cur, uuid.UUID(order_id), ShipmentRequest(shipment_status="created"))
    delivered = None
    shipment_id = uuid.UUID(shipment["shipment"]["shipment_id"])
    for _ in range(4):
        delivered = update_shipment_status_event(cur, shipment_id)
    return {"scenario": "paid_shipped_order", "steps": [checkout, payment, shipment, delivered]}


def run_order_lifecycle_step(cur, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    order_id = payload.get("order_id")
    if order_id:
        result = create_payment_event(cur, uuid.UUID(order_id), PaymentRequest(payment_status="paid"))
    else:
        result = update_shipment_status_event(cur)
    return {"scenario": "order_lifecycle_step", "steps": [result]}


def run_replenish_inventory(cur, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    ensure_catalog_ready(cur)
    payload = payload or {}
    if "quantity_change" in payload or "product_id" in payload or "warehouse_id" in payload:
        result = adjust_inventory_event(cur, InventoryAdjustRequest(**payload))
    else:
        result = restock_lowest_inventory_event(cur)
    return {"scenario": "replenish_inventory", "steps": [result]}


def run_sales_burst(cur, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    request = SalesBurstRequest(**payload)
    steps = [run_customer_cart_checkout(cur, {"item_count": request.item_count}) for _ in range(request.orders)]
    return {"scenario": "sales_burst", "steps": steps}


def run_random_activity(cur, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    ensure_catalog_ready(cur)
    choices: list[Callable[[Any, dict[str, Any] | None], dict[str, Any]]] = [
        run_customer_cart_checkout,
        run_replenish_inventory,
        lambda cur, payload: {"scenario": "price_update", "steps": [update_product_event(cur, None)]},
    ]
    if random.random() < 0.12:
        result = {"scenario": "random_activity", "steps": [deactivate_dormant_customer_event(cur)]}
    else:
        result = {"scenario": "random_activity", "steps": [random.choice(choices)(cur, payload)]}
    return result


SCENARIOS: dict[str, Callable[[Any, dict[str, Any] | None], dict[str, Any]]] = {
    "bootstrap_catalog": lambda cur, payload: bootstrap_catalog_event(cur, BootstrapRequest(**(payload or {}))),
    "customer_cart_checkout": run_customer_cart_checkout,
    "paid_shipped_order": run_paid_shipped_order,
    "order_lifecycle_step": run_order_lifecycle_step,
    "replenish_inventory": run_replenish_inventory,
    "sales_burst": run_sales_burst,
    "random_activity": run_random_activity,
}


def run_scenario(name: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    scenario = SCENARIOS.get(name)
    if not scenario:
        raise HTTPException(status_code=404, detail=f"Unknown scenario: {name}")
    with db_cursor() as cur:
        return scenario(cur, payload)


def schedule_to_state(job: ScheduleJob) -> ScheduleState:
    return ScheduleState(
        schedule_id=job.schedule_id,
        scenario=job.scenario,
        interval_seconds=job.interval_seconds,
        max_runs=job.max_runs,
        run_count=job.run_count,
        started_at=job.started_at.isoformat(),
        last_run_at=job.last_run_at.isoformat() if job.last_run_at else None,
        active=not job.stop_event.is_set() and bool(job.thread and job.thread.is_alive()),
        last_result=job.last_result,
        last_error=job.last_error,
    )


def schedule_worker(job: ScheduleJob) -> None:
    while not job.stop_event.is_set():
        if job.max_runs is not None and job.run_count >= job.max_runs:
            break
        try:
            job.last_result = run_scenario(job.scenario, job.payload)
            job.last_error = None
        except Exception as exc:
            job.last_error = str(exc)
        job.run_count += 1
        job.last_run_at = datetime.now(timezone.utc)
        job.stop_event.wait(job.interval_seconds)
    job.stop_event.set()


UI_HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Ecommerce Mock API Control Panel</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --panel-2: #eef2f6;
      --text: #17202a;
      --muted: #687385;
      --line: #d9e0e8;
      --accent: #1677ff;
      --accent-strong: #0f5dcc;
      --danger: #c83532;
      --ok: #16845b;
      --warn: #a36500;
      --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
      --sans: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: var(--sans);
      font-size: 14px;
    }
    header {
      position: sticky;
      top: 0;
      z-index: 10;
      background: rgba(255,255,255,.94);
      border-bottom: 1px solid var(--line);
      backdrop-filter: blur(10px);
    }
    .topbar {
      max-width: 1440px;
      margin: 0 auto;
      padding: 14px 20px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    h1 {
      margin: 0;
      font-size: 18px;
      line-height: 1.2;
      font-weight: 720;
      letter-spacing: 0;
    }
    .status-row {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .status-pill {
      border: 1px solid var(--line);
      background: var(--panel-2);
      border-radius: 999px;
      padding: 6px 10px;
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
    }
    .status-pill.ok { color: var(--ok); background: #e8f6ef; border-color: #bfe4d2; }
    .status-pill.err { color: var(--danger); background: #fff0ef; border-color: #efc2c0; }
    main {
      max-width: 1440px;
      margin: 0 auto;
      padding: 18px 20px 28px;
      display: grid;
      grid-template-columns: 370px minmax(0, 1fr);
      gap: 16px;
    }
    section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      min-width: 0;
    }
    .section-head {
      padding: 14px 14px 10px;
      border-bottom: 1px solid var(--line);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    h2 {
      margin: 0;
      font-size: 14px;
      font-weight: 700;
      letter-spacing: 0;
    }
    .panel-body { padding: 14px; }
    .stack { display: grid; gap: 12px; }
    .field-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    label {
      display: grid;
      gap: 5px;
      color: var(--muted);
      font-size: 12px;
    }
    input, select, textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: white;
      color: var(--text);
      padding: 8px 9px;
      font: inherit;
      min-height: 36px;
    }
    textarea {
      min-height: 88px;
      resize: vertical;
      font-family: var(--mono);
      font-size: 12px;
      line-height: 1.45;
    }
    button, a.button {
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: white;
      color: var(--text);
      padding: 8px 10px;
      font: inherit;
      cursor: pointer;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 7px;
      white-space: nowrap;
    }
    button:hover, a.button:hover { border-color: #aab7c5; background: #f9fbfd; }
    button.primary { background: var(--accent); border-color: var(--accent); color: white; }
    button.primary:hover { background: var(--accent-strong); border-color: var(--accent-strong); }
    button.danger { color: var(--danger); border-color: #efc2c0; }
    button:disabled { opacity: .6; cursor: not-allowed; }
    .button-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .quick-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .layout-right {
      display: grid;
      grid-template-rows: auto auto minmax(0, 1fr);
      gap: 16px;
      min-width: 0;
    }
    .metrics {
      display: grid;
      grid-template-columns: repeat(6, minmax(110px, 1fr));
      gap: 10px;
    }
    .metric {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      min-height: 74px;
    }
    .metric .label {
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 6px;
      overflow-wrap: anywhere;
    }
    .metric .value {
      font-size: 23px;
      line-height: 1;
      font-weight: 760;
      font-variant-numeric: tabular-nums;
    }
    .split {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(360px, .95fr);
      gap: 16px;
      min-width: 0;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }
    th, td {
      border-bottom: 1px solid var(--line);
      padding: 8px 7px;
      text-align: left;
      vertical-align: top;
      overflow-wrap: anywhere;
    }
    th {
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
      background: #fbfcfd;
    }
    td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
    pre {
      margin: 0;
      background: #111827;
      color: #d8e2f0;
      border-radius: 8px;
      padding: 12px;
      min-height: 240px;
      max-height: 620px;
      overflow: auto;
      font-family: var(--mono);
      font-size: 12px;
      line-height: 1.45;
      white-space: pre-wrap;
    }
    .log {
      display: grid;
      gap: 8px;
    }
    .scroll-box {
      max-height: 420px;
      overflow-y: auto;
      overflow-x: auto;
      padding-right: 4px;
    }
    .scroll-box table { min-width: 520px; }
    .log-item {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 9px 10px;
      background: #fbfcfd;
    }
    .log-item strong {
      display: block;
      font-size: 13px;
      margin-bottom: 3px;
    }
    .log-item span {
      color: var(--muted);
      font-size: 12px;
      font-family: var(--mono);
    }
    .muted { color: var(--muted); }
    .mono { font-family: var(--mono); }
    .divider { height: 1px; background: var(--line); margin: 4px 0; }
    @media (max-width: 1080px) {
      main { grid-template-columns: 1fr; }
      .metrics { grid-template-columns: repeat(3, minmax(110px, 1fr)); }
      .split { grid-template-columns: 1fr; }
    }
    @media (max-width: 640px) {
      .topbar { align-items: flex-start; flex-direction: column; }
      .status-row { justify-content: flex-start; }
      main { padding: 12px; }
      .field-grid, .quick-grid, .metrics { grid-template-columns: 1fr; }
      .button-row button, .quick-grid button, .button-row a { width: 100%; }
    }
  </style>
</head>
<body>
  <header>
    <div class="topbar">
      <h1>Ecommerce Mock API Control Panel</h1>
      <div class="status-row">
        <span id="dbStatus" class="status-pill">Database: checking</span>
        <span id="lastRefresh" class="status-pill">Refresh: --</span>
        <a class="button" href="/docs" target="_blank" rel="noreferrer">Open API Docs</a>
      </div>
    </div>
  </header>

  <main>
    <div class="stack">
      <section>
        <div class="section-head">
          <h2>Scenario Runner</h2>
        </div>
        <div class="panel-body stack">
          <div class="field-grid">
            <label>Bootstrap products
              <input id="bootstrapProducts" type="number" min="1" max="50" value="8" />
            </label>
            <label>Warehouses
              <input id="bootstrapWarehouses" type="number" min="1" max="__MAX_BOOTSTRAP_WAREHOUSES__" value="2" />
            </label>
          </div>
          <div class="quick-grid">
            <button class="primary" data-action="bootstrap">Bootstrap Catalog</button>
            <button class="primary" data-action="paidOrder">Paid Delivered Order</button>
            <button data-action="checkout">Cart Checkout</button>
            <button data-action="random">Random Activity</button>
            <button data-action="salesBurst">Sales Burst</button>
            <button data-action="restock">Restock Lowest</button>
            <button data-action="price">Update Product Price</button>
            <button data-action="heartbeat">Heartbeat</button>
          </div>
          <div class="field-grid">
            <label>Order item count
              <input id="itemCount" type="number" min="1" max="5" value="2" />
            </label>
            <label>Sales burst orders
              <input id="burstOrders" type="number" min="1" max="50" value="5" />
            </label>
          </div>
        </div>
      </section>

      <section>
        <div class="section-head">
          <h2>Scheduler</h2>
        </div>
        <div class="panel-body stack">
          <div class="field-grid">
            <label>Scenario
              <select id="scheduleScenario">
                <option value="random_activity">random_activity</option>
                <option value="bootstrap_catalog">bootstrap_catalog</option>
                <option value="paid_shipped_order">paid_shipped_order</option>
                <option value="customer_cart_checkout">customer_cart_checkout</option>
                <option value="replenish_inventory">replenish_inventory</option>
              </select>
            </label>
            <label>Interval seconds
              <input id="scheduleInterval" type="number" min="1" value="10" />
            </label>
            <label>Max runs
              <input id="scheduleMaxRuns" type="number" min="1" value="20" />
            </label>
            <label>Payload JSON
              <input id="schedulePayload" value="{}" />
            </label>
          </div>
          <div class="button-row">
            <button class="primary" data-action="startSchedule">Start Schedule</button>
            <button data-action="refreshSchedules">Refresh Schedules</button>
          </div>
          <div id="schedules"></div>
        </div>
      </section>

      <section>
        <div class="section-head">
          <h2>Custom Request</h2>
        </div>
        <div class="panel-body stack">
          <label>Endpoint
            <select id="customEndpoint">
              <option value="POST /business/customers/register">POST /business/customers/register</option>
              <option value="POST /business/catalog/products">POST /business/catalog/products</option>
              <option value="POST /business/inventory/adjustments">POST /business/inventory/adjustments</option>
              <option value="PATCH /business/catalog/products/random">PATCH /business/catalog/products/random</option>
            </select>
          </label>
          <label>JSON body
            <textarea id="customBody">{}</textarea>
          </label>
          <button class="primary" data-action="custom">Send Request</button>
        </div>
      </section>
    </div>

    <div class="layout-right">
      <div id="metrics" class="metrics"></div>

      <div class="split">
        <section>
          <div class="section-head">
            <h2>Table Counts</h2>
            <button data-action="refresh">Refresh</button>
          </div>
          <div class="panel-body">
            <div id="countsTable" class="scroll-box"></div>
          </div>
        </section>

        <section>
          <div class="section-head">
            <h2>Recent CDC Metrics</h2>
          </div>
          <div class="panel-body">
            <div id="eventTable" class="scroll-box"></div>
          </div>
        </section>
      </div>

      <div class="split">
        <section>
          <div class="section-head">
            <h2>Latest Result</h2>
            <button data-action="clearResult">Clear</button>
          </div>
          <div class="panel-body">
            <pre id="result">{}</pre>
          </div>
        </section>

        <section>
          <div class="section-head">
            <h2>Action Log</h2>
            <button data-action="clearLog">Clear</button>
          </div>
          <div class="panel-body">
            <div id="actionLog" class="log scroll-box"></div>
          </div>
        </section>
      </div>
    </div>
  </main>

  <script>
    const state = {
      busy: false,
      log: []
    };

    const $ = (id) => document.getElementById(id);

    function nowText() {
      return new Date().toLocaleTimeString();
    }

    function numberValue(id) {
      return Number($(id).value || 0);
    }

    function jsonInput(id) {
      const raw = $(id).value.trim();
      if (!raw) return {};
      return JSON.parse(raw);
    }

    function setResult(value) {
      $("result").textContent = JSON.stringify(value, null, 2);
    }

    function addLog(title, meta, ok = true) {
      state.log.unshift({ title, meta, ok, at: nowText() });
      state.log = state.log.slice(0, 80);
      renderLog();
    }

    function renderLog() {
      const root = $("actionLog");
      if (!state.log.length) {
        root.innerHTML = '<div class="muted">No actions yet.</div>';
        return;
      }
      root.innerHTML = state.log.map(item => `
        <div class="log-item">
          <strong>${escapeHtml(item.title)}</strong>
          <span>${escapeHtml(item.at)} | ${escapeHtml(item.meta)}</span>
        </div>
      `).join("");
    }

    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }

    async function api(path, options = {}) {
      const headers = Object.assign({ "Content-Type": "application/json" }, options.headers || {});
      const response = await fetch(path, Object.assign({}, options, { headers }));
      const text = await response.text();
      let body;
      try {
        body = text ? JSON.parse(text) : null;
      } catch {
        body = text;
      }
      if (!response.ok) {
        const detail = body && body.detail ? body.detail : response.statusText;
        throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
      }
      return body;
    }

    async function postJson(path, body = {}) {
      return api(path, { method: "POST", body: JSON.stringify(body) });
    }

    async function patchJson(path, body = {}) {
      return api(path, { method: "PATCH", body: JSON.stringify(body) });
    }

    async function del(path) {
      return api(path, { method: "DELETE" });
    }

    async function runAction(name, fn) {
      if (state.busy) return;
      state.busy = true;
      document.querySelectorAll("button").forEach(button => button.disabled = true);
      try {
        const result = await fn();
        setResult(result);
        addLog(name, "success", true);
        await refreshAll();
      } catch (error) {
        const result = { error: error.message };
        setResult(result);
        addLog(name, error.message, false);
      } finally {
        state.busy = false;
        document.querySelectorAll("button").forEach(button => button.disabled = false);
      }
    }

    async function refreshHealth() {
      try {
        const health = await api("/health");
        $("dbStatus").textContent = "Database: " + health.status;
        $("dbStatus").className = "status-pill ok";
      } catch {
        $("dbStatus").textContent = "Database: offline";
        $("dbStatus").className = "status-pill err";
      }
    }

    async function refreshSummary() {
      const summary = await api("/ui/summary");
      renderMetrics(summary);
      renderCounts(summary.table_counts);
      $("lastRefresh").textContent = "Refresh: " + nowText();
    }

    async function refreshEvents() {
      const events = await api("/ui/recent-events");
      renderEvents(events);
    }

    async function refreshSchedules() {
      const schedules = await api("/schedules");
      renderSchedules(schedules);
    }

    async function refreshAll() {
      await Promise.all([refreshHealth(), refreshSummary(), refreshEvents(), refreshSchedules()]);
    }

    function renderMetrics(summary) {
      const metrics = [
        ["Customers", summary.totals.customers || 0],
        ["Products", summary.totals.products || 0],
        ["Inventory", summary.totals.inventory || 0],
        ["Orders", summary.totals.orders || 0],
        ["Payments", summary.totals.payments || 0],
        ["Shipments", summary.totals.shipments || 0],
      ];
      $("metrics").innerHTML = metrics.map(([label, value]) => `
        <div class="metric">
          <div class="label">${escapeHtml(label)}</div>
          <div class="value">${Number(value).toLocaleString()}</div>
        </div>
      `).join("");
    }

    function renderCounts(rows) {
      if (!rows.length) {
        $("countsTable").innerHTML = '<div class="muted">No tables found.</div>';
        return;
      }
      $("countsTable").innerHTML = `
        <table>
          <thead><tr><th>Table</th><th class="num">Rows</th><th>Last Updated</th></tr></thead>
          <tbody>
            ${rows.map(row => `
              <tr>
                <td class="mono">${escapeHtml(row.table_name)}</td>
                <td class="num">${Number(row.row_count).toLocaleString()}</td>
                <td>${escapeHtml(row.last_updated || "")}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `;
    }

    function renderEvents(rows) {
      if (!rows.length) {
        $("eventTable").innerHTML = '<div class="muted">No CDC metrics yet.</div>';
        return;
      }
      $("eventTable").innerHTML = `
        <table>
          <thead><tr><th>Time</th><th>Table</th><th>Op</th><th class="num">Count</th></tr></thead>
          <tbody>
            ${rows.map(row => `
              <tr>
                <td>${escapeHtml(row.measured_time || "")}</td>
                <td class="mono">${escapeHtml(row.table_name)}</td>
                <td>${escapeHtml(row.operation)}</td>
                <td class="num">${Number(row.record_count).toLocaleString()}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `;
    }

    function renderSchedules(rows) {
      const root = $("schedules");
      if (!rows.length) {
        root.innerHTML = '<div class="muted">No schedules running.</div>';
        return;
      }
      root.innerHTML = `
        <table>
          <thead><tr><th>Scenario</th><th class="num">Runs</th><th>Status</th><th></th></tr></thead>
          <tbody>
            ${rows.map(row => `
              <tr>
                <td class="mono">${escapeHtml(row.scenario)}</td>
                <td class="num">${Number(row.run_count).toLocaleString()}</td>
                <td>${row.active ? "active" : "stopped"}</td>
                <td><button class="danger" data-stop-schedule="${escapeHtml(row.schedule_id)}">Stop</button></td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `;
      root.querySelectorAll("[data-stop-schedule]").forEach(button => {
        button.addEventListener("click", () => runAction("Stop schedule", async () => {
          return del("/schedules/" + button.dataset.stopSchedule);
        }));
      });
    }

    const actions = {
      bootstrap: () => postJson("/scenarios/bootstrap_catalog", {
        products: numberValue("bootstrapProducts"),
        warehouses: numberValue("bootstrapWarehouses")
      }),
      paidOrder: () => postJson("/scenarios/paid_shipped_order", {
        item_count: numberValue("itemCount")
      }),
      checkout: () => postJson("/scenarios/customer_cart_checkout", {
        item_count: numberValue("itemCount")
      }),
      random: () => postJson("/scenarios/random_activity", {}),
      salesBurst: () => postJson("/scenarios/sales_burst", {
        orders: numberValue("burstOrders"),
        item_count: numberValue("itemCount")
      }),
      restock: () => postJson("/business/inventory/restock-lowest", {}),
      price: () => patchJson("/business/catalog/products/random", {}),
      heartbeat: () => postJson("/business/heartbeat", {}),
      startSchedule: () => postJson("/schedules", {
        scenario: $("scheduleScenario").value,
        interval_seconds: numberValue("scheduleInterval"),
        max_runs: numberValue("scheduleMaxRuns") || null,
        payload: jsonInput("schedulePayload")
      }),
      refreshSchedules: () => refreshSchedules().then(() => ({ ok: true })),
      refresh: () => refreshAll().then(() => ({ ok: true })),
      clearResult: () => {
        setResult({});
        return Promise.resolve({ ok: true });
      },
      clearLog: () => {
        state.log = [];
        renderLog();
        return Promise.resolve({ ok: true });
      },
      custom: () => {
        const [method, path] = $("customEndpoint").value.split(" ");
        const body = jsonInput("customBody");
        if (method === "PATCH") return patchJson(path, body);
        return postJson(path, body);
      }
    };

    document.querySelectorAll("[data-action]").forEach(button => {
      button.addEventListener("click", () => {
        const action = button.dataset.action;
        runAction(button.textContent.trim(), actions[action]);
      });
    });

    renderLog();
    refreshAll();
    setInterval(() => {
      refreshSummary().catch(() => {});
      refreshEvents().catch(() => {});
      refreshSchedules().catch(() => {});
    }, 5000);
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def control_panel() -> str:
    return UI_HTML.replace("__MAX_BOOTSTRAP_WAREHOUSES__", str(len(WAREHOUSE_BLUEPRINTS)))


@app.get("/ui/summary")
def ui_summary() -> dict[str, Any]:
    with db_cursor() as cur:
        cur.execute("SELECT table_name, row_count, last_updated FROM ecommerce.get_table_counts() ORDER BY table_name")
        rows = [normalize_record(row) for row in cur.fetchall()]
        totals = {row["table_name"]: row["row_count"] for row in rows}
        cur.execute(
            """SELECT COALESCE(SUM(quantity_on_hand - quantity_reserved), 0) AS available_units
               FROM ecommerce.inventory"""
        )
        inventory = normalize_record(cur.fetchone())
    return {
        "table_counts": rows,
        "totals": totals,
        "inventory": inventory,
    }


@app.get("/ui/recent-events")
def ui_recent_events(limit: int = 30) -> list[dict[str, Any]]:
    safe_limit = max(1, min(limit, 100))
    with db_cursor() as cur:
        cur.execute(
            """SELECT table_name, operation, record_count, measured_time
               FROM ecommerce.cdc_metrics
               ORDER BY measured_time DESC, id DESC
               LIMIT %s""",
            (safe_limit,),
        )
        return [normalize_record(row) for row in cur.fetchall()]


@app.get("/health")
def health() -> dict[str, Any]:
    with db_cursor() as cur:
        cur.execute("SELECT 1 AS ok")
        return {"status": "ok", "database": normalize_record(cur.fetchone())}


@app.get("/actions")
def list_actions() -> dict[str, Any]:
    return {
        "primitive_actions": [
            "POST /business/bootstrap/catalog",
            "POST /business/customers/register",
            "POST /business/customers/{customer_id}/addresses",
            "PATCH /business/customers/{customer_id}",
            "POST /business/catalog/brands",
            "POST /business/catalog/categories",
            "POST /business/catalog/products",
            "PATCH /business/catalog/products/random",
            "PATCH /business/catalog/products/{product_id}",
            "POST /business/warehouses",
            "POST /business/warehouses/{warehouse_id}/inventory",
            "POST /business/carts",
            "POST /business/carts/{cart_id}/items",
            "POST /business/carts/{cart_id}/checkout",
            "POST /business/orders/{order_id}/payments",
            "POST /business/orders/{order_id}/shipments",
            "PATCH /business/shipments/random/status",
            "PATCH /business/shipments/{shipment_id}/status",
            "POST /business/inventory/adjustments",
            "POST /business/inventory/restock-lowest",
            "PATCH /business/customers/dormant/deactivate",
            "POST /business/heartbeat",
        ],
        "scenarios": sorted(SCENARIOS),
        "scheduled_scenarios": [
            "POST /schedules",
            "GET /schedules",
            "POST /schedules/{schedule_id}/trigger",
            "DELETE /schedules/{schedule_id}",
        ],
        "schema_note": "This API targets the docs/data-arch/business_flow.md ecommerce schema.",
    }


@app.post("/business/bootstrap/catalog")
def bootstrap_catalog(request: BootstrapRequest = BootstrapRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return bootstrap_catalog_event(cur, request)


@app.post("/business/customers/register")
def register_customer(request: RegisterCustomerRequest = RegisterCustomerRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return register_customer_event(cur, request)


@app.post("/business/customers/{customer_id}/addresses")
def create_customer_address(customer_id: uuid.UUID, request: CustomerAddressRequest = CustomerAddressRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return create_customer_address_event(cur, customer_id, request)


@app.patch("/business/customers/{customer_id}")
def update_customer(customer_id: uuid.UUID, request: UpdateCustomerRequest) -> dict[str, Any]:
    with db_cursor() as cur:
        return update_customer_event(cur, customer_id, request)


@app.patch("/business/customers/dormant/deactivate")
def deactivate_dormant_customer() -> dict[str, Any]:
    with db_cursor() as cur:
        return deactivate_dormant_customer_event(cur)


@app.post("/business/catalog/brands")
def create_brand(request: BrandRequest = BrandRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return create_brand_event(cur, request)


@app.post("/business/catalog/categories")
def create_category(request: CategoryRequest = CategoryRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return create_category_event(cur, request)


@app.post("/business/catalog/products")
def create_product(request: ProductRequest = ProductRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return create_product_event(cur, request)


@app.patch("/business/catalog/products/random")
def update_random_product(request: UpdateProductRequest = UpdateProductRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return update_product_event(cur, None, request)


@app.patch("/business/catalog/products/{product_id}")
def update_product(product_id: uuid.UUID, request: UpdateProductRequest = UpdateProductRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return update_product_event(cur, product_id, request)


@app.post("/business/warehouses")
def create_warehouse(request: WarehouseRequest = WarehouseRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return create_warehouse_event(cur, request)


@app.post("/business/warehouses/{warehouse_id}/inventory")
def create_inventory(warehouse_id: uuid.UUID, request: InventoryRequest = InventoryRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        request.warehouse_id = warehouse_id
        return create_inventory_event(cur, request)


@app.post("/business/carts")
def create_cart(request: CreateCartRequest = CreateCartRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return create_cart_event(cur, request)


@app.post("/business/carts/{cart_id}/items")
def add_cart_item(cart_id: uuid.UUID, request: AddCartItemRequest = AddCartItemRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return add_cart_item_event(cur, cart_id, request)


@app.post("/business/carts/{cart_id}/checkout")
def checkout_cart(cart_id: uuid.UUID, request: CheckoutRequest = CheckoutRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return checkout_cart_event(cur, cart_id, request)


@app.post("/business/orders/{order_id}/payments")
def create_payment(order_id: uuid.UUID, request: PaymentRequest = PaymentRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return create_payment_event(cur, order_id, request)


@app.post("/business/orders/{order_id}/shipments")
def create_shipment(order_id: uuid.UUID, request: ShipmentRequest = ShipmentRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return create_shipment_event(cur, order_id, request)


@app.patch("/business/shipments/random/status")
def update_random_shipment_status(request: StatusRequest = StatusRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return update_shipment_status_event(cur, None, request)


@app.patch("/business/shipments/{shipment_id}/status")
def update_shipment_status(shipment_id: uuid.UUID, request: StatusRequest = StatusRequest()) -> dict[str, Any]:
    with db_cursor() as cur:
        return update_shipment_status_event(cur, shipment_id, request)


@app.post("/business/inventory/adjustments")
def adjust_inventory(request: InventoryAdjustRequest = InventoryAdjustRequest(quantity_change=10)) -> dict[str, Any]:
    with db_cursor() as cur:
        return adjust_inventory_event(cur, request)


@app.post("/business/inventory/restock-lowest")
def restock_lowest_inventory() -> dict[str, Any]:
    with db_cursor() as cur:
        return restock_lowest_inventory_event(cur)


@app.post("/business/heartbeat")
def heartbeat() -> dict[str, Any]:
    with db_cursor() as cur:
        return heartbeat_event(cur)


@app.post("/scenarios/{scenario_name}")
def trigger_scenario(scenario_name: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return run_scenario(scenario_name, payload or {})


@app.post("/schedules")
def create_schedule(request: ScheduleRequest) -> ScheduleState:
    if request.scenario not in SCENARIOS:
        raise HTTPException(status_code=404, detail=f"Unknown scenario: {request.scenario}")

    job = ScheduleJob(
        schedule_id=str(uuid.uuid4()),
        scenario=request.scenario,
        interval_seconds=request.interval_seconds,
        payload=request.payload,
        max_runs=request.max_runs,
    )
    job.thread = threading.Thread(target=schedule_worker, args=(job,), daemon=True)
    with SCHEDULE_LOCK:
        SCHEDULES[job.schedule_id] = job
    job.thread.start()
    return schedule_to_state(job)


@app.get("/schedules")
def list_schedules() -> list[ScheduleState]:
    with SCHEDULE_LOCK:
        return [schedule_to_state(job) for job in SCHEDULES.values()]


@app.post("/schedules/{schedule_id}/trigger")
def trigger_schedule_once(schedule_id: str) -> dict[str, Any]:
    with SCHEDULE_LOCK:
        job = SCHEDULES.get(schedule_id)
    if not job:
        raise HTTPException(status_code=404, detail="Schedule was not found")

    result = run_scenario(job.scenario, job.payload)
    job.run_count += 1
    job.last_run_at = datetime.now(timezone.utc)
    job.last_result = result
    job.last_error = None
    return result


@app.delete("/schedules/{schedule_id}")
def stop_schedule(schedule_id: str) -> ScheduleState:
    with SCHEDULE_LOCK:
        job = SCHEDULES.get(schedule_id)
    if not job:
        raise HTTPException(status_code=404, detail="Schedule was not found")

    job.stop_event.set()
    if job.thread:
        job.thread.join(timeout=2)
    return schedule_to_state(job)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
