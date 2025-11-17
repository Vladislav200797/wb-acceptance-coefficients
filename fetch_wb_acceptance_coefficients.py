#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Выгрузка коэффициентов приёмки (WB supplies API) в Supabase/public.

Метод: GET https://supplies-api.wildberries.ru/api/v1/acceptance/coefficients

Поведение:
- Каждый запуск: ПОЛНЫЙ ПЕРЕЗАЛИВ (DELETE -> INSERT).
- В Supabase храним ближайшие 14 дней по всем складам и типам поставок.

Секреты / ENV:
  WB_SUPPLIES_TOKEN       - API ключ поставщика (HeaderApiKey для supplies-api)
  SUPABASE_URL
  SUPABASE_SERVICE_KEY    - service_role key
  SUPABASE_SCHEMA         - (опция, по умолчанию "public")
  SUPABASE_TABLE          - (опция, по умолчанию "wb_acceptance_coefficients")
"""

import os
import sys
from typing import Any, Dict, List, Optional

import requests
from supabase import create_client, Client


API_URL = "https://supplies-api.wildberries.ru/api/v1/acceptance/coefficients"

WB_SUPPLIES_TOKEN      = os.getenv("WB_SUPPLIES_TOKEN")
SUPABASE_URL           = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY   = os.getenv("SUPABASE_SERVICE_KEY")
SCHEMA                 = os.getenv("SUPABASE_SCHEMA", "public")
TABLE_NAME             = os.getenv("SUPABASE_TABLE", "wb_acceptance_coefficients")

HEADERS = {
    "Authorization": WB_SUPPLIES_TOKEN or "",
    "Content-Type": "application/json",
}


def fail(msg: str, code: int = 1):
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)


def safe_to_numeric(value: Any) -> Optional[float]:
    """Перевод WB-строк вида '123.45' в float. Если пусто или null — вернём None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None
        try:
            # WB почти всегда отдаёт с точкой, но на всякий случай
            v = v.replace(",", ".")
            return float(v)
        except ValueError:
            return None
    return None


def fetch_acceptance_coefficients() -> List[Dict[str, Any]]:
    """Дёргаем WB API и возвращаем список записей."""
    resp = requests.get(API_URL, headers=HEADERS, timeout=60)
    if resp.status_code != 200:
        fail(f"WB API {resp.status_code}: {resp.text}")

    data = resp.json()
    if not isinstance(data, list):
        fail(f"Unexpected WB response shape (expected list): {data}")

    return data


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Преобразуем одну запись WB в формат таблицы.
    WB:
      date: "2024-04-11T00:00:00Z"
      coefficient: number
      warehouseID: int
      warehouseName: str
      allowUnload: bool
      boxTypeID: int | null
      storageCoef, deliveryCoef: string | null
      deliveryBaseLiter, deliveryAdditionalLiter, storageBaseLiter, storageAdditionalLiter: string | null
      isSortingCenter: bool
    """

    raw_date = row.get("date")
    coeff_date = None
    if isinstance(raw_date, str) and "T" in raw_date:
        coeff_date = raw_date.split("T", 1)[0]
    else:
        coeff_date = raw_date  # вдруг WB вернёт "YYYY-MM-DD"

    return {
        "coeff_date": coeff_date,
        "warehouse_id": row.get("warehouseID"),
        "warehouse_name": row.get("warehouseName"),
        "box_type_id": row.get("boxTypeID"),
        "coefficient": row.get("coefficient"),
        "allow_unload": row.get("allowUnload"),

        "storage_coef": safe_to_numeric(row.get("storageCoef")),
        "delivery_coef": safe_to_numeric(row.get("deliveryCoef")),

        "delivery_base_liter": safe_to_numeric(row.get("deliveryBaseLiter")),
        "delivery_additional_liter": safe_to_numeric(row.get("deliveryAdditionalLiter")),
        "storage_base_liter": safe_to_numeric(row.get("storageBaseLiter")),
        "storage_additional_liter": safe_to_numeric(row.get("storageAdditionalLiter")),

        "is_sorting_center": row.get("isSortingCenter"),
    }


def chunked(seq: List[Dict[str, Any]], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def main():
    if not WB_SUPPLIES_TOKEN:
        fail("WB_SUPPLIES_TOKEN is empty (add it to GitHub Secrets)")

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        fail("Supabase URL or SERVICE KEY is empty")

    sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # 1) тянем данные из WB
    rows = fetch_acceptance_coefficients()
    print(f"Fetched {len(rows)} raw rows from WB", flush=True)

    # 2) нормализуем
    normalized = [normalize_row(r) for r in rows]
    print(f"Normalized rows: {len(normalized)}", flush=True)

    # 3) ПОЛНЫЙ ПЕРЕЗАЛИВ: удаляем все строки
    sb.schema(SCHEMA).table(TABLE_NAME).delete().execute()
    print("Cleared target table", flush=True)

    # 4) вставляем батчами по 500
    inserted = 0
    for batch in chunked(normalized, 500):
        sb.schema(SCHEMA).table(TABLE_NAME).insert(batch).execute()
        inserted += len(batch)

    print(f"Inserted rows: {inserted}", flush=True)
    print("Acceptance coefficients sync OK", flush=True)


if __name__ == "__main__":
    main()
