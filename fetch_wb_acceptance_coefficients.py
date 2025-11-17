import os
import sys
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

import requests
from supabase import create_client, Client


WB_ACCEPTANCE_URL = "https://supplies-api.wildberries.ru/api/v1/acceptance/coefficients"


def log(msg: str) -> None:
    """Простой лог в stdout."""
    print(msg, flush=True)


def get_env(name: str, required: bool = True, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name, default)
    if required and (value is None or value.strip() == ""):
        log(f"ERROR: {name} is empty (set it in GitHub Secrets or env)")
        sys.exit(1)
    return value


def fetch_acceptance_coefficients(token: str, warehouse_ids: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Вытягивает коэффициенты приёмки с WB API.
    Если warehouse_ids=None -> по всем складам.
    """
    headers = {
        "Authorization": token.strip()
    }

    params = {}
    if warehouse_ids:
        # пример: "507,117501"
        params["warehouseIDs"] = warehouse_ids

    log(f"Requesting WB acceptance coefficients (warehouseIDs={warehouse_ids or 'ALL'})...")
    resp = requests.get(WB_ACCEPTANCE_URL, headers=headers, params=params, timeout=60)

    if resp.status_code != 200:
        log(f"ERROR: WB API {resp.status_code}: {resp.text}")
        sys.exit(1)

    try:
        data = resp.json()
    except json.JSONDecodeError:
        log(f"ERROR: cannot decode WB response as JSON: {resp.text[:300]}")
        sys.exit(1)

    if not isinstance(data, list):
        log(f"ERROR: unexpected WB format, expected list, got: {type(data)}; body snippet: {str(data)[:300]}")
        sys.exit(1)

    log(f"Fetched {len(data)} raw rows from WB")
    return data


def to_decimal(value: Any) -> Optional[float]:
    """Аккуратное приведение к float, если возможно."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if s == "":
        return None
    try:
        # WB иногда может отдавать строки, в том числе с точкой.
        return float(s.replace(",", "."))
    except ValueError:
        return None


def normalize_rows(raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Приводим формат WB к нашему табличному виду.
    """
    norm: List[Dict[str, Any]] = []

    for row in raw_rows:
        # date: string ("2024-04-11T00:00:00Z") -> date
        date_str = row.get("date")  # пример: "2024-04-11T00:00:00Z"
        if date_str:
            try:
                coeff_date = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()
            except ValueError:
                # если вдруг формат странный — пропускаем строчку
                log(f"WARN: cannot parse date '{date_str}', skip row")
                continue
        else:
            # без даты смысла нет, пропускаем
            continue

        item = {
            "coeff_date": coeff_date.isoformat(),  # Supabase сам приведёт в date
            "warehouse_id": row.get("warehouseID"),
            "warehouse_name": row.get("warehouseName") or "",
            "box_type_id": row.get("boxTypeID"),
            "coefficient": to_decimal(row.get("coefficient")),
            "allow_unload": bool(row.get("allowUnload", False)),
            "storage_coef": to_decimal(row.get("storageCoef")),
            "delivery_coef": to_decimal(row.get("deliveryCoef")),
            "delivery_base_liter": to_decimal(row.get("deliveryBaseLiter")),
            "delivery_additional_liter": to_decimal(row.get("deliveryAdditionalLiter")),
            "storage_base_liter": to_decimal(row.get("storageBaseLiter")),
            "storage_additional_liter": to_decimal(row.get("storageAdditionalLiter")),
            "is_sorting_center": bool(row.get("isSortingCenter", False)),
        }

        norm.append(item)

    log(f"Normalized rows: {len(norm)}")
    return norm


def chunked(iterable: List[Dict[str, Any]], size: int) -> List[List[Dict[str, Any]]]:
    """Разбивает список на чанки фиксированного размера."""
    return [iterable[i : i + size] for i in range(0, len(iterable), size)]


def main() -> None:
    # 🔐 Читаем переменные окружения
    wb_token = get_env("WB_SUPPLIES_TOKEN", required=True)
    supabase_url = get_env("SUPABASE_URL", required=True)
    supabase_key = get_env("SUPABASE_SERVICE_KEY", required=True)
    schema = get_env("SUPABASE_SCHEMA", required=False, default="public")
    table_name = get_env("SUPABASE_TABLE", required=False, default="wb_acceptance_coefficients")
    warehouse_ids = get_env("WB_WAREHOUSE_IDS", required=False, default=None)  # можно не задавать

    # 📥 1) Тянем данные из WB
    raw_rows = fetch_acceptance_coefficients(wb_token, warehouse_ids=warehouse_ids)
    if not raw_rows:
        log("No rows from WB, nothing to sync.")
        return

    # 🧹 2) Нормализуем
    rows = normalize_rows(raw_rows)
    if not rows:
        log("No normalized rows, nothing to insert.")
        return

    # 🔗 3) Подключаемся к Supabase
    sb: Client = create_client(supabase_url, supabase_key)

    # 🗑 4) Удаляем старые данные ИЗ ТАБЛИЦЫ (нужно WHERE, иначе PostgREST ругается)
    # Используем "мягкий truncate": удаляем всё, где coeff_date >= '1900-01-01' — то есть фактически все строки.
    log(f"Deleting previous rows from {schema}.{table_name} ...")
    try:
        (
            sb.schema(schema)
            .table(table_name)
            .delete()
            .gte("coeff_date", "1900-01-01")
            .execute()
        )
        log("Previous rows deleted.")
    except Exception as e:
        log(f"ERROR while deleting old rows: {e}")
        sys.exit(1)

    # 📤 5) Вставляем новые данные чанками
    BATCH_SIZE = 1000
    for i, batch in enumerate(chunked(rows, BATCH_SIZE), start=1):
        log(f"Inserting batch {i} with {len(batch)} rows...")
        try:
            sb.schema(schema).table(table_name).insert(batch).execute()
        except Exception as e:
            log(f"ERROR while inserting batch {i}: {e}")
            sys.exit(1)

    log(f"Done. Inserted total {len(rows)} rows into {schema}.{table_name}.")


if __name__ == "__main__":
    main()
