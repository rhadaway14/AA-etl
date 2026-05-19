from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
import hashlib
import math

try:
    import orjson
except ImportError:  # pragma: no cover
    orjson = None


SOURCE_FIELDS = {
    "action",
    "atpsource",
    "batchnumber",
    "subdate",
    "subid",
    "subtime",
    "key_column",
}

# Fields that should remain strings even if they look numeric. These are codes,
# not numbers. Leading zeroes and sentinel values like 00XXX00 must be preserved.
FORCE_STRING_FIELDS = {
    "effective_date",
    "discontinue_date",
    "eff_date_cat14",
    "eff_date_cat15",
    "fn_travel_commence_from_date",
    "fn_travel_completion_date",
    "fn_travel_expiration_date",
    "ticket_first",
    "ticket_last",
    "res_last",
    "cat14_trvl_comm_on_after",
    "cat14_trvl_comm_on_bef",
    "cat14_trvl_return_by",
    "cat15_rsrv_on_after",
    "cat15_rsrv_on_bef",
    "cat15_tkt_on_after",
    "cat15_tkt_on_bef",
    "fare_rcvd",
    "subdate",
    "subtime",
    "routing_number",
    "rule_number",
    "fare_class_code",
    "fare_tariff_name",
    "carrier_code",
    "market",
    "market_real",
    "origin_city_code",
    "destination_city_code",
    "passenger_type",
    "passenger_type_code",
    "ow_rt_ind",
    "dom_fare_chg_type",
}

INT_FIELDS = {
    "action",
    "batchnumber",
    "fare_tariff_number",
    "rule_tariff_number",
    "link_nbr",
    "link_seq_nbr",
    "cat5_adv_res_last",
    "cat5_adv_tktg",
    "cat5_ap",
    "cat5_res_hold",
    "cat6_unit_of_tm",
    "cat7_unit_of_tm",
    "fare_amount",
}

DECIMAL_FIELDS = {
    "one_way_fare_amount",
    "untaxed_fare_amount",
    "fare_tax_amount",
    "fare_tax_rate",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def normalize_value(field: str, value: Any) -> Any:
    if is_blank(value):
        return None

    if isinstance(value, str):
        value = value.strip()

    if field in FORCE_STRING_FIELDS or field.startswith("record") or field.startswith("rec2"):
        return str(value)

    if field in INT_FIELDS:
        try:
            return int(Decimal(str(value)))
        except (InvalidOperation, ValueError):
            return str(value)

    if field in DECIMAL_FIELDS:
        try:
            # Use float for JSON friendliness. If exact financial math is required,
            # store cents as integers instead.
            return float(Decimal(str(value)))
        except (InvalidOperation, ValueError):
            return str(value)

    return value


def is_rule_field(field: str) -> bool:
    return field.startswith("record0_") or field.startswith("record1_") or field.startswith("record2_")


def canonical_json_bytes(value: Any) -> bytes:
    if orjson:
        return orjson.dumps(value, option=orjson.OPT_SORT_KEYS)
    import json

    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_business_hash(fare_data: dict[str, Any], rule: dict[str, Any]) -> str:
    payload = {
        "fare_data": fare_data,
        "rule": rule,
    }
    digest = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
    return f"sha256:{digest}"


@dataclass(frozen=True)
class FareDocumentParts:
    fare_key: str
    source_key: str
    fare_data: dict[str, Any]
    rule: dict[str, Any]
    source: dict[str, Any]
    business_hash: str


def row_to_parts(row: dict[str, Any], batch_id: str, source_file: str, row_number: int) -> FareDocumentParts:
    source_key_raw = row.get("key_column")
    if is_blank(source_key_raw):
        raise ValueError("Missing required field: key_column")

    source_key = str(source_key_raw).strip()
    fare_key = f"fare::{source_key}"

    fare_data: dict[str, Any] = {}
    rule: dict[str, Any] = {}
    source: dict[str, Any] = {
        "last_batch_id": batch_id,
        "source_file": source_file,
        "row_number": row_number,
    }

    for field, raw_value in row.items():
        value = normalize_value(field, raw_value)

        if field in SOURCE_FIELDS:
            if field != "key_column":
                source[field] = value
        elif is_rule_field(field):
            rule[field] = value
        else:
            fare_data[field] = value

    business_hash = sha256_business_hash(fare_data=fare_data, rule=rule)

    return FareDocumentParts(
        fare_key=fare_key,
        source_key=source_key,
        fare_data=fare_data,
        rule=rule,
        source=source,
        business_hash=business_hash,
    )


def build_current_document(parts: FareDocumentParts, now: str, existing_created_at: str | None = None) -> dict[str, Any]:
    return {
        "type": "fare_current",
        "fare_key": parts.fare_key,
        "source_key": parts.source_key,
        "business_hash": parts.business_hash,
        "fare_data": parts.fare_data,
        "rule": parts.rule,
        "source": parts.source,
        "created_at": existing_created_at or now,
        "updated_at": now,
    }


def route_summary(parts: FareDocumentParts) -> dict[str, Any]:
    fd = parts.fare_data
    return {
        "carrier_code": fd.get("carrier_code"),
        "origin_city_code": fd.get("origin_city_code"),
        "destination_city_code": fd.get("destination_city_code"),
        "market": fd.get("market"),
        "market_real": fd.get("market_real"),
    }


def fare_identity(parts: FareDocumentParts) -> dict[str, Any]:
    fd = parts.fare_data
    return {
        "fare_tariff_name": fd.get("fare_tariff_name"),
        "fare_tariff_number": fd.get("fare_tariff_number"),
        "fare_class_code": fd.get("fare_class_code"),
        "rule_number": fd.get("rule_number"),
        "rule_tariff_number": fd.get("rule_tariff_number"),
        "routing_number": fd.get("routing_number"),
        "link_nbr": fd.get("link_nbr"),
        "link_seq_nbr": fd.get("link_seq_nbr"),
    }


def flatten(prefix: str, value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, child in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else key
            result.update(flatten(child_prefix, child))
        return result
    return {prefix: value}


def build_diff(old_doc: dict[str, Any], new_doc: dict[str, Any]) -> tuple[list[str], dict[str, dict[str, Any]]]:
    old_business = {
        "fare_data": old_doc.get("fare_data", {}),
        "rule": old_doc.get("rule", {}),
    }
    new_business = {
        "fare_data": new_doc.get("fare_data", {}),
        "rule": new_doc.get("rule", {}),
    }

    old_flat = flatten("", old_business)
    new_flat = flatten("", new_business)
    changed_fields = sorted(set(old_flat.keys()) | set(new_flat.keys()))

    diff: dict[str, dict[str, Any]] = {}
    for field in changed_fields:
        old_value = old_flat.get(field)
        new_value = new_flat.get(field)
        if old_value != new_value:
            diff[field] = {"old": old_value, "new": new_value}

    return sorted(diff.keys()), diff


def build_history_document(
    *,
    parts: FareDocumentParts,
    change_type: str,
    old_doc: dict[str, Any] | None,
    new_doc: dict[str, Any],
    batch_id: str,
    now: str,
) -> dict[str, Any]:
    old_hash = old_doc.get("business_hash") if old_doc else None
    changed_fields: list[str]
    diff: dict[str, Any]

    if old_doc is None:
        changed_fields = []
        diff = {}
    else:
        changed_fields, diff = build_diff(old_doc, new_doc)

    return {
        "type": "fare_change",
        "fare_key": parts.fare_key,
        "source_key": parts.source_key,
        "change_type": change_type,
        "old_business_hash": old_hash,
        "new_business_hash": parts.business_hash,
        "changed_fields": changed_fields,
        "diff": diff,
        "route": route_summary(parts),
        "fare_identity": fare_identity(parts),
        "source": {
            **parts.source,
            "batch_id": batch_id,
        },
        "changed_at": now,
    }


def history_key(parts: FareDocumentParts, batch_id: str) -> str:
    safe_hash = parts.business_hash.replace("sha256:", "sha256-")
    return f"change::{parts.source_key}::{batch_id}::{safe_hash}"
