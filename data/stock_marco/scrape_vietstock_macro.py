import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://finance.vietstock.vn"
MACRO_PAGE = f"{BASE_URL}/macro-data"
OUT_FILE = Path(__file__).resolve().parent / "test"
DEFAULT_COOKIE_FILE = Path(__file__).resolve().parent / "vietstock_cookies.json"

# User-requested indicator themes to map against available categories/menu IDs.
INDICATOR_QUERIES = {
    "GDP": "GDP",
    "lam_phat_CPI": "CPI",
    "lai_suat": "lai suat",
    "ty_gia_USD_VND": "USD VND",
    "that_nghiep": "that nghiep",
    "cung_tien": "M2",
    "xuat_nhap_khau": "xuat nhap khau",
    "no_cong": "no cong",
    "chi_so_san_xuat_cong_nghiep": "chi so san xuat cong nghiep",
    "chinh_sach_ngan_hang_trung_uong": "ngan hang nha nuoc",
}


def five_year_window() -> tuple[str, str]:
    today = date.today()
    start = today.replace(year=today.year - 5)
    return start.isoformat(), today.isoformat()


def load_cookie_input() -> Any | None:
    # Priority 1: VIETSTOCK_COOKIES_JSON environment variable.
    json_blob = os.getenv("VIETSTOCK_COOKIES_JSON", "").strip()
    if json_blob:
        return json.loads(json_blob)

    # Priority 2: explicit cookie file path in env var.
    cookie_file_env = os.getenv("VIETSTOCK_COOKIE_FILE", "").strip()
    if cookie_file_env:
        cookie_file = Path(cookie_file_env)
        if cookie_file.exists():
            return json.loads(cookie_file.read_text(encoding="utf-8-sig"))
        raise FileNotFoundError(f"Cookie file not found: {cookie_file}")

    # Priority 3: default local cookie file near this script.
    if DEFAULT_COOKIE_FILE.exists():
        return json.loads(DEFAULT_COOKIE_FILE.read_text(encoding="utf-8-sig"))

    return None


def attach_auth_cookies(session: requests.Session) -> bool:
    cookie_input = load_cookie_input()
    if cookie_input is None:
        return False

    if isinstance(cookie_input, dict):
        # Simple format: {"cookie_name": "cookie_value", ...}
        for name, value in cookie_input.items():
            session.cookies.set(name, str(value), domain="finance.vietstock.vn", path="/")
        return True

    if isinstance(cookie_input, list):
        # Detailed format: [{"name": "...", "value": "...", "domain": "...", "path": "/"}, ...]
        for item in cookie_input:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            value = item.get("value")
            if not name or value is None:
                continue
            domain = item.get("domain") or "finance.vietstock.vn"
            path = item.get("path") or "/"
            session.cookies.set(str(name), str(value), domain=domain, path=path)
        return True

    raise ValueError("Unsupported cookie format. Use object or array JSON.")


def get_token_and_session() -> tuple[requests.Session, str]:
    session = requests.Session()
    attach_auth_cookies(session)
    html = session.get(MACRO_PAGE, headers={"User-Agent": "Mozilla/5.0"}, timeout=40).text
    soup = BeautifulSoup(html, "html.parser")
    token_input = soup.select_one('input[name="__RequestVerificationToken"]')
    if not token_input or not token_input.get("value"):
        raise RuntimeError("Cannot find __RequestVerificationToken on Vietstock macro page")
    return session, token_input["value"]


def xrw_headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Referer": MACRO_PAGE,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }


def get_categories(session: requests.Session, token: str) -> list[dict[str, Any]]:
    resp = session.post(
        f"{BASE_URL}/Macro/GetIndicatorCategoryData",
        data={"__RequestVerificationToken": token},
        headers=xrw_headers(),
        timeout=40,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_category_data(
    session: requests.Session,
    token: str,
    category: dict[str, Any],
    from_date: str,
    to_date: str,
) -> dict[str, Any]:
    payload = {
        "termTypeID": category.get("DefaultTermTypeIDSelected") or 0,
        "subTermTypeID": "",
        "fromDate": from_date,
        "toDate": to_date,
        "type": "CATEGORY",
        "listID": str(category.get("CategoryID")),
        "__RequestVerificationToken": token,
    }
    resp = session.post(
        f"{BASE_URL}/Macro/GetReportDataByIDs",
        data=payload,
        headers=xrw_headers(),
        timeout=40,
    )
    resp.raise_for_status()
    obj = resp.json()

    if isinstance(obj, dict) and obj.get("errorModel"):
        error_model = obj["errorModel"] or {}
        return {
            "status": "locked",
            "error_code": error_model.get("ErrorCode"),
            "error_message": error_model.get("ErrorMessage"),
            "row_count": 0,
            "rows": [],
        }

    if isinstance(obj, dict) and "Data" in obj:
        rows = obj.get("Data") or []
        return {
            "status": "ok",
            "error_code": None,
            "error_message": None,
            "row_count": len(rows),
            "rows": rows,
        }

    return {
        "status": "unknown",
        "error_code": None,
        "error_message": "Unexpected response format",
        "row_count": 0,
        "rows": [],
    }


def search_menu_ids(session: requests.Session, query: str) -> list[int]:
    resp = session.get(
        f"{BASE_URL}/search-macro-menu",
        params={"query": query, "languageId": 1},
        headers=xrw_headers(),
        timeout=40,
    )
    resp.raise_for_status()
    obj = resp.json()
    return obj.get("data") or []


def build_output() -> dict[str, Any]:
    from_date, to_date = five_year_window()
    session, token = get_token_and_session()
    categories = get_categories(session, token)

    cat_by_id = {c.get("CategoryID"): c for c in categories}
    category_results: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []

    for cat in categories:
        data_res = fetch_category_data(session, token, cat, from_date, to_date)
        entry = {
            "CategoryID": cat.get("CategoryID"),
            "ParentID": cat.get("ParentID"),
            "CategoryName": cat.get("CategoryName"),
            "SubPath": cat.get("SubPath"),
            "CatDataTypeID": cat.get("CatDataTypeID"),
            "DefaultTermTypeIDSelected": cat.get("DefaultTermTypeIDSelected"),
            "status": data_res["status"],
            "error_code": data_res["error_code"],
            "error_message": data_res["error_message"],
            "row_count": data_res["row_count"],
        }
        category_results.append(entry)

        if data_res["status"] == "ok" and data_res["rows"]:
            for row in data_res["rows"]:
                all_rows.append(
                    {
                        "category_id": cat.get("CategoryID"),
                        "category_name": cat.get("CategoryName"),
                        "data": row,
                    }
                )

    indicator_mapping: dict[str, Any] = {}
    for key, query in INDICATOR_QUERIES.items():
        ids = search_menu_ids(session, query)
        mapped = []
        for item_id in ids:
            c = cat_by_id.get(item_id)
            if c:
                mapped.append(
                    {
                        "id": item_id,
                        "name": c.get("CategoryName"),
                        "status": next((x["status"] for x in category_results if x["CategoryID"] == item_id), "unknown"),
                        "row_count": next((x["row_count"] for x in category_results if x["CategoryID"] == item_id), 0),
                    }
                )
            else:
                mapped.append({"id": item_id, "name": None, "status": "unknown", "row_count": 0})
        indicator_mapping[key] = {
            "query": query,
            "matched_ids": ids,
            "matched_items": mapped,
        }

    locked_count = sum(1 for c in category_results if c["status"] == "locked")
    ok_count = sum(1 for c in category_results if c["status"] == "ok")
    unknown_count = sum(1 for c in category_results if c["status"] == "unknown")

    return {
        "source": MACRO_PAGE,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "timeline": {"from": from_date, "to": to_date},
        "rules": {
            "skip_locked_pages": True,
            "requested_scope": "all macro categories on /macro-data within 5-year window",
        },
        "summary": {
            "total_categories": len(category_results),
            "ok_categories": ok_count,
            "locked_categories": locked_count,
            "unknown_categories": unknown_count,
            "total_rows_collected": len(all_rows),
        },
        "requested_indicator_mapping": indicator_mapping,
        "categories": category_results,
        "records": all_rows,
        "notes": [
            "The endpoint /Macro/GetReportDataByIDs was called for every category.",
            "When errorModel.ErrorCode is RequestUpgradeAccount_Permission, category is marked as locked and skipped.",
        ],
    }


def main() -> None:
    output = build_output()
    OUT_FILE.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = output["summary"]
    print(
        "saved",
        OUT_FILE,
        "| total_categories=",
        summary["total_categories"],
        "ok=",
        summary["ok_categories"],
        "locked=",
        summary["locked_categories"],
        "rows=",
        summary["total_rows_collected"],
    )
    if summary["locked_categories"] > 0:
        print("hint: add authenticated cookies via VIETSTOCK_COOKIES_JSON, VIETSTOCK_COOKIE_FILE, or test/vietstock_cookies.json")


if __name__ == "__main__":
    main()
