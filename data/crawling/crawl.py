import argparse
import json
import time
import sys
from datetime import datetime, timedelta
import requests
API_URL = "https://api.hsx.vn/mk/api/v1/market/quote-report"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9,vi-VN;q=0.8",
    "Content-Type": "application/json",
    "Origin": "https://www.hsx.vn",
    "Referer": "https://www.hsx.vn/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}
def fetch_day(date_str: str, rows: int = 50) -> list:
    """Lấy dữ liệu cho 1 ngày. date_str định dạng YYYY-MM-DD."""
    params = {
        "tradingBy": "VNFINSELECT",
        "date": date_str,
    }
    try:
        r = requests.post(
            API_URL,
            params=params,
            json={},
            headers=HEADERS,
            timeout=15,
        )
        r.raise_for_status()
        body = r.json()
        data = body.get("data", [])
        return data[:rows]
    except Exception as e:
        print(f"  [ERROR] {e}")
        return []


def get_trading_days(start: datetime, end: datetime, include_weekend: bool) -> list:
    days, cur = [], start
    while cur <= end:
        if include_weekend or cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def main():
    parser = argparse.ArgumentParser(
        description="Crawl dữ liệu cuối ngày từ HSX",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--start", "-s", required=True, help="Ngày bắt đầu YYYY-MM-DD")
    parser.add_argument("--end",   "-e", required=True, help="Ngày kết thúc YYYY-MM-DD")
    parser.add_argument("--output", "-o", default=None, help="File JSON đầu ra")
    parser.add_argument("--rows",  "-r", type=int, default=50, help="Số dòng/ngày (mặc định 50)")
    parser.add_argument("--delay", "-d", type=float, default=1.0, help="Giây chờ giữa các ngày (mặc định 1)")
    parser.add_argument("--all-days", action="store_true", help="Bao gồm cả T7, CN")
    args = parser.parse_args()

    try:
        start_dt = datetime.strptime(args.start, "%Y-%m-%d")
        end_dt   = datetime.strptime(args.end,   "%Y-%m-%d")
    except ValueError:
        sys.exit(1)

    if start_dt > end_dt:
        sys.exit(1)

    days = get_trading_days(start_dt, end_dt, args.all_days)
    if not days:
        sys.exit(0)

    print(f"  HSX Crawler – Dữ liệu cuối ngày")
    print(f"  Từ: {start_dt:%d/%m/%Y}  →  Đến: {end_dt:%d/%m/%Y}")
    print(f"  Số ngày: {len(days)}  |  Số dòng/ngày: {args.rows}")
    print(f"{'='*50}\n")

    all_data = {}
    ok, fail = 0, 0

    for i, day in enumerate(days, 1):
        date_str = day.strftime("%Y-%m-%d")
        print(f"[{i:3d}/{len(days)}] {day:%d/%m/%Y} ... ", end="", flush=True)

        rows_data = fetch_day(date_str, rows=args.rows)

        all_data[date_str] = {
            "date": day.strftime("%d/%m/%Y"),
            "total_rows": len(rows_data),
            "data": rows_data,
        }

        if rows_data:
            print(f"{len(rows_data)} dòng")
            ok += 1
        else:
            print("Không có dữ liệu")
            fail += 1

        if i < len(days):
            time.sleep(args.delay)

    output_file = args.output or f"hsx_{start_dt:%Y%m%d}_{end_dt:%Y%m%d}.json"
    result = {
        "metadata": {
            "source": API_URL,
            "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "start_date": args.start,
            "end_date": args.end,
            "total_days": len(days),
            "success_days": ok,
            "failed_days": fail,
            "rows_per_day": args.rows,
        },
        "daily_data": all_data,
    }
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f" {ok}/{len(days)} ngày thành công.")
    print(f"  File: {output_file}")
    print(f"{'='*50}\n")
if __name__ == "__main__":
    main()