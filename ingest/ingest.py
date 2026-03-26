"""
=============================================================================
Stock Prices Daily — Ingest Service
=============================================================================
Chức năng:
  1. Đọc danh sách mã CK từ company.xls.csv
  2. Với mỗi mã, fetch dữ liệu JSON từ CafeF API (không cần buffer file)
  3. Chuẩn hóa (normalize) schema tiếng Việt → schema chuẩn tiếng Anh
  4. Xử lý kiểu số Việt Nam (dấu phẩy → dấu chấm)
  5. Tách cột ThayDoi thành price_change + price_change_pct
  6. Thêm metadata: symbol, source, ingest_time, event_id
  7. Gửi từng record như 1 Kafka message (JSON) vào topic
  8. Lặp lại định kỳ (mặc định mỗi 24h)

Thiết kế: dễ mở rộng sang các topic khác bằng cách tạo thêm normalizer.
=============================================================================
"""

from __future__ import annotations

import os
import re
import csv
import json
import math
import time
import logging
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# =============================================================================
# Cấu hình logging
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ingest")

# =============================================================================
# Đọc biến môi trường
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-prices-daily")
SEND_DELAY_MS = int(os.getenv("SEND_DELAY_MS", "10"))

# CafeF API & company config
COMPANY_CSV = os.getenv("COMPANY_CSV", "/app/company.xls.csv")
NUM_COMPANIES = int(os.getenv("NUM_COMPANIES", "50"))
CAFEF_BASE_URL = os.getenv(
    "CAFEF_BASE_URL",
    "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx",
)
# Delay giữa các request tới CafeF (ms) — tránh bị rate-limit
API_DELAY_MS = int(os.getenv("API_DELAY_MS", "500"))
# Khoảng thời gian cào lại (giây). Mặc định 86400 = 1 ngày. Đặt 0 = chạy 1 lần rồi thoát.
SCRAPE_INTERVAL_SECONDS = int(os.getenv("SCRAPE_INTERVAL_SECONDS", "86400"))
# Khoảng thời gian lấy dữ liệu về quá khứ (ngày). Mặc định 30 = 1 tháng trước.
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "30"))

# =============================================================================
# Mapping cột tiếng Việt → tiếng Anh
# Dễ mở rộng: thêm topic mới chỉ cần thêm 1 dict mapping tương tự
# =============================================================================
COLUMN_MAPPING = {
    "Ngay": "trade_date",
    "GiaDieuChinh": "adjusted_close",
    "GiaDongCua": "close_price",
    # ThayDoi sẽ được tách riêng, không map trực tiếp
    "KhoiLuongKhopLenh": "matched_volume",
    "GiaTriKhopLenh": "matched_value",
    "KLThoaThuan": "negotiated_volume",
    "GtThoaThuan": "negotiated_value",
    "GiaMoCua": "open_price",
    "GiaCaoNhat": "high_price",
    "GiaThapNhat": "low_price",
}


# =============================================================================
# Utility functions
# =============================================================================

def convert_vn_number(value) -> float | None:
    """
    Chuyển đổi số kiểu Việt Nam sang float.
    Ví dụ:
      '23,95'       → 23.95
      '1.234.567'   → 1234567.0  (dấu chấm là thousand separator)
      '1.234,56'    → 1234.56
      23.95         → 23.95  (nếu đã là số)
    """
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()
    if s == "" or s == "-":
        return None

    # Xác định: nếu có cả dấu chấm VÀ dấu phẩy
    #   → dấu chấm là thousand separator, dấu phẩy là decimal
    # Nếu chỉ có dấu phẩy → nó là decimal separator
    # Nếu chỉ có dấu chấm → kiểm tra context (mặc định: decimal)
    has_dot = "." in s
    has_comma = "," in s

    if has_dot and has_comma:
        # 1.234,56 → remove dots, replace comma
        s = s.replace(".", "").replace(",", ".")
    elif has_comma:
        # 23,95 → 23.95
        s = s.replace(",", ".")
    # Nếu chỉ có dot thì giữ nguyên (decimal point)

    try:
        return float(s)
    except ValueError:
        logger.warning(f"Không thể convert số: '{value}'")
        return None


def parse_thay_doi(value) -> tuple[float | None, float | None]:
    """
    Tách cột ThayDoi thành (price_change, price_change_pct).

    Các format thường gặp:
      '-0,6(-2,44 %)'   → (-0.6, -2.44)
      '0,1(0,41 %)'     → (0.1, 0.41)
      '0(0,00 %)'       → (0.0, 0.0)
      '-0.5(-1.2%)'     → (-0.5, -1.2)
      '0'               → (0.0, None)
    """
    if pd.isna(value):
        return None, None

    s = str(value).strip()
    if s == "" or s == "-":
        return None, None

    # Pattern: số(số %)
    # Hỗ trợ cả dấu phẩy và dấu chấm
    pattern = r"^([+-]?[\d.,]+)\(([+-]?[\d.,]+)\s*%?\s*\)$"
    match = re.match(pattern, s)

    if match:
        change = convert_vn_number(match.group(1))
        pct = convert_vn_number(match.group(2))
        return change, pct
    else:
        # Fallback: chỉ có giá trị thay đổi, không có %
        change = convert_vn_number(s)
        return change, None


def load_companies_from_csv(csv_path: str, num_companies: int) -> list[str]:
    """
    Đọc danh sách mã chứng khoán từ file CSV.
    Lấy tối đa num_companies mã đầu tiên.
    """
    symbols = []
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=";")
        next(reader, None)  # bỏ qua header
        for row in reader:
            if len(row) < 5:
                continue
            symbol = row[4].strip().upper()
            if symbol:
                symbols.append(symbol)
            if len(symbols) >= num_companies:
                break
    logger.info(f"Đọc được {len(symbols)} symbols từ {csv_path}")
    return symbols


def fetch_cafef_json(
    symbol: str,
    session: requests.Session,
    start_date: datetime,
    end_date: datetime,
    page_size: int = 200,
    max_retries: int = 3,
) -> pd.DataFrame:
    """
    Fetch lịch sử giá từ CafeF JSON API cho 1 mã chứng khoán.
    Tự động phân trang. Không cần buffer file — JSON parse trực tiếp.
    Trả về DataFrame với cột tiếng Việt gốc.
    """
    all_records: list[dict] = []
    page_index = 1
    total_count: int | None = None

    while True:
        params = {
            "Symbol": symbol,
            "StartDate": start_date.strftime("%m/%d/%Y"),
            "EndDate": end_date.strftime("%m/%d/%Y"),
            "PageIndex": page_index,
            "PageSize": page_size,
        }

        resp_json = None
        for attempt in range(1, max_retries + 1):
            try:
                resp = session.get(CAFEF_BASE_URL, params=params, timeout=30)
                resp.raise_for_status()
                resp_json = resp.json()
                break
            except Exception as e:
                logger.warning(
                    f"  Retry {attempt}/{max_retries} cho {symbol} "
                    f"page {page_index}: {e}"
                )
                if attempt < max_retries:
                    time.sleep(2 * attempt)

        if resp_json is None:
            logger.error(
                f"  Không thể fetch {symbol} page {page_index} "
                f"sau {max_retries} lần thử"
            )
            break

        if not resp_json.get("Success"):
            logger.warning(f"  API trả về Success=false cho {symbol}")
            break

        data_wrapper = resp_json.get("Data") or {}
        if total_count is None:
            total_count = data_wrapper.get("TotalCount", 0)

        records = data_wrapper.get("Data") or []
        if not records:
            break

        all_records.extend(records)

        # Đủ dữ liệu hoặc hết trang?
        if len(all_records) >= total_count:
            break
        total_pages = math.ceil(total_count / page_size)
        if page_index >= total_pages:
            break

        page_index += 1
        time.sleep(API_DELAY_MS / 1000.0)

    if not all_records:
        return pd.DataFrame()

    return pd.DataFrame(all_records)


def normalize_trade_date(value) -> str | None:
    """
    Chuẩn hóa ngày giao dịch về ISO format (YYYY-MM-DD).
    Input thường là dd/mm/yyyy.
    """
    if pd.isna(value):
        return None

    s = str(value).strip()

    # Thử parse dd/mm/yyyy
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    logger.warning(f"Không thể parse ngày: '{value}'")
    return s  # trả về nguyên gốc nếu không parse được


# =============================================================================
# Normalizer chính cho stock_prices_daily
# =============================================================================

def normalize_stock_prices(df: pd.DataFrame, symbol: str) -> list[dict]:
    """
    Normalize toàn bộ DataFrame thành list các event dict chuẩn.
    Mỗi dict = 1 Kafka message.
    """
    events = []
    ingest_time = datetime.now(timezone.utc).isoformat()

    for _, row in df.iterrows():
        try:
            # Parse ngày
            trade_date = normalize_trade_date(row.get("Ngay"))
            if trade_date is None:
                logger.warning(f"Bỏ qua row thiếu ngày: {row.to_dict()}")
                continue

            # Tách ThayDoi
            price_change, price_change_pct = parse_thay_doi(row.get("ThayDoi"))

            # Build event
            event = {
                "symbol": symbol,
                "trade_date": trade_date,
                "adjusted_close": convert_vn_number(row.get("GiaDieuChinh")),
                "close_price": convert_vn_number(row.get("GiaDongCua")),
                "price_change": price_change,
                "price_change_pct": price_change_pct,
                "matched_volume": int(row.get("KhoiLuongKhopLenh", 0) or 0),
                "matched_value": int(row.get("GiaTriKhopLenh", 0) or 0),
                "negotiated_volume": int(row.get("KLThoaThuan", 0) or 0),
                "negotiated_value": int(row.get("GtThoaThuan", 0) or 0),
                "open_price": convert_vn_number(row.get("GiaMoCua")),
                "high_price": convert_vn_number(row.get("GiaCaoNhat")),
                "low_price": convert_vn_number(row.get("GiaThapNhat")),
                "source": "cafef_api",
                "ingest_time": ingest_time,
                "event_id": f"{symbol}_{trade_date}",
            }
            events.append(event)

        except Exception as e:
            logger.error(f"Lỗi xử lý row: {e} | row={row.to_dict()}")
            continue

    return events


# =============================================================================
# Kafka Producer — với retry logic
# =============================================================================

def create_kafka_producer(max_retries: int = 10, retry_delay: int = 5) -> KafkaProducer:
    """
    Tạo Kafka producer với retry khi broker chưa sẵn sàng.
    """
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",               # đảm bảo message được ghi
                retries=3,                 # retry nếu gửi thất bại
                max_block_ms=30000,        # timeout khi buffer đầy
            )
            logger.info(f"Kết nối Kafka thành công (attempt {attempt})")
            return producer
        except NoBrokersAvailable:
            logger.warning(
                f"Kafka chưa sẵn sàng (attempt {attempt}/{max_retries}), "
                f"đợi {retry_delay}s..."
            )
            time.sleep(retry_delay)

    raise RuntimeError(f"Không thể kết nối Kafka sau {max_retries} lần thử")


def send_events_to_kafka(producer: KafkaProducer, topic: str, events: list[dict]) -> int:
    """
    Gửi list events vào Kafka topic. Trả về số message gửi thành công.
    """
    success_count = 0

    for event in events:
        try:
            # Dùng event_id làm key → đảm bảo cùng symbol vào cùng partition
            key = event.get("event_id", "")
            future = producer.send(topic, key=key, value=event)
            future.get(timeout=10)  # blocking để đảm bảo message đã gửi
            success_count += 1

            # Delay nhỏ giữa các message (tránh flood)
            if SEND_DELAY_MS > 0:
                time.sleep(SEND_DELAY_MS / 1000.0)

        except Exception as e:
            logger.error(f"Lỗi gửi message: {e} | event_id={event.get('event_id')}")

    return success_count


# =============================================================================
# Main — Luồng chạy chính
# =============================================================================

def run_once(producer: KafkaProducer, session: requests.Session, symbols: list[str]) -> int:
    """
    Thực hiện 1 vòng cào dữ liệu cho tất cả symbols.
    Trả về tổng số message đã gửi.
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    logger.info(
        f"Khoảng thời gian: {start_date.strftime('%m/%d/%Y')} "
        f"→ {end_date.strftime('%m/%d/%Y')} ({LOOKBACK_DAYS} ngày)"
    )

    total_sent = 0
    total_companies = 0

    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"--- [{idx}/{len(symbols)}] Đang xử lý: {symbol} ---")

        try:
            # Fetch dữ liệu JSON từ CafeF (không cần buffer file)
            df = fetch_cafef_json(symbol, session, start_date, end_date)
            if df.empty:
                logger.warning(f"  Không có dữ liệu cho {symbol} — bỏ qua")
                continue

            logger.info(f"  Fetch được {len(df)} rows từ CafeF")

            # Kiểm tra cột tối thiểu
            required_cols = ["Ngay", "GiaDieuChinh", "GiaDongCua"]
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                logger.warning(f"  Thiếu cột: {missing} — bỏ qua")
                logger.debug(f"  Các cột có: {list(df.columns)}")
                continue

            # Normalize
            events = normalize_stock_prices(df, symbol)
            logger.info(f"  Normalize xong: {len(events)} events")

            # Gửi vào Kafka
            sent = send_events_to_kafka(producer, KAFKA_TOPIC, events)
            total_sent += sent
            total_companies += 1
            logger.info(f"  Gửi thành công: {sent}/{len(events)} messages")

        except Exception as e:
            logger.error(f"  Lỗi xử lý {symbol}: {e}")
            continue

        # Delay giữa các request tới CafeF
        if API_DELAY_MS > 0 and idx < len(symbols):
            time.sleep(API_DELAY_MS / 1000.0)

    logger.info(f"Vòng cào xong: {total_companies} companies, {total_sent} messages")
    return total_sent


def main():
    logger.info("=" * 60)
    logger.info("STOCK PRICES DAILY — INGEST SERVICE")
    logger.info("=" * 60)
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Topic: {KAFKA_TOPIC}")
    logger.info(f"Data source: CafeF JSON API ({CAFEF_BASE_URL})")
    logger.info(f"Company CSV: {COMPANY_CSV}")
    logger.info(f"Num companies: {NUM_COMPANIES}")
    logger.info(f"Lookback: {LOOKBACK_DAYS} ngày")
    logger.info(
        f"Scrape interval: "
        f"{SCRAPE_INTERVAL_SECONDS}s"
        f"{' (chạy liên tục)' if SCRAPE_INTERVAL_SECONDS > 0 else ' (chạy 1 lần)'}"
    )

    # 1. Đọc danh sách mã chứng khoán từ CSV
    symbols = load_companies_from_csv(COMPANY_CSV, NUM_COMPANIES)
    if not symbols:
        logger.error(f"Không tìm thấy symbol nào trong {COMPANY_CSV}")
        return

    # 2. Tạo Kafka producer
    producer = create_kafka_producer()

    # 3. Tạo HTTP session (reuse connections)
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; StockIngest/1.0)",
    })

    # 4. Vòng lặp chính
    try:
        round_num = 0
        while True:
            round_num += 1
            logger.info(f"{'=' * 60}")
            logger.info(f"BẮT ĐẦU VÒNG CÀO #{round_num}")
            logger.info(f"{'=' * 60}")

            run_once(producer, session, symbols)

            if SCRAPE_INTERVAL_SECONDS <= 0:
                logger.info("SCRAPE_INTERVAL_SECONDS=0 → chạy 1 lần, thoát.")
                break

            logger.info(
                f"Đợi {SCRAPE_INTERVAL_SECONDS}s "
                f"({SCRAPE_INTERVAL_SECONDS / 3600:.1f}h) trước vòng tiếp..."
            )
            time.sleep(SCRAPE_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Nhận tín hiệu dừng (Ctrl+C)...")
    finally:
        session.close()
        producer.flush()
        producer.close()
        logger.info("Đã đóng Kafka producer và HTTP session.")


if __name__ == "__main__":
    main()
