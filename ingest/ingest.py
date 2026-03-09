"""
=============================================================================
Stock Prices Daily — Ingest Service
=============================================================================
Chức năng:
  1. Đọc file Excel (.xlsx) từ thư mục raw data (mounted volume)
  2. Chuẩn hóa (normalize) schema tiếng Việt → schema chuẩn tiếng Anh
  3. Xử lý kiểu số Việt Nam (dấu phẩy → dấu chấm)
  4. Tách cột ThayDoi thành price_change + price_change_pct
  5. Thêm metadata: symbol, source, ingest_time, event_id
  6. Gửi từng record như 1 Kafka message (JSON) vào topic

Thiết kế: dễ mở rộng sang các topic khác bằng cách tạo thêm normalizer.
=============================================================================
"""

import os
import re
import json
import glob
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokerAvailable

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
DATA_DIR = os.getenv("DATA_DIR", "/data/stock_prices/data_raw")
MAX_FILES = int(os.getenv("MAX_FILES", "0"))        # 0 = tất cả
SEND_DELAY_MS = int(os.getenv("SEND_DELAY_MS", "10"))

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


def extract_symbol_from_filename(filepath: str) -> str:
    """
    Trích xuất mã chứng khoán từ tên file.
    Ví dụ:
      'ACB.xlsx' → 'ACB'
      'LichSuGia_BHC_03_03_2016_03_03_2026.xlsx' → 'BHC'
    """
    filename = Path(filepath).stem  # bỏ extension

    # Trường hợp đặc biệt: LichSuGia_SYMBOL_...
    lich_su_match = re.match(r"LichSuGia_([A-Z0-9]+)_", filename)
    if lich_su_match:
        return lich_su_match.group(1)

    # Trường hợp thông thường: tên file chính là symbol
    return filename.upper()


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
                "source": "sample_excel",
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
        except NoBrokerAvailable:
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

def main():
    logger.info("=" * 60)
    logger.info("STOCK PRICES DAILY — INGEST SERVICE")
    logger.info("=" * 60)
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Topic: {KAFKA_TOPIC}")
    logger.info(f"Data dir: {DATA_DIR}")
    logger.info(f"Max files: {MAX_FILES if MAX_FILES > 0 else 'ALL'}")

    # 1. Tìm tất cả file xlsx trong thư mục data
    xlsx_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.xlsx")))
    if not xlsx_files:
        logger.error(f"Không tìm thấy file .xlsx nào trong {DATA_DIR}")
        return

    logger.info(f"Tìm thấy {len(xlsx_files)} file xlsx")

    # Giới hạn số file nếu cần (để test nhanh)
    if MAX_FILES > 0:
        xlsx_files = xlsx_files[:MAX_FILES]
        logger.info(f"Giới hạn xử lý {MAX_FILES} file đầu tiên")

    # 2. Tạo Kafka producer
    producer = create_kafka_producer()

    # 3. Xử lý từng file
    total_sent = 0
    total_files = 0

    for filepath in xlsx_files:
        filename = os.path.basename(filepath)
        symbol = extract_symbol_from_filename(filepath)
        logger.info(f"--- Đang xử lý: {filename} (symbol={symbol}) ---")

        try:
            # Đọc file Excel
            df = pd.read_excel(filepath, engine="openpyxl")
            logger.info(f"  Đọc được {len(df)} rows")

            # Kiểm tra cột tối thiểu
            required_cols = ["Ngay", "GiaDieuChinh", "GiaDongCua"]
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                logger.warning(f"  Thiếu cột: {missing} — bỏ qua file")
                continue

            # Normalize
            events = normalize_stock_prices(df, symbol)
            logger.info(f"  Normalize xong: {len(events)} events")

            # Gửi vào Kafka
            sent = send_events_to_kafka(producer, KAFKA_TOPIC, events)
            total_sent += sent
            total_files += 1
            logger.info(f"  Gửi thành công: {sent}/{len(events)} messages")

        except Exception as e:
            logger.error(f"  Lỗi xử lý file {filename}: {e}")
            continue

    # 4. Flush và close producer
    producer.flush()
    producer.close()

    logger.info("=" * 60)
    logger.info(f"HOÀN TẤT: {total_files} files, {total_sent} messages")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
