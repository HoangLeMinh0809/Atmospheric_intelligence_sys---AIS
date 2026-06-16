# File nay: xu ly Sentinel-5P thanh bang satellite silver/summary cho Ha Noi.
"""Compatibility wrapper.

Keep this file for backward compatibility with older scripts that still reference
sentinel5p_streaming.py. The canonical processor now lives in
sentinel5p_summary_streaming.py.
"""

from sentinel5p_summary_streaming import main


if __name__ == "__main__":
    main()
