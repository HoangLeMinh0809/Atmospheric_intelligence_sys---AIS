from ais_architecture_logic import select_asof_context, staleness_days, staleness_hours


def test_daily_and_hourly_context_selection_is_latest_asof_without_future_rows():
    base_time = "2026-05-31T14:30:00Z"
    era5 = [{"hour": item} for item in ["2026-05-31T12:00:00Z", "2026-05-31T13:00:00Z", "2026-05-31T15:00:00Z"]]
    hysplit = [{"hour": item} for item in ["2026-05-31T11:00:00Z", "2026-05-31T14:00:00Z", "2026-05-31T16:00:00Z"]]
    s5p = [{"date": item} for item in ["2026-05-29", "2026-05-30", "2026-06-01"]]
    maiac = [{"date": item} for item in ["2026-05-28", "2026-05-31", "2026-06-01"]]

    era5_row = select_asof_context(era5, "hour", base_time)
    hysplit_row = select_asof_context(hysplit, "hour", base_time)
    s5p_row = select_asof_context(s5p, "date", "2026-05-31T00:00:00Z")
    maiac_row = select_asof_context(maiac, "date", "2026-05-31T00:00:00Z")

    assert era5_row["hour"] == "2026-05-31T13:00:00Z"
    assert hysplit_row["hour"] == "2026-05-31T14:00:00Z"
    assert s5p_row["date"] == "2026-05-30"
    assert maiac_row["date"] == "2026-05-31"
    assert staleness_hours(base_time, era5_row["hour"]) == 1.5
    assert staleness_hours(base_time, hysplit_row["hour"]) == 0.5
    assert staleness_days("2026-05-31", s5p_row["date"]) == 1
    assert staleness_days("2026-05-31", maiac_row["date"]) == 0
