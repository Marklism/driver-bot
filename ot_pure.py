
from datetime import datetime, timedelta
from typing import List, Tuple, Optional

# Return rows: (start_dt, end_dt, morning_hours, evening_hours, rate, note)

def calc_weekday_ot_pure(
    in_dt: Optional[datetime],
    out_dt: Optional[datetime],
) -> List[Tuple[datetime, datetime, float, float, str, str]]:
    rows = []
    if not in_dt and not out_dt:
        return rows

    # Morning OT: IN between 04:00 and 07:00 -> 08:00 - IN
    if in_dt:
        t4 = in_dt.replace(hour=4, minute=0, second=0, microsecond=0)
        t7 = in_dt.replace(hour=7, minute=0, second=0, microsecond=0)
        t8 = in_dt.replace(hour=8, minute=0, second=0, microsecond=0)
        if t4 < in_dt < t7:
            h = (t8 - in_dt).total_seconds() / 3600
            if h > 0:
                rows.append((in_dt, t8, round(h, 2), 0.0, "150%", "Weekday morning OT"))

    # Evening OT: OUT >= 18:30, independent of morning
    if out_dt:
        start18 = out_dt.replace(hour=18, minute=0, second=0, microsecond=0)
        if out_dt.hour > 18 or (out_dt.hour == 18 and out_dt.minute >= 30):
            # same day
            if out_dt.date() == start18.date():
                h = (out_dt - start18).total_seconds() / 3600
                if h > 0:
                    rows.append((start18, out_dt, 0.0, round(h, 2), "150%", "Weekday evening OT"))
            else:
                # cross day split
                end2359 = start18.replace(hour=23, minute=59, second=59)
                h1 = (end2359 - start18).total_seconds() / 3600
                if h1 > 0:
                    rows.append((start18, end2359, 0.0, round(h1, 2), "150%", "Weekday evening OT (before midnight)"))
                start0000 = out_dt.replace(hour=0, minute=0, second=0, microsecond=0)
                h2 = (out_dt - start0000).total_seconds() / 3600
                if h2 > 0:
                    rows.append((start0000, out_dt, 0.0, round(h2, 2), "150%", "Weekday evening OT (after midnight)"))
    return rows


def calc_weekend_or_holiday_ot_pure(
    in_dt: Optional[datetime],
    out_dt: Optional[datetime],
) -> List[Tuple[datetime, datetime, float, float, str, str]]:
    rows = []
    if not in_dt or not out_dt:
        return rows
    if out_dt < in_dt:
        out_dt = out_dt + timedelta(days=1)
    h = (out_dt - in_dt).total_seconds() / 3600
    if h > 0:
        rows.append((in_dt, out_dt, 0.0, round(h, 2), "200%", "Weekend/Holiday full-shift OT"))
    return rows
