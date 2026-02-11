#!/usr/bin/env python3
"""Watchdog: scheduler heartbeat monitor.

Checks data/heartbeat mtime and sends Telegram alert if stale (>35 min).
Runs as independent launchd job (10 min interval).
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

HEARTBEAT_PATH = Path(__file__).resolve().parent.parent / "data" / "heartbeat"
ALERT_FLAG_PATH = Path(__file__).resolve().parent.parent / "data" / ".watchdog_alerted"
STALE_THRESHOLD_SEC = 35 * 60  # 35分


def main() -> None:
    if not HEARTBEAT_PATH.exists():
        # 初回起動時やハートビート未生成 — アラートしない
        print("No heartbeat file found, skipping check")
        return

    age_sec = time.time() - HEARTBEAT_PATH.stat().st_mtime
    age_min = age_sec / 60

    if age_sec > STALE_THRESHOLD_SEC:
        # ハートビートが古い → アラート
        if ALERT_FLAG_PATH.exists():
            print(f"Heartbeat stale ({age_min:.0f}min) — already alerted, skipping")
            return

        print(f"Heartbeat stale ({age_min:.0f}min) — sending alert")
        try:
            from src.notifications.telegram import send_message

            send_message(
                f"*Scheduler Watchdog Alert*\n"
                f"Heartbeat stale: {age_min:.0f} min since last tick\n"
                f"Check scheduler logs and launchd status"
            )
        except Exception as e:
            print(f"Failed to send alert: {e}")

        # フラグファイル作成 (連続送信防止)
        ALERT_FLAG_PATH.write_text(f"alerted at age={age_min:.0f}min\n")
    else:
        # ハートビート正常
        if ALERT_FLAG_PATH.exists():
            # 復旧通知
            print(f"Heartbeat recovered ({age_min:.0f}min) — sending recovery notice")
            try:
                from src.notifications.telegram import send_message

                send_message(
                    f"*Scheduler Recovered*\n"
                    f"Heartbeat age: {age_min:.0f} min — back to normal"
                )
            except Exception as e:
                print(f"Failed to send recovery notice: {e}")

            ALERT_FLAG_PATH.unlink(missing_ok=True)
        else:
            print(f"Heartbeat OK ({age_min:.0f}min)")


if __name__ == "__main__":
    main()
