from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


FREE_PLAN = "free"
PRO_PLAN = "pro150"
VALID_MODES = {"insider", "probability", "hot", "both"}


@dataclass
class UserSubscription:
    user_id: int
    plan: str
    mode: str
    created_at: datetime
    last_sent_at: datetime | None = None
    last_sent_insider_at: datetime | None = None
    last_sent_probability_at: datetime | None = None
    last_sent_hot_at: datetime | None = None
    paid_until: datetime | None = None
    is_active: bool = True
    remind_7_for: str | None = None
    remind_3_for: str | None = None

    def effective_plan(self) -> str:
        if self.plan == PRO_PLAN and self.paid_until and self.paid_until > datetime.now(timezone.utc):
            return PRO_PLAN
        return FREE_PLAN


class SubscriptionStore:
    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id INTEGER PRIMARY KEY,
                plan TEXT NOT NULL,
                mode TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_sent_at TEXT,
                last_sent_insider_at TEXT,
                last_sent_probability_at TEXT,
                last_sent_hot_at TEXT,
                paid_until TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                remind_7_for TEXT,
                remind_3_for TEXT
            )
            """
        )
        self._ensure_columns()
        self._conn.commit()

    def _ensure_columns(self) -> None:
        columns = {
            row[1] for row in self._conn.execute("PRAGMA table_info(subscriptions)").fetchall()
        }
        if "remind_7_for" not in columns:
            self._conn.execute("ALTER TABLE subscriptions ADD COLUMN remind_7_for TEXT")
        if "remind_3_for" not in columns:
            self._conn.execute("ALTER TABLE subscriptions ADD COLUMN remind_3_for TEXT")
        if "last_sent_insider_at" not in columns:
            self._conn.execute("ALTER TABLE subscriptions ADD COLUMN last_sent_insider_at TEXT")
        if "last_sent_probability_at" not in columns:
            self._conn.execute("ALTER TABLE subscriptions ADD COLUMN last_sent_probability_at TEXT")
        if "last_sent_hot_at" not in columns:
            self._conn.execute("ALTER TABLE subscriptions ADD COLUMN last_sent_hot_at TEXT")

    def _row_to_sub(self, row: sqlite3.Row) -> UserSubscription:
        return UserSubscription(
            user_id=int(row["user_id"]),
            plan=str(row["plan"]),
            mode=str(row["mode"]),
            created_at=_parse_dt(str(row["created_at"])) or datetime.now(timezone.utc),
            last_sent_at=_parse_dt(row["last_sent_at"]),
            last_sent_insider_at=_parse_dt(row["last_sent_insider_at"]),
            last_sent_probability_at=_parse_dt(row["last_sent_probability_at"]),
            last_sent_hot_at=_parse_dt(row["last_sent_hot_at"]),
            paid_until=_parse_dt(row["paid_until"]),
            is_active=bool(row["is_active"]),
            remind_7_for=row["remind_7_for"],
            remind_3_for=row["remind_3_for"],
        )

    def get(self, user_id: int) -> UserSubscription | None:
        row = self._conn.execute(
            "SELECT * FROM subscriptions WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if not row:
            return None
        return self._row_to_sub(row)

    def ensure_free(self, user_id: int) -> UserSubscription:
        sub = self.get(user_id)
        if sub:
            self._conn.execute(
                "UPDATE subscriptions SET is_active = 1 WHERE user_id = ?",
                (user_id,),
            )
            self._conn.commit()
            return self.get(user_id)  # type: ignore[return-value]

        now = datetime.now(timezone.utc)
        self._conn.execute(
            """
            INSERT INTO subscriptions (
                user_id, plan, mode, created_at, last_sent_at,
                last_sent_insider_at, last_sent_probability_at, last_sent_hot_at,
                paid_until, is_active, remind_7_for, remind_3_for
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, FREE_PLAN, "both", _dump_dt(now), None, None, None, None, None, 1, None, None),
        )
        self._conn.commit()
        return self.get(user_id)  # type: ignore[return-value]

    def set_mode(self, user_id: int, mode: str) -> UserSubscription:
        if mode not in VALID_MODES:
            raise ValueError("mode must be insider/probability/hot/both")
        self.ensure_free(user_id)
        self._conn.execute(
            "UPDATE subscriptions SET mode = ? WHERE user_id = ?",
            (mode, user_id),
        )
        self._conn.commit()
        return self.get(user_id)  # type: ignore[return-value]

    def deactivate(self, user_id: int) -> None:
        self._conn.execute(
            "UPDATE subscriptions SET is_active = 0 WHERE user_id = ?",
            (user_id,),
        )
        self._conn.commit()

    def grant_pro(self, user_id: int) -> UserSubscription:
        sub = self.ensure_free(user_id)
        now = datetime.now(timezone.utc)
        base = sub.paid_until if sub.paid_until and sub.paid_until > now else now
        paid_until = base + timedelta(days=30)
        paid_until_iso = _dump_dt(paid_until)
        self._conn.execute(
            """
            UPDATE subscriptions
            SET plan = ?, paid_until = ?, is_active = 1, remind_7_for = NULL, remind_3_for = NULL
            WHERE user_id = ?
            """,
            (PRO_PLAN, paid_until_iso, user_id),
        )
        self._conn.commit()
        return self.get(user_id)  # type: ignore[return-value]

    def active_users(self) -> list[UserSubscription]:
        rows = self._conn.execute("SELECT * FROM subscriptions WHERE is_active = 1").fetchall()
        return [self._row_to_sub(row) for row in rows]

    def due_renewal_reminders(self, now: datetime) -> list[tuple[int, int, datetime]]:
        rows = self._conn.execute(
            "SELECT * FROM subscriptions WHERE is_active = 1 AND plan = ? AND paid_until IS NOT NULL",
            (PRO_PLAN,),
        ).fetchall()
        reminders: list[tuple[int, int, datetime]] = []
        for row in rows:
            sub = self._row_to_sub(row)
            if not sub.paid_until or sub.paid_until <= now:
                continue
            remaining = sub.paid_until - now
            paid_until_iso = _dump_dt(sub.paid_until)
            if remaining <= timedelta(days=7) and remaining > timedelta(days=6):
                if sub.remind_7_for != paid_until_iso:
                    reminders.append((sub.user_id, 7, sub.paid_until))
            if remaining <= timedelta(days=3) and remaining > timedelta(days=2):
                if sub.remind_3_for != paid_until_iso:
                    reminders.append((sub.user_id, 3, sub.paid_until))
        return reminders

    def mark_reminder_sent(self, user_id: int, days: int, paid_until: datetime) -> None:
        paid_until_iso = _dump_dt(paid_until)
        if days == 7:
            self._conn.execute(
                "UPDATE subscriptions SET remind_7_for = ? WHERE user_id = ?",
                (paid_until_iso, user_id),
            )
        elif days == 3:
            self._conn.execute(
                "UPDATE subscriptions SET remind_3_for = ? WHERE user_id = ?",
                (paid_until_iso, user_id),
            )
        self._conn.commit()

    def mark_sent(self, user_id: int, label: str, sent_at: datetime) -> None:
        column_map = {
            "insider": "last_sent_insider_at",
            "probability": "last_sent_probability_at",
            "hot": "last_sent_hot_at",
        }
        column = column_map.get(label)
        if not column:
            return
        self._conn.execute(
            f"UPDATE subscriptions SET {column} = ?, last_sent_at = ? WHERE user_id = ?",
            (_dump_dt(sent_at), _dump_dt(sent_at), user_id),
        )
        self._conn.commit()


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def _dump_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()
