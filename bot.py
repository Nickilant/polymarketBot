from __future__ import annotations

import asyncio
from collections import Counter
import html
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from telegram import LabeledPrice, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    PreCheckoutQueryHandler,
    filters,
)

from polymarket_bot.analyzer import Analyzer, _rich_to_insider
from polymarket_bot.config import Settings, load_settings
from polymarket_bot.formatters import (
    format_rich_insider_message,
    format_insider_message,
    format_probability_message,
    format_hot_message,
)
from polymarket_bot.models import InsiderSignal, MarketView, ProbabilitySignal
from polymarket_bot.polymarket_client import PolymarketClient
from polymarket_bot.signals import RichSignal
from polymarket_bot.subscriptions import FREE_PLAN, PRO_PLAN, SubscriptionStore, UserSubscription, VALID_MODES
from polymarket_bot.telegram_sender import TelegramSender
from polymarket_bot.translator import RuTranslator


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("polymarket-telegram-bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

INSIDER_CHECK_INTERVAL_SECONDS = 60
INSIDER_ANALYSIS_INTERVAL = timedelta(minutes=1)
PROBABILITY_ANALYSIS_INTERVAL = timedelta(hours=12)
HOT_ANALYSIS_INTERVAL = timedelta(hours=1)
PRO_PRICE_STARS = 150
PRO_INVOICE_PAYLOAD_PREFIX = "pro150"
EKB_TZ = ZoneInfo("Asia/Yekaterinburg")
FREE_PROBABILITY_HOURS_EKB = (8,)
PRO_PROBABILITY_HOURS_EKB = (8, 19)


def welcome_text() -> str:
    return (
        "Привет! Я бот с сигналами Polymarket.\n\n"
        "Что отправляю:\n"
        "• Крупные ставки — крупные ставки от одного кошелька (каждую минуту).\n"
        "• Высокая вероятность — по расписанию: Free в 08:00, Pro/админ в 08:00 и 19:00 (Екатеринбург).\n"
        "• Горячие ставки — по одной лучшей ставке каждый час без повторов в течение суток.\n\n"
        "Тарифы:\n"
        "• Free: только ставки высокой вероятности каждый день в 08:00 (Екатеринбург)\n"
        "• Pro: полный комплект информации\n"
        "Купить Pro: /buy (Telegram Stars).\n"
        "Команды: /mode insider|probability|hot|both, /my, /buy, /stop"
    )


class BotService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = SubscriptionStore(settings.subscriptions_db)
        self.sender = TelegramSender(settings.telegram_token)
        self.client = PolymarketClient(settings.polymarket_markets_url, settings.polymarket_trades_url)
        self.analyzer = Analyzer(
            translator=RuTranslator(),
            insider_min_trade_usd=settings.insider_min_trade_usd,
            probability_gap_threshold=settings.probability_gap_threshold,
            probability_min_value=settings.probability_min_value,
        )
        self.application = Application.builder().token(settings.telegram_token).build()
        self._last_admin_heartbeat_sent: datetime | None = None
        self._analysis_cache_path = Path(settings.subscriptions_db).resolve().parent / "latest_analysis.json"
        self._analysis_lock = asyncio.Lock()

        self._last_insider_analysis_at: datetime | None = None
        self._last_probability_analysis_at: datetime | None = None
        self._last_hot_analysis_at: datetime | None = None

        # ── Кэши последних результатов — сохраняются между циклами ──────────
        self._last_probability_signals: list[ProbabilitySignal] = []
        # [FIX] RichSignal кэш: используется для отправки, сохраняется между циклами
        self._last_rich_insider_signals: list[RichSignal] = []
        # [FIX] InsiderSignal кэш для snapshot/signature — не теряется если цикл не отработал
        self._last_insider_signals: list[InsiderSignal] = []

        self._register_handlers()

    # ──────────────────────────────────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _format_ekb_time(value: datetime) -> str:
        return value.astimezone(EKB_TZ).strftime("%d.%m.%Y %H:%M")

    @staticmethod
    def _parse_iso_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _probability_hours_for_sub(self, sub: UserSubscription) -> tuple[int, ...]:
        if sub.user_id != self.settings.admin_chat_id and sub.effective_plan() == FREE_PLAN:
            return FREE_PROBABILITY_HOURS_EKB
        return PRO_PROBABILITY_HOURS_EKB

    @staticmethod
    def _slot_at(ekb_now: datetime, hour: int) -> datetime:
        return ekb_now.replace(hour=hour, minute=0, second=0, microsecond=0)

    def _is_probability_due(self, now: datetime, hours: tuple[int, ...], last_sent: datetime | None) -> bool:
        ekb_now = now.astimezone(EKB_TZ)
        last_ekb = last_sent.astimezone(EKB_TZ) if last_sent else None
        for hour in sorted(hours, reverse=True):
            slot = self._slot_at(ekb_now, hour)
            if ekb_now < slot:
                continue
            if last_ekb is None or last_ekb < slot:
                return True
            return False
        return False

    def _next_probability_send_at(self, now: datetime, hours: tuple[int, ...], last_sent: datetime | None) -> datetime:
        ekb_now = now.astimezone(EKB_TZ)
        last_ekb = last_sent.astimezone(EKB_TZ) if last_sent else None
        for hour in sorted(hours):
            slot = self._slot_at(ekb_now, hour)
            if ekb_now > slot:
                continue
            if last_ekb is None or last_ekb < slot:
                return slot.astimezone(timezone.utc)
        next_day = ekb_now + timedelta(days=1)
        return self._slot_at(next_day, min(hours)).astimezone(timezone.utc)

    def _next_probability_schedule_slot(self, now: datetime, hours: tuple[int, ...]) -> datetime:
        ekb_now = now.astimezone(EKB_TZ)
        for hour in sorted(hours):
            slot = self._slot_at(ekb_now, hour)
            if ekb_now <= slot:
                return slot.astimezone(timezone.utc)
        next_day = ekb_now + timedelta(days=1)
        return self._slot_at(next_day, min(hours)).astimezone(timezone.utc)

    def _analysis_enabled(self, option: str) -> bool:
        return self.settings.analysis_mode == "both" or self.settings.analysis_mode == option

    def _hot_signature(self, signals: list[ProbabilitySignal]) -> str:
        payload = [(s.market_id, s.leading_outcome, round(s.leading_probability, 4), round(s.gap, 4)) for s in signals]
        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def _hot_market_key(signal: ProbabilitySignal) -> str:
        base_name = signal.market_name_en or signal.market_name_ru or signal.market_id
        return " ".join(base_name.lower().split())

    def _insider_signature(self, signals: list[InsiderSignal]) -> str:
        payload = [
            (s.market_id, s.wallet, s.outcome, round(s.amount_usd, 2), round(s.price, 4))
            for s in signals
        ]
        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def _daily_hot_reset_time(now: datetime) -> datetime:
        return datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + timedelta(days=1)

    def _pick_next_hot_signal(self, hot_signals: list[ProbabilitySignal], now: datetime) -> ProbabilitySignal | None:
        if not hot_signals:
            return None
        sent_keys, reset_at = self.store.get_global_hot_progress()
        if reset_at is None or now >= reset_at:
            sent_keys = []
            reset_at = self._daily_hot_reset_time(now)
        sent_set = set(sent_keys)
        available = [s for s in hot_signals if self._hot_market_key(s) not in sent_set]
        if not available:
            sent_keys = []
            reset_at = self._daily_hot_reset_time(now)
            available = hot_signals
        selected = available[0]
        sent_keys.append(self._hot_market_key(selected))
        self.store.set_global_hot_progress(sent_keys, reset_at)
        return selected

    def _interval_for_label(self, sub: UserSubscription, label: str) -> timedelta | None:
        if sub.user_id == self.settings.admin_chat_id:
            if label == "insider":
                return INSIDER_ANALYSIS_INTERVAL
            if label == "probability":
                return PROBABILITY_ANALYSIS_INTERVAL
            if label == "hot":
                return HOT_ANALYSIS_INTERVAL
            return None
        if sub.effective_plan() == FREE_PLAN:
            if label == "probability":
                return timedelta(days=1)
            return None
        if label == "insider":
            return INSIDER_ANALYSIS_INTERVAL
        if label == "probability":
            return PROBABILITY_ANALYSIS_INTERVAL
        if label == "hot":
            return HOT_ANALYSIS_INTERVAL
        return None

    def _last_sent_for_label(self, sub: UserSubscription, label: str) -> datetime | None:
        if label == "insider":
            return sub.last_sent_insider_at
        if label == "probability":
            return sub.last_sent_probability_at
        if label == "hot":
            return sub.last_sent_hot_at
        return None

    def _next_due_for_label(self, sub: UserSubscription, label: str, now: datetime) -> str:
        if label == "probability":
            last_sent = self._last_sent_for_label(sub, label)
            hours = self._probability_hours_for_sub(sub)
            if self._is_probability_due(now, hours, last_sent):
                return self._format_ekb_time(now + timedelta(minutes=5))
            return self._format_ekb_time(self._next_probability_send_at(now, hours, last_sent))
        interval = self._interval_for_label(sub, label)
        if interval is None:
            return "недоступно для текущего тарифа/режима"
        analysis_due_at = self._next_analysis_due_at(label, now)
        last = self._last_sent_for_label(sub, label)
        if last is None:
            return self._format_ekb_time(analysis_due_at) if analysis_due_at else "сразу при следующем цикле"
        due_at = last + interval
        if analysis_due_at is not None and analysis_due_at > due_at:
            due_at = analysis_due_at
        if due_at <= now:
            return self._format_ekb_time(now + timedelta(minutes=5))
        return self._format_ekb_time(due_at)

    def _is_due(self, sub: UserSubscription, label: str, now: datetime) -> bool:
        if label == "probability":
            return self._is_probability_due(
                now,
                self._probability_hours_for_sub(sub),
                self._last_sent_for_label(sub, label),
            )
        interval = self._interval_for_label(sub, label)
        if interval is None:
            return False
        last = self._last_sent_for_label(sub, label)
        return last is None or now - last >= interval

    def _mode_allows_label(self, sub: UserSubscription, label: str) -> bool:
        if sub.user_id == self.settings.admin_chat_id:
            return True
        if sub.effective_plan() == FREE_PLAN:
            return label == "probability"
        return sub.mode in {label, "both"}

    def _is_analysis_due(self, label: str, now: datetime) -> bool:
        due_at = self._next_analysis_due_at(label, now)
        return due_at is not None and due_at <= now

    def _next_analysis_due_at(self, label: str, now: datetime) -> datetime | None:
        if label == "insider":
            last = self._last_insider_analysis_at
            interval = INSIDER_ANALYSIS_INTERVAL
        elif label == "probability":
            return now
        elif label == "hot":
            last = self._last_hot_analysis_at
            interval = HOT_ANALYSIS_INTERVAL
        else:
            return None
        if last is None:
            return now
        return last + interval

    def _is_probability_dispatch_due(self, now: datetime) -> bool:
        for sub in self.store.active_users():
            if not self._mode_allows_label(sub, "probability"):
                continue
            if self._is_due(sub, "probability", now):
                return True
        return False

    # ──────────────────────────────────────────────────────────────────────────
    # HANDLERS
    # ──────────────────────────────────────────────────────────────────────────

    def _register_handlers(self) -> None:
        self.application.add_handler(CommandHandler("start", self.cmd_start))
        self.application.add_handler(CommandHandler("help", self.cmd_start))
        self.application.add_handler(CommandHandler("mode", self.cmd_mode))
        self.application.add_handler(CommandHandler("my", self.cmd_my))
        self.application.add_handler(CommandHandler("buy", self.cmd_buy))
        self.application.add_handler(CommandHandler("stop", self.cmd_stop))
        self.application.add_handler(CommandHandler("grant", self.cmd_grant))
        self.application.add_handler(CommandHandler("analysis", self.cmd_analysis))
        self.application.add_handler(CommandHandler("next_analysis", self.cmd_next_analysis))
        self.application.add_handler(PreCheckoutQueryHandler(self.precheckout_callback))
        self.application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, self.successful_payment_callback))

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat:
            return
        self.store.ensure_free(update.effective_user.id)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=welcome_text())

    async def cmd_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat:
            return
        if not context.args:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Используй: /mode insider|probability|hot|both")
            return
        mode = context.args[0].strip().lower()
        if mode not in VALID_MODES:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Допустимо: insider, probability, hot, both")
            return
        self.store.set_mode(update.effective_user.id, mode)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Режим обновлён: {mode}")

    async def cmd_my(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat:
            return
        sub = self.store.ensure_free(update.effective_user.id)
        plan = "Pro150" if sub.effective_plan() == PRO_PLAN else "Free"
        paid_until = sub.paid_until.isoformat() if sub.paid_until else "-"
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"Тариф: {plan}\nРежим: {sub.mode}\nPro до: {paid_until}",
        )

    async def cmd_buy(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat:
            return
        payload = f"{PRO_INVOICE_PAYLOAD_PREFIX}:{update.effective_user.id}:{int(datetime.now(timezone.utc).timestamp())}"
        await context.bot.send_invoice(
            chat_id=update.effective_chat.id,
            title="Подписка Pro150 на 30 дней",
            description="Сигналы Pro: высокая вероятность в 08:00 и 19:00, крупные ставки каждую минуту, горячие ставки раз в час.",
            payload=payload,
            currency="XTR",
            prices=[LabeledPrice("Pro150", PRO_PRICE_STARS)],
            provider_token=self.settings.telegram_payments_provider_token,
            start_parameter="pro150-stars",
        )

    async def precheckout_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.pre_checkout_query:
            return
        query = update.pre_checkout_query
        if not query.invoice_payload.startswith(f"{PRO_INVOICE_PAYLOAD_PREFIX}:"):
            await query.answer(ok=False, error_message="Неверный payload платежа")
            return
        if query.currency != "XTR" or query.total_amount != PRO_PRICE_STARS:
            await query.answer(ok=False, error_message="Неверная сумма платежа")
            return
        await query.answer(ok=True)

    async def successful_payment_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message or not update.effective_user:
            return
        payment = update.message.successful_payment
        if payment is None:
            return
        if not payment.invoice_payload.startswith(f"{PRO_INVOICE_PAYLOAD_PREFIX}:"):
            logger.warning("Неизвестный payload успешной оплаты: %s", payment.invoice_payload)
            return
        if payment.currency != "XTR" or payment.total_amount != PRO_PRICE_STARS:
            logger.warning("Неожиданные параметры успешной оплаты: currency=%s amount=%s", payment.currency, payment.total_amount)
            return
        sub = self.store.grant_pro(update.effective_user.id)
        await update.message.reply_text(f"✅ Оплата получена! Pro150 активирован до {sub.paid_until.date().isoformat()}")
        await self.sender.send_to(
            self.settings.admin_chat_id,
            f"💳 Успешная оплата Stars: user_id={update.effective_user.id}, сумма={payment.total_amount} XTR",
        )

    async def cmd_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat:
            return
        self.store.deactivate(update.effective_user.id)
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Подписка остановлена. Вернуться: /start")

    async def cmd_grant(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat:
            return
        if update.effective_user.id != self.settings.admin_chat_id:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Команда только для админа")
            return
        if len(context.args) < 1:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Используй: /grant <user_id>")
            return
        try:
            user_id = int(context.args[0])
        except ValueError:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Неверный формат")
            return
        sub = self.store.grant_pro(user_id)
        paid_until_text = self._format_ekb_time(sub.paid_until) if sub.paid_until else "-"
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"Выдан Pro150 пользователю {user_id} до {paid_until_text} (Екатеринбург)",
        )

    async def cmd_analysis(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat:
            return
        if update.effective_user.id != self.settings.admin_chat_id:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Команда только для админа")
            return

        payload = await self._load_analysis_snapshot()
        if not payload:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Анализ ещё не запускался.")
            return

        chat_id = update.effective_chat.id
        generated_at = self._parse_iso_datetime(str(payload.get("generated_at") or ""))
        generated_at_text = self._format_ekb_time(generated_at) if generated_at else "-"
        await context.bot.send_message(chat_id=chat_id, text=(
            "🧾 Последние данные анализа\n"
            f"Время (Екатеринбург): {generated_at_text}\n"
            f"Рынков: {payload.get('markets_count', 0)}\n"
            f"Крупных ставок: {payload.get('insider_signals_count', 0)}\n"
            f"Вероятностных сигналов: {payload.get('probability_signals_count', 0)}\n"
            f"Горячих сигналов: {payload.get('hot_signals_count', 0)}"
        ))

        insider_signals = [InsiderSignal(**raw) for raw in payload.get("insider_signals", [])]
        probability_signals = [ProbabilitySignal(**raw) for raw in payload.get("probability_signals", [])]
        hot_signals = [ProbabilitySignal(**raw) for raw in payload.get("hot_signals", [])]

        # [FIX] Используем rich кэш если есть, иначе fallback на snapshot
        insider_text = (
            format_rich_insider_message(self._last_rich_insider_signals)
            if self._last_rich_insider_signals
            else format_insider_message(insider_signals)
        )
        await context.bot.send_message(chat_id=chat_id, text=insider_text, parse_mode="HTML")
        await context.bot.send_message(chat_id=chat_id, text=format_probability_message(probability_signals), parse_mode="HTML")
        await context.bot.send_message(chat_id=chat_id, text=format_hot_message(hot_signals), parse_mode="HTML")

    async def cmd_next_analysis(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat:
            return
        if update.effective_user.id != self.settings.admin_chat_id:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Команда только для админа")
            return

        now = datetime.now(timezone.utc)
        admin_sub = self.store.ensure_free(self.settings.admin_chat_id)
        active_users = self.store.active_users()
        pro_count = sum(1 for s in active_users if s.effective_plan() == PRO_PLAN)
        free_count = sum(1 for s in active_users if s.effective_plan() == FREE_PLAN)

        # Последние результаты из кэша
        rich = self._last_rich_insider_signals
        signal_breakdown = ""
        if rich:
            kinds = Counter(s.kind.value for s in rich)
            breakdown_lines = [f"  {kind}: {cnt}" for kind, cnt in kinds.most_common()]
            signal_breakdown = "\n".join(breakdown_lines)
        else:
            signal_breakdown = "  нет данных (анализ ещё не запускался)"

        lines = [
            "⏱ <b>Статус анализатора</b>",
            "",
            "👥 <b>Подписчики:</b>",
            f"  Всего активных: {len(active_users)}",
            f"  Pro: {pro_count}  |  Free: {free_count}",
            "",
            "🕐 <b>Следующая рассылка (Екатеринбург):</b>",
            f"  💰 Крупные ставки (Pro+админ): {self._next_due_for_label(admin_sub, 'insider', now)}",
            f"  📈 Вероятность (Pro+админ):    {self._next_due_for_label(admin_sub, 'probability', now)}",
            f"  📈 Вероятность (Free):         {self._format_ekb_time(self._next_probability_schedule_slot(now, FREE_PROBABILITY_HOURS_EKB))}",
            f"  🔥 Горячие (Pro+админ):        {self._next_due_for_label(admin_sub, 'hot', now)}",
            "",
            "📊 <b>Последний анализ инсайдеров:</b>",
            f"  Время: {self._format_ekb_time(self._last_insider_analysis_at) if self._last_insider_analysis_at else 'ещё не запускался'}",
            f"  Сигналов найдено: {len(rich)}",
            signal_breakdown,
            "",
            "📈 <b>Последний анализ вероятностей:</b>",
            f"  Время: {self._format_ekb_time(self._last_probability_analysis_at) if self._last_probability_analysis_at else 'ещё не запускался'}",
            f"  Сигналов: {len(self._last_probability_signals)}",
            "",
            "⚙️ <b>Режим анализа:</b> {mode}  |  Интервал цикла: {interval}с".format(
                mode=self.settings.analysis_mode,
                interval=INSIDER_CHECK_INTERVAL_SECONDS,
            ),
        ]
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="\n".join(lines),
            parse_mode="HTML",
        )

    # ──────────────────────────────────────────────────────────────────────────
    # MAIN LOOP
    # ──────────────────────────────────────────────────────────────────────────

    async def analysis_loop(self) -> None:
        logger.info("=" * 60)
        logger.info("Цикл анализа запущен. Интервал: %ds", INSIDER_CHECK_INTERVAL_SECONDS)
        logger.info("Режим анализа: %s", self.settings.analysis_mode)
        logger.info("Порог инсайдера: $%.0f", self.settings.insider_min_trade_usd)
        logger.info("=" * 60)
        while True:
            now = datetime.now(timezone.utc)
            try:
                await self._maybe_send_admin_heartbeat(now)
                await self._run_cycle(now)
            except Exception as exc:
                logger.exception("Ошибка в цикле анализа: %s", exc)
            await asyncio.sleep(INSIDER_CHECK_INTERVAL_SECONDS)

    async def _maybe_send_admin_heartbeat(self, now: datetime) -> None:
        if self._last_admin_heartbeat_sent and (now - self._last_admin_heartbeat_sent).total_seconds() < 3600:
            return
        active = self.store.active_users()
        heartbeat_text = (
            "🫀 <b>Служебный пинг бота</b>\n"
            f"Время (Екатеринбург): {self._format_ekb_time(now)}\n"
            f"Статус: бот запущен и выполняет цикл анализа.\n"
            f"Активных подписчиков: {len(active)}"
        )
        await self.sender.send_to(self.settings.admin_chat_id, heartbeat_text, parse_mode="HTML")
        self._last_admin_heartbeat_sent = now

    # ──────────────────────────────────────────────────────────────────────────
    # RUN CYCLE
    # ──────────────────────────────────────────────────────────────────────────

    async def _run_cycle(self, now: datetime) -> None:
        self.store.ensure_free(self.settings.admin_chat_id)
        cycle_ts = now.strftime("%H:%M:%S")

        insider_due = self._analysis_enabled("insider") and self._is_analysis_due("insider", now)
        probability_due = self._analysis_enabled("probability") and self._is_probability_dispatch_due(now)
        hot_due = self._analysis_enabled("hot") and self._is_analysis_due("hot", now)

        logger.info(
            "[%s] Цикл: insider_due=%s probability_due=%s hot_due=%s",
            cycle_ts, insider_due, probability_due, hot_due,
        )

        # [FIX] Используем кэшированные значения — не затираем если анализ не запускался
        insider_signals: list[InsiderSignal] = self._last_insider_signals
        probability_signals: list[ProbabilitySignal] = self._last_probability_signals
        hot_signals: list[ProbabilitySignal] = []

        # ── Анализ инсайдеров ─────────────────────────────────────────────────
        if insider_due:
            logger.info("[%s] → Запуск анализа инсайдеров...", cycle_ts)
            markets = await self.client.fetch_markets()
            trades = await self.client.fetch_recent_trades()
            logger.info("[%s]   Рынков: %d | Трейдов: %d", cycle_ts, len(markets), len(trades))

            rich_signals = await asyncio.to_thread(
                self.analyzer.rich_insider_signals, trades, markets, self.settings.insider_top_n
            )
            self._last_rich_insider_signals = rich_signals
            insider_signals = [_rich_to_insider(s) for s in rich_signals]
            self._last_insider_signals = insider_signals
            self._last_insider_analysis_at = now

            # Лог разбивки по типам
            if rich_signals:
                kinds = Counter(s.kind.value for s in rich_signals)
                breakdown = " | ".join(f"{k}:{v}" for k, v in kinds.most_common())
                strong = sum(1 for s in rich_signals if s.is_strong)
                logger.info(
                    "[%s]   Сигналов: %d (сильных: %d) | %s",
                    cycle_ts, len(rich_signals), strong, breakdown,
                )
            else:
                logger.info("[%s]   Сигналов не найдено", cycle_ts)
        else:
            # Если анализ не запускался — берём рынки только для probability/hot
            markets = None

        # ── Анализ вероятностей ───────────────────────────────────────────────
        if probability_due:
            logger.info("[%s] → Запуск анализа вероятностей...", cycle_ts)
            if markets is None:
                markets = await self.client.fetch_markets()
                logger.info("[%s]   Рынков: %d", cycle_ts, len(markets))
            probability_signals = await asyncio.to_thread(
                self.analyzer.probability_signals, markets, self.settings.probability_top_n
            )
            self._last_probability_signals = probability_signals
            self._last_probability_analysis_at = now
            logger.info("[%s]   Вероятностных сигналов: %d", cycle_ts, len(probability_signals))

        # ── Анализ горячих ────────────────────────────────────────────────────
        if hot_due:
            logger.info("[%s] → Запуск анализа горячих ставок...", cycle_ts)
            if markets is None:
                markets = await self.client.fetch_markets()
                logger.info("[%s]   Рынков: %d", cycle_ts, len(markets))
            hot_signals = await asyncio.to_thread(
                self.analyzer.hot_signals, markets, self.settings.hot_top_n
            )
            self._last_hot_analysis_at = now
            self._log_hot_signals_snapshot(hot_signals, now)

        # ── Snapshot ──────────────────────────────────────────────────────────
        previous_snapshot = await self._load_analysis_snapshot() or {}
        previous_hot_sig = str(previous_snapshot.get("hot_signature") or "")
        previous_insider_sig = str(previous_snapshot.get("insider_signature") or "")
        hot_signature = self._hot_signature(hot_signals) if hot_due else previous_hot_sig
        insider_signature = self._insider_signature(insider_signals) if insider_due else previous_insider_sig
        hot_changed = hot_due and hot_signature != previous_hot_sig
        insider_changed = insider_due and insider_signature != previous_insider_sig

        if insider_due or probability_due or hot_due:
            await self._save_analysis_snapshot(
                now,
                markets or [],
                insider_signals,
                probability_signals,
                hot_signals,
                hot_signature,
                insider_signature,
            )

        # ── Выбор горячего сигнала ────────────────────────────────────────────
        selected_hot_signal = self._pick_next_hot_signal(hot_signals, now) if hot_due else None

        # ── Формирование сообщений ────────────────────────────────────────────
        # [FIX] insider — всегда используем rich кэш если он есть
        insider_msg = (
            format_rich_insider_message(self._last_rich_insider_signals)
            if self._last_rich_insider_signals
            else format_insider_message(insider_signals)
        )
        messages = {
            "insider": insider_msg,
            "probability": format_probability_message(probability_signals),
            "hot": format_hot_message([selected_hot_signal]) if selected_hot_signal else "",
        }

        logger.info(
            "[%s] Сообщения сформированы | insider=%d chars | probability=%d chars | hot=%d chars",
            cycle_ts,
            len(messages["insider"]),
            len(messages["probability"]),
            len(messages["hot"]),
        )

        # ── Рассылка ──────────────────────────────────────────────────────────
        for user_id, days, paid_until in self.store.due_renewal_reminders(now):
            reminder_text = (
                f"⏳ До окончания Pro150 осталось {days} дн.\n"
                f"Продли подписку (150 ⭐) командой /buy.\n"
                f"Текущий срок до: {paid_until.date().isoformat()}"
            )
            await self.sender.send_to(user_id, reminder_text)
            self.store.mark_reminder_sent(user_id, days, paid_until)

        sent_users: set[int] = set()
        sent_by_label: dict[str, int] = {"insider": 0, "probability": 0, "hot": 0}

        for sub in self.store.active_users():
            for label in ("insider", "probability", "hot"):
                if not self._analysis_enabled(label):
                    continue
                if label == "insider" and not insider_due:
                    continue
                if label == "probability" and not probability_due:
                    continue
                if label == "hot" and not hot_due:
                    continue
                if not self._mode_allows_label(sub, label):
                    continue
                if label != "hot" and not self._is_due(sub, label, now):
                    continue
                if label == "insider" and not self._last_rich_insider_signals:
                    logger.debug("[%s] Пропуск insider для user=%d — нет сигналов", cycle_ts, sub.user_id)
                    continue
                if label == "hot" and not messages["hot"]:
                    continue

                parse_mode = "HTML"
                logger.debug(
                    "[%s] → Отправка [%s] → user_id=%d plan=%s",
                    cycle_ts, label, sub.user_id, sub.effective_plan(),
                )
                try:
                    await self.sender.send_to(sub.user_id, messages[label], parse_mode=parse_mode)
                    self.store.mark_sent(sub.user_id, label, now)
                    sent_users.add(sub.user_id)
                    sent_by_label[label] += 1
                except Exception as exc:
                    logger.error(
                        "[%s] Ошибка отправки [%s] → user_id=%d: %s",
                        cycle_ts, label, sub.user_id, exc,
                    )

        # ── Итог цикла ────────────────────────────────────────────────────────
        due_by_label = {"insider": insider_due, "probability": probability_due, "hot": hot_due}
        for label, count in sent_by_label.items():
            if due_by_label[label] and count > 0:
                await self._notify_admin_distribution(label, count, now)

        logger.info(
            "[%s] ✓ Цикл завершён | Отправлено: insider=%d probability=%d hot=%d | Уникальных получателей: %d",
            cycle_ts,
            sent_by_label["insider"],
            sent_by_label["probability"],
            sent_by_label["hot"],
            len(sent_users),
        )

    # ──────────────────────────────────────────────────────────────────────────
    # ADMIN NOTIFICATIONS
    # ──────────────────────────────────────────────────────────────────────────

    async def _notify_admin_distribution(self, label: str, sent_count: int, now: datetime) -> None:
        labels = {
            "insider": "крупных ставок",
            "probability": "высокой вероятности",
            "hot": "горячих ставок",
        }
        await self.sender.send_to(
            self.settings.admin_chat_id,
            (
                f"📣 Выполнена рассылка {labels.get(label, label)}\n"
                f"Время (Екатеринбург): {self._format_ekb_time(now)}\n"
                f"Получателей: {sent_count}"
            ),
        )
    def _log_hot_signals_snapshot(self, hot_signals: list[ProbabilitySignal], now: datetime) -> None:
        if not hot_signals:
            logger.info("Hot snapshot %s UTC: сигналов не найдено", now.strftime("%H:%M:%S"))
            return
        lines = [
            f"  {idx}. {s.market_name_ru} | p={s.leading_probability:.2f} | gap={s.gap:.2f}"
            for idx, s in enumerate(hot_signals, 1)
        ]
        logger.info(
            "Hot snapshot %s UTC (%d сигналов):\n%s",
            now.strftime("%H:%M:%S"), len(hot_signals), "\n".join(lines),
        )

    # ──────────────────────────────────────────────────────────────────────────
    # SNAPSHOT
    # ──────────────────────────────────────────────────────────────────────────

    async def _save_analysis_snapshot(
        self,
        now: datetime,
        markets: list[MarketView],
        insider_signals: list[InsiderSignal],
        probability_signals: list[ProbabilitySignal],
        hot_signals: list[ProbabilitySignal],
        hot_signature: str,
        insider_signature: str,
    ) -> None:
        payload = {
            "generated_at": now.isoformat(timespec="seconds"),
            "markets_count": len(markets),
            "insider_signals_count": len(insider_signals),
            "probability_signals_count": len(probability_signals),
            "hot_signals_count": len(hot_signals),
            "hot_signature": hot_signature,
            "insider_signature": insider_signature,
            "insider_signals": [s.__dict__ for s in insider_signals],
            "probability_signals": [s.__dict__ for s in probability_signals],
            "hot_signals": [s.__dict__ for s in hot_signals],
        }
        async with self._analysis_lock:
            self._analysis_cache_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
            )

    async def _load_analysis_snapshot(self) -> dict[str, Any] | None:
        async with self._analysis_lock:
            if not self._analysis_cache_path.exists():
                return None
            try:
                return json.loads(self._analysis_cache_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                return None

    # ──────────────────────────────────────────────────────────────────────────
    # STARTUP / RUN
    # ──────────────────────────────────────────────────────────────────────────

    async def run(self) -> None:
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling(drop_pending_updates=True)
        await self._send_startup_message_to_admin()
        analysis_task = asyncio.create_task(self.analysis_loop(), name="analysis-loop")
        try:
            await asyncio.Event().wait()
        finally:
            analysis_task.cancel()
            await asyncio.gather(analysis_task, return_exceptions=True)
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()

    async def _send_startup_message_to_admin(self) -> None:
        active = self.store.active_users()
        startup_text = (
            "✅ <b>Бот запущен и готов к работе.</b>\n\n"
            f"Режим анализа: <b>{self.settings.analysis_mode}</b>\n"
            f"Интервал цикла: {INSIDER_CHECK_INTERVAL_SECONDS}с\n"
            f"Порог сигнала: ${self.settings.insider_min_trade_usd:,.0f}\n"
            f"Активных подписчиков: {len(active)}\n\n"
            "Детекторы активны:\n"
            "• Whale Trade / Whale Position\n"
            "• Position Sweep / Stealth Accumulation\n"
            "• Coordinated / Clustered Coordination\n"
            "• Cross-Market Insider\n\n"
            "Админ-команды: /analysis /next_analysis /grant"
        )
        await self.sender.send_to(self.settings.admin_chat_id, startup_text, parse_mode="HTML")


async def main() -> None:
    service = BotService(load_settings())
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())