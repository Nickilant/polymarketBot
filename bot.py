from __future__ import annotations

import asyncio
import html
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from telegram import LabeledPrice, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    PreCheckoutQueryHandler,
    filters,
)

from polymarket_bot.analyzer import Analyzer
from polymarket_bot.config import Settings, load_settings
from polymarket_bot.models import InsiderSignal, MarketView, ProbabilitySignal
from polymarket_bot.polymarket_client import PolymarketClient
from polymarket_bot.subscriptions import FREE_PLAN, PRO_PLAN, SubscriptionStore, UserSubscription, VALID_MODES
from polymarket_bot.telegram_sender import TelegramSender
from polymarket_bot.translator import RuTranslator


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("polymarket-telegram-bot")

INSIDER_CHECK_INTERVAL_SECONDS = 600
PRO_PRICE_STARS = 150
PRO_INVOICE_PAYLOAD_PREFIX = "pro150"


def format_insider_message(signals: list[InsiderSignal]) -> str:
    lines = ["🕵️ Инсайдерская торговля (топ-3):"]
    for idx, signal in enumerate(signals, start=1):
        price = max(0.01, min(0.99, signal.price))
        profit = (1.0 / price) - 1.0
        lines.append(
            (
                f"{idx}) {signal.market_name_ru}\n"
                f"Кошелёк: {signal.wallet} | Исход: {signal.outcome}\n"
                f"Объём: ${signal.amount_usd:,.0f} | Профит с $1: ${profit:.2f}"
            )
        )
    return "\n\n".join(lines)


def _format_link(signal: ProbabilitySignal) -> str:
    safe_url = html.escape(signal.market_url, quote=True)
    return f"<a href=\"{safe_url}\">ссылка на маркет</a>" if signal.market_url else "ссылка на маркет: недоступна"


def format_probability_message(signals: list[ProbabilitySignal]) -> str:
    if not signals:
        return "📈 Высокая вероятность: подходящих рынков сейчас нет."

    lines = ["📈 Высокая вероятность (топ-10):"]
    for idx, signal in enumerate(signals, start=1):
        safe_name = html.escape(signal.market_name_ru)
        safe_outcome = html.escape(signal.leading_outcome)
        lines.append(
            (
                f"{idx}) {safe_name}\n"
                f"Лидер: {safe_outcome} ({signal.leading_probability * 100:.1f}%)"
                f" | Отрыв: {signal.gap * 100:.1f}%\n"
                f"Профит с $1: ${signal.win_if_1_dollar:.2f}\n"
                f"{_format_link(signal)}"
            )
        )
    return "\n\n".join(lines)


def format_hot_message(signals: list[ProbabilitySignal]) -> str:
    if not signals:
        return "🔥 Горячие ставки: подходящих рынков сейчас нет."

    lines = ["🔥 Горячие ставки (закрытие в ближайшие 5 дней):"]
    for idx, signal in enumerate(signals, start=1):
        safe_name = html.escape(signal.market_name_ru)
        safe_outcome = html.escape(signal.leading_outcome)
        lines.append(
            (
                f"{idx}) {safe_name}\n"
                f"Лидер: {safe_outcome} ({signal.leading_probability * 100:.1f}%)"
                f" | Отрыв: {signal.gap * 100:.1f}%\n"
                f"Профит с $1: ${signal.win_if_1_dollar:.2f}\n"
                f"{_format_link(signal)}"
            )
        )
    return "\n\n".join(lines)


def welcome_text() -> str:
    return (
        "Привет! Я бот с сигналами Polymarket.\n\n"
        "Что отправляю:\n"
        "• Инсайдерские сделки — крупные ставки от одного кошелька (по мере обнаружения).\n"
        "• Высокая вероятность — ставки с высокой вероятностью выигрыша (2 раза в день).\n"
        "• Горячие ставки — ставки с высокой вероятностю выигрыша и близким закрытием.\n\n"
        "Тарифы:\n"
        "• Free: только ставки высокой вероятности раз в 2 дня\n"
        "• Pro: полный комплект информации\n"
        "Купить Pro : /buy (Telegram Stars).\n"
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
        self._register_handlers()

    def _register_handlers(self) -> None:
        self.application.add_handler(CommandHandler("start", self.cmd_start))
        self.application.add_handler(CommandHandler("help", self.cmd_start))
        self.application.add_handler(CommandHandler("mode", self.cmd_mode))
        self.application.add_handler(CommandHandler("my", self.cmd_my))
        self.application.add_handler(CommandHandler("buy", self.cmd_buy))
        self.application.add_handler(CommandHandler("stop", self.cmd_stop))
        self.application.add_handler(CommandHandler("grant", self.cmd_grant))
        self.application.add_handler(CommandHandler("analysis", self.cmd_analysis))
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
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Тариф: {plan}\nРежим: {sub.mode}\nPro до: {paid_until}")

    async def cmd_buy(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat:
            return

        title = "Подписка Pro150 на 30 дней"
        description = "Сигналы Pro: высокая вероятность 2 раза в день, hot раз в 3 часа (при изменениях), инсайдеры каждые 10 минут при новых сделках."
        payload = f"{PRO_INVOICE_PAYLOAD_PREFIX}:{update.effective_user.id}:{int(datetime.now(timezone.utc).timestamp())}"

        await context.bot.send_invoice(
            chat_id=update.effective_chat.id,
            title=title,
            description=description,
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
        await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Выдан Pro150 пользователю {user_id} до {sub.paid_until.isoformat()}")

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

        await context.bot.send_message(chat_id=update.effective_chat.id, text=(
            "🧾 Последние данные анализа\n"
            f"Время UTC: {payload.get('generated_at', '-')}\n"
            f"Рынков: {payload.get('markets_count', 0)}\n"
            f"Инсайдерских сигналов: {payload.get('insider_signals_count', 0)}\n"
            f"Вероятностных сигналов: {payload.get('probability_signals_count', 0)}\n"
            f"Горячих сигналов: {payload.get('hot_signals_count', 0)}"
        ))

    async def analysis_loop(self) -> None:
        logger.info("Запущен цикл анализа. Интервал проверки инсайдеров: %s сек.", INSIDER_CHECK_INTERVAL_SECONDS)
        while True:
            now = datetime.now(timezone.utc)
            try:
                await self._maybe_send_admin_heartbeat(now)
                await self._run_cycle(now)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Ошибка в цикле анализа: %s", exc)
            await asyncio.sleep(INSIDER_CHECK_INTERVAL_SECONDS)

    async def _maybe_send_admin_heartbeat(self, now: datetime) -> None:
        if self._last_admin_heartbeat_sent and (now - self._last_admin_heartbeat_sent).total_seconds() < 3600:
            return
        heartbeat_text = (
            "🫀 Служебный пинг бота\n"
            f"Время UTC: {now.isoformat(timespec='seconds')}\n"
            "Статус: бот запущен и выполняет цикл анализа."
        )
        await self.sender.send_to(self.settings.admin_chat_id, heartbeat_text)
        self._last_admin_heartbeat_sent = now

    def _analysis_enabled(self, option: str) -> bool:
        return self.settings.analysis_mode == "both" or self.settings.analysis_mode == option

    def _hot_signature(self, signals: list[ProbabilitySignal]) -> str:
        payload = [(s.market_id, s.leading_outcome, round(s.leading_probability, 4), round(s.gap, 4)) for s in signals]
        return json.dumps(payload, ensure_ascii=False)

    def _insider_signature(self, signals: list[InsiderSignal]) -> str:
        payload = [
            (s.market_id, s.wallet, s.outcome, round(s.amount_usd, 2), round(s.price, 4))
            for s in signals
        ]
        return json.dumps(payload, ensure_ascii=False)

    def _is_due(self, sub: UserSubscription, label: str, now: datetime) -> bool:
        if sub.effective_plan() == FREE_PLAN:
            if label != "probability":
                return False
            last = sub.last_sent_probability_at
            return last is None or now - last >= timedelta(days=2)

        if label == "insider":
            last = sub.last_sent_insider_at
            interval = timedelta(hours=1)
        elif label == "probability":
            last = sub.last_sent_probability_at
            interval = timedelta(hours=12)
        elif label == "hot":
            last = sub.last_sent_hot_at
            interval = timedelta(hours=3)
        else:
            return False
        return last is None or now - last >= interval

    def _mode_allows_label(self, sub: UserSubscription, label: str) -> bool:
        if sub.effective_plan() == FREE_PLAN:
            return label == "probability"
        return sub.mode in {label, "both"}

    async def _run_cycle(self, now: datetime) -> None:
        markets = await self.client.fetch_markets()
        insider_signals: list[InsiderSignal] = []
        probability_signals: list[ProbabilitySignal] = []
        hot_signals: list[ProbabilitySignal] = []

        if self._analysis_enabled("insider"):
            trades = await self.client.fetch_recent_trades()
            insider_signals = await asyncio.to_thread(self.analyzer.insider_signals, trades, markets, self.settings.insider_top_n)

        if self._analysis_enabled("probability"):
            probability_signals = await asyncio.to_thread(self.analyzer.probability_signals, markets, self.settings.probability_top_n)

        if self._analysis_enabled("hot"):
            hot_signals = await asyncio.to_thread(self.analyzer.hot_signals, markets, self.settings.hot_top_n)

        previous_snapshot = await self._load_analysis_snapshot() or {}
        previous_hot_signature = str(previous_snapshot.get("hot_signature") or "")
        previous_insider_signature = str(previous_snapshot.get("insider_signature") or "")
        hot_signature = self._hot_signature(hot_signals)
        insider_signature = self._insider_signature(insider_signals)
        hot_changed = hot_signature != previous_hot_signature
        insider_changed = insider_signature != previous_insider_signature

        await self._save_analysis_snapshot(
            now,
            markets,
            insider_signals,
            probability_signals,
            hot_signals,
            hot_signature,
            insider_signature,
        )

        messages = {
            "insider": format_insider_message(insider_signals) if insider_signals else "",
            "probability": format_probability_message(probability_signals),
            "hot": format_hot_message(hot_signals),
        }

        for user_id, days, paid_until in self.store.due_renewal_reminders(now):
            reminder_text = (
                f"⏳ До окончания Pro150 осталось {days} дн.\n"
                f"Продли подписку (150 ⭐) в @PremiumBot.\n"
                f"Текущий срок до: {paid_until.date().isoformat()}"
            )
            await self.sender.send_to(user_id, reminder_text)
            self.store.mark_reminder_sent(user_id, days, paid_until)

        sent_users: set[int] = set()
        for sub in self.store.active_users():
            for label in ("insider", "probability", "hot"):
                if not self._analysis_enabled(label):
                    continue
                if not self._mode_allows_label(sub, label):
                    continue
                if not self._is_due(sub, label, now):
                    continue
                if label == "insider" and (not insider_signals or not insider_changed):
                    continue
                if label == "hot" and not hot_changed:
                    continue

                parse_mode = "HTML" if label in {"probability", "hot"} else None
                await self.sender.send_to(sub.user_id, messages[label], parse_mode=parse_mode)
                self.store.mark_sent(sub.user_id, label, now)
                sent_users.add(sub.user_id)

        logger.info("Цикл завершён. Пользователей, получивших рассылку: %s", len(sent_users))

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
            "insider_signals": [signal.__dict__ for signal in insider_signals],
            "probability_signals": [signal.__dict__ for signal in probability_signals],
            "hot_signals": [signal.__dict__ for signal in hot_signals],
        }
        async with self._analysis_lock:
            self._analysis_cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    async def _load_analysis_snapshot(self) -> dict[str, Any] | None:
        async with self._analysis_lock:
            if not self._analysis_cache_path.exists():
                return None
            try:
                return json.loads(self._analysis_cache_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                return None

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
        startup_text = (
            "✅ Бот запущен и готов к работе.\n\n"
            f"Режим анализа: {self.settings.analysis_mode}\n"
            f"Интервал цикла: {self.settings.polling_interval_seconds} сек.\n\n"
            "Справка по командам:\n"
            f"{welcome_text()}\n\n"
            "Админ-команда: /analysis — показать последние данные анализа."
        )
        await self.sender.send_to(self.settings.admin_chat_id, startup_text)


async def main() -> None:
    service = BotService(load_settings())
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
