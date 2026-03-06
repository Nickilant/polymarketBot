from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from polymarket_bot.analyzer import Analyzer
from polymarket_bot.config import Settings, load_settings
from polymarket_bot.polymarket_client import PolymarketClient
from polymarket_bot.subscriptions import PRO_PLAN, SubscriptionStore, VALID_MODES
from polymarket_bot.telegram_sender import TelegramSender
from polymarket_bot.translator import RuTranslator


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("polymarket-telegram-bot")


def format_insider_message(signals):
    if not signals:
        return "🕵️ Инсайдерские сделки: за последний период крупных сделок не найдено."

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


def format_probability_message(signals):
    if not signals:
        return "📈 Высокая вероятность: подходящих рынков сейчас нет."

    lines = ["📈 Высокая вероятность (топ-10):"]
    for idx, signal in enumerate(signals, start=1):
        lines.append(
            (
                f"{idx}) {signal.market_name_ru}\n"
                f"Лидер: {signal.leading_outcome} ({signal.leading_probability * 100:.1f}%)"
                f" | Отрыв: {signal.gap * 100:.1f}%\n"
                f"Профит с $1: ${signal.win_if_1_dollar:.2f}"
            )
        )
    return "\n\n".join(lines)


def welcome_text() -> str:
    return (
        "Привет! Я бот с сигналами Polymarket.\n\n"
        "Что отправляю:\n"
        "• Инсайдерские сделки (крупные входы кошельков).\n"
        "• Высоковероятные рынки (сильный отрыв лидера).\n"
        "• В каждом сигнале есть профит при ставке $1.\n\n"
        "Тарифы:\n"
        "• Free — активируется автоматически после /start, 1 раз в день.\n"
        "• Pro150 — 150 Telegram Stars за 30 дней, сигналы каждый час.\n\n"
        "Покупка Stars: @PremiumBot\n"
        "Команды: /mode insider|probability|both, /my, /buy, /stop"
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
        self._register_handlers()

    def _register_handlers(self) -> None:
        self.application.add_handler(CommandHandler("start", self.cmd_start))
        self.application.add_handler(CommandHandler("help", self.cmd_start))
        self.application.add_handler(CommandHandler("mode", self.cmd_mode))
        self.application.add_handler(CommandHandler("my", self.cmd_my))
        self.application.add_handler(CommandHandler("buy", self.cmd_buy))
        self.application.add_handler(CommandHandler("stop", self.cmd_stop))
        self.application.add_handler(CommandHandler("grant", self.cmd_grant))

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat:
            return
        self.store.ensure_free(update.effective_user.id)
        await context.bot.send_message(chat_id=update.effective_chat.id, text=welcome_text())

    async def cmd_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat:
            return
        if not context.args:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Используй: /mode insider|probability|both")
            return
        mode = context.args[0].strip().lower()
        if mode not in VALID_MODES:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Допустимо: insider, probability, both")
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
        if not update.effective_chat:
            return
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Тариф Pro150 = 150 ⭐ за 30 дней. Купи Stars в @PremiumBot и напиши админу для активации.",
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
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"Выдан Pro150 пользователю {user_id} до {sub.paid_until.isoformat()}",
        )
        try:
            await context.bot.send_message(chat_id=user_id, text="Тариф Pro150 активирован ✅")
        except Exception:
            logger.warning("Could not notify user %s", user_id)

    async def analysis_loop(self) -> None:
        while True:
            now = datetime.now(timezone.utc)
            try:
                await self._run_cycle(now)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Cycle failed: %s", exc)
            await asyncio.sleep(self.settings.polling_interval_seconds)

    async def _run_cycle(self, now: datetime) -> None:
        logger.info("Starting analysis cycle")
        markets = await self.client.fetch_markets()
        insider_signals = []
        probability_signals = []

        if self.settings.analysis_mode in {"insider", "both"}:
            trades = await self.client.fetch_recent_trades()
            insider_signals = self.analyzer.insider_signals(trades, markets, top_n=self.settings.insider_top_n)

        if self.settings.analysis_mode in {"probability", "both"}:
            probability_signals = self.analyzer.probability_signals(markets, top_n=self.settings.probability_top_n)

        insider_text = format_insider_message(insider_signals)
        probability_text = format_probability_message(probability_signals)

        # Admin always receives both reports every cycle.
        await self.sender.send_to(self.settings.admin_chat_id, insider_text)
        await self.sender.send_to(self.settings.admin_chat_id, probability_text)

        for user_id, days, paid_until in self.store.due_renewal_reminders(now):
            reminder_text = (
                f"⏳ До окончания Pro150 осталось {days} дн.\n"
                f"Продли подписку (150 ⭐) в @PremiumBot.\n"
                f"Текущий срок до: {paid_until.date().isoformat()}"
            )
            await self.sender.send_to(user_id, reminder_text)
            self.store.mark_reminder_sent(user_id, days, paid_until)

        for sub in self.store.active_users_due(now):
            messages: list[str] = []
            if sub.mode in {"insider", "both"} and self.settings.analysis_mode in {"insider", "both"}:
                messages.append(insider_text)
            if sub.mode in {"probability", "both"} and self.settings.analysis_mode in {"probability", "both"}:
                messages.append(probability_text)
            if not messages:
                continue
            for message in messages:
                await self.sender.send_to(sub.user_id, message)
            self.store.mark_sent(sub.user_id, now)

    async def run(self) -> None:
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        try:
            await self.analysis_loop()
        finally:
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()


async def main() -> None:
    service = BotService(load_settings())
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
