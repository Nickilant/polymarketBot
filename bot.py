from __future__ import annotations

import asyncio
import html
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from polymarket_bot.analyzer import Analyzer
from polymarket_bot.config import Settings, load_settings
from polymarket_bot.models import InsiderSignal, ProbabilitySignal
from polymarket_bot.polymarket_client import PolymarketClient
from polymarket_bot.subscriptions import PRO_PLAN, SubscriptionStore, VALID_MODES
from polymarket_bot.telegram_sender import TelegramSender
from polymarket_bot.translator import RuTranslator


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("polymarket-telegram-bot")


def format_insider_message(signals: list[InsiderSignal]) -> str:
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


def format_probability_message(signals: list[ProbabilitySignal]) -> str:
    if not signals:
        return "📈 Высокая вероятность: подходящих рынков сейчас нет."

    lines = ["📈 Высокая вероятность (топ-10):"]
    for idx, signal in enumerate(signals, start=1):
        safe_name = html.escape(signal.market_name_ru)
        safe_outcome = html.escape(signal.leading_outcome)
        safe_url = html.escape(signal.market_url, quote=True)
        link_line = f"<a href=\"{safe_url}\">ссылка на маркет</a>" if signal.market_url else "ссылка на маркет: недоступна"
        lines.append(
            (
                f"{idx}) {safe_name}\n"
                f"Лидер: {safe_outcome} ({signal.leading_probability * 100:.1f}%)"
                f" | Отрыв: {signal.gap * 100:.1f}%\n"
                f"Профит с $1: ${signal.win_if_1_dollar:.2f}\n"
                f"{link_line}"
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

        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                "🧾 Последние данные анализа\n"
                f"Время UTC: {payload.get('generated_at', '-') }\n"
                f"Рынков: {payload.get('markets_count', 0)}\n"
                f"Инсайдерских сигналов: {payload.get('insider_signals_count', 0)}\n"
                f"Вероятностных сигналов: {payload.get('probability_signals_count', 0)}"
            ),
        )
        if self.settings.analysis_mode in {"insider", "both"}:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=format_insider_message(self._restore_insider_signals(payload.get("insider_signals", []))),
            )
        if self.settings.analysis_mode in {"probability", "both"}:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=format_probability_message(self._restore_probability_signals(payload.get("probability_signals", []))),
                disable_web_page_preview=True,
                parse_mode="HTML",
            )

    async def analysis_loop(self) -> None:
        logger.info("Запущен цикл анализа. Интервал опроса: %s сек.", self.settings.polling_interval_seconds)
        while True:
            now = datetime.now(timezone.utc)
            try:
                await self._maybe_send_admin_heartbeat(now)
                await self._run_cycle(now)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Ошибка в цикле анализа: %s", exc)
            await asyncio.sleep(self.settings.polling_interval_seconds)

    async def _maybe_send_admin_heartbeat(self, now: datetime) -> None:
        if self._last_admin_heartbeat_sent and (now - self._last_admin_heartbeat_sent).total_seconds() < 3600:
            return

        heartbeat_text = (
            "🫀 Служебный пинг бота\n"
            f"Время UTC: {now.isoformat(timespec='seconds')}\n"
            "Статус: бот запущен и выполняет цикл анализа."
        )
        logger.info("Отправляю служебный часовой пинг админу %s", self.settings.admin_chat_id)
        await self.sender.send_to(self.settings.admin_chat_id, heartbeat_text)
        self._last_admin_heartbeat_sent = now

    async def _run_cycle(self, now: datetime) -> None:
        logger.info("Старт цикла анализа за %s", now.isoformat(timespec="seconds"))
        logger.info("Запрашиваю рынки Polymarket")
        markets = await self.client.fetch_markets()
        logger.info("Получено рынков: %s", len(markets))
        insider_signals: list[InsiderSignal] = []
        probability_signals: list[ProbabilitySignal] = []

        if self.settings.analysis_mode in {"insider", "both"}:
            logger.info("Режим включает инсайдерский анализ — загружаю последние сделки")
            trades = await self.client.fetch_recent_trades()
            insider_signals = self.analyzer.insider_signals(trades, markets, top_n=self.settings.insider_top_n)
            logger.info("Найдено инсайдерских сигналов: %s", len(insider_signals))

        if self.settings.analysis_mode in {"probability", "both"}:
            logger.info("Режим включает анализ вероятностей — строю сигналы")
            probability_signals = self.analyzer.probability_signals(markets, top_n=self.settings.probability_top_n)
            logger.info("Найдено сигналов высокой вероятности: %s", len(probability_signals))

        await self._save_analysis_snapshot(now, markets, insider_signals, probability_signals)

        insider_text = format_insider_message(insider_signals)
        probability_text = format_probability_message(probability_signals)

        sent_labels: set[tuple[int, str]] = set()

        logger.info("Отправляю оба отчёта админу %s", self.settings.admin_chat_id)
        await self._send_unique(self.settings.admin_chat_id, "insider", insider_text, sent_labels)
        await self._send_unique(self.settings.admin_chat_id, "probability", probability_text, sent_labels)

        reminder_count = 0
        for user_id, days, paid_until in self.store.due_renewal_reminders(now):
            reminder_text = (
                f"⏳ До окончания Pro150 осталось {days} дн.\n"
                f"Продли подписку (150 ⭐) в @PremiumBot.\n"
                f"Текущий срок до: {paid_until.date().isoformat()}"
            )
            await self.sender.send_to(user_id, reminder_text)
            self.store.mark_reminder_sent(user_id, days, paid_until)
            reminder_count += 1

        if reminder_count:
            logger.info("Отправлено напоминаний о продлении: %s", reminder_count)
        else:
            logger.info("Напоминаний о продлении в этом цикле нет")

        sent_users = 0
        for sub in self.store.active_users_due(now):
            if sub.user_id == self.settings.admin_chat_id:
                continue
            messages: list[tuple[str, str]] = []
            if sub.mode in {"insider", "both"} and self.settings.analysis_mode in {"insider", "both"}:
                messages.append(("insider", insider_text))
            if sub.mode in {"probability", "both"} and self.settings.analysis_mode in {"probability", "both"}:
                messages.append(("probability", probability_text))
            if not messages:
                continue
            for label, message in messages:
                await self._send_unique(sub.user_id, label, message, sent_labels)
            self.store.mark_sent(sub.user_id, now)
            sent_users += 1

        logger.info("Цикл завершён. Пользователей, получивших рассылку: %s", sent_users)

    async def _send_unique(self, chat_id: int, label: str, text: str, sent_labels: set[tuple[int, str]]) -> None:
        key = (chat_id, label)
        if key in sent_labels:
            return
        parse_mode = "HTML" if label == "probability" else None
        await self.sender.send_to(chat_id, text, parse_mode=parse_mode)
        sent_labels.add(key)

    async def _save_analysis_snapshot(
        self,
        now: datetime,
        markets: list[Any],
        insider_signals: list[InsiderSignal],
        probability_signals: list[ProbabilitySignal],
    ) -> None:
        payload = {
            "generated_at": now.isoformat(timespec="seconds"),
            "markets_count": len(markets),
            "insider_signals_count": len(insider_signals),
            "probability_signals_count": len(probability_signals),
            "insider_signals": [signal.__dict__ for signal in insider_signals],
            "probability_signals": [signal.__dict__ for signal in probability_signals],
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
                logger.warning("Повреждён файл кэша анализа: %s", self._analysis_cache_path)
                return None

    @staticmethod
    def _restore_insider_signals(rows: list[dict[str, Any]]) -> list[InsiderSignal]:
        return [InsiderSignal(**row) for row in rows if isinstance(row, dict)]

    @staticmethod
    def _restore_probability_signals(rows: list[dict[str, Any]]) -> list[ProbabilitySignal]:
        return [ProbabilitySignal(**row) for row in rows if isinstance(row, dict)]

    async def run(self) -> None:
        logger.info("Инициализирую Telegram-приложение")
        await self.application.initialize()
        logger.info("Запускаю Telegram-приложение")
        await self.application.start()
        logger.info("Включаю long polling")
        await self.application.updater.start_polling()
        await self._send_startup_message_to_admin()
        analysis_task = asyncio.create_task(self.analysis_loop(), name="analysis-loop")
        try:
            await asyncio.Event().wait()
        finally:
            analysis_task.cancel()
            await asyncio.gather(analysis_task, return_exceptions=True)
            logger.info("Останавливаю long polling")
            await self.application.updater.stop()
            logger.info("Останавливаю Telegram-приложение")
            await self.application.stop()
            logger.info("Завершаю Telegram-приложение")
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
        logger.info("Отправляю приветственное сообщение при старте админу %s", self.settings.admin_chat_id)
        await self.sender.send_to(self.settings.admin_chat_id, startup_text)


async def main() -> None:
    service = BotService(load_settings())
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
