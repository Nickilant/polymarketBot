"""
formatters.py — красивые Telegram HTML-сообщения для каждого типа сигнала.

Использование в bot.py:
    from polymarket_bot.formatters import format_rich_insider_message, format_insider_message

format_rich_insider_message принимает list[RichSignal] и возвращает str (HTML).
format_insider_message оставлен для обратной совместимости.
"""
from __future__ import annotations

import html
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from polymarket_bot.signals import RichSignal
    from polymarket_bot.models import InsiderSignal, ProbabilitySignal

from polymarket_bot.signals import SignalKind


# ── Эмодзи и заголовки ────────────────────────────────────────────────────────

_KIND_HEADER: dict[SignalKind, str] = {
    SignalKind.WHALE_TRADE:           "🐋 Whale Trade",
    SignalKind.WHALE_POSITION:        "🐳 Whale Position",
    SignalKind.POSITION_SWEEP:        "⚡ Position Sweep",
    SignalKind.STEALTH_ACCUMULATION:  "🕵️ Stealth Accumulation",
    SignalKind.COORDINATED:           "🎯 Coordinated Trade",
    SignalKind.CLUSTERED_COORD:       "🔗 Clustered Coordination",
    SignalKind.CROSS_MARKET_INSIDER:  "🧠 Cross-Market Insider",
    SignalKind.LIQUIDITY_SWEEP:       "💥 Liquidity Sweep",
}

_SIDE_EMOJI: dict[str, str] = {
    "BUY": "🟢",
    "SELL": "🔴",
    "UNKNOWN": "⚪",
}

_STRENGTH_STARS = ["", "★", "★★", "★★★"]


def _score_bar(score: int) -> str:
    """Визуальная шкала силы сигнала: ░░░░░ → █████"""
    filled = min(score, 5)
    return "█" * filled + "░" * (5 - filled)


def _market_link(name: str, url: str) -> str:
    if url:
        return f'<a href="{html.escape(url, quote=True)}">{html.escape(name)}</a>'
    return html.escape(name)


def _ts_to_str(ts: float) -> str:
    if not ts:
        return "—"
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S UTC")
    except (OSError, ValueError):
        return "—"


# ──────────────────────────────────────────────────────────────────────────────
# ГЛАВНАЯ ФУНКЦИЯ
# ──────────────────────────────────────────────────────────────────────────────

def format_rich_insider_message(signals: list[RichSignal]) -> str:
    """
    Форматирует список RichSignal в одно Telegram HTML-сообщение.
    Каждый сигнал — отдельный блок со своим заголовком.
    """
    if not signals:
        return "💰 <b>Крупные ставки:</b> сигналов не обнаружено."

    blocks: list[str] = [f"💰 <b>Крупные ставки — топ {len(signals)}</b>"]
    blocks.append("━━━━━━━━━━━━━━━━━━━━")

    for idx, sig in enumerate(signals, 1):
        blocks.append(_format_one(idx, sig))
        blocks.append("━━━━━━━━━━━━━━━━━━━━")

    return "\n".join(blocks)


def _format_one(idx: int, sig: RichSignal) -> str:
    header = _KIND_HEADER.get(sig.kind, f"⚡ {sig.kind.value}")
    strong_badge = " 🔥<b>STRONG</b>" if sig.is_strong else ""
    side_e = _SIDE_EMOJI.get(sig.side, "⚪")
    score_visual = _score_bar(sig.signal_score)

    market_display = _market_link(sig.market_name_ru or sig.market_name_en, sig.market_url)

    lines: list[str] = [
        f"<b>{idx}. {header}{strong_badge}</b>",
        f"",
        f"📊 {market_display}",
        f"🎲 <b>Исход:</b> {html.escape(sig.outcome)}",
        f"{side_e} <b>Сторона:</b> {sig.side}",
        f"",
    ]

    # Специфичные блоки по типу
    if sig.kind == SignalKind.WHALE_TRADE:
        lines += _block_whale_trade(sig)
    elif sig.kind == SignalKind.WHALE_POSITION:
        lines += _block_whale_position(sig)
    elif sig.kind == SignalKind.POSITION_SWEEP:
        lines += _block_position_sweep(sig)
    elif sig.kind == SignalKind.STEALTH_ACCUMULATION:
        lines += _block_stealth(sig)
    elif sig.kind in (SignalKind.COORDINATED, SignalKind.CLUSTERED_COORD):
        lines += _block_coordination(sig)
    elif sig.kind == SignalKind.CROSS_MARKET_INSIDER:
        lines += _block_cross_market(sig)
    elif sig.kind == SignalKind.LIQUIDITY_SWEEP:
        lines += _block_liquidity_sweep(sig)
    else:
        lines += _block_generic(sig)

    # Footer: scoring + timestamp
    lines += [
        f"",
        f"📈 <b>Цена входа:</b> {sig.price * 100:.1f}%"
        + (f"  |  💵 Профит с $1: ${(1/max(sig.price, 0.01) - 1):.2f}" if sig.price > 0 else ""),
        f"🏆 <b>Сила сигнала:</b> {score_visual} ({sig.signal_score}/9)",
        f"🕐 {_ts_to_str(sig.timestamp)}",
    ]

    return "\n".join(lines)


# ── Блоки по типу ─────────────────────────────────────────────────────────────

def _block_whale_trade(sig: RichSignal) -> list[str]:
    return [
        f"👛 <b>Кошелёк:</b> <code>{html.escape(sig.wallet)}</code>",
        f"💰 <b>Объём сделки:</b> ${sig.usd_value:,.0f}",
    ]


def _block_whale_position(sig: RichSignal) -> list[str]:
    return [
        f"👛 <b>Кошелёк:</b> <code>{html.escape(sig.wallet)}</code>",
        f"💰 <b>Крупнейшая сделка:</b> ${sig.usd_value:,.0f}",
        f"📦 <b>Суммарный объём:</b> ${sig.total_volume:,.0f}",
        f"🔢 <b>Сделок:</b> {sig.trade_count}",
    ]


def _block_position_sweep(sig: RichSignal) -> list[str]:
    return [
        f"👛 <b>Кошелёк:</b> <code>{html.escape(sig.wallet)}</code>",
        f"💰 <b>Суммарный объём:</b> ${sig.total_volume:,.0f}",
        f"🔢 <b>Сделок:</b> {sig.trade_count} за {sig.window_sec}с",
        f"⚡ <i>Агрессивный набор позиции</i>",
    ]


def _block_stealth(sig: RichSignal) -> list[str]:
    minutes = sig.window_sec // 60 if sig.window_sec else "?"
    return [
        f"👛 <b>Кошелёк:</b> <code>{html.escape(sig.wallet)}</code>",
        f"💰 <b>Накоплено:</b> ${sig.total_volume:,.0f}",
        f"🔢 <b>Сделок:</b> {sig.trade_count} за ~{minutes} мин",
        f"🕵️ <i>Скрытое накопление позиции мелкими частями</i>",
    ]


def _block_coordination(sig: RichSignal) -> list[str]:
    clustered = sig.kind == SignalKind.CLUSTERED_COORD
    cluster_note = "\n🔗 <i>Кошельки из известного кластера</i>" if clustered else ""
    return [
        f"👥 <b>Участников:</b> {sig.wallet}",
        f"💰 <b>Суммарный объём:</b> ${sig.total_volume:,.0f}",
        f"🔢 <b>Сделок в окне:</b> {sig.trade_count} за {sig.window_sec}с",
        f"⚠️ <i>Согласованные действия в одну сторону</i>{cluster_note}",
    ]


def _block_cross_market(sig: RichSignal) -> list[str]:
    strength = sig.signal_strength
    stars = _STRENGTH_STARS[min(strength, 3)]
    same_side_note = " (все в одну сторону ✅)" if sig.side != "UNKNOWN" else ""
    markets_list = ""
    if sig.markets_involved:
        trimmed = [html.escape(m[:50]) for m in sig.markets_involved[:5]]
        markets_list = "\n" + "\n".join(f"  • {m}" for m in trimmed)
        if len(sig.markets_involved) > 5:
            markets_list += f"\n  <i>...и ещё {len(sig.markets_involved) - 5}</i>"

    return [
        f"👛 <b>Кошелёк:</b> <code>{html.escape(sig.wallet)}</code>",
        f"🌐 <b>Рынков:</b> {len(sig.markets_involved)} за {sig.window_sec}с{same_side_note}",
        f"💰 <b>Суммарный объём:</b> ${sig.total_volume:,.0f}",
        f"⚡ <b>Сила паттерна:</b> {stars} (уровень {strength}){markets_list}",
        f"🧠 <i>Возможна инсайдерская активность на нескольких рынках</i>",
    ]


def _block_liquidity_sweep(sig: RichSignal) -> list[str]:
    pct = sig.price_change_pct
    return [
        f"👛 <b>Кошелёк:</b> <code>{html.escape(sig.wallet)}</code>",
        f"💰 <b>Объём сделки:</b> ${sig.usd_value:,.0f}",
        f"📉 <b>Движение цены:</b> {pct:+.1f}%",
        f"💥 <i>Крупная сделка сдвинула рынок</i>",
    ]


def _block_generic(sig: RichSignal) -> list[str]:
    return [
        f"👛 <b>Кошелёк:</b> <code>{html.escape(sig.wallet)}</code>",
        f"💰 <b>Объём:</b> ${sig.total_volume:,.0f}",
        f"🔢 <b>Сделок:</b> {sig.trade_count}",
    ]


# ──────────────────────────────────────────────────────────────────────────────
# ОБРАТНАЯ СОВМЕСТИМОСТЬ — оригинальные функции из bot.py
# ──────────────────────────────────────────────────────────────────────────────

def format_insider_message(signals: list[InsiderSignal]) -> str:
    """
    Используется в bot.py как раньше.
    Теперь внутри вызывает красивый форматтер через конвертацию.
    """
    if not signals:
        return "💰 Крупные ставки: сигналов нет."

    lines = [f"💰 <b>Крупные ставки (топ-{len(signals)}):</b>"]
    lines.append("━━━━━━━━━━━━━━━━━━━━")

    for idx, signal in enumerate(signals, start=1):
        price = max(0.01, min(0.99, signal.price))
        profit = (1.0 / price) - 1.0
        whale_label = " 🐋 <b>WHALE</b>" if signal.is_whale else ""
        market_display = _market_link(signal.market_name_ru, signal.market_url)

        lines.append(
            f"<b>{idx}.</b> {market_display}{whale_label}\n"
            f"👛 <code>{html.escape(signal.wallet)}</code>"
            f"  |  🎲 {html.escape(signal.outcome)}\n"
            f"💰 Крупнейшая: <b>${signal.amount_usd:,.0f}</b>"
            f"  |  📦 Итого: <b>${signal.total_volume:,.0f}</b>\n"
            f"🔢 Сделок: {signal.trade_count}"
            f"  |  📈 Вход: {price * 100:.1f}%"
            f"  |  💵 Профит: <b>${profit:.2f}</b>"
        )
        lines.append("━━━━━━━━━━━━━━━━━━━━")

    return "\n".join(lines)


def format_probability_message(signals: list[ProbabilitySignal]) -> str:
    if not signals:
        return "📈 Высокая вероятность: подходящих рынков сейчас нет."

    lines = ["📈 <b>Высокая вероятность (топ-10):</b>", "━━━━━━━━━━━━━━━━━━━━"]
    for idx, signal in enumerate(signals, start=1):
        market_display = _market_link(signal.market_name_ru, signal.market_url)
        lines.append(
            f"<b>{idx}.</b> {market_display}\n"
            f"🥇 {html.escape(signal.leading_outcome)}"
            f" — <b>{signal.leading_probability * 100:.1f}%</b>"
            f"  |  Отрыв: {signal.gap * 100:.1f}%\n"
            f"💵 Профит с $1: <b>${signal.win_if_1_dollar:.2f}</b>"
        )
        lines.append("━━━━━━━━━━━━━━━━━━━━")

    return "\n".join(lines)


def format_hot_message(signals: list[ProbabilitySignal]) -> str:
    if not signals:
        return "🔥 Горячие ставки: подходящих рынков сейчас нет."

    lines = ["🔥 <b>Горячие ставки (закрытие ≤ 5 дней):</b>", "━━━━━━━━━━━━━━━━━━━━"]
    for idx, signal in enumerate(signals, start=1):
        market_display = _market_link(signal.market_name_ru, signal.market_url)
        lines.append(
            f"<b>{idx}.</b> {market_display}\n"
            f"🥇 {html.escape(signal.leading_outcome)}"
            f" — <b>{signal.leading_probability * 100:.1f}%</b>"
            f"  |  Отрыв: {signal.gap * 100:.1f}%\n"
            f"💵 Профит с $1: <b>${signal.win_if_1_dollar:.2f}</b>"
        )
        lines.append("━━━━━━━━━━━━━━━━━━━━")

    return "\n".join(lines)
