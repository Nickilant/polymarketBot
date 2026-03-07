from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import logging
from typing import Any

from polymarket_bot.models import InsiderSignal, MarketView, ProbabilitySignal
from polymarket_bot.signals import RichSignal, SignalKind
from polymarket_bot.translator import RuTranslator

logger = logging.getLogger(__name__)

# ── Константы ─────────────────────────────────────────────────────────────────

COORD_WINDOW_SEC: int = 30           # окно координации (секунды)
COORD_MIN_WALLETS: int = 3           # минимум кошельков для координации

CLUSTER_WINDOW_SEC: int = 5          # окно кластеризации кошельков
CLUSTER_MIN_CO_TRADES: int = 5       # порог совместных появлений → кластер

MM_TRADE_COUNT_THRESHOLD: int = 20   # сделок в окне → маркетмейкер
MM_WINDOW_SEC: int = 60

SWEEP_MIN_TRADES: int = 5            # POSITION_SWEEP: мин. сделок
SWEEP_WINDOW_SEC: int = 120          # POSITION_SWEEP: окно (секунды)
SWEEP_MIN_USD: float = 5_000.0       # POSITION_SWEEP: мин. объём

STEALTH_MIN_TRADES: int = 10         # STEALTH_ACCUMULATION: мин. сделок
STEALTH_WINDOW_SEC: int = 1_200      # STEALTH_ACCUMULATION: 20 минут
STEALTH_MIN_USD: float = 8_000.0     # STEALTH_ACCUMULATION: мин. объём

CROSS_MARKET_WINDOW_SEC: int = 60    # CROSS_MARKET_INSIDER: окно
CROSS_MARKET_MIN_MARKETS: int = 3    # CROSS_MARKET_INSIDER: мин. рынков
CROSS_MARKET_MIN_USD: float = 3_000.0

DEDUP_BUCKET_SEC: int = 300          # дедупликация: 5-минутные бакеты


class Analyzer:
    def __init__(
        self,
        translator: RuTranslator,
        insider_min_trade_usd: float,
        probability_gap_threshold: float,
        probability_min_value: float,
    ) -> None:
        self._translator = translator
        self._insider_min_trade_usd = insider_min_trade_usd
        self._probability_gap_threshold = probability_gap_threshold
        self._probability_min_value = probability_min_value

    # ──────────────────────────────────────────────────────────────────────────
    # PUBLIC — основной метод
    # ──────────────────────────────────────────────────────────────────────────

    def insider_signals(
        self,
        trades: list[dict[str, Any]],
        markets: list[MarketView],
        top_n: int,
    ) -> list[InsiderSignal]:
        """
        Обёртка над rich_insider_signals для обратной совместимости с bot.py.
        Конвертирует RichSignal → InsiderSignal.
        """
        rich = self.rich_insider_signals(trades, markets, top_n)
        return [_rich_to_insider(s) for s in rich]

    def rich_insider_signals(
        self,
        trades: list[dict[str, Any]],
        markets: list[MarketView],
        top_n: int,
    ) -> list[RichSignal]:
        """
        Возвращает RichSignal со scoring и всеми метаданными.

        Типы сигналов:
          1. WHALE_TRADE           — одна сделка >= порога
          2. WHALE_POSITION        — серия одного кошелька суммарно >= порога
          3. POSITION_SWEEP        — >= 5 сделок за 120с, объём >= 5000$
          4. STEALTH_ACCUMULATION  — >= 10 сделок за 20 мин, объём >= 8000$
          5. COORDINATED           — >= 3 кошелька, одна сторона, 30с
          6. CLUSTERED_COORD       — координация кошельков из кластера
          7. CROSS_MARKET_INSIDER  — один кошелёк на >= 3 рынках за 60с
        """
        market_by_condition_id: dict[str, MarketView] = {
            m.condition_id: m for m in markets if m.condition_id
        }

        # ── 1. Парсинг + Anti-MM ──────────────────────────────────────────────
        parsed: list[_ParsedTrade] = []
        wallet_activity: dict[str, list[float]] = defaultdict(list)

        for raw in trades:
            if str(raw.get("type") or "").upper() != "TRADE":
                continue
            pt = _parse_trade(raw)
            if pt is None:
                continue
            wallet_activity[pt.wallet].append(pt.ts)
            parsed.append(pt)

        mm_wallets = _detect_market_makers(wallet_activity)
        if mm_wallets:
            logger.debug("Anti-MM: исключено %d кошельков", len(mm_wallets))

        clean: list[_ParsedTrade] = [p for p in parsed if p.wallet not in mm_wallets]

        # ── 2. Агрегация ──────────────────────────────────────────────────────
        by_wmo: dict[tuple[str, str, int], _WMOStats] = defaultdict(_WMOStats)
        coord_buffer: dict[tuple[str, int, str], list[_ParsedTrade]] = defaultdict(list)
        cluster_counter: dict[tuple[str, str], int] = defaultdict(int)
        # Cross-market: wallet → [_ParsedTrade]
        by_wallet_all: dict[str, list[_ParsedTrade]] = defaultdict(list)

        for pt in clean:
            by_wmo[(pt.wallet, pt.market_id, pt.outcome_index)].add(pt)
            coord_buffer[(pt.market_id, pt.outcome_index, pt.side)].append(pt)
            by_wallet_all[pt.wallet].append(pt)

        _update_cluster_counter(clean, cluster_counter)
        known_clusters = _build_cluster_sets(cluster_counter)

        # ── 3. Дедупликация ───────────────────────────────────────────────────
        emitted: set[tuple[str, str, int]] = set()
        signals: list[RichSignal] = []

        # ── Типы 1–4: один кошелёк ────────────────────────────────────────────
        for (wallet, market_id, outcome_index), s in by_wmo.items():
            if s.total_volume < self._insider_min_trade_usd:
                continue

            market = market_by_condition_id.get(market_id)
            if not market:
                continue

            time_bucket = int(s.last_ts // DEDUP_BUCKET_SEC)
            dedup_key = (wallet, market_id, time_bucket)
            if dedup_key in emitted:
                continue
            emitted.add(dedup_key)

            name_ru = self._translator.translate(market.market_name)
            outcome_label = _outcome_label(s.best_trade, outcome_index)
            price = s.best_trade.get("_price_f", 0.5)
            is_single = s.max_trade >= self._insider_min_trade_usd
            duration = s.last_ts - s.first_ts

            # STEALTH_ACCUMULATION (приоритет над POSITION_SWEEP)
            if (
                s.trade_count >= STEALTH_MIN_TRADES
                and s.side_consistent
                and duration <= STEALTH_WINDOW_SEC
                and s.total_volume >= STEALTH_MIN_USD
            ):
                kind = SignalKind.STEALTH_ACCUMULATION
                logger.info(
                    "STEALTH_ACCUMULATION | wallet=%s | market=%s | trades=%d | usd=%.0f",
                    _short_wallet(wallet), market.market_name[:60], s.trade_count, s.total_volume,
                )

            # POSITION_SWEEP
            elif (
                s.trade_count >= SWEEP_MIN_TRADES
                and s.side_consistent
                and duration <= SWEEP_WINDOW_SEC
                and s.total_volume >= SWEEP_MIN_USD
            ):
                kind = SignalKind.POSITION_SWEEP
                logger.info(
                    "POSITION_SWEEP | wallet=%s | market=%s | trades=%d | usd=%.0f",
                    _short_wallet(wallet), market.market_name[:60], s.trade_count, s.total_volume,
                )

            # WHALE_TRADE — одиночная крупная сделка
            elif is_single:
                kind = SignalKind.WHALE_TRADE
                logger.info(
                    "WHALE_TRADE | wallet=%s | market=%s | usd=%.0f",
                    _short_wallet(wallet), market.market_name[:60], s.max_trade,
                )

            # WHALE_POSITION — серия
            else:
                kind = SignalKind.WHALE_POSITION
                logger.info(
                    "WHALE_POSITION | wallet=%s | market=%s | total_usd=%.0f | trades=%d",
                    _short_wallet(wallet), market.market_name[:60], s.total_volume, s.trade_count,
                )

            signals.append(RichSignal.build(
                kind=kind,
                wallet=_short_wallet(wallet),
                market_id=market_id,
                market_name_en=market.market_name,
                market_name_ru=name_ru,
                market_url=market.market_url,
                outcome=outcome_label,
                side=s.dominant_side(),
                usd_value=s.max_trade,
                total_volume=s.total_volume,
                trade_count=s.trade_count,
                price=price,
                timestamp=s.last_ts,
                window_sec=int(duration),
            ))

        # ── Типы 5–6: координация ─────────────────────────────────────────────
        signals.extend(self._detect_coordinated(
            coord_buffer, market_by_condition_id, known_clusters, emitted
        ))

        # ── Тип 7: Cross-market insider ───────────────────────────────────────
        signals.extend(self._detect_cross_market(
            by_wallet_all, market_by_condition_id, emitted
        ))

        signals.sort(key=lambda x: (x.signal_score, x.total_volume), reverse=True)
        return signals[:top_n]

    def probability_signals(self, markets: list[MarketView], top_n: int) -> list[ProbabilitySignal]:
        signals = self._collect_probability_signals(markets)
        signals.sort(key=lambda x: (x.gap, x.leading_probability), reverse=True)
        return signals[:top_n]

    def hot_signals(self, markets: list[MarketView], top_n: int) -> list[ProbabilitySignal]:
        now = datetime.now(timezone.utc)
        deadline = now + timedelta(days=5)
        eligible = [m for m in markets if m.end_datetime and now <= m.end_datetime <= deadline]
        signals = self._collect_probability_signals(eligible)
        signals.sort(key=lambda x: (x.gap, x.leading_probability), reverse=True)
        return signals[:top_n]

    # ──────────────────────────────────────────────────────────────────────────
    # COORDINATION DETECTOR
    # ──────────────────────────────────────────────────────────────────────────

    def _detect_coordinated(
        self,
        coord_buffer: dict[tuple[str, int, str], list[_ParsedTrade]],
        market_by_condition_id: dict[str, MarketView],
        known_clusters: list[set[str]],
        emitted: set[tuple[str, str, int]],
    ) -> list[RichSignal]:
        signals: list[RichSignal] = []
        coord_emitted: set[tuple[str, int, str]] = set()

        for (market_id, outcome_index, side), entries in coord_buffer.items():
            if side == "UNKNOWN":
                continue
            if len(entries) < COORD_MIN_WALLETS:
                continue

            entries_sorted = sorted(entries, key=lambda e: e.ts)
            best = _best_coord_window(entries_sorted, self._insider_min_trade_usd)
            if best is None:
                continue

            window_entries, total_usd, wallets = best
            market = market_by_condition_id.get(market_id)
            if not market:
                continue

            coord_key = (market_id, outcome_index, side)
            if coord_key in coord_emitted:
                continue

            best_entry = max(window_entries, key=lambda e: e.usd_value)
            time_bucket = int(best_entry.ts // DEDUP_BUCKET_SEC)
            dedup_key = (f"__coord_{outcome_index}_{side}", market_id, time_bucket)
            if dedup_key in emitted:
                continue

            emitted.add(dedup_key)
            coord_emitted.add(coord_key)

            is_clustered = any(len(wallets & cl) >= 2 for cl in known_clusters)
            kind = SignalKind.CLUSTERED_COORD if is_clustered else SignalKind.COORDINATED
            outcome_label = _outcome_label(best_entry.raw, outcome_index)
            name_ru = self._translator.translate(market.market_name)

            logger.info(
                "%s | market=%s | side=%s | wallets=%d | usd=%.0f",
                kind.value, market.market_name[:60], side, len(wallets), total_usd,
            )

            signals.append(RichSignal.build(
                kind=kind,
                wallet=f"{len(wallets)} кошельков",
                market_id=market_id,
                market_name_en=market.market_name,
                market_name_ru=name_ru,
                market_url=market.market_url,
                outcome=outcome_label,
                side=side,
                usd_value=best_entry.usd_value,
                total_volume=total_usd,
                trade_count=len(window_entries),
                price=best_entry.raw.get("_price_f", 0.5),
                timestamp=best_entry.ts,
                window_sec=COORD_WINDOW_SEC,
            ))

        return signals

    # ──────────────────────────────────────────────────────────────────────────
    # CROSS MARKET INSIDER DETECTOR
    # ──────────────────────────────────────────────────────────────────────────

    def _detect_cross_market(
        self,
        by_wallet: dict[str, list[_ParsedTrade]],
        market_by_condition_id: dict[str, MarketView],
        emitted: set[tuple[str, str, int]],
    ) -> list[RichSignal]:
        """
        Один кошелёк торгует на >= CROSS_MARKET_MIN_MARKETS рынках
        за CROSS_MARKET_WINDOW_SEC секунд суммарно >= CROSS_MARKET_MIN_USD.
        Если все сделки в одну сторону — сигнал усиливается.
        signal_strength: 3 рынка=1, 4=2, >=5=3
        """
        signals: list[RichSignal] = []

        for wallet, trades_list in by_wallet.items():
            if len(trades_list) < CROSS_MARKET_MIN_MARKETS:
                continue

            trades_sorted = sorted(trades_list, key=lambda t: t.ts)
            n = len(trades_sorted)
            j = 0
            best_window: list[_ParsedTrade] | None = None
            best_total = 0.0

            for i in range(n):
                while j < n and trades_sorted[j].ts - trades_sorted[i].ts <= CROSS_MARKET_WINDOW_SEC:
                    j += 1
                window = trades_sorted[i:j]
                unique_markets = {t.market_id for t in window}
                if len(unique_markets) < CROSS_MARKET_MIN_MARKETS:
                    continue
                total = sum(t.usd_value for t in window)
                if total < CROSS_MARKET_MIN_USD:
                    continue
                if total > best_total:
                    best_total = total
                    best_window = window

            if best_window is None:
                continue

            unique_markets = {t.market_id for t in best_window}
            sides = {t.side for t in best_window if t.side != "UNKNOWN"}
            same_side = len(sides) == 1
            dominant_side = sides.pop() if sides else "UNKNOWN"

            # Сигнал усиления: чем больше рынков, тем выше strength
            n_markets = len(unique_markets)
            if n_markets >= 5:
                strength = 3
            elif n_markets == 4:
                strength = 2
            else:
                strength = 1

            # extra_score: +1 если все в одну сторону
            extra = (1 if same_side else 0)

            # Берём рынок с максимальным объёмом для основного market_id в сигнале
            market_volumes: dict[str, float] = defaultdict(float)
            for t in best_window:
                market_volumes[t.market_id] += t.usd_value
            main_market_id = max(market_volumes, key=market_volumes.__getitem__)
            market = market_by_condition_id.get(main_market_id)
            if not market:
                continue

            time_bucket = int(best_window[-1].ts // DEDUP_BUCKET_SEC)
            dedup_key = (f"__cross_{wallet}", main_market_id, time_bucket)
            if dedup_key in emitted:
                continue
            emitted.add(dedup_key)

            best_trade = max(best_window, key=lambda t: t.usd_value)
            name_ru = self._translator.translate(market.market_name)
            markets_names = [
                (market_by_condition_id[mid].market_name if mid in market_by_condition_id else mid)
                for mid in unique_markets
            ]

            logger.info(
                "CROSS_MARKET_INSIDER | wallet=%s | markets=%d | side=%s | usd=%.0f | strength=%d",
                _short_wallet(wallet), n_markets, dominant_side, best_total, strength,
            )

            signals.append(RichSignal.build(
                kind=SignalKind.CROSS_MARKET_INSIDER,
                wallet=_short_wallet(wallet),
                market_id=main_market_id,
                market_name_en=market.market_name,
                market_name_ru=name_ru,
                market_url=market.market_url,
                outcome=f"{n_markets} рынков одновременно",
                side=dominant_side,
                usd_value=best_trade.usd_value,
                total_volume=best_total,
                trade_count=len(best_window),
                price=best_trade.price,
                timestamp=best_window[-1].ts,
                extra_score=extra,
                signal_strength=strength,
                markets_involved=markets_names,
                window_sec=CROSS_MARKET_WINDOW_SEC,
            ))

        return signals

    # ──────────────────────────────────────────────────────────────────────────
    # PROBABILITY / HOT
    # ──────────────────────────────────────────────────────────────────────────

    def _collect_probability_signals(self, markets: list[MarketView]) -> list[ProbabilitySignal]:
        signals: list[ProbabilitySignal] = []
        seen: set[str] = set()
        for market in markets:
            if market.market_id in seen:
                continue
            paired = sorted(zip(market.outcomes, market.probabilities), key=lambda x: x[1], reverse=True)
            if len(paired) < 2:
                continue
            lead_outcome, lead_prob = paired[0]
            _, second_prob = paired[1]
            gap = lead_prob - second_prob
            if lead_prob < self._probability_min_value or gap < self._probability_gap_threshold:
                continue
            win = _win_if_one_dollar(lead_prob)
            if win < 0.1:
                continue
            name_ru = self._translator.translate(market.market_name)
            seen.add(market.market_id)
            signals.append(ProbabilitySignal(
                market_id=market.market_id,
                market_name_en=market.market_name,
                market_name_ru=name_ru,
                leading_outcome=lead_outcome,
                leading_probability=lead_prob,
                second_probability=second_prob,
                gap=gap,
                win_if_1_dollar=win,
                market_url=market.market_url,
            ))
        return signals


# ──────────────────────────────────────────────────────────────────────────────
# КОНВЕРТЕР RichSignal → InsiderSignal (обратная совместимость)
# ──────────────────────────────────────────────────────────────────────────────

def _rich_to_insider(s: RichSignal) -> InsiderSignal:
    from polymarket_bot.models import InsiderSignal as _IS
    return _IS(
        market_id=s.market_id,
        market_name_en=s.market_name_en,
        market_name_ru=s.market_name_ru,
        wallet=s.wallet,
        amount_usd=s.usd_value,
        outcome=s.outcome,
        price=s.price,
        total_volume=s.total_volume,
        trade_count=s.trade_count,
        is_whale=s.signal_score >= 2,
        market_url=s.market_url,
    )


# ──────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ──────────────────────────────────────────────────────────────────────────────

class _ParsedTrade:
    __slots__ = ("wallet", "market_id", "outcome_index", "side", "usd_value", "price", "ts", "raw")

    def __init__(
        self,
        wallet: str,
        market_id: str,
        outcome_index: int,
        side: str,
        usd_value: float,
        price: float,
        ts: float,
        raw: dict[str, Any],
    ) -> None:
        self.wallet = wallet
        self.market_id = market_id
        self.outcome_index = outcome_index
        self.side = side
        self.usd_value = usd_value
        self.price = price
        self.ts = ts
        self.raw = raw


class _WMOStats:
    __slots__ = (
        "total_volume", "trade_count", "max_trade",
        "best_trade", "last_ts", "first_ts", "sides", "side_consistent",
    )

    def __init__(self) -> None:
        self.total_volume: float = 0.0
        self.trade_count: int = 0
        self.max_trade: float = 0.0
        self.best_trade: dict[str, Any] = {}
        self.last_ts: float = 0.0
        self.first_ts: float = float("inf")
        self.sides: set[str] = set()
        self.side_consistent: bool = True

    def add(self, pt: _ParsedTrade) -> None:
        self.total_volume += pt.usd_value
        self.trade_count += 1
        if pt.ts > self.last_ts:
            self.last_ts = pt.ts
        if pt.ts < self.first_ts:
            self.first_ts = pt.ts
        if pt.usd_value >= self.max_trade:
            self.max_trade = pt.usd_value
            self.best_trade = pt.raw
        self.sides.add(pt.side)
        self.side_consistent = len(self.sides) == 1

    def dominant_side(self) -> str:
        if len(self.sides) == 1:
            return next(iter(self.sides))
        return "UNKNOWN"


# ──────────────────────────────────────────────────────────────────────────────
# WALLET CLUSTER DETECTOR
# ──────────────────────────────────────────────────────────────────────────────

def _update_cluster_counter(
    trades: list[_ParsedTrade],
    cluster_counter: dict[tuple[str, str], int],
) -> None:
    by_market_side: dict[tuple[str, str], list[_ParsedTrade]] = defaultdict(list)
    for pt in trades:
        by_market_side[(pt.market_id, pt.side)].append(pt)

    for entries in by_market_side.values():
        s_entries = sorted(entries, key=lambda e: e.ts)
        n = len(s_entries)
        j = 0
        for i in range(n):
            while j < n and s_entries[j].ts - s_entries[i].ts < CLUSTER_WINDOW_SEC:
                j += 1
            unique = list({e.wallet for e in s_entries[i:j]})
            for a in range(len(unique)):
                for b in range(a + 1, len(unique)):
                    pair = (min(unique[a], unique[b]), max(unique[a], unique[b]))
                    cluster_counter[pair] += 1


def _build_cluster_sets(cluster_counter: dict[tuple[str, str], int]) -> list[set[str]]:
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent.get(x, x), x)
            x = parent.get(x, x)
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for (wa, wb), count in cluster_counter.items():
        if count >= CLUSTER_MIN_CO_TRADES:
            union(wa, wb)

    groups: dict[str, set[str]] = defaultdict(set)
    for wallet in parent:
        groups[find(wallet)].add(wallet)
    return [g for g in groups.values() if len(g) >= 2]


# ──────────────────────────────────────────────────────────────────────────────
# ANTI MARKET MAKER
# ──────────────────────────────────────────────────────────────────────────────

def _detect_market_makers(wallet_activity: dict[str, list[float]]) -> set[str]:
    mm: set[str] = set()
    for wallet, timestamps in wallet_activity.items():
        if len(timestamps) < MM_TRADE_COUNT_THRESHOLD:
            continue
        ts_sorted = sorted(timestamps)
        n = len(ts_sorted)
        j = 0
        for i in range(n):
            while j < n and ts_sorted[j] - ts_sorted[i] <= MM_WINDOW_SEC:
                j += 1
            if (j - i) >= MM_TRADE_COUNT_THRESHOLD:
                mm.add(wallet)
                break
    return mm


# ──────────────────────────────────────────────────────────────────────────────
# SLIDING WINDOW
# ──────────────────────────────────────────────────────────────────────────────

def _best_coord_window(
    entries: list[_ParsedTrade],
    min_total_usd: float,
) -> tuple[list[_ParsedTrade], float, set[str]] | None:
    best: tuple[list[_ParsedTrade], float, set[str]] | None = None
    n = len(entries)
    j = 0
    for i in range(n):
        while j < n and entries[j].ts - entries[i].ts <= COORD_WINDOW_SEC:
            j += 1
        window = entries[i:j]
        wallets = {e.wallet for e in window}
        if len(wallets) < COORD_MIN_WALLETS:
            continue
        total = sum(e.usd_value for e in window)
        if total < min_total_usd:
            continue
        if best is None or total > best[1]:
            best = (window, total, wallets)
    return best


# ──────────────────────────────────────────────────────────────────────────────
# TRADE PARSING
# ──────────────────────────────────────────────────────────────────────────────

def _parse_trade(raw: dict[str, Any]) -> _ParsedTrade | None:
    wallet = str(raw.get("proxyWallet") or "").strip()
    market_id = str(raw.get("conditionId") or "").strip()
    if not wallet or not market_id:
        return None

    outcome_index = _outcome_index(raw.get("outcomeIndex"))
    if outcome_index is None:
        return None

    try:
        price = abs(float(raw.get("price") or 0))
    except (TypeError, ValueError):
        return None
    if price <= 0:
        return None

    size_raw = raw.get("size")
    usdc_raw = raw.get("usdcSize")
    if size_raw not in (None, ""):
        try:
            usd_value = abs(float(size_raw)) * price
        except (TypeError, ValueError):
            return None
    elif usdc_raw not in (None, ""):
        try:
            usd_value = abs(float(usdc_raw))
        except (TypeError, ValueError):
            return None
    else:
        return None

    if usd_value <= 0:
        return None

    ts = _trade_timestamp(raw)
    side = _trade_side(raw)
    enriched = dict(raw)
    enriched["_price_f"] = price

    return _ParsedTrade(
        wallet=wallet,
        market_id=market_id,
        outcome_index=outcome_index,
        side=side,
        usd_value=usd_value,
        price=price,
        ts=ts,
        raw=enriched,
    )


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _outcome_index(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _outcome_label(trade: dict[str, Any], outcome_index: int | None) -> str:
    outcome = str(trade.get("outcome") or "").strip()
    if outcome:
        return outcome
    if outcome_index is None:
        return "N/A"
    return f"outcome #{outcome_index}"


def _trade_side(trade: dict[str, Any]) -> str:
    raw = str(trade.get("side") or "").strip().upper()
    if raw in ("BUY", "LONG"):
        return "BUY"
    if raw in ("SELL", "SHORT"):
        return "SELL"
    return "UNKNOWN"


def _trade_timestamp(trade: dict[str, Any]) -> float:
    for field in ("timestamp", "createdAt", "created_at", "time", "blockTimestamp"):
        raw = trade.get(field)
        if raw is None:
            continue
        try:
            ts = float(raw)
            if ts > 1e12:
                ts /= 1000.0
            return ts
        except (TypeError, ValueError):
            pass
        text = str(raw).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            pass
    return 0.0


def _win_if_one_dollar(probability: float) -> float:
    probability = max(0.01, min(0.99, probability))
    return (1.0 / probability) - 1.0


def _short_wallet(wallet: str) -> str:
    if len(wallet) <= 12:
        return wallet
    return f"{wallet[:6]}...{wallet[-4:]}"