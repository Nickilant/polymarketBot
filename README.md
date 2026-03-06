# Polymarket Telegram Bot

Бот присылает сигналы Polymarket в личные сообщения подписчикам.

## Что изменено
- Подписка в ЛС: пользователь активирует Free автоматически через `/start`.
- Админ (`ADMIN_CHAT_ID`) всегда получает все сигналы в каждом цикле.
- Админ может выдать Pro150 через `/grant <user_id>`.
- 2 тарифа:
  - **Free** — все сигналы, но 1 раз в день.
  - **Pro150** — все сигналы каждый час, 150 Telegram Stars за 30 дней.
- Приветственное сообщение включает описание и ссылку на покупку Stars: `@PremiumBot`.

## Сигналы
1. **Инсайдерская торговля (топ-3)** — крупные входы кошельков за последний период.
2. **Высокая вероятность (топ-10)** — рынок с сильным отрывом лидера.

В каждом сигнале есть расчёт возможной прибыли при ставке **$1**.

## Команды бота
- `/start` — активирует Free и показывает приветствие.
- `/mode insider|probability|both` — выбрать типы сигналов.
- `/my` — текущий тариф и параметры.
- `/buy` — инструкция по покупке Stars.
- `/stop` — отключить подписку.
- `/grant <user_id>` — админская команда выдачи Pro150.

## Хранение данных
Подписки и состояние рассылки хранятся в SQLite базе (`SUBSCRIPTIONS_DB`, по умолчанию `subscriptions.db`).

## Настройка
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Ключевые переменные:
- `TELEGRAM_BOT_TOKEN` — токен бота.
- `ADMIN_CHAT_ID` — Telegram ID админа.
- `SUBSCRIPTIONS_DB` — путь к SQLite базе подписок.

Запуск вручную:
```bash
python bot.py
```

## Запуск как демон (Ubuntu + systemd)
1. Создай пользователя под сервис (опционально):
```bash
sudo useradd -r -m -d /opt/polymarketbot -s /usr/sbin/nologin polymarketbot
```
2. Скопируй проект в `/opt/polymarketbot/app`, настрой `.env`, установи зависимости.
3. Создай unit-файл `/etc/systemd/system/polymarketbot.service`:
```ini
[Unit]
Description=Polymarket Telegram Bot
After=network.target

[Service]
Type=simple
User=polymarketbot
WorkingDirectory=/opt/polymarketbot/app
EnvironmentFile=/opt/polymarketbot/app/.env
ExecStart=/opt/polymarketbot/app/.venv/bin/python /opt/polymarketbot/app/bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```
4. Включи и запусти сервис:
```bash
sudo systemctl daemon-reload
sudo systemctl enable polymarketbot
sudo systemctl start polymarketbot
```
5. Проверка статуса и логов:
```bash
sudo systemctl status polymarketbot
sudo journalctl -u polymarketbot -f
```
