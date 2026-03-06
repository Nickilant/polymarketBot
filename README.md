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
- При старте бот отправляет админу приветственное служебное сообщение со статусом запуска.
- Раз в час бот отправляет только админу принудительный служебный пинг о том, что бот жив.
- В логах добавлены подробные сообщения на русском о каждом этапе работы цикла.

## Сигналы
1. **Инсайдерская торговля (топ-3)** — крупные входы кошельков за последний период.
2. **Высокая вероятность (топ-10)** — рынок с сильным отрывом лидера.

В каждом сигнале есть расчёт возможной прибыли при ставке **$1**.

## Команды бота
- `/start` — активирует Free и показывает приветствие.
- `/mode insider|probability|both` — выбрать типы сигналов.
- `/my` — текущий тариф и параметры.
- `/buy` — встроенная оплата Pro150 через Telegram Stars (счёт внутри бота).
- `/stop` — отключить подписку.
- `/grant <user_id>` — админская команда ручной выдачи Pro150 (резервный вариант).

## Хранение данных
Подписки и состояние рассылки хранятся в SQLite базе. При запуске бот сам проверяет БД в директории проекта: если `subscriptions.db` есть — использует её, если нет — создаёт автоматически.

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
- `SUBSCRIPTIONS_DB` — опционально: путь к SQLite базе подписок.

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


## Подключение встроенной оплаты Telegram Stars
1. Открой @BotFather и выбери своего бота.
2. Убедись, что у бота включены платежи (Payments).
3. Для Stars-платежей в `sendInvoice` используется валюта `XTR`; в большинстве случаев `provider_token` можно оставить пустым. Если BotFather выдал провайдер-токен, укажи его в `.env` как `TELEGRAM_PAYMENTS_PROVIDER_TOKEN`.
4. Добавь в `.env`:
```env
TELEGRAM_PAYMENTS_PROVIDER_TOKEN=
```
5. Перезапусти бота. После этого команда `/buy` будет выставлять счёт на **150 Stars** и автоматически активировать Pro150 после успешной оплаты.

### Как это работает
- Пользователь запускает `/buy`.
- Бот отправляет инвойс Telegram Stars.
- На `pre_checkout_query` бот проверяет payload/сумму.
- После `successful_payment` бот автоматически продлевает Pro на 30 дней и шлёт подтверждение пользователю.

> Если оплата не проходит, проверь настройки платежей в @BotFather и корректность `TELEGRAM_PAYMENTS_PROVIDER_TOKEN` (если используется).
