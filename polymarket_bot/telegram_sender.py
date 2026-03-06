from __future__ import annotations

from telegram import Bot


class TelegramSender:
    def __init__(self, token: str) -> None:
        self._bot = Bot(token=token)

    async def send_to(self, chat_id: int, text: str) -> None:
        await self._bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True)
