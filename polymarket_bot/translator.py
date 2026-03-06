from __future__ import annotations

from deep_translator import GoogleTranslator


class RuTranslator:
    def __init__(self) -> None:
        self._cache: dict[str, str] = {}
        self._translator = GoogleTranslator(source="auto", target="ru")

    def translate(self, text: str) -> str:
        if not text:
            return text
        cached = self._cache.get(text)
        if cached:
            return cached

        try:
            translated = self._translator.translate(text)
        except Exception:
            translated = text

        self._cache[text] = translated
        return translated
