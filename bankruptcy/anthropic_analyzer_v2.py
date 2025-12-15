# -*- coding: utf-8 -*-
"""
Аналізатор резолютивних частин за допомогою Anthropic Claude API v2
Використовує офіційну бібліотеку anthropic
"""

from anthropic import Anthropic
import json
import time
from typing import Dict, List, Optional, Tuple
from django.utils import timezone
from .models import LLMAnalysisLog, Creditor, CreditorClaim
from .llm_analyzer import LLMAnalyzer

class AnthropicAnalyzerV2(LLMAnalyzer):
    """Аналізатор резолютивних частин за допомогою Anthropic Claude API v2"""

    def __init__(self, api_key: str = "sk--mxWRMIzOJpGNxCqziVodN3CSyCj50497aoZzESKWZ1wpREhNulc35h7C_Rx8-iPX4vpB5nPHkskkkVzg6JPzw"):
        self.api_key = api_key
        self.model_name = "claude-sonnet-4-20250514"

        # Ініціалізуємо клієнт Anthropic
        self.client = Anthropic(
            base_url="https://api.langdock.com/anthropic/eu/",
            api_key=api_key
        )

        # Налаштування для контролю швидкості
        self.rate_limit_delay = 2.0  # 2 секунди між запитами
        self.max_retries = 3
        self.retry_delay = 5.0  # 5 секунд перед повтором

    def test_connection(self) -> bool:
        """Тестує з'єднання з Anthropic API"""
        try:
            response = self.send_anthropic_request("Тест з'єднання")
            return response.get("success", False)
        except:
            return False

    def create_creditor_extraction_prompt(self, resolution_text: str) -> str:
        """Створює промпт для витягування даних кредиторів"""
        return f"""Проаналізуй цю резолютивну частину українського судового рішення по банкрутству та знайди всіх кредиторів.

ЗАВДАННЯ:
1. Знайди в тексті конструкції "Визнати грошові вимоги [КРЕДИТОР] до [БОРЖНИК]"
2. Витягни назви кредиторів та суми їх вимог
3. Визнач черги кредиторів (якщо не вказана черга - це 4-а черга)
4. Поверни результат строго у форматі JSON

ФОРМАТ ВІДПОВІДІ JSON:
{{
    "creditors": [
        {{
            "name": "Точна назва кредитора",
            "amounts": {{
                "1st_queue": 0.0,
                "2nd_queue": 0.0,
                "3rd_queue": 0.0,
                "4th_queue": 0.0,
                "5th_queue": 0.0,
                "6th_queue": 0.0
            }}
        }}
    ],
    "confidence": 0.9
}}

ТЕКСТ ДЛЯ АНАЛІЗУ:
{resolution_text}

Відповідь у форматі JSON:"""

    def send_anthropic_request(self, prompt: str, max_tokens: int = 2000) -> Dict:
        """Надсилає запит до Anthropic Claude API через офіційну бібліотеку"""

        start_time = time.time()

        for attempt in range(self.max_retries):
            try:
                # Контроль швидкості - затримка перед запитом
                if attempt > 0:
                    time.sleep(self.retry_delay)
                else:
                    time.sleep(self.rate_limit_delay)

                response = self.client.messages.create(
                    model=self.model_name,
                    max_tokens=min(max_tokens, 1000),  # Обмежуємо для контролю швидкості
                    temperature=0.1,
                    messages=[
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                )

                processing_time = time.time() - start_time

                # Витягуємо текст відповіді
                response_text = ""
                if response.content and len(response.content) > 0:
                    response_text = response.content[0].text

                return {
                    "success": True,
                    "response": response_text,
                    "processing_time": processing_time,
                    "error": None,
                    "usage": {
                        "input_tokens": response.usage.input_tokens,
                        "output_tokens": response.usage.output_tokens
                    }
                }

            except Exception as e:
                processing_time = time.time() - start_time
                error_msg = str(e)
                if attempt == self.max_retries - 1:  # Останній спроба
                    return {
                        "success": False,
                        "response": "",
                        "processing_time": processing_time,
                        "error": error_msg
                    }

        return {
            "success": False,
            "response": "",
            "processing_time": time.time() - start_time,
            "error": "Перевищено максимальну кількість спроб"
        }

    def send_llm_request(self, prompt: str, max_tokens: int = 2000) -> Dict:
        """Інтерфейс сумісності з базовим класом"""
        return self.send_anthropic_request(prompt, max_tokens)

    def unload_model(self):
        """Для API не потрібно вивантажувати модель"""
        return True


def get_anthropic_analyzer_v2() -> AnthropicAnalyzerV2:
    """Отримує екземпляр Anthropic аналізатора v2"""
    return AnthropicAnalyzerV2()


def test_anthropic_connection_v2() -> bool:
    """Тестує з"єднання з Anthropic API v2"""
    analyzer = get_anthropic_analyzer_v2()
    return analyzer.test_connection()