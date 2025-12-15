# -*- coding: utf-8 -*-
"""
Аналізатор резолютивних частин за допомогою Anthropic Claude API
"""

import requests
import json
import time
from typing import Dict, List, Optional, Tuple
from django.utils import timezone
from .models import LLMAnalysisLog, Creditor, CreditorClaim
from .llm_analyzer import LLMAnalyzer

class AnthropicAnalyzer(LLMAnalyzer):
    """Аналізатор резолютивних частин за допомогою Anthropic Claude API"""
    
    def __init__(self, api_key: str = "sk--mxWRMIzOJpGNxCqziVodN3CSyCj50497aoZzESKWZ1wpREhNulc35h7C_Rx8-iPX4vpB5nPHkskkkVzg6JPzw"):
        self.api_key = api_key
        self.base_url = "https://api.langdock.com/anthropic/eu"
        self.api_url = f"{self.base_url}/v1/messages"
        self.model_name = "claude-sonnet-4-20250514"
        
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
        """Надсилає запит до Anthropic Claude API"""
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01"
        }
        
        payload = {
            "model": self.model_name,
            "max_tokens": min(max_tokens, 1000),  # Обмежуємо для контролю швидкості
            "temperature": 0.1,
            "messages": [
                {
                    "role": "user", 
                    "content": prompt
                }
            ]
        }
        
        start_time = time.time()
        
        for attempt in range(self.max_retries):
            try:
                # Контроль швидкості - затримка перед запитом
                if attempt > 0:
                    time.sleep(self.retry_delay)
                else:
                    time.sleep(self.rate_limit_delay)
                
                response = requests.post(
                    self.api_url,
                    headers=headers,
                    json=payload,
                    timeout=120  # 2 хвилини timeout
                )
                
                processing_time = time.time() - start_time
                
                if response.status_code == 200:
                    result = response.json()
                    
                    # Витягуємо текст відповіді
                    content = result.get("content", [])
                    response_text = ""
                    if content and len(content) > 0:
                        response_text = content[0].get("text", "")
                    
                    return {
                        "success": True,
                        "response": response_text,
                        "processing_time": processing_time,
                        "error": None,
                        "usage": result.get("usage", {})
                    }
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    if attempt == self.max_retries - 1:  # Останній спроба
                        return {
                            "success": False,
                            "response": "",
                            "processing_time": processing_time,
                            "error": error_msg
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


def get_anthropic_analyzer() -> AnthropicAnalyzer:
    """Отримує екземпляр Anthropic аналізатора"""
    return AnthropicAnalyzer()


def test_anthropic_connection() -> bool:
    """Тестує з"єднання з Anthropic API"""
    analyzer = get_anthropic_analyzer()
    return analyzer.test_connection()