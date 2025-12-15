"""
Сервіс для обробки резолютивних частин судових рішень через Mistral AI.
"""
import os
import json
import logging
from typing import Dict, Optional, List
from mistralai import Mistral
from django.conf import settings

logger = logging.getLogger(__name__)


class MistralAnalysisService:
    """Сервіс для аналізу резолютивних частин через Mistral AI."""

    def __init__(self):
        self.api_key = "HeZDpzwyGunS0Vh6YHZ9JCvnSE81ntSM"
        self.model = "ministral-8b-latest"  # Використовуємо найшвидшу та найдешевшу модель
        self.client = Mistral(api_key=self.api_key)

    def analyze_resolutive_part(self, resolutive_text: str) -> Dict:
        """
        Аналізує резолютивну частину судового рішення та витягує структуровану інформацію.

        Args:
            resolutive_text (str): Текст резолютивної частини

        Returns:
            Dict: Структурована інформація про кредиторів та суми
        """
        if not resolutive_text or not resolutive_text.strip():
            return {"error": "Порожній текст резолютивної частини"}

        prompt = self._create_analysis_prompt(resolutive_text)

        import time

        # Retry логіка для rate limiting
        max_retries = 3
        retry_delay = 60  # 1 хвилина затримки при rate limiting

        for attempt in range(max_retries):
            try:
                chat_response = self.client.chat.complete(
                    model=self.model,
                    messages=[{
                        "role": "user",
                        "content": prompt
                    }],
                    temperature=0.1,  # Низька температура для стабільності
                    max_tokens=1000  # Зменшуємо для швидкості
                )

                response_content = chat_response.choices[0].message.content
                logger.info(f"Mistral відповідь: {response_content}")

                # Спробуємо парсити JSON відповідь
                return self._parse_mistral_response(response_content)

            except Exception as e:
                error_str = str(e)
                logger.error(f"Помилка при аналізі через Mistral (спроба {attempt + 1}/{max_retries}): {e}")

                # Якщо це rate limiting - чекаємо довше
                if "429" in error_str or "capacity exceeded" in error_str:
                    if attempt < max_retries - 1:  # Не чекати на останній спробі
                        logger.info(f"Rate limiting виявлено. Чекаємо {retry_delay} секунд...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Експоненціальне збільшення затримки
                        continue

                # Для інших помилок - повертаємо відразу
                return {"error": f"Помилка API після {attempt + 1} спроб: {error_str}"}

    def _create_analysis_prompt(self, resolutive_text: str) -> str:
        """Створює промпт для аналізу резолютивної частини."""
        return f"""
Проаналізуй резолютивну частину українського судового рішення у справі про банкрутство та витягни всіх кредиторів та їхні грошові вимоги.

ЗАВДАННЯ:
1. Знайди в тексті конструкції "Визнати грошові вимоги [КРЕДИТОР] до [БОРЖНИК]" або "Визнати кредиторські вимоги [КРЕДИТОР] до [БОРЖНИК]"
   ВАЖЛИВО: ці фрази можуть з'являтися НЕ ТІЛЬКИ на початку резолютивної частини, а й ПОСЕРЕДИНІ тексту
2. Витягни назви кредиторів та суми їх вимог за чергами ТІЛЬКИ ДЛЯ ВИЗНАНИХ ВИМОГ
3. ІГНОРУЙ вимоги які відхилені, відмовлені або призначені до розгляду
4. Назви кредиторів зазвичай вказані у родовому відмінку - переведи їх у називний відмінок
5. СТАНДАРТИЗУЙ ЛАПКИ: всі лапки в назвах кредиторів замінюй на подвійні прямі лапки " "
6. Визнач черги кредиторів (якщо не вказана черга - це 4-а черга)
7. Судовий збір НЕ є окремим кредитором - він належить кредитору, чиї вимоги визнаються
8. Поверни результат строго у форматі JSON українською мовою

КРИТИЧНІ ПРАВИЛА:
- Після "Визнати грошові вимоги" або "Визнати кредиторські вимоги" = КРЕДИТОР (витягуй цього + ВСІ його суми)
- Після "до" = БОРЖНИК (НІКОЛИ не витягуй як кредитора)
- ТІЛЬКИ ВИЗНАНІ ВИМОГИ: шукай слова "визнати", "задовольнити", "включити до реєстру" (ці слова можуть бути в БУДЬ-ЯКІЙ частині тексту, не тільки на початку)
- НЕ ВИТЯГУЙ: "відмовити", "відхилити", "залишити без розгляду", "призначити до розгляду"
- Черги: перша черга, друга черга, третя черга, четверта черга, п'ята черга, шоста черга
- В одній резолютивці може бути декілька кредиторів і декілька сум однієї черги
- Суми витягуй в гривнях (грн)

ВАЖЛИВО про судовий збір та винагороду арбітражного керуючого:
- Судовий збір → ЗАВЖДИ помісти в 1-шу чергу ДО ТОГО КРЕДИТОРА
- Винагорода арбітражного керуючого → ЗАВЖДИ помісти в 1-шу чергу ДО ТОГО КРЕДИТОРА
- Інші суми без зазначеної черги → помісти в 4-ту чергу ДО ТОГО КРЕДИТОРА
- Якщо черга зазначена явно → використовуй цю чергу ДО ТОГО КРЕДИТОРА

Приклади:
1) "Визнати грошові вимоги ПриватБанку до ТОВ "Компанія": судовий збір 5000 грн, основний борг 100000 грн"
   → ОДИН кредитор: ПриватБанк з 1-ша черга = 5000 (судовий збір) + 4-та черга = 100000 (основний борг)

2) "Визнати грошові вимоги Банку до ТОВ "Компанія": перша черга 10000 грн, четверта черга 50000 грн"
   → ОДИН кредитор: Банк з 1-ша черга = 10000 + 4-та черга = 50000

3) "Визнати кредиторські вимоги АТ "Компанія" до ТОВ "Борг""
   → ОДИН кредитор: АТ "Компанія" (всі лапки стандартизовані до " ")

ФОРМАТ ВІДПОВІДІ JSON:
{{
  "creditors": [
    {{
      "name": "Назва кредитора у називному відмінку",
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
{resolutive_text}

ЗАБОРОНЕНО:
- Будь-які пояснення, коментарі або додатковий текст
- Markdown форматування (```json```)
- Поля "explanation", "note", "comment" в JSON
- Текст до або після JSON блоку

ДОЗВОЛЕНО ТІЛЬКИ:
- Чистий JSON без додаткових символів
- Тільки поля: creditors, confidence

Відповідь у форматі JSON:"""

    def _parse_mistral_response(self, response_content: str) -> Dict:
        """Парсить відповідь від Mistral та повертає структуровані дані."""
        try:
            # Очищаємо відповідь від можливого markdown форматування
            cleaned_response = response_content.strip()
            if cleaned_response.startswith("```json"):
                cleaned_response = cleaned_response[7:]
            if cleaned_response.endswith("```"):
                cleaned_response = cleaned_response[:-3]

            # Знаходимо JSON частину - беремо все до першого пояснення
            lines = cleaned_response.split("\n")
            json_lines = []
            in_json = False
            brace_count = 0

            for line in lines:
                line = line.strip()
                if line.startswith("{"):
                    in_json = True
                    brace_count += line.count("{") - line.count("}")
                    json_lines.append(line)
                elif in_json:
                    brace_count += line.count("{") - line.count("}")
                    json_lines.append(line)
                    if brace_count <= 0:
                        break

            if json_lines:
                cleaned_response = "\n".join(json_lines)

            cleaned_response = cleaned_response.strip()

            # Парсимо JSON
            result = json.loads(cleaned_response)

            # Валідуємо структуру
            if not isinstance(result, dict):
                return {"error": "Невалідна структура відповіді"}

            # Перевіряємо наявність обов'язкових полів
            if "creditors" not in result:
                result["creditors"] = []

            return result

        except json.JSONDecodeError as e:
            logger.error(f"Помилка парсингу JSON відповіді: {e}")
            logger.error(f"Відповідь: {response_content}")
            return {
                "error": "Помилка парсингу JSON",
                "raw_response": response_content
            }
        except Exception as e:
            logger.error(f"Неочікувана помилка парсингу: {e}")
            return {"error": f"Помилка обробки: {str(e)}"}

    def analyze_batch(self, resolutive_texts: List[str]) -> List[Dict]:
        """
        Обробляє масив резолютивних частин.

        Args:
            resolutive_texts (List[str]): Список текстів для аналізу

        Returns:
            List[Dict]: Результати аналізу для кожного тексту
        """
        results = []
        for i, text in enumerate(resolutive_texts):
            logger.info(f"Обробка резолютивної частини {i+1}/{len(resolutive_texts)}")
            result = self.analyze_resolutive_part(text)
            results.append(result)

        return results

    def test_connection(self) -> bool:
        """Тестує з'єднання з Mistral API."""
        try:
            response = self.client.chat.complete(
                model=self.model,
                messages=[{
                    "role": "user",
                    "content": "Привіт! Відповідь одним словом: OK"
                }],
                max_tokens=10
            )
            return True
        except Exception as e:
            logger.error(f"Тест з'єднання не пройшов: {e}")
            return False