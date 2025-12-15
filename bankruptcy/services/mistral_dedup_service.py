"""
Сервіс для обробки резолютивних частин через другий Mistral API з дедуплікацією.
"""
import os
import json
import logging
from typing import Dict, Optional, List, Tuple
from mistralai import Mistral
from django.conf import settings
from django.db import transaction
from bankruptcy.models import Creditor, CreditorClaim, BankruptcyCase, DeduplicationLog

logger = logging.getLogger(__name__)


class MistralDeduplicationService:
    """Сервіс для аналізу резолютивних частин через другий Mistral API з дедуплікацією."""

    def __init__(self):
        # ДОДАЙТЕ СЮДИ ДРУГИЙ API КЛЮЧ
        self.api_key = "oyo1li90eL5R0yPmrlQ9OCUHwpvEmfKD"
        self.model = "ministral-8b-latest"
        self.client = Mistral(api_key=self.api_key)

    def analyze_resolutive_part_with_dedup(self, resolutive_text: str, case: BankruptcyCase) -> Dict:
        """
        Аналізує резолютивну частину з дедуплікацією.

        Args:
            resolutive_text (str): Текст резолютивної частини
            case (BankruptcyCase): Справа банкрутства

        Returns:
            Dict: Результат з статистикою дедуплікації
        """
        if not resolutive_text or not resolutive_text.strip():
            return {"error": "Порожній текст резолютивної частини"}

        # Спочатку отримуємо аналіз від Mistral
        analysis_result = self._analyze_with_mistral(resolutive_text)
        if "error" in analysis_result:
            return analysis_result

        # Визначаємо тип документа за ключовими словами
        doc_type = self._determine_document_type(resolutive_text)

        # Потім виконуємо дедуплікацію
        dedup_result = self._deduplicate_creditors(analysis_result, case, doc_type=doc_type)

        return {
            "success": True,
            "analysis": analysis_result,
            "deduplication": dedup_result
        }

    def _analyze_with_mistral(self, resolutive_text: str) -> Dict:
        """Аналіз через Mistral API з retry логікою."""
        import time

        prompt = self._create_analysis_prompt(resolutive_text)

        # Retry логіка для rate limiting
        max_retries = 3
        retry_delay = 60

        for attempt in range(max_retries):
            try:
                chat_response = self.client.chat.complete(
                    model=self.model,
                    messages=[{
                        "role": "user",
                        "content": prompt
                    }],
                    temperature=0.1,
                    max_tokens=1000
                )

                response_content = chat_response.choices[0].message.content
                logger.info(f"Mistral відповідь (процес 2): {response_content}")

                return self._parse_mistral_response(response_content)

            except Exception as e:
                error_str = str(e)
                logger.error(f"Помилка при аналізі через Mistral (спроба {attempt + 1}/{max_retries}): {e}")

                if "429" in error_str or "capacity exceeded" in error_str:
                    if attempt < max_retries - 1:
                        logger.info(f"Rate limiting виявлено. Чекаємо {retry_delay} секунд...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue

                return {"error": f"Помилка API після {attempt + 1} спроб: {error_str}"}

    def _determine_document_type(self, text: str) -> str:
        """Визначає тип документа за ключовими словами"""
        text_lower = text.lower()

        if "підсумками попереднього засідання" in text_lower or "перераховуються" in text_lower:
            return "summary"
        elif "повна версія" in text_lower or "додатково" in text_lower:
            return "full"
        else:
            return "initial"

    def _deduplicate_creditors(self, analysis_result: Dict, case: BankruptcyCase, doc_id: str = "", doc_type: str = "unknown") -> Dict:
        """
        Виконує дедуплікацію кредиторів та їх вимог.

        Returns:
            Dict: Статистика дедуплікації
        """
        if "creditors" not in analysis_result:
            return {
                "added_creditors": 0,
                "duplicates_removed": 0,
                "updated_claims": 0,
                "message": "Немає кредиторів для обробки"
            }

        added_creditors = 0
        duplicates_removed = 0
        updated_claims = 0

        with transaction.atomic():
            for creditor_data in analysis_result["creditors"]:
                if not creditor_data.get("name"):
                    continue

                creditor_name = creditor_data["name"]
                normalized_name = self._normalize_creditor_name(creditor_name)

                # Знаходимо або створюємо кредитора
                creditor, created = Creditor.objects.get_or_create(
                    name=creditor_name,
                    normalized_name=normalized_name
                )

                if created:
                    added_creditors += 1

                # Перевіряємо чи існує вже вимога цього кредитора до цієї справи
                existing_claim = CreditorClaim.objects.filter(
                    case=case,
                    creditor=creditor
                ).first()

                amounts = creditor_data.get("amounts", {})

                if existing_claim:
                    # Порівнюємо суми - якщо вони однакові, це дублікат
                    if self._are_amounts_duplicate(existing_claim, amounts):
                        duplicates_removed += 1
                        logger.info(f"Дублікат виявлено: {creditor_name} у справі {case.case_number}")

                        # Логуємо видалення дублікату
                        DeduplicationLog.objects.create(
                            case=case,
                            operation_type="duplicate_removed",
                            document_type=doc_type,
                            creditor_name=creditor_name,
                            decision_doc_id=doc_id,
                            details={"reason": "duplicate_amounts", "amounts": amounts}
                        )
                        continue
                    else:
                        # Оновлюємо існуючу вимогу більшими сумами
                        old_total = existing_claim.total_amount
                        if self._update_claim_with_larger_amounts(existing_claim, amounts):
                            updated_claims += 1
                            logger.info(f"Оновлено вимогу: {creditor_name} у справі {case.case_number}")

                            # Логуємо оновлення вимоги
                            existing_claim.refresh_from_db()
                            DeduplicationLog.objects.create(
                                case=case,
                                operation_type="claim_updated",
                                document_type=doc_type,
                                creditor_name=creditor_name,
                                old_amount=old_total,
                                new_amount=existing_claim.total_amount,
                                decision_doc_id=doc_id,
                                details={"updated_amounts": amounts}
                            )
                else:
                    # Створюємо нову вимогу
                    new_claim = CreditorClaim.objects.create(
                        case=case,
                        creditor=creditor,
                        amount_1st_queue=amounts.get("1st_queue", 0),
                        amount_2nd_queue=amounts.get("2nd_queue", 0),
                        amount_3rd_queue=amounts.get("3rd_queue", 0),
                        amount_4th_queue=amounts.get("4th_queue", 0),
                        amount_5th_queue=amounts.get("5th_queue", 0),
                        amount_6th_queue=amounts.get("6th_queue", 0),
                        llm_analysis_result=creditor_data,
                        confidence_score=analysis_result.get("confidence", 0.5)
                    )
                    added_creditors += 1

                    # Логуємо додавання кредитора
                    DeduplicationLog.objects.create(
                        case=case,
                        operation_type="creditor_added",
                        document_type=doc_type,
                        creditor_name=creditor_name,
                        new_amount=new_claim.total_amount,
                        decision_doc_id=doc_id,
                        details={"amounts": amounts, "created_new_creditor": created}
                    )

        # Логуємо обробку справи
        if added_creditors > 0 or duplicates_removed > 0 or updated_claims > 0:
            DeduplicationLog.objects.create(
                case=case,
                operation_type="case_processed",
                document_type=doc_type,
                decision_doc_id=doc_id,
                details={
                    "added_creditors": added_creditors,
                    "duplicates_removed": duplicates_removed,
                    "updated_claims": updated_claims
                }
            )

        return {
            "added_creditors": added_creditors,
            "duplicates_removed": duplicates_removed,
            "updated_claims": updated_claims,
            "message": f"Додано {added_creditors} кредиторів, видалено {duplicates_removed} дублікатів, оновлено {updated_claims} вимог"
        }

    def _are_amounts_duplicate(self, existing_claim: CreditorClaim, new_amounts: Dict) -> bool:
        """Перевіряє чи є нові суми дублікатом існуючих."""
        tolerance = 0.01  # Допустимі розбіжності в 1 копійку

        existing_amounts = {
            "1st_queue": existing_claim.amount_1st_queue or 0,
            "2nd_queue": existing_claim.amount_2nd_queue or 0,
            "3rd_queue": existing_claim.amount_3rd_queue or 0,
            "4th_queue": existing_claim.amount_4th_queue or 0,
            "5th_queue": existing_claim.amount_5th_queue or 0,
            "6th_queue": existing_claim.amount_6th_queue or 0,
        }

        for queue in existing_amounts:
            existing_amount = existing_amounts[queue]
            new_amount = new_amounts.get(queue, 0)

            # Якщо різниця більша за допустиму, це не дублікат
            if abs(existing_amount - new_amount) > tolerance:
                return False

        return True

    def _update_claim_with_larger_amounts(self, claim: CreditorClaim, new_amounts: Dict) -> bool:
        """Оновлює вимогу більшими сумами. Повертає True якщо було оновлення."""
        updated = False

        amount_fields = [
            ("amount_1st_queue", "1st_queue"),
            ("amount_2nd_queue", "2nd_queue"),
            ("amount_3rd_queue", "3rd_queue"),
            ("amount_4th_queue", "4th_queue"),
            ("amount_5th_queue", "5th_queue"),
            ("amount_6th_queue", "6th_queue"),
        ]

        for field_name, queue_key in amount_fields:
            existing_amount = getattr(claim, field_name) or 0
            new_amount = new_amounts.get(queue_key, 0)

            if new_amount > existing_amount:
                setattr(claim, field_name, new_amount)
                updated = True

        if updated:
            claim.save()

        return updated

    def _normalize_creditor_name(self, name: str) -> str:
        """Нормалізує назву кредитора для групування."""
        import re

        normalized = re.sub(
            r"\b(ТОВ|ПАТ|АТ|ПрАТ|КП|ДП|ФОП|СПД|ООО|ЗАТ|ВАТ)\b\s*",
            "",
            name,
            flags=re.IGNORECASE
        ).strip()

        normalized = re.sub(r'["\'"„""«»]', "", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()

        return normalized

    def _create_analysis_prompt(self, resolutive_text: str) -> str:
        """Створює промпт для аналізу резолютивної частини."""
        return f"""
Проаналізуй резолютивну частину українського судового рішення у справі про банкрутство та витягни всіх кредиторів та їхні грошові вимоги.

ЗАВДАННЯ:
1. Знайди в тексті конструкції "Визнати грошові вимоги [КРЕДИТОР] до [БОРЖНИК]" або "Визнати кредиторські вимоги [КРЕДИТОР] до [БОРЖНИК]"
2. Витягни назви кредиторів та суми їх вимог за чергами ТІЛЬКИ ДЛЯ ВИЗНАНИХ ВИМОГ
3. ІГНОРУЙ вимоги які відхилені, відмовлені або призначені до розгляду
4. Назви кредиторів зазвичай вказані у родовому відмінку - переведи їх у називний відмінок
5. СТАНДАРТИЗУЙ ЛАПКИ: всі лапки в назвах кредиторів замінюй на подвійні прямі лапки " "
6. Поверни результат строго у форматі JSON українською мовою

КРИТИЧНІ ПРАВИЛА ДЛЯ ДЕДУПЛІКАЦІЇ:
- Якщо це ухвала за підсумками попереднього засідання - ПОЗНАЧЬ ЦЕ в результаті
- Якщо це повна версія рішення після вступної частини - ПОЗНАЧЬ ЦЕ в результаті
- У полі "document_type" вкажи: "initial" (вступна), "full" (повна) або "summary" (підсумкова)

Формат відповіді:
{{
  "document_type": "initial|full|summary",
  "confidence": 0.8,
  "creditors": [
    {{
      "name": "Назва кредитора",
      "amounts": {{
        "1st_queue": 1000.0,
        "2nd_queue": 0,
        "3rd_queue": 0,
        "4th_queue": 5000.0,
        "5th_queue": 0,
        "6th_queue": 0
      }}
    }}
  ]
}}

ТЕКСТ ДЛЯ АНАЛІЗУ:
{resolutive_text}
"""

    def _parse_mistral_response(self, response_content: str) -> Dict:
        """Парсить JSON відповідь від Mistral."""
        try:
            # Видаляємо можливі markdown блоки
            if "```json" in response_content:
                start = response_content.find("```json") + 7
                end = response_content.rfind("```")
                response_content = response_content[start:end].strip()
            elif "```" in response_content:
                start = response_content.find("```") + 3
                end = response_content.rfind("```")
                response_content = response_content[start:end].strip()

            return json.loads(response_content)
        except json.JSONDecodeError as e:
            logger.error(f"Помилка парсингу JSON від Mistral: {e}")
            logger.error(f"Контент відповіді: {response_content}")
            return {"error": f"Помилка парсингу JSON: {str(e)}"}

    def test_connection(self) -> bool:
        """Тестує з'єднання з Mistral API."""
        try:
            chat_response = self.client.chat.complete(
                model=self.model,
                messages=[{
                    "role": "user",
                    "content": "Привіт! Це тест з'єднання."
                }],
                max_tokens=10
            )
            return True
        except Exception as e:
            logger.error(f"Помилка тестування з'єднання з другим API: {e}")
            return False