# -*- coding: utf-8 -*-
"""
Сервіс для аналізу резолютивних частин судових рішень 
за допомогою мовної моделі Mistral 7B через Ollama
"""

import subprocess
import json
import time
import re
from typing import Dict, List, Optional, Tuple
from django.utils import timezone
from .models import LLMAnalysisLog, Creditor, CreditorClaim

class LLMAnalyzer:
    """Аналізатор резолютивних частин за допомогою LLM"""
    
    def __init__(self, model_name: str = "mistral:latest"):
        self.model_name = model_name
    
    def test_connection(self) -> bool:
        """Тестує з"єднання з Ollama"""
        try:
            result = subprocess.run(["ollama", "list"], capture_output=True, text=True, timeout=10)
            return result.returncode == 0
        except:
            return False
    
    def create_creditor_extraction_prompt(self, resolution_text: str) -> str:
        """Створює промпт для витягування кредиторів та сум по чергам"""
        prompt = f"""Extract creditors and their debt amounts by priority queues from Ukrainian court resolution text.

Find pattern: "Визнати грошові вимоги [CREDITOR] до [DEBTOR]" and then extract ALL amounts for that creditor.

CRITICAL RULES:
- After "Визнати грошові вимоги" = CREDITOR (extract this + ALL their amounts)
- After "до" = DEBTOR (NEVER extract this as creditor)
- Priority queues: перша черга, друга черга, третя черга, четверта черга, п"ята черга, шоста черга
- One resolution can have multiple creditors and multiple amounts per queue
- Extract amounts in UAH (грн)

IMPORTANT: Court fee ("судовий збір") is NOT a separate creditor!
- Court fee belongs TO THE CREDITOR whose claims are recognized
- Court fee without explicit queue → put in 1st queue FOR THAT CREDITOR
- Other amounts without explicit queue → put in 4th queue FOR THAT CREDITOR
- If explicit queue mentioned → use that queue FOR THAT CREDITOR

Examples:
1) "Визнати грошові вимоги ПриватБанк до ТОВ Компанія: судовий збір 5000 грн, основний борг 100000 грн"
   → ONE creditor: ПриватБанк with 1st queue = 5000 (court fee) + 4th queue = 100000 (main debt)

2) "Визнати грошові вимоги Банк до Компанія: перша черга 10000 грн, четверта черга 50000 грн"
   → ONE creditor: Банк with 1st queue = 10000 + 4th queue = 50000

Return JSON only:
{{
    "creditors": [
        {{
            "name": "ПриватБанк",
            "amounts": {{
                "1st_queue": 5000.0,
                "2nd_queue": 0.0,
                "3rd_queue": 0.0,
                "4th_queue": 100000.0,
                "5th_queue": 0.0,
                "6th_queue": 0.0
            }}
        }}
    ],
    "confidence": 0.9
}}

Text to analyze:
{resolution_text}

JSON:"""
        return prompt
    
    def unload_model(self):
        """Вивантажує модель з пам"яті для звільнення ресурсів"""
        try:
            # Використовуємо ollama stop для вивантаження моделі
            result = subprocess.run(["ollama", "stop", self.model_name],
                                  capture_output=True, text=True, timeout=10)
            return result.returncode == 0
        except:
            return False
    
    def send_llm_request(self, prompt: str, max_tokens: int = 2000) -> Dict:
        """Надсилає запит до Ollama через CLI"""
        start_time = time.time()

        try:
            # Використовуємо ollama run безпосередньо через subprocess
            cmd = ["ollama", "run", self.model_name]

            result = subprocess.run(
                cmd,
                input=prompt,
                capture_output=True,
                text=True,
                timeout=180  # 3 хвилини timeout
            )

            processing_time = time.time() - start_time

            if result.returncode == 0:
                response_text = result.stdout.strip()
                return {
                    "success": True,
                    "response": response_text,
                    "processing_time": processing_time,
                    "error": None
                }
            else:
                error_msg = result.stderr.strip() if result.stderr else f"Exit code: {result.returncode}"
                return {
                    "success": False,
                    "response": "",
                    "processing_time": processing_time,
                    "error": error_msg
                }

        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "response": "",
                "processing_time": time.time() - start_time,
                "error": "Timeout: команда виконувалась більше 2 хвилин"
            }
        except Exception as e:
            return {
                "success": False,
                "response": "",
                "processing_time": time.time() - start_time,
                "error": str(e)
            }
    
    def parse_llm_response(self, response_text: str) -> Dict:
        """Парсить відповідь LLM та витягує JSON"""
        try:
            # Шукаємо JSON у відповіді
            json_match = re.search(r"\{.*\}", response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                return json.loads(json_str)
            else:
                # Якщо JSON не знайдено, створюємо порожню структуру
                return {
                    "creditors": [],
                    "confidence": 0.0,
                    "error": "JSON не знайдено у відповіді"
                }
        except json.JSONDecodeError as e:
            return {
                "creditors": [],
                "confidence": 0.0,
                "error": f"Помилка парсингу JSON: {str(e)}"
            }
    
    def normalize_creditor_name(self, name: str) -> str:
        """Нормалізує назву кредитора для групування"""
        if not name:
            return ""
        
        # Видаляємо лишні символи та пробіли
        name = re.sub(r"\s+", " ", name.strip())
        
        # Видаляємо типові префікси/суфікси
        patterns_to_remove = [
            r"\(код ЄДРПОУ[^)]*\)",
            r"\(ідентифікаційний код[^)]*\)",
            r"\([0-9\s]+\)",  # коди в дужках
            r",\s*м\.\s*[А-Яа-яІіЇїЄє\s]+$",  # адреси міст
            r",\s*[А-Яа-яІіЇїЄє\s]+область[^,]*$",  # області
            r"код ЄДРПОУ.*$",
        ]
        
        normalized = name
        for pattern in patterns_to_remove:
            normalized = re.sub(pattern, "", normalized, flags=re.IGNORECASE)
        
        # Очищаємо від зайвих пробілів та ком
        normalized = re.sub(r"\s*,\s*$", "", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        
        return normalized
    
    def convert_amount_to_float(self, amount_str: str) -> float:
        """Конвертує строкову суму в float"""
        if not amount_str:
            return 0.0
        
        try:
            # Видаляємо всі символи крім цифр, коми, крапки
            cleaned = re.sub(r"[^\d,.]", "", str(amount_str))
            
            # Замінюємо кому на крапку для десяткових
            if "," in cleaned and "." in cleaned:
                # Якщо є і кома і крапка, кома - тисячі, крапка - десяткові
                cleaned = cleaned.replace(",", "")
            elif "," in cleaned:
                # Якщо тільки кома, перевіряємо контекст
                parts = cleaned.split(",")
                if len(parts) == 2 and len(parts[1]) <= 2:
                    # Ймовірно десяткові (123,45)
                    cleaned = cleaned.replace(",", ".")
                else:
                    # Ймовірно тисячі (1,000,000)
                    cleaned = cleaned.replace(",", "")
            
            return float(cleaned) if cleaned else 0.0
            
        except:
            return 0.0
    
    def analyze_resolution_text(self, resolution_text: str, case=None) -> Dict:
        """
        Аналізує резолютивну частину та витягує дані кредиторів
        
        Args:
            resolution_text: Текст резолютивної частини
            case: Справа банкрутства (опціонально)
            
        Returns:
            Dict з результатами аналізу
        """
        
        # Створюємо лог
        log_entry = LLMAnalysisLog.objects.create(
            case=case,
            analysis_type="creditor_extraction",
            status="processing",
            input_text=resolution_text[:5000]  # Обмежуємо довжину для логу
        )
        
        try:
            # Створюємо промпт
            prompt = self.create_creditor_extraction_prompt(resolution_text)
            
            # Надсилаємо запит
            llm_result = self.send_llm_request(prompt)
            
            if not llm_result["success"]:
                log_entry.status = "failed"
                log_entry.error_message = llm_result["error"]
                log_entry.processing_time_seconds = llm_result["processing_time"]
                log_entry.completed_at = timezone.now()
                log_entry.save()
                
                return {
                    "success": False,
                    "error": llm_result["error"],
                    "creditors": []
                }
            
            # Парсимо відповідь
            parsed_data = self.parse_llm_response(llm_result["response"])
            
            # Оновлюємо лог
            log_entry.output_text = llm_result["response"][:5000]
            log_entry.processing_time_seconds = llm_result["processing_time"]
            log_entry.token_count_input = len(prompt.split())
            log_entry.token_count_output = len(llm_result["response"].split())
            log_entry.completed_at = timezone.now()
            
            if "error" in parsed_data:
                log_entry.status = "failed"
                log_entry.error_message = parsed_data["error"]
            else:
                log_entry.status = "completed"
            
            log_entry.save()
            
            # Вивантажуємо модель після успішного аналізу для економії ресурсів
            self.unload_model()
            
            return {
                "success": True,
                "creditors": parsed_data.get("creditors", []),
                "confidence": parsed_data.get("confidence", 0.0),
                "llm_response": llm_result["response"],
                "processing_time": llm_result["processing_time"],
                "log_id": log_entry.id
            }
            
        except Exception as e:
            # Оновлюємо лог з помилкою
            log_entry.status = "failed"
            log_entry.error_message = str(e)
            log_entry.completed_at = timezone.now()
            log_entry.save()
            
            return {
                "success": False,
                "error": str(e),
                "creditors": []
            }
    
    def save_creditor_claims(self, case, analysis_result: Dict) -> List[CreditorClaim]:
        """Зберігає результати аналізу в базу даних (кредитори з сумами по чергам)"""
        created_claims = []

        if not analysis_result.get("success", False):
            return created_claims

        for creditor_data in analysis_result.get("creditors", []):
            creditor_name = creditor_data.get("name", "").strip()
            if not creditor_name:
                continue

            # Створюємо або отримуємо кредитора
            normalized_name = self.normalize_creditor_name(creditor_name)
            creditor, created = Creditor.objects.get_or_create(
                name=creditor_name,
                defaults={"normalized_name": normalized_name}
            )

            # Конвертуємо суми
            amounts = creditor_data.get("amounts", {})
            amount_1st = self.convert_amount_to_float(amounts.get("1st_queue", 0))
            amount_2nd = self.convert_amount_to_float(amounts.get("2nd_queue", 0))
            amount_3rd = self.convert_amount_to_float(amounts.get("3rd_queue", 0))
            amount_4th = self.convert_amount_to_float(amounts.get("4th_queue", 0))
            amount_5th = self.convert_amount_to_float(amounts.get("5th_queue", 0))
            amount_6th = self.convert_amount_to_float(amounts.get("6th_queue", 0))

            # Створюємо або оновлюємо вимогу кредитора З СУМАМИ
            claim, created = CreditorClaim.objects.get_or_create(
                case=case,
                creditor=creditor,
                defaults={
                    "amount_1st_queue": amount_1st if amount_1st > 0 else None,
                    "amount_2nd_queue": amount_2nd if amount_2nd > 0 else None,
                    "amount_3rd_queue": amount_3rd if amount_3rd > 0 else None,
                    "amount_4th_queue": amount_4th if amount_4th > 0 else None,
                    "amount_5th_queue": amount_5th if amount_5th > 0 else None,
                    "amount_6th_queue": amount_6th if amount_6th > 0 else None,
                    "llm_analysis_result": analysis_result,
                    "confidence_score": analysis_result.get("confidence", 0.0)
                }
            )

            if not created:
                # Оновлюємо існуючу вимогу
                claim.amount_1st_queue = amount_1st if amount_1st > 0 else None
                claim.amount_2nd_queue = amount_2nd if amount_2nd > 0 else None
                claim.amount_3rd_queue = amount_3rd if amount_3rd > 0 else None
                claim.amount_4th_queue = amount_4th if amount_4th > 0 else None
                claim.amount_5th_queue = amount_5th if amount_5th > 0 else None
                claim.amount_6th_queue = amount_6th if amount_6th > 0 else None
                claim.llm_analysis_result = analysis_result
                claim.confidence_score = analysis_result.get("confidence", 0.0)
                claim.save()

            created_claims.append(claim)

        return created_claims


def get_analyzer() -> LLMAnalyzer:
    """Отримує екземпляр аналізатора"""
    return LLMAnalyzer()


def test_llm_connection() -> bool:
    """Тестує з"єднання з LLM"""
    analyzer = get_analyzer()
    return analyzer.test_connection()