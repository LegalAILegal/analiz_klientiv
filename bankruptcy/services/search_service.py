import os
import re
import time
import requests
import logging
import tempfile
import threading
import queue
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

try:
    import striprtf.striprtf as striprtf
    HAS_STRIPRTF = True
except ImportError:
    HAS_STRIPRTF = False

from django.conf import settings
from django.utils import timezone
from django.db import models, connection
from bankruptcy.models import TrackedCourtDecision, TrackedBankruptcyCase
from bankruptcy.utils.fast_court_search import FastCourtSearch

logger = logging.getLogger(__name__)

# Патерни для пошуку резолютивної частини (з SR_AI config.py)
RESOLUTION_PATTERNS = [
    r"УХВАЛИВ:(.*)",
    r"УХВАЛИВ:(.*)",
    r"УХВАЛИВ:(.*)",
    r"УХВАЛИВ :(.*)",
    r"У Х В А Л И В :(.*)",
    r"У Х В А Л И В(.*)",
    r"У Х В А Л И В:(.*)",
    r"ПОСТАНОВИВ:(.*)",
    r"ПОСТАНОВИВ :(.*)",
    r"П О С Т А Н О В И В :(.*)",
    r"П О С Т А Н О В И В(.*)",
    r"П О С Т А Н О В И В:(.*)",
    r"ВИРІШИВ:(.*)",
    r"ВИРІШИВ :(.*)",
    r"В И Р І Ш И В :(.*)",
    r"В И Р І Ш И В:(.*)",
]

# Налаштування завантаження (з SR_AI config.py)
DOWNLOAD_TIMEOUT = 15
MAX_RETRIES = 2  
REQUEST_DELAY = 0.1
TEMP_DIR = os.path.join(settings.BASE_DIR, "temp_documents")
os.makedirs(TEMP_DIR, exist_ok=True)


class DocumentResolutionExtractor:
    """
    Сервіс для витягування резолютивних частин з судових документів.
    Реалізує алгоритми з SR_AI системи.
    """
    
    def __init__(self):
        self.temp_dir = TEMP_DIR
        # Використовуємо простіший requests session (як в SR_AI utils.py)
        self.session = requests.Session()
        self.session.trust_env = False
        
        # Еталонні рішення з SR_AI: попереднє завантаження та черга
        self.preload_queue = queue.Queue(maxsize=100)  # Черга для попереднього завантаження
        self.preload_thread = None
        self.stop_preload_event = threading.Event()
        self.file_locks = {}  # Словник блокувань для кожного файлу
        self.lock_for_locks = threading.Lock()  # Блокування для словника блокувань
        self.document_cache = {}  # Кеш завантажених документів
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
        })
    
    def start_preloading(self, documents):
        """
        Починає попереднє завантаження документів у фоновому режимі (з еталонного SR_AI).
        """
        if not documents:
            return
            
        def preload_worker():
            for doc in documents:
                if self.stop_preload_event.is_set():
                    break
                    
                doc_url = getattr(doc, "doc_url", "") if hasattr(doc, "doc_url") else doc.get("doc_url", "")
                doc_id = getattr(doc, "doc_id", "") if hasattr(doc, "doc_id") else doc.get("doc_id", "")
                
                if doc_url and doc_url != "nan" and doc_url.startswith("http"):
                    try:
                        # Завантажуємо документ і додаємо його в чергу
                        file_path = self.download_document(doc_url, doc_id)
                        if file_path:
                            self.preload_queue.put((doc_id, file_path), block=False)
                    except queue.Full:
                        # Якщо черга заповнена, пропускаємо документ
                        pass
                    except Exception as e:
                        logger.debug(f"Помилка при попередньому завантаженні документа {doc_id}: {e}")
        
        # Запускаємо фоновий потік для попереднього завантаження
        self.stop_preload_event.clear()
        self.preload_thread = threading.Thread(target=preload_worker)
        self.preload_thread.daemon = True
        self.preload_thread.start()
        logger.info(f"Запущено фонове попереднє завантаження документів...")
    
    def stop_preloading(self):
        """
        Зупиняє процес попереднього завантаження (з еталонного SR_AI).
        """
        if self.preload_thread and self.preload_thread.is_alive():
            self.stop_preload_event.set()
            self.preload_thread.join(timeout=1.0)
            logger.info("Фонове попереднє завантаження зупинено.")
    
    def get_preloaded_document(self, doc_id):
        """
        Отримує попередньо завантажений документ з черги (з еталонного SR_AI).
        Повертає шлях до файлу або None, якщо документ не знайдено.
        """
        try:
            # Перевіряємо чергу без блокування
            for _ in range(self.preload_queue.qsize()):
                id, path = self.preload_queue.get(block=False)
                if id == doc_id:
                    return path
                else:
                    # Повертаємо інші документи назад у чергу
                    self.preload_queue.put((id, path), block=False)
        except (queue.Empty, Exception):
            pass
        
        return None
    
    def download_document(self, url: str, doc_id: str) -> Optional[str]:
        """
        Завантажує документ за URL та зберігає в тимчасовий файл.
        Алгоритм з SR_AI utils.py
        """
        if not url or url == "nan":
            return None

        # Створюємо унікальне ім"я файлу з розширенням .rtf (з еталонного SR_AI)
        # Додаємо унікальний ідентифікатор для уникнення конфліктів
        unique_suffix = str(uuid.uuid4())[:8]
        file_path = os.path.join(self.temp_dir, f"document_{doc_id}_{unique_suffix}.rtf")
        
        # Додаємо мінімальну випадкову затримку перед запитом
        import random
        time.sleep(REQUEST_DELAY + random.uniform(0, 0.2))
        
        # Механізм повторних спроб з експоненційною затримкою
        retries = 0
        max_retries = MAX_RETRIES
        
        while retries <= max_retries:
            try:
                # Додаємо невелику випадкову затримку для уникнення збігів запитів
                if retries > 0:
                    delay = min(15, (1.2 ** retries) + random.uniform(0, 1))
                    time.sleep(delay)
                    logger.info(f"Спроба #{retries+1} завантаження документа {doc_id}... (затримка: {delay:.2f}с)")
                
                response = self.session.get(url, timeout=DOWNLOAD_TIMEOUT)
                response.raise_for_status()

                # Перевіряємо, чи відповідь не порожня
                if not response.content:
                    raise Exception("Отримано порожній вміст документа")

                # Зберігаємо вміст у файл з покращеною обробкою помилок доступу
                save_attempts = 0
                max_save_attempts = 3
                while save_attempts < max_save_attempts:
                    try:
                        with open(file_path, "wb") as f:
                            f.write(response.content)
                        break  # Успішно збережено
                    except PermissionError:
                        save_attempts += 1
                        if save_attempts >= max_save_attempts:
                            # Якщо всі спроби невдалі, створюємо файл з унікальним іменем
                            unique_id = str(int(time.time())) + str(random.randint(1000, 9999))
                            alt_file_path = os.path.join(self.temp_dir, f"document_{doc_id}_{unique_id}.rtf")
                            try:
                                with open(alt_file_path, "wb") as f:
                                    f.write(response.content)
                                file_path = alt_file_path
                                break
                            except Exception as e:
                                logger.error(f"Критична помилка збереження файлу {doc_id}: {e}")
                                raise Exception(f"Не вдалося зберегти файл після {max_save_attempts} спроб")
                        else:
                            # Чекаємо перед наступною спробою
                            time.sleep(0.3 * save_attempts)
                    except Exception as e:
                        logger.error(f"Неочікувана помилка збереження файлу {doc_id}: {e}")
                        raise
                    
                # Даємо час файловій системі для завершення запису
                time.sleep(0.1)
                
                # Перевіряємо, чи файл було успішно створено з повторними спробами
                file_check_attempts = 0
                max_check_attempts = 3
                while file_check_attempts < max_check_attempts:
                    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                        break
                    file_check_attempts += 1
                    if file_check_attempts < max_check_attempts:
                        logger.warning(f"Файл {file_path} ще не готовий, чекаємо... (спроба {file_check_attempts})")
                        time.sleep(0.2)
                    else:
                        raise Exception("Файл не було створено або він порожній після кількох спроб перевірки")

                logger.info(f"Документ {doc_id} успішно завантажено: {file_path}")
                return file_path
                
            except requests.exceptions.Timeout:
                # Для таймаутів збільшуємо час очікування з кожною спробою
                retries += 1
                error_msg = f"Таймаут при завантаженні документа {doc_id} (спроба {retries}/{max_retries+1})"
                logger.warning(error_msg)
                
                if retries > max_retries:
                    error_msg = f"Вичерпано максимальну кількість спроб завантаження документа {doc_id}"
                    logger.error(error_msg)
                    break
                    
            except requests.exceptions.HTTPError as e:
                # Для HTTP помилок (404, 500 тощо)
                error_msg = f"HTTP помилка при завантаженні документа {doc_id}: {e}"
                logger.error(error_msg)
                break
                
            except Exception as e:
                error_msg = f"Помилка при завантаженні документа {doc_id}: {e}"
                logger.error(error_msg)
                break
        
        return None
    
    def extract_resolution_text(self, file_path: str, judgment_name: str = "") -> str:
        """
        Витягує резолютивну частину з RTF документа.
        Точний алгоритм з SR_AI utils.py з покращеною обробкою помилок
        """
        if not file_path:
            logger.warning("Шлях до файлу порожній")
            return "Не вдалося отримати документ: шлях до файлу порожній"
            
        # Розширена перевірка існування файлу з повторними спробами
        max_file_check_attempts = 5
        for check_attempt in range(max_file_check_attempts):
            if os.path.exists(file_path):
                logger.info(f"Файл {file_path} знайдено на спробі {check_attempt + 1}")
                break
            
            if check_attempt < max_file_check_attempts - 1:
                wait_time = 0.5 + (check_attempt * 0.3)
                logger.warning(f"Файл {file_path} не знайдено, чекаємо {wait_time:.1f}с (спроба {check_attempt + 1}/{max_file_check_attempts})")
                time.sleep(wait_time)
            else:
                # Остання спроба - файл справді не існує
                error_msg = f"Файл не знайдено після {max_file_check_attempts} спроб: {file_path}"
                logger.error(error_msg)
                return "Не вдалося отримати документ: файл недоступний або був видалений системою безпеки"

        # Додаємо затримку перед доступом до файлу
        time.sleep(0.2)

        try:
            # Читаємо RTF файл з повторними спробами при помилках доступу
            rtf_text = None
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        rtf_text = f.read()
                    break  # Успішно прочитали файл
                except PermissionError as e:
                    if attempt < max_attempts - 1:
                        # Якщо це не остання спроба, чекаємо і пробуємо знову
                        delay = 0.5 * (attempt + 1)
                        logger.warning(f"Файл {file_path} заблокований, повторна спроба через {delay} секунд...")
                        time.sleep(delay)
                    else:
                        # Якщо всі спроби невдалі, повертаємо помилку
                        error_msg = f"Не вдалося отримати доступ до файлу: {str(e)}"
                        logger.error(error_msg)
                        return error_msg

            if not rtf_text:
                return "Не вдалося отримати документ: помилка читання файлу"

            # Конвертуємо RTF в простий текст
            try:
                if HAS_STRIPRTF:
                    plain_text = striprtf.rtf_to_text(rtf_text)
                else:
                    logger.warning("striprtf не встановлено, використовуємо базову обробку")
                    # Базова обробка RTF (як в SR_AI)
                    plain_text = re.sub(r"\\[a-z0-9]+", " ", rtf_text)
                    plain_text = re.sub(r"\{|\}|\\|\n", " ", plain_text)
                    plain_text = re.sub(r"\s+", " ", plain_text)
            except Exception as rtf_error:
                error_msg = f"Помилка при конвертації RTF: {rtf_error}"
                logger.error(error_msg)
                # Альтернативний спосіб обробки
                plain_text = re.sub(r"\\[a-z0-9]+", " ", rtf_text)
                plain_text = re.sub(r"\{|\}|\\|\n", " ", plain_text)
                plain_text = re.sub(r"\s+", " ", plain_text)

            # Пробуємо знайти будь-який з патернів резолютивної частини
            for pattern in RESOLUTION_PATTERNS:
                matches = re.search(pattern, plain_text, re.DOTALL | re.IGNORECASE)
                if matches:
                    # Видаляємо зайві пробіли та повертаємо результат
                    resolution_text = re.sub(r"\s+", " ", matches.group(1).strip())
                    logger.info(f"Знайдено резолютивну частину за патерном: {pattern}")
                    return resolution_text

            return "Резолютивна частина не знайдена"
            
        except Exception as e:
            error_msg = f"Помилка при обробці документа {file_path}: {e}"
            logger.error(error_msg)
            return f"Помилка обробки: {str(e)}"
        finally:
            # Безпечно видаляємо файл після обробки з повторними спробами
            self._safe_delete_file(file_path)
    
    def _safe_delete_file(self, file_path: str, max_attempts: int = 3):
        """
        Безпечно видаляє файл з повторними спробами.
        Алгоритм з SR_AI utils.py
        """
        for attempt in range(max_attempts):
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    return True
            except Exception as e:
                if attempt < max_attempts - 1:
                    # Якщо це не остання спроба, чекаємо і пробуємо знову
                    delay = 0.5 * (attempt + 1)
                    logger.warning(f"Не вдалося видалити файл {file_path}: {e}, спроба {attempt+1}, чекаємо {delay}s")
                    time.sleep(delay)
                else:
                    # Якщо всі спроби невдалі, логуємо помилку
                    error_msg = f"Не вдалося видалити файл {file_path} після {max_attempts} спроб: {e}"
                    logger.error(error_msg)
                    return False
    
    def process_tracked_court_decision(self, decision: TrackedCourtDecision, retry_attempt: int = 0) -> bool:
        """
        Обробляє TrackedCourtDecision та витягує резолютивну частину з автоматичними повторними спробами.
        Повертає True якщо резолютивна частина була успішно витягнута.
        """
        max_retries = 5
        
        # Перевіряємо чи є URL або чи резолютивна частина вже витягнута
        if (not decision.doc_url or 
            decision.doc_url == "" or 
            decision.doc_url == "nan" or
            (decision.resolution_text and 
             decision.resolution_text not in ["", "Резолютивна частина не знайдена"] and
             not decision.resolution_text.startswith("Не вдалося") and
             not decision.resolution_text.startswith("Помилка обробки") and
             "No such file or directory" not in decision.resolution_text)):
            if retry_attempt == 0:  # Логуємо тільки при першій спробі
                logger.info(f"Пропускаємо документ {decision.doc_id}: URL={decision.doc_url}, резолютивна частина={decision.resolution_text[:50] if decision.resolution_text else "None"}")
            return False
        
        try:
            if retry_attempt == 0:
                logger.info(f"Починаємо витягування резолютивної частини для документа {decision.doc_id}")
            else:
                logger.info(f"ПОВТОРНА СПРОБА #{retry_attempt + 1}/{max_retries + 1} для документа {decision.doc_id}")
                # Додаємо прогресивну затримку між спробами
                retry_delay = min(10, 1 + (retry_attempt * 0.5))
                logger.info(f"Затримка перед повторною спробою: {retry_delay:.1f}с")
                time.sleep(retry_delay)
            
            logger.info(f"URL документа: {decision.doc_url}")
            
            # Спочатку перевіряємо, чи є документ у черзі попередньо завантажених (з еталонного SR_AI)
            file_path = self.get_preloaded_document(decision.doc_id)
            
            # Якщо документ не знайдено в черзі, завантажуємо його звичайним способом
            if not file_path:
                file_path = self.download_document(decision.doc_url, decision.doc_id)
            
            if not file_path:
                error_msg = "Не вдалося завантажити документ"
                
                # Якщо це не остання спроба, повторюємо
                if retry_attempt < max_retries:
                    logger.warning(f"{error_msg} - спроба {retry_attempt + 1}/{max_retries + 1}, повторюємо...")
                    return self.process_tracked_court_decision(decision, retry_attempt + 1)
                
                # Остання спроба невдала
                decision.resolution_text = f"{error_msg} (після {max_retries + 1} спроб)"
                decision.save()
                return False
            
            # Створюємо блокування для даного файлу (з еталонного SR_AI)
            with self.lock_for_locks:
                if file_path not in self.file_locks:
                    self.file_locks[file_path] = threading.Lock()
            
            # Використовуємо блокування для безпечного доступу до файлу
            with self.file_locks[file_path]:
                # Витягуємо резолютивну частину
                resolution_text = self.extract_resolution_text(file_path, decision.judgment_code or "")
            
            # Перевіряємо чи є помилки доступу до файлу в результаті
            file_error_indicators = [
                "No such file or directory",
                "файл не знайдено",
                "файл недоступний або був видалений системою безпеки",
                "Не вдалося отримати документ: файл не знайдено"
            ]
            
            has_file_error = any(indicator in resolution_text for indicator in file_error_indicators)
            
            if has_file_error and retry_attempt < max_retries:
                logger.warning(f"Виявлено помилку доступу до файлу: {resolution_text[:100]}...")
                logger.warning(f"Повторна спроба {retry_attempt + 1}/{max_retries + 1}...")
                return self.process_tracked_court_decision(decision, retry_attempt + 1)
            
            # Очищуємо NUL bytes перед збереженням у PostgreSQL
            if resolution_text:
                resolution_text = resolution_text.replace("\x00", "")
            
            # Якщо це була повторна спроба і вона успішна, додаємо позначку
            if retry_attempt > 0 and not has_file_error:
                resolution_text = f"[УСПІШНО після {retry_attempt + 1} спроб] {resolution_text}"
                logger.info(f"Документ {decision.doc_id} успішно оброблено після {retry_attempt + 1} спроб!")
            
            # Зберігаємо результат
            decision.resolution_text = resolution_text
            decision.save()
            
            if retry_attempt == 0:
                logger.info(f"Резолютивна частина успішно витягнута для документа {decision.doc_id}: {resolution_text[:100]}...")
            else:
                logger.info(f"Резолютивна частина успішно витягнута для документа {decision.doc_id} після {retry_attempt + 1} спроб: {resolution_text[:100]}...")
            
            return True
            
        except Exception as e:
            error_msg = f"Помилка обробки документа {decision.doc_id}: {e}"
            logger.error(error_msg)
            
            # Перевіряємо чи це помилка доступу до файлу
            file_access_errors = [
                "No such file or directory",
                "FileNotFoundError", 
                "PermissionError"
            ]
            
            is_file_access_error = any(error in str(e) for error in file_access_errors)
            
            if is_file_access_error and retry_attempt < max_retries:
                logger.warning(f"Виявлено помилку доступу до файлу: {e}")
                logger.warning(f"Повторна спроба {retry_attempt + 1}/{max_retries + 1}...")
                return self.process_tracked_court_decision(decision, retry_attempt + 1)
            
            # Остання спроба або інша помилка
            if retry_attempt > 0:
                decision.resolution_text = f"Помилка: {str(e)} (після {retry_attempt + 1} спроб)"
            else:
                decision.resolution_text = f"Помилка: {str(e)}"
            
            decision.save()
            return False


class ResolutionExtractionService:
    """
    Сервіс для фонового витягування резолютивних частин.
    """
    
    def __init__(self):
        self.extractor = DocumentResolutionExtractor()
    
    def extract_resolutions_for_case(self, case_id: int) -> Dict[str, Any]:
        """
        Витягує резолютивні частини для всіх документів справи з багатопоточністю.
        """
        try:
            # Розширюємо фільтр для кращого пошуку документів
            decisions = TrackedCourtDecision.objects.filter(
                tracked_case_id=case_id
            ).filter(
                models.Q(resolution_text__isnull=True) | 
                models.Q(resolution_text="") |
                models.Q(resolution_text="Резолютивна частина не знайдена") |
                models.Q(resolution_text__startswith="Не вдалося")
            )
            
            total_count = decisions.count()
            logger.info(f"Починаємо багатопоточне витягування резолютивних частин для {total_count} документів справи {case_id}")
            
            # Запускаємо попереднє завантаження документів (з еталонного SR_AI)
            self.extractor.start_preloading(decisions)
            
            if total_count == 0:
                return {
                    "success": True,
                    "total_count": 0,
                    "processed_count": 0,
                    "success_count": 0,
                }
            
            # Багатопоточне витягування резолютивних частин з керуванням з"єднаннями
            max_workers = min(2, total_count)  # Зменшено до 2 потоків для економії з"єднань
            processed_count = 0
            success_count = 0
            
            # Закриваємо поточні з"єднання перед багатопоточністю
            from django.db import connections
            connections.close_all()
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_decision = {
                    executor.submit(self._process_single_decision_safe, decision): decision
                    for decision in decisions
                }
                
                for future in as_completed(future_to_decision):
                    decision = future_to_decision[future]
                    try:
                        success = future.result()
                        if success:
                            success_count += 1
                        processed_count += 1
                        
                        if processed_count % 10 == 0:
                            logger.info(f"Оброблено {processed_count}/{total_count} документів (успішно: {success_count})")
                        
                    except Exception as e:
                        logger.error(f"Помилка обробки документа {decision.doc_id}: {e}")
                        processed_count += 1
            
            # Закриваємо з"єднання після багатопоточності
            connections.close_all()
            
            logger.info(f"Завершено багатопоточне витягування для справи {case_id}: {success_count}/{processed_count} успішно")
            
            # Зупиняємо попереднє завантаження (з еталонного SR_AI)
            self.extractor.stop_preloading()
            
            return {
                "success": True,
                "total_count": total_count,
                "processed_count": processed_count,
                "success_count": success_count,
            }
            
        except Exception as e:
            logger.error(f"Помилка витягування резолютивних частин для справи {case_id}: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def _process_single_decision(self, decision) -> bool:
        """Обробляє один документ (для багатопоточного виконання)"""
        try:
            logger.debug(f"Обробляємо документ {decision.doc_id} з URL: {decision.doc_url}")
            result = self.extractor.process_tracked_court_decision(decision)
            
            # Коротка пауза для запобігання перевантаженню сервера
            time.sleep(REQUEST_DELAY)
            
            return result
        except Exception as e:
            logger.error(f"Помилка обробки документа {decision.doc_id}: {e}")
            return False
    
    def _process_single_decision_safe(self, decision) -> bool:
        """Безпечна обробка одного документа з керуванням з"єднаннями"""
        try:
            # Створюємо нові з"єднання для цього потоку
            from django.db import connections
            connections.close_all()
            
            logger.debug(f"Обробляємо документ {decision.doc_id} з URL: {decision.doc_url}")
            result = self.extractor.process_tracked_court_decision(decision)
            
            # Коротка пауза для запобігання перевантаженню сервера
            time.sleep(REQUEST_DELAY)
            
            # Закриваємо з"єднання цього потоку
            connections.close_all()
            
            return result
        except Exception as e:
            logger.error(f"Помилка обробки документа {decision.doc_id}: {e}")
            # Закриваємо з"єднання навіть при помилці
            try:
                from django.db import connections
                connections.close_all()
            except:
                pass
            return False
    
    def extract_resolutions_in_background(self, case_id: int):
        """
        Запускає витягування резолютивних частин у фоновому потоці.
        """
        def background_task():
            self.extract_resolutions_for_case(case_id)
        
        thread = threading.Thread(target=background_task)
        thread.daemon = True
        thread.start()
        
        logger.info(f"Запущено фонове витягування резолютивних частин для справи {case_id}")
        
    def start_extraction_for_case(self, case_id):
        """
        Запускає процес витягування резолютивних частин для всіх рішень справи
        """


class BankruptcyCaseSearchService:
    """
    Сервіс для пошуку судових рішень по справах банкрутства
    Аналогічний до SRAIIntegrationService з еталонного проекту
    """
    
    def __init__(self):
        logger.info("BankruptcyCaseSearchService ініціалізовано")
        # Ініціалізуємо швидкий пошуковий сервіс (адаптація SR_AI)
        self.fast_search = None  # Створюємо тільки при використанні
        self.use_fast_search = getattr(settings, "USE_FAST_COURT_SEARCH", True)
    
    def search_and_save_court_decisions(self, tracked_case: TrackedBankruptcyCase) -> int:
        """
        Шукає судові рішення для відстежуваної справи банкрутства та зберігає їх
        Повертає кількість знайдених рішень
        
        Використовує швидкий пошук (адаптація SR_AI) якщо доступний, 
        інакше використовує стандартний метод
        """
        case_number = tracked_case.bankruptcy_case.case_number
        logger.info(f"Початок пошуку судових рішень для справи {case_number}")
        
        try:
            # Використовуємо швидкий точний пошук (SR_AI адаптація)
            if self.use_fast_search:
                logger.info(f"Використовується швидкий точний пошук для справи {case_number}")
                
                # Створюємо новий екземпляр для кожного пошуку (уникаємо проблем з з"єднаннями)
                if not self.fast_search:
                    self.fast_search = FastCourtSearch()
                
                found_decisions = self.fast_search.search_single_case_exact(tracked_case)
                decisions_count = len(found_decisions)
                
                # Оновлюємо статус пошуку
                tracked_case.search_decisions_status = "completed"
                tracked_case.search_decisions_completed_at = timezone.now()
                tracked_case.save(update_fields=[
                    "search_decisions_status", 
                    "search_decisions_completed_at"
                ])
                
                logger.info(f"Швидкий пошук завершено для справи {case_number}. Знайдено {decisions_count} рішень.")
                return decisions_count
            
            # Використовуємо стандартний пошук (fallback)
            logger.info(f"Використовується стандартний пошук для справи {case_number}")
            search_results = self._search_in_court_decisions_tables(case_number)
            
            new_decisions_count = 0
            
            for result in search_results:
                # Перевіряємо чи рішення вже існує
                existing = TrackedCourtDecision.objects.filter(
                    tracked_case=tracked_case,
                    doc_id=result.get("doc_id", "")
                ).first()
                
                if not existing:
                    # Створюємо нове судове рішення з очищенням від NUL байтів
                    tracked_decision = TrackedCourtDecision(
                        tracked_case=tracked_case,
                        doc_id=self._clean_nul_bytes(result.get("doc_id", "")),
                        court_code=self._clean_nul_bytes(result.get("court_code", "")),
                        judgment_code=self._clean_nul_bytes(result.get("judgment_code", "")),
                        justice_kind=self._clean_nul_bytes(result.get("justice_kind", "")),
                        category_code=self._clean_nul_bytes(result.get("category_code", "")),
                        cause_num=self._clean_nul_bytes(result.get("cause_num", "")),
                        adjudication_date=self._clean_nul_bytes(result.get("adjudication_date", "")),
                        receipt_date=self._clean_nul_bytes(result.get("receipt_date", "")),
                        judge=self._clean_nul_bytes(result.get("judge", "")),
                        doc_url=self._clean_nul_bytes(result.get("doc_url", "")),
                        status=self._clean_nul_bytes(result.get("status", "")),
                        date_publ=self._clean_nul_bytes(result.get("date_publ", "")),
                        database_source=self._clean_nul_bytes(result.get("database_source", "")),
                        court_name=self._clean_nul_bytes(result.get("court_name", "")),
                        judgment_name=self._clean_nul_bytes(result.get("judgment_name", "")),
                        justice_kind_name=self._clean_nul_bytes(result.get("justice_kind_name", "")),
                        category_name=self._clean_nul_bytes(result.get("category_name", "")),
                    )
                    
                    # Шукаємо RTF посилання якщо його немає
                    if not tracked_decision.doc_url and tracked_decision.doc_id:
                        rtf_url = self._find_rtf_url_in_databases(tracked_decision.doc_id)
                        if rtf_url:
                            tracked_decision.doc_url = rtf_url
                    
                    tracked_decision.save()
                    new_decisions_count += 1
                    
                    # Витягуємо резолютивну частину якщо є RTF документ
                    if tracked_decision.needs_resolution_extraction():
                        self._extract_resolution_text(tracked_decision)
            
            logger.info(f"Пошук завершено для справи {case_number}. Знайдено {new_decisions_count} нових рішень")
            return new_decisions_count
            
        except Exception as e:
            logger.error(f"Помилка пошуку судових рішень для справи {case_number}: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return 0
    
    def _search_in_court_decisions_tables(self, case_number: str) -> List[Dict]:
        """Шукає судові рішення в PostgreSQL таблицях по номеру справи з багатопоточним пошуком"""
        from django.db import connection
        results = []
        
        # Отримуємо всі доступні таблиці судових рішень
        available_tables = self._get_available_court_decision_tables()
        
        # Визначаємо рік справи та оптимізуємо пошук
        case_year = self._extract_year_from_case_number(case_number)
        tables_to_search = self._optimize_table_selection(available_tables, case_year)
        
        logger.info(f"Пошук для справи {case_number} (рік: {case_year}): багатопоточний пошук в {len(tables_to_search)} таблицях")
        
        # Багатопоточний пошук в таблицях
        max_workers = min(6, len(tables_to_search))  # Максимум 6 потоків
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {
                executor.submit(self._search_single_table, table, case_number): table
                for table in tables_to_search
            }
            
            for future in as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    table_results = future.result()
                    if table_results:
                        results.extend(table_results)
                        logger.debug(f"Знайдено {len(table_results)} результатів в таблиці {table}")

                        # ОПТИМІЗАЦІЯ: Ранне зупинення якщо знайдено багато рішень
                        if len(results) >= 50:  # Достатньо рішень для більшості справ
                            logger.debug(f"Достатньо рішень знайдено ({len(results)}), зупиняємо пошук в інших таблицях")
                            # Скасовуємо ще не завершені задачі
                            for remaining_future in future_to_table:
                                if not remaining_future.done():
                                    remaining_future.cancel()
                            break

                except Exception as e:
                    logger.warning(f"Помилка пошуку в таблиці {table}: {e}")
                    continue
        
        logger.info(f"Загалом знайдено {len(results)} рішень для справи {case_number}")
        return results
    
    def _search_single_table(self, table: str, case_number: str) -> List[Dict]:
        """Шукає рішення в одній таблиці (для багатопоточного виконання)"""
        from django.db import connection
        results = []
        
        cursor = connection.cursor()
        try:
            # ОПТИМІЗАЦІЯ: Убираємо перевірку існування таблиці (економимо 1 запит)
            # Шукаємо за номером справи з LIMIT для швидкості
            cursor.execute(f"""
                SELECT doc_id, court_code, judgment_code, justice_kind,
                       category_code, cause_num, adjudication_date,
                       receipt_date, judge, doc_url, status, date_publ,
                       '{table}' as database_source
                FROM {table}
                WHERE cause_num = %s
                ORDER BY adjudication_date DESC
                LIMIT 100
            """, [case_number])
            
            columns = [desc[0] for desc in cursor.description]
            
            for row in cursor.fetchall():
                result = dict(zip(columns, row))
                results.append(result)
                
        except Exception as e:
            logger.warning(f"Помилка пошуку в таблиці {table}: {e}")
        finally:
            cursor.close()
        
        return results
    
    def _get_available_court_decision_tables(self) -> List[str]:
        """Отримує список доступних таблиць судових рішень"""
        from django.db import connection
        
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name LIKE 'court_decisions_%'
                ORDER BY table_name DESC
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            logger.debug(f"Знайдено таблиці судових рішень: {tables}")
            return tables
        finally:
            cursor.close()
    
    def _extract_year_from_case_number(self, case_number: str) -> Optional[int]:
        """Витягує рік справи з номера справи (після останнього /)"""
        try:
            # Номер справи має формат: 904/6740/20 -> рік 20 (2020)
            if "/" in case_number:
                year_part = case_number.split("/")[-1]
                # Прибираємо все після дефісу якщо є (напр. 20-г -> 20)
                year_part = year_part.split("-")[0]
                
                if year_part.isdigit():
                    year = int(year_part)
                    # Конвертуємо 2-значний рік у 4-значний
                    if year < 100:
                        if year >= 90:  # 90-99 -> 1990-1999
                            year += 1900
                        else:  # 00-89 -> 2000-2089
                            year += 2000
                    
                    logger.debug(f"Витягнутий рік зі справи {case_number}: {year}")
                    return year
        except Exception as e:
            logger.warning(f"Не вдалося витягнути рік зі справи {case_number}: {e}")
        
        return None
    
    def _optimize_table_selection(self, available_tables: List[str], case_year: Optional[int]) -> List[str]:
        """Оптимізований вибір таблиць для швидкого пошуку"""
        # ОПТИМІЗАЦІЯ: Обмежуємо пошук тільки релевантними таблицями
        sorted_tables = []

        if case_year:
            # 1. Спочатку таблиця року справи (якщо є)
            case_year_table = f"court_decisions_{case_year}"
            if case_year_table in available_tables:
                sorted_tables.append(case_year_table)

            # 2. Потім таблиці суміжних років (±2 роки для апеляцій/касацій)
            for year_offset in [-2, -1, 1, 2]:
                adjacent_year = case_year + year_offset
                adjacent_table = f"court_decisions_{adjacent_year}"
                if adjacent_table in available_tables and adjacent_table not in sorted_tables:
                    sorted_tables.append(adjacent_table)

            # 3. Якщо потрібно більше, додаємо найновіші таблиці (2023-2025)
            recent_tables = [t for t in available_tables
                           if any(year in t for year in ['2023', '2024', '2025'])
                           and t not in sorted_tables]
            sorted_tables.extend(sorted(recent_tables, reverse=True))

            # Обмежуємо максимум 8 таблицями замість 19
            sorted_tables = sorted_tables[:8]
        else:
            # Якщо рік невідомий - тільки найновіші 5 таблиць
            sorted_tables = sorted(available_tables, reverse=True)[:5]

        logger.debug(f"ОПТИМІЗОВАНО: шукаємо в {len(sorted_tables)} таблицях замість {len(available_tables)}")
        logger.debug(f"Порядок пошуку: {sorted_tables}")
        return sorted_tables
    
    def _find_rtf_url_in_databases(self, doc_id: str) -> Optional[str]:
        """Шукає RTF посилання в PostgreSQL таблицях"""
        if not doc_id or doc_id.strip() == "":
            return None
        
        doc_id = doc_id.strip()
        tables = self._get_available_court_decision_tables()
        
        cursor = connection.cursor()
        try:
            # Оптимізований пошук з UNION
            union_query = " UNION ALL ".join([
                    f"SELECT doc_url, '{table}' as source_table FROM {table} WHERE doc_id = %s AND doc_url IS NOT NULL AND doc_url LIKE %s AND LENGTH(doc_url) = LENGTH(REPLACE(doc_url, CHR(0), ''))"
                    for table in tables
            ])
            
            full_query = f"""
                SELECT doc_url, source_table FROM ({union_query}) AS combined
                ORDER BY CASE source_table
                    WHEN 'court_decisions_2025' THEN 1
                    WHEN 'court_decisions_2024' THEN 2
                    ELSE 3 END
                LIMIT 1
            """
            
            # Параметри для кожного UNION запиту
            params = []
            for _ in tables:
                params.extend([doc_id, "%.rtf"])
            
            cursor.execute(full_query, params)
            result = cursor.fetchone()
            
            if result and result[0]:
                rtf_url = result[0].strip()
                if "\x00" not in rtf_url:
                    logger.debug(f"RTF знайдено для {doc_id}: {rtf_url[:50]}...")
                    return rtf_url
                    
        except Exception as e:
            logger.warning(f"Помилка пошуку RTF для {doc_id}: {e}")
        finally:
            cursor.close()
        
        return None
    
    def _extract_resolution_text(self, tracked_decision: TrackedCourtDecision) -> bool:
        """Витягує резолютивну частину з RTF документа використовуючи DocumentResolutionExtractor"""
        try:
            if not tracked_decision.doc_url or not tracked_decision.has_rtf_document():
                return False
            
            # Використовуємо DocumentResolutionExtractor для витягування резолютивної частини
            extractor = DocumentResolutionExtractor()
            return extractor.process_tracked_court_decision(tracked_decision)
            
        except Exception as e:
            logger.error(f"Помилка витягування резолютивної частини для {tracked_decision.doc_id}: {e}")
            return False
    
    def _clean_nul_bytes(self, value: str) -> str:
        """Очищує рядок від NUL байтів та інших проблемних символів"""
        if not isinstance(value, str):
            return str(value) if value else ""
        
        # Видаляємо NUL байти
        cleaned = value.replace("\x00", "")
        
        # Видаляємо інші проблемні символи (залишаємо тільки допустимі)
        cleaned = "".join(char for char in cleaned if ord(char) >= 32 or char in ["\n", "\r", "\t"])
        
        return cleaned.strip()


    def get_statistics(self) -> Dict:
        """Повертає статистику відстежуваних справ та знайдених рішень"""
        try:
            from bankruptcy.models import TrackedBankruptcyCase, TrackedCourtDecision
            
            total_tracked = TrackedBankruptcyCase.objects.count()
            active_tracked = TrackedBankruptcyCase.objects.filter(status="active").count()
            total_decisions = TrackedCourtDecision.objects.count()
            
            # Статистика по статусам пошуку
            pending_searches = TrackedBankruptcyCase.objects.filter(search_decisions_status="pending").count()
            running_searches = TrackedBankruptcyCase.objects.filter(search_decisions_status="running").count()
            completed_searches = TrackedBankruptcyCase.objects.filter(search_decisions_status="completed").count()
            failed_searches = TrackedBankruptcyCase.objects.filter(search_decisions_status="failed").count()
            
            return {
                "total_tracked_cases": total_tracked,
                "active_tracked_cases": active_tracked,
                "total_court_decisions": total_decisions,
                "search_status": {
                    "pending": pending_searches,
                    "running": running_searches,
                    "completed": completed_searches,
                    "failed": failed_searches
                }
            }
            
        except Exception as e:
            logger.error(f"Помилка отримання статистики: {e}")
            return {}
    
    def search_decisions_for_all_tracked_cases(self) -> Dict:
        """Запускає пошук судових рішень для всіх відстежуваних справ"""
        try:
            # Отримуємо справи що потребують пошуку
            cases_needing_search = TrackedBankruptcyCase.objects.filter(
                status="active",
                search_decisions_status__in=["pending", "failed"]
            ).order_by("-priority", "-created_at")
            
            total_cases = cases_needing_search.count()
            processed_cases = 0
            total_decisions = 0
            
            logger.info(f"Розпочинається пошук для {total_cases} справ")
            
            for tracked_case in cases_needing_search:
                try:
                    found_decisions = self.search_and_save_court_decisions(tracked_case)
                    total_decisions += found_decisions
                    processed_cases += 1
                    
                    logger.info(f"Оброблено {processed_cases}/{total_cases} справ. "
                               f"Справа {tracked_case.bankruptcy_case.case_number}: {found_decisions} рішень")
                    
                except Exception as e:
                    logger.error(f"Помилка обробки справи {tracked_case.bankruptcy_case.case_number}: {e}")
                    continue
            
            result = {
                "processed_cases": processed_cases,
                "total_cases": total_cases,
                "total_decisions_found": total_decisions,
                "success": True
            }
            
            logger.info(f"Пошук завершено: {processed_cases} справ оброблено, {total_decisions} рішень знайдено")
            return result
            
        except Exception as e:
            logger.error(f"Помилка масового пошуку судових рішень: {e}")
            return {
                "processed_cases": 0,
                "total_cases": 0,
                "total_decisions_found": 0,
                "success": False,
                "error": str(e)
            }


class BankruptcyAutoTrackingService:
    """
    Сервіс для автоматичного відстеження нових справ банкрутства
    """
    
    def __init__(self):
        logger.info("BankruptcyAutoTrackingService ініціалізовано")
    
    def setup_tracking_for_all_bankruptcy_cases(self) -> Dict:
        """
        Налаштовує відстеження для всіх справ банкрутства з таблиці BankruptcyCase
        Нові справи отримують вищий пріоритет
        """
        try:
            from bankruptcy.models import BankruptcyCase, TrackedBankruptcyCase
            
            # Отримуємо всі справи банкрутства
            all_cases = BankruptcyCase.objects.all().order_by("-created_at")  # Нові справи спочатку
            
            created_count = 0
            existing_count = 0
            
            for i, bankruptcy_case in enumerate(all_cases):
                # Перевіряємо чи справа вже відстежується
                existing_tracked = TrackedBankruptcyCase.objects.filter(
                    bankruptcy_case=bankruptcy_case
                ).first()
                
                if not existing_tracked:
                    # Створюємо нове відстеження з пріоритетом (нові справи мають вищий пріоритет)
                    priority = len(all_cases) - i  # Нові справи отримують вищий номер
                    
                    tracked_case = TrackedBankruptcyCase.objects.create(
                        bankruptcy_case=bankruptcy_case,
                        status="active",
                        priority=priority,
                        search_decisions_status="pending"
                    )
                    
                    created_count += 1
                    logger.info(f"Створено відстеження для справи {bankruptcy_case.case_number} (пріоритет: {priority})")
                    
                    # Запускаємо фоновий пошук
                    tracked_case.trigger_background_search_decisions()
                    
                else:
                    existing_count += 1
            
            result = {
                "total_cases": len(all_cases),
                "created_tracking": created_count,
                "existing_tracking": existing_count,
                "success": True
            }
            
            logger.info(f"Налаштування відстеження завершено: {created_count} нових, {existing_count} існуючих")
            return result
            
        except Exception as e:
            logger.error(f"Помилка налаштування автоматичного відстеження: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def add_case_to_tracking(self, bankruptcy_case, priority: int = 50):
        """
        Додає конкретну справу банкрутства до відстеження
        """
        try:
            from bankruptcy.models import TrackedBankruptcyCase
            
            # Перевіряємо чи справа вже відстежується
            existing_tracked = TrackedBankruptcyCase.objects.filter(
                bankruptcy_case=bankruptcy_case
            ).first()
            
            if existing_tracked:
                logger.info(f"Справа {bankruptcy_case.case_number} вже відстежується")
                return existing_tracked
            
            # Створюємо нове відстеження
            tracked_case = TrackedBankruptcyCase.objects.create(
                bankruptcy_case=bankruptcy_case,
                status="active",
                priority=priority,
                search_decisions_status="pending"
            )
            
            logger.info(f"Створено відстеження для справи {bankruptcy_case.case_number} (пріоритет: {priority})")
            
            # Запускаємо фоновий пошук
            tracked_case.trigger_background_search_decisions()
            
            return tracked_case
            
        except Exception as e:
            logger.error(f"Помилка додавання справи {bankruptcy_case.case_number} до відстеження: {e}")
            raise
    
    def process_new_bankruptcy_cases(self) -> Dict:
        """
        Обробляє нові справи банкрутства що з"явились в системі
        """
        try:
            from bankruptcy.models import BankruptcyCase, TrackedBankruptcyCase
            
            # Знаходимо справи що ще не відстежуються
            tracked_case_ids = TrackedBankruptcyCase.objects.values_list("bankruptcy_case_id", flat=True)
            new_cases = BankruptcyCase.objects.exclude(id__in=tracked_case_ids).order_by("-created_at")
            
            if not new_cases.exists():
                return {
                    "new_cases_found": 0,
                    "tracking_created": 0,
                    "success": True
                }
            
            # Визначаємо найвищий існуючий пріоритет
            max_priority = TrackedBankruptcyCase.objects.aggregate(
                max_priority=models.Max("priority")
            )["max_priority"] or 0
            
            tracking_created = 0
            
            for i, bankruptcy_case in enumerate(new_cases):
                # Нові справи отримують найвищий пріоритет
                priority = max_priority + len(new_cases) - i
                
                tracked_case = TrackedBankruptcyCase.objects.create(
                    bankruptcy_case=bankruptcy_case,
                    status="active",
                    priority=priority,
                    search_decisions_status="pending"
                )
                
                tracking_created += 1
                logger.info(f"Створено відстеження для нової справи {bankruptcy_case.case_number} (пріоритет: {priority})")
                
                # Запускаємо фоновий пошук
                tracked_case.trigger_background_search_decisions()
            
            result = {
                "new_cases_found": len(new_cases),
                "tracking_created": tracking_created,
                "success": True
            }
            
            logger.info(f"Оброблено нові справи: {len(new_cases)} знайдено, {tracking_created} взято на відстеження")
            return result
            
        except Exception as e:
            logger.error(f"Помилка обробки нових справ банкрутства: {e}")
            return {
                "new_cases_found": 0,
                "tracking_created": 0,
                "success": False,
                "error": str(e)
            }