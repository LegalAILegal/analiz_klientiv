import os
import time
import re
import requests
import random
import logging
import threading
import queue
import uuid
import concurrent.futures
from datetime import datetime, timedelta
import striprtf.striprtf as striprtf
from django.conf import settings
from django.utils import timezone
from django.db import models
from bankruptcy.models import TrackedCourtDecision


class FastResolutionExtractor:
    """
    Високопродуктивний сервіс витягування резолютивних частин на основі алгоритмів SR_AI
    """
    
    def __init__(self):
        # 🚀 МАКСИМАЛЬНО АГРЕСИВНІ ПАРАМЕТРИ ДЛЯ ШВИДКОДІЇ
        self.max_workers = getattr(settings, "RESOLUTION_MAX_WORKERS", 15)  # УЛЬТРА: 15 потоків для ультра швидкості витягування
        self.batch_size = getattr(settings, "RESOLUTION_BATCH_SIZE", 1000)  # КРИТИЧНО: збільшено до 1000 за раз
        self.download_timeout = getattr(settings, "RESOLUTION_DOWNLOAD_TIMEOUT", 15)  # Скорочено до 15 секунд
        self.temp_dir = getattr(settings, "TEMP_DIR", "/tmp/resolution_temp")
        self.request_delay = getattr(settings, "REQUEST_DELAY", 0.01)  # КРИТИЧНО: мінімальна затримка 0.01с
        
        # Створюємо тимчасову директорію
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # 🚀 АГРЕСИВНІ ПАРАМЕТРИ КЕШУВАННЯ ДЛЯ ШВИДКОДІЇ
        self.preload_queue = queue.Queue(maxsize=1000)  # ЗБІЛЬШЕНО до 1000 
        self.preload_thread = None
        self.stop_preload_event = threading.Event()
        
        # Блокування для безпечного доступу до файлів
        self.file_locks = {}
        self.lock_for_locks = threading.Lock()
        
        # 🚀 РОЗШИРЕНИЙ КЕШ ДЛЯ МАКСИМАЛЬНОЇ ШВИДКОДІЇ
        self.document_cache = {}  # Кеш завантажених документів
        self.url_cache = {}       # Кеш URL для швидкого доступу
        self.session_pool = {}    # Пул сесій для повторного використання
        
        # Патерни для пошуку резолютивних частин
        # Використовуємо ТОЧНО ті ж патерни що в SR_AI - проста, ефективна система
        self.resolution_patterns = [
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
            # Додаткові варіанти для кращого покриття
            r"ПОСТАНОВИЛ[АИ]?:(.*)",
            r"РІШИВ:(.*)",
        ]
        
        # Налаштування логування
        self.setup_logging()
    
    def setup_logging(self):
        """Налаштовує логування для сервісу"""
        log_dir = os.path.join(settings.BASE_DIR, "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"resolution_extractor_{datetime.now().strftime("%Y%m%d")}.log")
        
        self.logger = logging.getLogger("fast_resolution_extractor")
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            handler = logging.FileHandler(log_file, encoding="utf-8")
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def start_preloading(self, decisions):
        """
        Запускає фонове завантаження документів
        """
        if not decisions:
            return
            
        def preload_worker():
            for decision in decisions:
                if self.stop_preload_event.is_set():
                    break
                    
                if decision.doc_url and decision.doc_url.startswith("http"):
                    try:
                        file_path = self.download_document(decision.doc_url, str(decision.id))
                        if file_path:
                            self.preload_queue.put((decision.id, file_path), block=False)
                    except queue.Full:
                        pass
                    except Exception as e:
                        self.logger.error(f"Помилка при попередньому завантаженні рішення {decision.id}: {e}")
        
        self.stop_preload_event.clear()
        self.preload_thread = threading.Thread(target=preload_worker)
        self.preload_thread.daemon = True
        self.preload_thread.start()
        print(f"Запущено фонове попереднє завантаження документів...")
    
    def stop_preloading(self):
        """
        Зупиняє процес попереднього завантаження
        """
        if self.preload_thread and self.preload_thread.is_alive():
            self.stop_preload_event.set()
            self.preload_thread.join(timeout=1.0)
            print("Фонове попереднє завантаження зупинено.")
    
    def get_preloaded_document(self, decision_id):
        """
        Отримує попередньо завантажений документ з черги
        """
        try:
            for _ in range(self.preload_queue.qsize()):
                doc_id, path = self.preload_queue.get(block=False)
                if doc_id == decision_id:
                    return path
                else:
                    self.preload_queue.put((doc_id, path), block=False)
        except (queue.Empty, Exception):
            pass
        
        return None
    
    def download_document(self, url, doc_id):
        """
        Завантажує документ за URL з оптимізованими налаштуваннями
        """
        if not url or url == "nan":
            return None

        cache_key = f"{url}_{doc_id}"
        if cache_key in self.document_cache:
            return self.document_cache[cache_key]

        file_path = os.path.join(self.temp_dir, f"document_{doc_id}_{uuid.uuid4().hex[:8]}.rtf")
        
        time.sleep(self.request_delay + random.uniform(0, 0.005))  # 🚀 КРИТИЧНО: мінімальна випадкова затримка
        
        max_retries = 5  # Збільшено до 5 згідно з еталонним проектом
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    delay = (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(delay)
                    self.logger.info(f"Спроба #{attempt+1} завантаження документа {doc_id}")
                
                # 🚀 КРИТИЧНО: ВИКОРИСТОВУЄМО ПУЛ СЕСІЙ ДЛЯ ШВИДКОДІЇ
                thread_id = threading.current_thread().ident
                if thread_id not in self.session_pool:
                    session = requests.Session()
                    session.trust_env = False
                    # 🚀 ОПТИМІЗОВАНІ ЗАГОЛОВКИ ДЛЯ ШВИДКОСТІ
                    session.headers.update({
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        "Accept": "application/rtf,*/*;q=0.8",
                        "Accept-Language": "uk-UA,uk;q=0.9",
                        "Connection": "keep-alive",
                    })
                    self.session_pool[thread_id] = session
                else:
                    session = self.session_pool[thread_id]
                
                response = session.get(url, timeout=self.download_timeout)
                response.raise_for_status()

                if not response.content:
                    raise Exception("Отримано порожній вміст документа")

                with open(file_path, "wb") as f:
                    f.write(response.content)
                
                if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                    raise Exception("Файл не було створено або він порожній")

                self.document_cache[cache_key] = file_path
                return file_path
                
            except requests.exceptions.Timeout:
                if attempt >= max_retries:
                    self.logger.error(f"Таймаут при завантаженні документа {doc_id}")
                    break
                    
            except requests.exceptions.HTTPError as e:
                self.logger.error(f"HTTP помилка при завантаженні документа {doc_id}: {e}")
                break
                
            except Exception as e:
                self.logger.error(f"Помилка при завантаженні документа {doc_id}: {e}")
                if attempt >= max_retries:
                    break
        
        return None
    
    def extract_resolution_text(self, file_path, decision_name=""):
        """
        Витягує текст резолютивної частини з RTF документа
        """
        if not file_path or not os.path.exists(file_path):
            return "Не вдалося отримати документ: файл не знайдено"

        time.sleep(0.005)  # 🚀 КРИТИЧНО: мінімальна затримка для максимальної швидкості

        try:
            rtf_text = None
            max_attempts = 1  # 🚀 КРИТИЧНО: тільки одна спроба для швидкості
            
            for attempt in range(max_attempts):
                try:
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        rtf_text = f.read()
                    break
                except PermissionError as e:
                    if attempt < max_attempts - 1:
                        time.sleep(0.1)  # 🚀 КРИТИЧНО: скорочена затримка
                    else:
                        return f"Не вдалося отримати доступ до файлу: {str(e)}"

            if not rtf_text:
                return "Не вдалося отримати документ: помилка читання файлу"

            # КРИТИЧНО ВАЖЛИВО: Очищуємо NUL bytes перед обробкою (виправлення PostgreSQL помилок)
            rtf_text = rtf_text.replace("\x00", "")

            try:
                plain_text = striprtf.rtf_to_text(rtf_text)
            except Exception as rtf_error:
                self.logger.error(f"Помилка при конвертації RTF: {rtf_error}")
                plain_text = re.sub(r"\\[a-z0-9]+", " ", rtf_text)
                plain_text = re.sub(r"\{|\}|\\|\n", " ", plain_text)
                plain_text = re.sub(r"\s+", " ", plain_text)

            # Очищуємо NUL bytes з результату також
            if plain_text:
                plain_text = plain_text.replace("\x00", "")

            # Пошук за патернами
            for pattern in self.resolution_patterns:
                matches = re.search(pattern, plain_text, re.DOTALL | re.IGNORECASE)
                if matches:
                    resolution_text = re.sub(r"\s+", " ", matches.group(1).strip())
                    # Фінальне очищення NUL bytes
                    resolution_text = resolution_text.replace("\x00", "") 
                    return resolution_text

            return "Резолютивна частина не знайдена"
            
        except Exception as e:
            self.logger.error(f"Помилка при обробці документа {file_path}: {e}")
            return f"Помилка обробки: {str(e)}"
        finally:
            self.try_safely_delete_file(file_path)
    
    def try_safely_delete_file(self, file_path, max_attempts=2):
        """
        Безпечно видаляє файл з повторними спробами
        """
        for attempt in range(max_attempts):
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    return True
            except Exception as e:
                if attempt < max_attempts - 1:
                    time.sleep(0.3)
                else:
                    self.logger.warning(f"Не вдалося видалити файл {file_path}: {e}")
                    return False
        return True
    
    def process_single_decision(self, decision):
        """
        Обробляє одне судове рішення
        """
        try:
            if not decision.doc_url or not decision.doc_url.startswith("http"):
                decision.resolution_text = "URL документа відсутній або некоректний"
                decision.resolution_extracted_at = timezone.now()
                decision.save(update_fields=[
                "resolution_text", "resolution_extracted_at", "has_trigger_words", "trigger_words_found", 
                "trigger_types", "is_critical_decision"
            ])
                return decision
            
            # Спочатку перевіряємо попередньо завантажені документи
            file_path = self.get_preloaded_document(decision.id)
            
            # Якщо документ не знайдено в черзі, завантажуємо звичайним способом
            if not file_path:
                unique_suffix = str(uuid.uuid4())[:8]
                file_path = self.download_document(decision.doc_url, f"{decision.id}_{unique_suffix}")
            
            if file_path:
                # Створюємо блокування для файлу
                with self.lock_for_locks:
                    if file_path not in self.file_locks:
                        self.file_locks[file_path] = threading.Lock()
                
                # Витягуємо резолютивну частину
                with self.file_locks[file_path]:
                    resolution_text = self.extract_resolution_text(file_path, "")
                    decision.resolution_text = resolution_text
                    decision.resolution_extracted_at = timezone.now()
                    
                    # Аналізуємо тригерні слова після встановлення тексту
                    if resolution_text and not resolution_text.startswith("Не вдалося") and not resolution_text.startswith("Помилка"):
                        self.analyze_triggers(decision)
                
                # Видаляємо блокування файлу
                with self.lock_for_locks:
                    if file_path in self.file_locks:
                        del self.file_locks[file_path]
            else:
                decision.resolution_text = "Не вдалося завантажити документ"
                decision.resolution_extracted_at = timezone.now()
            
            decision.save(update_fields=[
                "resolution_text", "resolution_extracted_at", "has_trigger_words", "trigger_words_found", 
                "trigger_types", "is_critical_decision"
            ])
            return decision
            
        except Exception as e:
            self.logger.error(f"Помилка при обробці рішення {decision.id}: {e}")
            decision.resolution_text = f"Помилка: {str(e)}"
            decision.resolution_extracted_at = timezone.now()
            decision.save(update_fields=[
                "resolution_text", "resolution_extracted_at", "has_trigger_words", "trigger_words_found", 
                "trigger_types", "is_critical_decision"
            ])
            return decision
    
    def process_single_decision_enhanced(self, decision):
        """
        Покращена обробка одного рішення з використанням попередньо завантажених файлів
        (Адаптовано з SR_AI підходу)
        """
        try:
            if not decision.doc_url or decision.doc_url in ["nan", ""]:
                decision.resolution_text = "URL документа відсутній або некоректний"
                decision.resolution_extracted_at = timezone.now()
                decision.save(update_fields=[
                "resolution_text", "resolution_extracted_at", "has_trigger_words", "trigger_words_found", 
                "trigger_types", "is_critical_decision"
            ])
                return decision
            
            # Спочатку перевіряємо попередньо завантажені документи (SR_AI стиль)
            file_path = self.get_preloaded_document(str(decision.id))
            
            # Якщо документ не знайдено в черзі, завантажуємо звичайним способом
            if not file_path:
                unique_suffix = str(uuid.uuid4())[:8]
                file_path = self.download_document(decision.doc_url, f"{decision.id}_{unique_suffix}")
            
            if file_path:
                # Створюємо блокування для файлу (thread safety)
                with self.lock_for_locks:
                    if file_path not in self.file_locks:
                        self.file_locks[file_path] = threading.Lock()
                
                # Витягуємо резолютивну частину з блокуванням
                with self.file_locks[file_path]:
                    resolution_text = self.extract_resolution_text(file_path, "")
                    decision.resolution_text = resolution_text
                    decision.resolution_extracted_at = timezone.now()
                    
                    # Аналізуємо тригерні слова після встановлення тексту
                    if resolution_text and not resolution_text.startswith("Не вдалося") and not resolution_text.startswith("Помилка"):
                        self.analyze_triggers(decision)
                
                # Видаляємо блокування файлу після обробки
                with self.lock_for_locks:
                    if file_path in self.file_locks:
                        del self.file_locks[file_path]
                        
                # Видаляємо тимчасовий файл (оптимізація пам"яті)
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except:
                    pass
                        
            else:
                decision.resolution_text = "Не вдалося завантажити документ"
                decision.resolution_extracted_at = timezone.now()
            
            decision.save(update_fields=[
                "resolution_text", "resolution_extracted_at", "has_trigger_words", "trigger_words_found", 
                "trigger_types", "is_critical_decision"
            ])
            return decision
            
        except Exception as e:
            self.logger.error(f"Помилка при покращеній обробці рішення {decision.id}: {e}")
            decision.resolution_text = f"Помилка: {str(e)}"
            decision.resolution_extracted_at = timezone.now()
            decision.save(update_fields=[
                "resolution_text", "resolution_extracted_at", "has_trigger_words", "trigger_words_found", 
                "trigger_types", "is_critical_decision"
            ])
            return decision
    
    def extract_resolutions_batch(self, limit=None):
        """
        Витягує резолютивні частини для рішень без тексту (батчами)
        """
        if limit is None:
            limit = self.batch_size
        
        # Отримуємо рішення без резолютивного тексту (NULL або порожні)
        decisions_to_process = TrackedCourtDecision.objects.filter(
            doc_url__isnull=False
        ).filter(
            models.Q(resolution_text__isnull=True) | models.Q(resolution_text__exact="")
        ).exclude(
            doc_url__exact=""
        ).exclude(
            doc_url__exact="nan"
        ).order_by("-found_at")[:limit]
        
        if not decisions_to_process:
            return {
                "success": True,
                "processed": 0,
                "message": "Немає рішень для обробки"
            }
        
        print(f"Початок витягування резолютивних частин для {len(decisions_to_process)} рішень...")
        
        # Запускаємо попереднє завантаження
        self.start_preloading(decisions_to_process)
        
        # 🚀 СТВОРЮЄМО БІЛЬШІ БАТЧІ ДЛЯ МАКСИМАЛЬНОЇ ШВИДКОДІЇ (SR_AI підхід)
        sub_batch_size = getattr(settings, "RESOLUTION_SUB_BATCH_SIZE", 200)  # КРИТИЧНО: збільшено до 200
        batches = [decisions_to_process[i:i+sub_batch_size] for i in range(0, len(decisions_to_process), sub_batch_size)]
        
        total_processed = 0
        total_decisions = len(decisions_to_process)
        success_count = 0
        
        print(f"Розділено на {len(batches)} батчів по {sub_batch_size} рішень максимум")
        
        try:
            # Обробляємо кожен батч паралельно (SR_AI стиль)
            for batch_index, batch in enumerate(batches):
                print(f"Обробка батчу {batch_index+1}/{len(batches)}...")
                
                # 🚀 БІЛЬШІ МІНІ-БАТЧІ ДЛЯ МАКСИМАЛЬНОГО ВИКОРИСТАННЯ ПОТОКІВ
                mini_batch_size = getattr(settings, "RESOLUTION_MINI_BATCH_SIZE", 75)  # КРИТИЧНО: збільшено до 75
                mini_batches = [batch[i:i+mini_batch_size] for i in range(0, len(batch), mini_batch_size)]
                
                for mini_batch in mini_batches:
                    # Використовуємо ThreadPoolExecutor з максимальною кількістю потоків
                    with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        # Створюємо завдання для кожного рішення
                        future_to_decision = {
                            executor.submit(self.process_single_decision_enhanced, decision): decision
                            for decision in mini_batch
                        }
                        
                        # Обробляємо результати по мірі завершення
                        for future in concurrent.futures.as_completed(future_to_decision):
                            try:
                                processed_decision = future.result()
                                # ВИПРАВЛЕНИЙ критерій успішності - виключаємо також "Резолютивна частина не знайдена"
                                if (processed_decision and processed_decision.resolution_text and 
                                    not processed_decision.resolution_text.startswith("Не вдалося") and
                                    not processed_decision.resolution_text.startswith("Помилка") and
                                    processed_decision.resolution_text != "Резолютивна частина не знайдена" and
                                    processed_decision.resolution_text != "URL документа відсутній або некоректний"):
                                    success_count += 1
                                total_processed += 1
                                
                                # Показуємо прогрес
                                if total_processed % 10 == 0 or total_processed == total_decisions:
                                    print(f"Прогрес: {total_processed}/{total_decisions} "
                                          f"({total_processed / total_decisions * 100:.1f}%), "
                                          f"успішно: {success_count}")
                                
                            except Exception as e:
                                self.logger.error(f"Помилка при обробці рішення: {e}")
                                total_processed += 1
                
                print(f"Завершено батч {batch_index+1}/{len(batches)}. "
                      f"Загальний прогрес: {total_processed}/{total_decisions} "
                      f"({total_processed / total_decisions * 100:.1f}%)")
        
        finally:
            # Зупиняємо попереднє завантаження
            self.stop_preloading()
        
        print(f"Витягування резолютивних частин завершено. "
              f"Оброблено: {total_processed}, Успішно: {success_count}")
        
        return {
            "success": True,
            "processed": total_processed,
            "successful": success_count,
            "failed": total_processed - success_count
        }
    
    def extract_resolutions_for_case(self, case_id, limit=None):
        """
        Витягує резолютивні частини для конкретної справи
        """
        from django.db import models
        
        if limit is None:
            limit = self.batch_size
        
        # Отримуємо рішення без резолютивного тексту для конкретної справи
        decisions_to_process = TrackedCourtDecision.objects.filter(
            tracked_case_id=case_id,
            doc_url__isnull=False
        ).filter(
            models.Q(resolution_text__isnull=True) | models.Q(resolution_text__exact="")
        ).exclude(
            doc_url__exact=""
        ).exclude(
            doc_url__exact="nan"
        ).order_by("-found_at")[:limit]
        
        if not decisions_to_process:
            return {
                "success": True,
                "processed": 0,
                "successful": 0,
                "failed": 0
            }
        
        decisions_list = list(decisions_to_process)
        print(f"Початок витягування резолютивних частин для {len(decisions_list)} рішень справи {case_id}...")
        
        # Запускаємо попереднє завантаження
        self.start_preloading(decisions_list)
        
        # Створюємо батчі для обробки (SR_AI підхід)
        sub_batch_size = getattr(settings, "RESOLUTION_SUB_BATCH_SIZE", 25)
        batches = [decisions_list[i:i+sub_batch_size] for i in range(0, len(decisions_list), sub_batch_size)]
        
        total_processed = 0
        success_count = 0
        
        print(f"Розділено на {len(batches)} батчів по {sub_batch_size} рішень максимум")
        
        try:
            # Обробляємо кожен батч паралельно
            for batch_index, batch in enumerate(batches):
                print(f"Обробка батчу {batch_index+1}/{len(batches)}...")
                
                # 🚀 БІЛЬШІ МІНІ-БАТЧІ ДЛЯ МАКСИМАЛЬНОГО ВИКОРИСТАННЯ ПОТОКІВ
                mini_batch_size = getattr(settings, "RESOLUTION_MINI_BATCH_SIZE", 75)  # КРИТИЧНО: збільшено до 75
                mini_batches = [batch[i:i+mini_batch_size] for i in range(0, len(batch), mini_batch_size)]
                
                for mini_batch in mini_batches:
                    # Використовуємо ThreadPoolExecutor з максимальною кількістю потоків
                    with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        # Створюємо завдання для кожного рішення
                        future_to_decision = {
                            executor.submit(self.process_single_decision_enhanced, decision): decision
                            for decision in mini_batch
                        }
                        
                        # Обробляємо результати по мірі завершення
                        for future in concurrent.futures.as_completed(future_to_decision):
                            try:
                                processed_decision = future.result()
                                # ВИПРАВЛЕНИЙ критерій успішності - виключаємо також "Резолютивна частина не знайдена"
                                if (processed_decision and processed_decision.resolution_text and 
                                    not processed_decision.resolution_text.startswith("Не вдалося") and
                                    not processed_decision.resolution_text.startswith("Помилка") and
                                    processed_decision.resolution_text != "Резолютивна частина не знайдена" and
                                    processed_decision.resolution_text != "URL документа відсутній або некоректний"):
                                    success_count += 1
                                total_processed += 1
                                
                                # Показуємо прогрес
                                if total_processed % 5 == 0 or total_processed == len(decisions_list):
                                    print(f"Прогрес: {total_processed}/{len(decisions_list)} "
                                          f"({total_processed / len(decisions_list) * 100:.1f}%), "
                                          f"успішно: {success_count}")
                                
                            except Exception as e:
                                self.logger.error(f"Помилка при обробці рішення: {e}")
                                total_processed += 1
                
                print(f"Завершено батч {batch_index+1}/{len(batches)}. "
                      f"Загальний прогрес: {total_processed}/{len(decisions_list)} "
                      f"({total_processed / len(decisions_list) * 100:.1f}%)")
                
        except Exception as e:
            self.logger.error(f"Помилка при витягуванні резолютивних частин для справи {case_id}: {e}")
            return {
                "success": False,
                "error": str(e),
                "processed": total_processed,
                "successful": success_count,
                "failed": total_processed - success_count
            }
        finally:
            # Зупиняємо попереднє завантаження
            self.stop_preloading()
        
        print(f"Витягування резолютивних частин для справи {case_id} завершено. "
              f"Оброблено: {total_processed}, Успішно: {success_count}")
        
        return {
            "success": True,
            "processed": total_processed,
            "successful": success_count,
            "failed": total_processed - success_count
        }
    
    def extract_resolutions_batch_custom(self, decisions_list, progress_callback=None):
        """
        Витягує резолютивні частини для переданого списку рішень з підтримкою callback для прогресу
        """        
        if not decisions_list:
            return {
                "success": True,
                "processed": 0,
                "successful": 0,
                "failed": 0
            }
        
        print(f"Початок швидкого витягування резолютивних частин для {len(decisions_list)} рішень...")
        
        # Запускаємо попереднє завантаження
        self.start_preloading(decisions_list)
        
        # Створюємо батчі для обробки (SR_AI підхід)
        sub_batch_size = getattr(settings, "RESOLUTION_SUB_BATCH_SIZE", 25)
        batches = [decisions_list[i:i+sub_batch_size] for i in range(0, len(decisions_list), sub_batch_size)]
        
        total_processed = 0
        success_count = 0
        
        print(f"Розділено на {len(batches)} батчів по {sub_batch_size} рішень максимум")
        
        try:
            # Обробляємо кожен батч паралельно
            for batch_index, batch in enumerate(batches):
                # 🚀 БІЛЬШІ МІНІ-БАТЧІ ДЛЯ МАКСИМАЛЬНОГО ВИКОРИСТАННЯ ПОТОКІВ
                mini_batch_size = getattr(settings, "RESOLUTION_MINI_BATCH_SIZE", 75)  # КРИТИЧНО: збільшено до 75
                mini_batches = [batch[i:i+mini_batch_size] for i in range(0, len(batch), mini_batch_size)]
                
                for mini_batch in mini_batches:
                    # Використовуємо ThreadPoolExecutor з максимальною кількістю потоків
                    with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        # Створюємо завдання для кожного рішення
                        future_to_decision = {
                            executor.submit(self.process_single_decision_enhanced, decision): decision
                            for decision in mini_batch
                        }
                        
                        # Обробляємо результати по мірі завершення
                        for future in concurrent.futures.as_completed(future_to_decision):
                            try:
                                processed_decision = future.result()
                                # ВИПРАВЛЕНИЙ критерій успішності - виключаємо також "Резолютивна частина не знайдена"
                                if (processed_decision and processed_decision.resolution_text and 
                                    not processed_decision.resolution_text.startswith("Не вдалося") and
                                    not processed_decision.resolution_text.startswith("Помилка") and
                                    processed_decision.resolution_text != "Резолютивна частина не знайдена" and
                                    processed_decision.resolution_text != "URL документа відсутній або некоректний"):
                                    success_count += 1
                                total_processed += 1
                                
                                # Викликаємо callback для оновлення прогресу кожні 100 рішень
                                if progress_callback and total_processed % 100 == 0:
                                    progress_callback(total_processed, len(decisions_list), success_count)
                                
                            except Exception as e:
                                self.logger.error(f"Помилка при обробці рішення: {e}")
                                total_processed += 1
                
        except Exception as e:
            self.logger.error(f"Помилка при швидкому витягуванні резолютивних частин: {e}")
            return {
                "success": False,
                "error": str(e),
                "processed": total_processed,
                "successful": success_count,
                "failed": total_processed - success_count
            }
        finally:
            # Зупиняємо попереднє завантаження
            self.stop_preloading()
            
            # Фінальний виклик callback
            if progress_callback:
                progress_callback(total_processed, len(decisions_list), success_count)
        
        return {
            "success": True,
            "processed": total_processed,
            "successful": success_count,
            "failed": total_processed - success_count
        }
    
    def extract_resolutions_continuous(self):
        """
        Безперервне витягування резолютивних частин
        """
        print("Запуск безперервного витягування резолютивних частин...")
        
        while True:
            try:
                # Перевіряємо чи є рішення для обробки
                pending_count = TrackedCourtDecision.objects.filter(
                    resolution_text__isnull=True,
                    doc_url__isnull=False
                ).exclude(
                    doc_url__exact=""
                ).exclude(
                    doc_url__exact="nan"
                ).count()
                
                if pending_count == 0:
                    print("Всі резолютивні частини витягнуто. Очікування нових рішень...")
                    time.sleep(60)  # Очікуємо 1 хвилину
                    continue
                
                print(f"Знайдено {pending_count} рішень для витягування резолютивних частин")
                
                # Обробляємо батч
                result = self.extract_resolutions_batch(self.batch_size)
                
                if result["processed"] == 0:
                    print("Немає рішень для обробки. Очікування...")
                    time.sleep(30)
                else:
                    print(f"Оброблено {result["processed"]} рішень, "
                          f"успішно витягнуто {result["successful"]} резолютивних частин")
                
                # Невелика пауза між батчами
                time.sleep(5)
                
            except KeyboardInterrupt:
                print("Зупинка безперервного витягування за запитом користувача")
                break
            except Exception as e:
                self.logger.error(f"Помилка в безперервному витягуванні: {e}")
                print(f"Помилка в безперервному витягуванні: {e}")
                time.sleep(10)  # Пауза перед повторною спробою

    def analyze_triggers(self, decision):
        """
        Аналізує тригерні слова у резолютивній частині рішення
        """
        try:
            from bankruptcy.trigger_words import has_trigger_words
            
            if not decision.resolution_text:
                return
                
            analysis = has_trigger_words(decision.resolution_text)
            
            # Оновлюємо поля на основі аналізу
            decision.has_trigger_words = analysis["has_triggers"]
            decision.trigger_words_found = analysis["found_triggers"]
            decision.trigger_types = analysis["trigger_types"]
            decision.is_critical_decision = analysis["is_critical"]
            
            # Логуємо знахідки тригерів
            if analysis["has_triggers"]:
                self.logger.info(f"Знайдено тригери в рішенні {decision.id}: {analysis["found_triggers"]}")
                
        except Exception as e:
            self.logger.error(f"Помилка аналізу тригерів для рішення {decision.id}: {e}")
    
    def extract_resolutions_incremental(self, limit=None):
        """
        Інкрементальне витягування резолютивних частин - тільки для нових рішень
        """
        if limit is None:
            limit = self.batch_size
        
        # Отримуємо тільки рішення БЕЗ резолютивного тексту (інкрементально)
        decisions_to_process = TrackedCourtDecision.objects.filter(
            models.Q(resolution_text__isnull=True) | models.Q(resolution_text__exact="")
        ).filter(
            doc_url__isnull=False
        ).exclude(
            doc_url__exact=""
        ).exclude(
            doc_url__exact="nan"
        ).order_by("-found_at")[:limit]
        
        if not decisions_to_process:
            return {
                "success": True,
                "processed": 0,
                "successful": 0,
                "failed": 0,
                "message": "Всі рішення вже оброблені (інкрементальний режим)"
            }
        
        print(f"Інкрементальне витягування для {len(decisions_to_process)} нових рішень...")
        
        # Використовуємо той же підхід що і в звичайному витягуванні
        return self.extract_resolutions_for_list(decisions_to_process)
    
    def should_use_incremental_mode(self):
        """
        Перевіряє чи потрібно використовувати інкрементальний режим
        """
        # Якщо менше 10% рішень потребують обробки - використовуємо інкрементальний режим
        total_decisions = TrackedCourtDecision.objects.filter(
            doc_url__isnull=False
        ).exclude(
            doc_url__exact=""
        ).exclude(
            doc_url__exact="nan"
        ).count()
        
        pending_decisions = TrackedCourtDecision.objects.filter(
            models.Q(resolution_text__isnull=True) | models.Q(resolution_text__exact="")
        ).filter(
            doc_url__isnull=False
        ).exclude(
            doc_url__exact=""
        ).exclude(
            doc_url__exact="nan"
        ).count()
        
        if total_decisions == 0:
            return False
            
        pending_percentage = (pending_decisions / total_decisions) * 100
        
        # Інкрементальний режим, якщо менше 10% потребують обробки
        return pending_percentage < 10.0


# Утилітні функції для використання в командах та views
def extract_resolutions_fast(limit=None):
    """
    Швидке витягування резолютивних частин
    """
    extractor = FastResolutionExtractor()
    return extractor.extract_resolutions_batch(limit)


def start_continuous_extraction():
    """
    Запускає безперервне витягування резолютивних частин
    """
    extractor = FastResolutionExtractor()
    extractor.extract_resolutions_continuous()


def get_extraction_statistics():
    """
    Отримує статистику витягування резолютивних частин з тригерами
    Використовує менеджер з"єднань для безпечної роботи
    """
    try:
        # Використовуємо менеджер з"єднань для безпечних запитів
        from bankruptcy.utils.connection_manager import safe_db_connection
        
        with safe_db_connection() as connection:
            total_decisions = TrackedCourtDecision.objects.count()
            extracted_decisions = TrackedCourtDecision.objects.filter(
                resolution_text__isnull=False
            ).exclude(resolution_text__exact="").count()
            
            pending_decisions = TrackedCourtDecision.objects.filter(
                resolution_text__isnull=True,
                doc_url__isnull=False
            ).exclude(
                doc_url__exact=""
            ).exclude(
                doc_url__exact="nan"
            ).count()
            
            # Статистика тригерів
            decisions_with_triggers = TrackedCourtDecision.objects.filter(
                has_trigger_words=True
            ).count()
            
            critical_decisions = TrackedCourtDecision.objects.filter(
                is_critical_decision=True
            ).count()
            
            resolution_triggers = TrackedCourtDecision.objects.filter(
                trigger_types__contains=["resolution"]
            ).count()
    except Exception as e:
        # Якщо помилка з"єднання - повертаємо базову статистику
        logger = logging.getLogger(__name__)
        logger.error(f"Помилка отримання статистики витягування: {e}")
        return {
            "total_decisions": 0,
            "extracted_decisions": 0,
            "pending_decisions": 0,
            "extraction_percentage": 0,
            "decisions_with_triggers": 0,
            "critical_decisions": 0,
            "resolution_triggers": 0,
            "trigger_percentage": 0,
            "error": str(e)
        }
    
    return {
        "total_decisions": total_decisions,
        "extracted_decisions": extracted_decisions,
        "pending_decisions": pending_decisions,
        "extraction_percentage": (extracted_decisions / total_decisions * 100) if total_decisions > 0 else 0,
        "decisions_with_triggers": decisions_with_triggers,
        "critical_decisions": critical_decisions,
        "resolution_triggers": resolution_triggers,
        "trigger_percentage": (decisions_with_triggers / extracted_decisions * 100) if extracted_decisions > 0 else 0
    }