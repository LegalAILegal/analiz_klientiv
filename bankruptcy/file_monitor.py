import os
import time
import threading
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from django.core.management import call_command
from django.conf import settings
import logging
from bankruptcy.utils.index_optimizer import index_optimizer

logger = logging.getLogger(__name__)

class CSVFileHandler(FileSystemEventHandler):
    """Обробник подій файлової системи для CSV файлів"""
    
    def __init__(self, csv_filename, documents_pattern=r"documents_\d{2}\.csv"):
        super().__init__()
        self.csv_filename = csv_filename
        self.documents_pattern = documents_pattern
        self.last_processed = 0
        self.processing = False
        import re
        self.documents_regex = re.compile(documents_pattern)
        
    def on_modified(self, event):
        """Викликається при зміні файлу"""
        if event.is_directory:
            return
            
        filename = os.path.basename(event.src_path)
        
        # Перевіряємо чи це файл банкрутства або документів судових рішень
        is_bankruptcy_file = filename == self.csv_filename
        is_documents_file = self.documents_regex.match(filename)
        
        if is_bankruptcy_file or is_documents_file:
            current_time = time.time()
            
            # Уникаємо множинних викликів (debounce)
            if current_time - self.last_processed < 5:  # 5 секунд затримка
                return
                
            if self.processing:
                return
                
            self.last_processed = current_time
            self.processing = True
            
            # Визначаємо тип файлу та запускаємо відповідне оновлення
            if is_bankruptcy_file:
                update_thread = threading.Thread(target=self._update_bankruptcy_database, args=(event.src_path,))
            else:
                update_thread = threading.Thread(target=self._update_documents_database, args=(event.src_path,))
                
            update_thread.daemon = True
            update_thread.start()
    
    def _update_bankruptcy_database(self, file_path):
        """Оновлює базу даних банкрутства з CSV файлу"""
        try:
            logger.info(f"Виявлено зміни у файлі {file_path}. Починаю інкрементальне оновлення банкрутства...")
            
            # Чекаємо трохи, щоб файл повністю записався
            time.sleep(2)
            
            # Викликаємо команду для інкрементального оновлення
            call_command("load_bankruptcy_data", file=file_path, incremental=True)
            
            logger.info(f"Інкрементальне оновлення банкрутства завершено успішно в {datetime.now()}")
            
            # Автоматично шукаємо судові рішення для нових справ
            try:
                from bankruptcy.models import MonitoringStatistics
                
                logger.info(f"Починаю автоматичний пошук судових рішень для нових справ банкрутства...")
                
                # Позначаємо початок процесу пошуку
                MonitoringStatistics.start_processing("auto_search_new_bankruptcy", 0)
                
                # Використовуємо стандартну команду пошуку (тільки для pending справ)
                call_command("search_court_decisions", limit=50)
                
                # Позначаємо завершення процесу
                MonitoringStatistics.finish_processing("auto_search_new_bankruptcy")
                
                logger.info(f"Автоматичний пошук судових рішень для нових справ завершено в {datetime.now()}")
                
                # Автоматично оновлюємо RTF посилання після пошуку нових справ
                try:
                    logger.info(f"Починаю автоматичне оновлення RTF посилань для нових справ...")
                    
                    # Позначаємо початок процесу оновлення RTF
                    MonitoringStatistics.start_processing("auto_update_rtf_new_cases", 0)
                    
                    # Оновлюємо RTF посилання (загальне оновлення)
                    call_command("update_rtf_links", limit=500, batch_size=50)
                    
                    # Позначаємо завершення процесу
                    MonitoringStatistics.finish_processing("auto_update_rtf_new_cases")
                    
                    logger.info(f"Автоматичне оновлення RTF посилань для нових справ завершено в {datetime.now()}")
                except Exception as rtf_error:
                    # Завершуємо процес навіть при помилці
                    try:
                        MonitoringStatistics.finish_processing("auto_update_rtf_new_cases")
                    except:
                        pass
                    logger.error(f"Помилка при автоматичному оновленні RTF посилань для нових справ: {str(rtf_error)}")
            except Exception as search_error:
                # Завершуємо процес навіть при помилці
                try:
                    from bankruptcy.models import MonitoringStatistics
                    MonitoringStatistics.finish_processing("auto_search_new_bankruptcy")
                except:
                    pass
                logger.error(f"Помилка при автоматичному пошуку судових рішень для нових справ: {str(search_error)}")
            
        except Exception as e:
            logger.error(f"Помилка при автоматичному оновленні банкрутства: {str(e)}")
        finally:
            self.processing = False
    
    def _update_documents_database(self, file_path):
        """Оновлює базу даних судових рішень з CSV файлу"""
        try:
            logger.info(f"Виявлено зміни у файлі {file_path}. Починаю інкрементальне оновлення судових рішень...")
            
            # Чекаємо трохи, щоб файл повністю записався
            time.sleep(2)
            
            # Визначаємо рік з назви файлу (documents_25.csv -> 2025)
            filename = os.path.basename(file_path)
            year_match = __import__("re").search(r"documents_(\d{2})\.csv", filename)
            
            if year_match:
                short_year = int(year_match.group(1))
                # Конвертуємо короткий рік у повний (25 -> 2025, 05 -> 2005)
                year = 2000 + short_year if short_year <= 30 else 1900 + short_year
                
                # Викликаємо команду для інкрементального оновлення судових рішень
                call_command("import_court_decisions", year=year, batch_size=5000)
                
                logger.info(f"Інкрементальне оновлення судових рішень за {year} рік завершено успішно в {datetime.now()}")
                
                # Автоматично витягуємо резолютивні частини з нових рішень
                try:
                    from bankruptcy.models import MonitoringStatistics
                    
                    logger.info(f"Починаю автоматичне витягування резолютивних частин за {year} рік...")
                    
                    # Позначаємо початок процесу витягування
                    MonitoringStatistics.start_processing(f"extract_resolutions_auto_{year}", 1000)
                    
                    call_command("extract_resolutions_fast", limit=1000)
                    
                    # Позначаємо завершення процесу
                    MonitoringStatistics.finish_processing(f"extract_resolutions_auto_{year}")
                    
                    logger.info(f"Автоматичне витягування резолютивних частин завершено в {datetime.now()}")
                except Exception as extract_error:
                    # Завершуємо процес навіть при помилці
                    try:
                        from bankruptcy.models import MonitoringStatistics
                        MonitoringStatistics.finish_processing(f"extract_resolutions_auto_{year}")
                    except:
                        pass
                    logger.error(f"Помилка при автоматичному витягуванні резолютивних частин: {str(extract_error)}")
                
                # Автоматично шукаємо нові судові рішення для відстежуваних справ
                try:
                    from bankruptcy.models import MonitoringStatistics
                    
                    logger.info(f"Починаю автоматичний пошук нових судових рішень після оновлення бази {year} року...")
                    
                    # Позначаємо початок процесу пошуку
                    MonitoringStatistics.start_processing(f"auto_search_decisions_{year}", 0)
                    
                    # Використовуємо стандартну команду пошуку (тільки для pending справ)
                    call_command("search_court_decisions", limit=100)
                    
                    # Позначаємо завершення процесу
                    MonitoringStatistics.finish_processing(f"auto_search_decisions_{year}")
                    
                    logger.info(f"Автоматичний пошук судових рішень завершено в {datetime.now()}")
                    
                    # Автоматично оновлюємо RTF посилання після пошуку
                    try:
                        logger.info(f"Починаю автоматичне оновлення RTF посилань після оновлення бази {year} року...")
                        
                        # Позначаємо початок процесу оновлення RTF
                        MonitoringStatistics.start_processing(f"auto_update_rtf_{year}", 0)
                        
                        # Оновлюємо RTF посилання для цього року
                        call_command("update_rtf_links", year=year, limit=1000, batch_size=100)
                        
                        # Позначаємо завершення процесу
                        MonitoringStatistics.finish_processing(f"auto_update_rtf_{year}")
                        
                        logger.info(f"Автоматичне оновлення RTF посилань завершено в {datetime.now()}")
                    except Exception as rtf_error:
                        # Завершуємо процес навіть при помилці
                        try:
                            MonitoringStatistics.finish_processing(f"auto_update_rtf_{year}")
                        except:
                            pass
                        logger.error(f"Помилка при автоматичному оновленні RTF посилань: {str(rtf_error)}")
                        
                except Exception as search_error:
                    # Завершуємо процес навіть при помилці
                    try:
                        from bankruptcy.models import MonitoringStatistics
                        MonitoringStatistics.finish_processing(f"auto_search_decisions_{year}")
                    except:
                        pass
                    logger.error(f"Помилка при автоматичному пошуку судових рішень: {str(search_error)}")
                    
            else:
                logger.error(f"Не вдалося визначити рік з назви файлу {filename}")
            
        except Exception as e:
            logger.error(f"Помилка при автоматичному оновленні судових рішень: {str(e)}")
        finally:
            self.processing = False


class FileMonitorService:
    """Сервіс моніторингу файлів CSV"""
    
    def __init__(self):
        self.observer = None
        self.is_running = False
        self.data_dir = os.path.join(settings.BASE_DIR, "data")
        self.csv_filename = "Відомості про справи про банкрутство.csv"
        self.csv_filepath = os.path.join(self.data_dir, self.csv_filename)
        self.state_file = os.path.join(self.data_dir, ".monitor_state")
        self.documents_state_file = os.path.join(self.data_dir, ".documents_monitor_state")
        self.last_modified_time = self._load_last_modified_time()
        self.documents_last_modified = self._load_documents_state()
        self.periodic_thread = None
        self.stop_periodic = False
        
    def _check_global_system_state(self):
        """Перевіряє чи не активний примусовий процес"""
        try:
            from bankruptcy.models import SystemProcessControl
            return not SystemProcessControl.is_any_process_forced()
        except Exception as e:
            logger.warning(f"Не вдалося перевірити глобальний стан системи: {e}")
            return True  # Якщо не можемо перевірити, дозволяємо роботу
    
    def start_monitoring(self):
        """Початок моніторингу файлів"""
        if self.is_running:
            return
            
        if not os.path.exists(self.data_dir):
            logger.warning(f"Директорія {self.data_dir} не існує. Створюю...")
            os.makedirs(self.data_dir, exist_ok=True)
        
        # Створюємо обробник подій
        event_handler = CSVFileHandler(self.csv_filename)
        
        # Створюємо і запускаємо спостерігач
        self.observer = Observer()
        self.observer.schedule(event_handler, self.data_dir, recursive=False)
        self.observer.start()
        
        self.is_running = True
        logger.info(f"Розпочато моніторинг директорії {self.data_dir} для файлу {self.csv_filename}")
        
        # Запускаємо періодичну перевірку файлу
        self._start_periodic_check()
        
    def stop_monitoring(self):
        """Зупинка моніторингу файлів"""
        if self.observer and self.is_running:
            self.observer.stop()
            self.observer.join()
            self.is_running = False
            logger.info("Моніторинг файлів зупинено")
            
        # Зупиняємо періодичну перевірку
        self.stop_periodic = True
        if self.periodic_thread and self.periodic_thread.is_alive():
            self.periodic_thread.join(timeout=1)
    
    def _start_periodic_check(self):
        """Запуск періодичної перевірки файлу кожні 30 секунд"""
        if os.path.exists(self.csv_filepath):
            self.last_modified_time = os.path.getmtime(self.csv_filepath)
        
        self.stop_periodic = False
        self.periodic_thread = threading.Thread(target=self._periodic_check_loop)
        self.periodic_thread.daemon = True
        self.periodic_thread.start()
        logger.info("Запущено періодичну перевірку файлу кожні 30 секунд")
        
        # Виконуємо першу перевірку відразу
        self._perform_initial_check()
    
    def _periodic_check_loop(self):
        """Цикл періодичної перевірки файлів"""
        while not self.stop_periodic:
            try:
                # Перевіряємо файл банкрутства
                self._check_bankruptcy_file()
                
                # Перевіряємо файли документів
                self._check_documents_files()
                
                # Періодично перевіряємо та оновлюємо RTF посилання
                self._periodic_rtf_check()
                        
            except Exception as e:
                logger.error(f"Помилка при періодичній перевірці файлів: {str(e)}")
            
            # Очікуємо 30 секунд або зупинку
            for _ in range(30):
                if self.stop_periodic:
                    break
                time.sleep(1)
    
    def _check_bankruptcy_file(self):
        """Перевірка файлу банкрутства"""
        if os.path.exists(self.csv_filepath):
            current_modified_time = os.path.getmtime(self.csv_filepath)
            
            # Перевіряємо чи база даних порожня
            from bankruptcy.models import BankruptcyCase
            db_is_empty = BankruptcyCase.objects.count() == 0
            
            # Завантажуємо дані якщо:
            # 1. Файл оновився після останньої перевірки, АБО
            # 2. База даних порожня, а файл містить дані
            should_update = (current_modified_time > self.last_modified_time) or \
                          (db_is_empty and self._file_has_data())
            
            if should_update:
                if current_modified_time > self.last_modified_time:
                    logger.info(f"Виявлено оновлення файлу {self.csv_filename} (періодична перевірка)")
                elif db_is_empty:
                    logger.info(f"База даних порожня, завантажую дані з {self.csv_filename}")
                
                self.last_modified_time = current_modified_time
                self._save_last_modified_time()
                
                # Запускаємо оновлення в окремому потоці
                update_thread = threading.Thread(
                    target=self._update_database_incremental, 
                    args=(self.csv_filepath,)
                )
                update_thread.daemon = True
                update_thread.start()
    
    def _check_documents_files(self):
        """Перевірка файлів документів"""
        import re
        documents_pattern = re.compile(r"documents_(\d{2})\.csv")
        
        # Знаходимо всі файли документів у директорії
        for filename in os.listdir(self.data_dir):
            match = documents_pattern.match(filename)
            if match:
                file_path = os.path.join(self.data_dir, filename)
                if os.path.exists(file_path):
                    current_modified_time = os.path.getmtime(file_path)
                    last_modified = self.documents_last_modified.get(filename, 0)
                    
                    # Перевіряємо чи це новий файл (не має запису в стані)
                    is_new_file = filename not in self.documents_last_modified
                    
                    if current_modified_time > last_modified or is_new_file:
                        if is_new_file:
                            logger.info(f"Виявлено новий файл {filename} (періодична перевірка)")
                        else:
                            logger.info(f"Виявлено оновлення файлу {filename} (періодична перевірка)")
                        
                        # Оновлюємо час модифікації
                        self.documents_last_modified[filename] = current_modified_time
                        self._save_documents_state()
                        
                        # Запускаємо оновлення в окремому потоці
                        update_thread = threading.Thread(
                            target=self._update_documents_database_incremental, 
                            args=(file_path,)
                        )
                        update_thread.daemon = True
                        update_thread.start()
    
    def _update_database_incremental(self, file_path):
        """Інкрементальне оновлення бази даних"""
        try:
            # Перевіряємо глобальний стан системи
            if not self._check_global_system_state():
                logger.info("Інкрементальне оновлення пропущено - активний примусовий процес")
                return
                
            logger.info(f"Починаю інкрементальне оновлення з файлу {file_path}...")
            
            # Чекаємо трохи, щоб файл повністю записався
            time.sleep(2)
            
            # Викликаємо команду для інкрементального оновлення
            call_command("load_bankruptcy_data", file=file_path, incremental=True)
            
            logger.info(f"Інкрементальне оновлення завершено успішно в {datetime.now()}")
            
        except Exception as e:
            logger.error(f"Помилка при автоматичному інкрементальному оновленні: {str(e)}")
    
    def _update_documents_database_incremental(self, file_path):
        """Інкрементальне оновлення бази даних судових рішень"""
        try:
            # Перевіряємо глобальний стан системи
            if not self._check_global_system_state():
                logger.info("Інкрементальне оновлення судових рішень пропущено - активний примусовий процес")
                return
                
            logger.info(f"Починаю інкрементальне оновлення судових рішень з файлу {file_path}...")
            
            # Чекаємо трохи, щоб файл повністю записався
            time.sleep(2)
            
            # Визначаємо рік з назви файлу (documents_25.csv -> 2025)
            filename = os.path.basename(file_path)
            year_match = __import__("re").search(r"documents_(\d{2})\.csv", filename)
            
            if year_match:
                short_year = int(year_match.group(1))
                # Конвертуємо короткий рік у повний (25 -> 2025, 05 -> 2005)
                year = 2000 + short_year if short_year <= 30 else 1900 + short_year
                
                # Викликаємо команду для інкрементального оновлення судових рішень
                call_command("import_court_decisions", year=year, batch_size=5000)
                
                logger.info(f"Інкрементальне оновлення судових рішень за {year} рік завершено успішно в {datetime.now()}")
                
                # Автоматично витягуємо резолютивні частини з нових рішень
                try:
                    from bankruptcy.models import MonitoringStatistics
                    
                    logger.info(f"Починаю автоматичне витягування резолютивних частин за {year} рік...")
                    
                    # Позначаємо початок процесу витягування
                    MonitoringStatistics.start_processing(f"extract_resolutions_auto_{year}", 1000)
                    
                    call_command("extract_resolutions_fast", limit=1000)
                    
                    # Позначаємо завершення процесу
                    MonitoringStatistics.finish_processing(f"extract_resolutions_auto_{year}")
                    
                    logger.info(f"Автоматичне витягування резолютивних частин завершено в {datetime.now()}")
                except Exception as extract_error:
                    # Завершуємо процес навіть при помилці
                    try:
                        from bankruptcy.models import MonitoringStatistics
                        MonitoringStatistics.finish_processing(f"extract_resolutions_auto_{year}")
                    except:
                        pass
                    logger.error(f"Помилка при автоматичному витягуванні резолютивних частин: {str(extract_error)}")
            else:
                logger.error(f"Не вдалося визначити рік з назви файлу {filename}")
            
        except Exception as e:
            logger.error(f"Помилка при автоматичному інкрементальному оновленні судових рішень: {str(e)}")

    def _perform_initial_check(self):
        """Виконує початкову перевірку при запуску системи"""
        try:
            # Перевіряємо файл банкрутства
            self._initial_check_bankruptcy()
            
            # Перевіряємо файли документів
            self._initial_check_documents()
            
            # Перевіряємо наявність судових рішень для справ без них
            self._initial_check_missing_court_decisions()
            
            # Перевіряємо резолютивні частини
            self._initial_check_missing_resolutions()
            
        except Exception as e:
            logger.error(f"Помилка при початковій перевірці: {str(e)}")
    
    def _initial_check_bankruptcy(self):
        """Початкова перевірка файлу банкрутства"""
        try:
            from bankruptcy.models import BankruptcyCase
            db_is_empty = BankruptcyCase.objects.count() == 0
            
            if os.path.exists(self.csv_filepath) and self._file_has_data():
                current_modified_time = os.path.getmtime(self.csv_filepath)
                
                # Перевіряємо чи потрібно завантажити дані
                should_load = False
                reason = ""
                
                if db_is_empty:
                    should_load = True
                    reason = "база даних банкрутства порожня"
                elif current_modified_time > self.last_modified_time:
                    should_load = True
                    reason = f"файл банкрутства оновлений ({datetime.fromtimestamp(current_modified_time)} > {datetime.fromtimestamp(self.last_modified_time)})"
                
                if should_load:
                    logger.info(f"Початкова перевірка: {reason}. Запускаю завантаження...")
                    # Запускаємо завантаження в окремому потоці
                    initial_load_thread = threading.Thread(
                        target=self._update_database_incremental, 
                        args=(self.csv_filepath,)
                    )
                    initial_load_thread.daemon = True
                    initial_load_thread.start()
                else:
                    # Оновлюємо збережений стан для відстеження майбутніх змін
                    self.last_modified_time = current_modified_time
                    self._save_last_modified_time()
                    logger.info(f"Початкова перевірка: дані банкрутства актуальні. Час файлу: {datetime.fromtimestamp(current_modified_time)}")
            else:
                logger.info("Початкова перевірка: файл банкрутства не існує або порожній")
        except Exception as e:
            logger.error(f"Помилка при початковій перевірці банкрутства: {str(e)}")
    
    def _initial_check_documents(self):
        """Початкова перевірка файлів документів"""
        try:
            import re
            documents_pattern = re.compile(r"documents_(\d{2})\.csv")
            
            # Знаходимо всі файли документів у директорії
            for filename in os.listdir(self.data_dir):
                match = documents_pattern.match(filename)
                if match:
                    file_path = os.path.join(self.data_dir, filename)
                    if os.path.exists(file_path):
                        current_modified_time = os.path.getmtime(file_path)
                        last_modified = self.documents_last_modified.get(filename, 0)
                        
                        # Перевіряємо чи це новий файл (не має запису в стані)
                        is_new_file = filename not in self.documents_last_modified
                        
                        if current_modified_time > last_modified or is_new_file:
                            if is_new_file:
                                logger.info(f"Початкова перевірка: виявлено новий файл {filename}. Запускаю завантаження...")
                            else:
                                logger.info(f"Початкова перевірка: файл {filename} оновлений. Запускаю завантаження...")
                            
                            # Оновлюємо час модифікації
                            self.documents_last_modified[filename] = current_modified_time
                            self._save_documents_state()
                            
                            # Запускаємо завантаження в окремому потоці
                            initial_load_thread = threading.Thread(
                                target=self._update_documents_database_incremental, 
                                args=(file_path,)
                            )
                            initial_load_thread.daemon = True
                            initial_load_thread.start()
                        else:
                            logger.info(f"Початкова перевірка: файл {filename} актуальний. Час файлу: {datetime.fromtimestamp(current_modified_time)}")
        
        except Exception as e:
            logger.error(f"Помилка при початковій перевірці документів: {str(e)}")

    def _file_has_data(self):
        """Перевіряє чи файл містить дані (більше ніж тільки заголовок)"""
        try:
            with open(self.csv_filepath, "r", encoding="utf-8") as f:
                line_count = sum(1 for _ in f)
            return line_count > 1  # Більше ніж один рядок (заголовок)
        except Exception as e:
            logger.error(f"Помилка при перевірці вмісту файлу: {str(e)}")
            return False

    def _load_last_modified_time(self):
        """Завантажує час останньої модифікації з файлу стану"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, "r") as f:
                    return float(f.read().strip())
        except Exception as e:
            logger.warning(f"Не вдалося завантажити стан моніторингу: {str(e)}")
        return 0
    
    def _save_last_modified_time(self):
        """Зберігає час останньої модифікації у файл стану"""
        try:
            with open(self.state_file, "w") as f:
                f.write(str(self.last_modified_time))
        except Exception as e:
            logger.error(f"Не вдалося зберегти стан моніторингу: {str(e)}")
    
    def _load_documents_state(self):
        """Завантажує стан документів з файлу"""
        try:
            if os.path.exists(self.documents_state_file):
                with open(self.documents_state_file, "r") as f:
                    import json
                    return json.loads(f.read())
        except Exception as e:
            logger.warning(f"Не вдалося завантажити стан документів: {str(e)}")
        return {}
    
    def _save_documents_state(self):
        """Зберігає стан документів у файл"""
        try:
            with open(self.documents_state_file, "w") as f:
                import json
                f.write(json.dumps(self.documents_last_modified))
        except Exception as e:
            logger.error(f"Не вдалося зберегти стан документів: {str(e)}")

    def _initial_check_missing_court_decisions(self):
        """Початкова перевірка справ без судових рішень"""
        try:
            # Перевіряємо глобальний стан системи
            if not self._check_global_system_state():
                logger.info("Початкова перевірка судових рішень пропущена - активний примусовий процес")
                return
            from bankruptcy.models import BankruptcyCase, TrackedCourtDecision
            
            # Підраховуємо справи без судових рішень
            total_cases = BankruptcyCase.objects.count()
            cases_with_decisions = BankruptcyCase.objects.filter(
                id__in=TrackedCourtDecision.objects.values("tracked_case_id").distinct()
            ).count()
            cases_without_decisions = total_cases - cases_with_decisions
            
            logger.info(f"Початкова перевірка судових рішень:")
            logger.info(f"  - Загальна кількість справ: {total_cases}")
            logger.info(f"  - Справи з рішеннями: {cases_with_decisions}")
            logger.info(f"  - Справи БЕЗ рішень: {cases_without_decisions}")
            
            # ВИПРАВЛЕНО: НЕ запускаємо початковий пошук через стандартну команду
            # Це заважає безперервному пошуку та створює конфлікт
            # if cases_without_decisions > 0:
            #     logger.info("Запускаю автоматичний пошук судових рішень для справ без них...")
            #     search_thread = threading.Thread(
            #         target=self._search_missing_court_decisions, 
            #         args=(100,)
            #     )
            #     search_thread.daemon = True
            #     search_thread.start()
                
            # Запускаємо БЕЗПЕРЕРВНИЙ пошук судових рішень
            logger.info("Запускаю ТІЛЬКИ безперервний пошук - без початкового пошуку")
            self._start_continuous_search()
                
        except Exception as e:
            logger.error(f"Помилка при початковій перевірці судових рішень: {str(e)}")
    
    def _initial_check_missing_resolutions(self):
        """Початкова перевірка резолютивних частин"""
        try:
            # Перевіряємо глобальний стан системи
            if not self._check_global_system_state():
                logger.info("Початкова перевірка резолютивних частин пропущена - активний примусовий процес")
                return
            from bankruptcy.models import TrackedCourtDecision
            from django.db import models
            
            # Рішення БЕЗ резолютивних частин (NULL або порожній рядок)
            decisions_without_resolutions = TrackedCourtDecision.objects.filter(
                models.Q(resolution_text__isnull=True) | models.Q(resolution_text="")
            ).count()
            
            # Рішення з резолютивними частинами
            decisions_with_resolutions = TrackedCourtDecision.objects.filter(
                resolution_text__isnull=False
            ).exclude(resolution_text="").count()
            
            # Рішення без RTF файлів (NULL, порожній або "nan")
            decisions_without_rtf = TrackedCourtDecision.objects.filter(
                models.Q(doc_url__isnull=True) | 
                models.Q(doc_url="") | 
                models.Q(doc_url="nan")
            ).count()
            
            total_decisions = TrackedCourtDecision.objects.count()
            
            logger.info(f"Початкова перевірка резолютивних частин:")
            logger.info(f"  - Загальна кількість рішень: {total_decisions}")
            logger.info(f"  - Рішення з резолютивними частинами: {decisions_with_resolutions}")
            logger.info(f"  - Рішення БЕЗ резолютивних частин: {decisions_without_resolutions}")
            logger.info(f"  - Рішення БЕЗ RTF файлів: {decisions_without_rtf}")
            
            # ВИПРАВЛЕНО: НЕ запускаємо початкове витягування через стандартну команду
            # Це заважає безперервному витягуванню та створює конфлікт
            # if decisions_without_resolutions > 0:
            #     logger.info("Запускаю автоматичне витягування резолютивних частин...")
            #     extract_thread = threading.Thread(
            #         target=self._extract_missing_resolutions, 
            #         args=(100,)
            #     )
            #     extract_thread.daemon = True
            #     extract_thread.start()
                
            # Запускаємо БЕЗПЕРЕРВНЕ витягування резолютивних частин
            logger.info("Запускаю ТІЛЬКИ безперервне витягування - без початкового витягування")
            self._start_continuous_extraction()
                
        except Exception as e:
            logger.error(f"Помилка при початковій перевірці резолютивних частин: {str(e)}")
    
    def _search_missing_court_decisions(self, limit=None):
        """Пошук судових рішень для справ без них"""
        try:
            from bankruptcy.models import MonitoringStatistics
            
            logger.info(f"Починаю пошук судових рішень без обмежень...")
            
            # Позначаємо початок процесу пошуку
            MonitoringStatistics.start_processing("search_court_decisions", 0)  # 0 означає без ліміту
            
            call_command("search_court_decisions")
            
            # Позначаємо завершення процесу
            MonitoringStatistics.finish_processing("search_court_decisions")
            
            logger.info(f"Пошук судових рішень завершено в {datetime.now()}")
        except Exception as e:
            # Завершуємо процес навіть при помилці
            try:
                from bankruptcy.models import MonitoringStatistics
                MonitoringStatistics.finish_processing("search_court_decisions")
            except:
                pass
            logger.error(f"Помилка при пошуку судових рішень: {str(e)}")
    
    def _extract_missing_resolutions(self, limit=100):
        """Витягування резолютивних частин для рішень без них"""
        try:
            from bankruptcy.models import MonitoringStatistics
            
            logger.info(f"Починаю витягування резолютивних частин для {limit} рішень...")
            
            # Позначаємо початок процесу витягування
            MonitoringStatistics.start_processing("extract_resolutions", limit)
            
            # ВИПРАВЛЕНО: НЕ використовуємо --all щоб не блокувати систему
            # Замість цього обробляємо тільки справи з документами
            from bankruptcy.models import TrackedCourtDecision
            from django.db import models
            
            # Знаходимо справи з документами що потребують витягування
            cases_with_pending_resolutions = TrackedCourtDecision.objects.filter(
                models.Q(resolution_text__isnull=True) | 
                models.Q(resolution_text="") | 
                models.Q(resolution_text="Резолютивна частина не знайдена"),
                doc_url__isnull=False
            ).exclude(doc_url="").exclude(doc_url="nan").values_list("tracked_case_id", flat=True).distinct()[:limit]
            
            if cases_with_pending_resolutions:
                logger.info(f"Витягую резолютивні частини тільки для {len(cases_with_pending_resolutions)} справ з документами")
                for case_id in cases_with_pending_resolutions:
                    try:
                        call_command("extract_resolutions_fast", case_id=case_id)
                    except Exception as e:
                        logger.error(f"Помилка витягування для справи {case_id}: {e}")
                        continue
            else:
                logger.info("Немає справ з документами що потребують витягування")
            
            # Позначаємо завершення процесу
            MonitoringStatistics.finish_processing("extract_resolutions")
            
            logger.info(f"Витягування резолютивних частин завершено в {datetime.now()}")
        except Exception as e:
            # Завершуємо процес навіть при помилці
            try:
                from bankruptcy.models import MonitoringStatistics
                MonitoringStatistics.finish_processing("extract_resolutions")
            except:
                pass
            logger.error(f"Помилка при витягуванні резолютивних частин: {str(e)}")

    def _start_continuous_search(self):
        """Запуск БЕЗПЕРЕРВНОГО пошуку судових рішень до повного завершення"""
        def continuous_search():
            logger.info("🚀 БЕЗПЕРЕРВНИЙ ПОШУК СУДОВИХ РІШЕНЬ ЗАПУЩЕНО - БЕЗ ЛІМІТІВ ТА ПЕРЕРВ!")
            
            while self.observer and self.observer.is_alive():
                try:
                    from bankruptcy.models import TrackedBankruptcyCase
                    
                    # Знаходимо ВСІ справи що потребують пошуку (від нових до старих)
                    from django.db import models
                    pending_cases_query = TrackedBankruptcyCase.objects.filter(
                        models.Q(search_decisions_status__in=["pending", "failed"]) |
                        models.Q(search_decisions_status="completed", search_decisions_found=0)
                    ).order_by("bankruptcy_case__date", "created_at")
                    
                    pending_count = pending_cases_query.count()
                    
                    if pending_count > 0:
                        logger.info(f"🔍 БЕЗПЕРЕРВНА ОБРОБКА: залишилось {pending_count} справ для пошуку")
                        
                        # Обробляємо ВЕЛИКИМИ батчами по 1000 справ одночасно
                        batch_size = 1000
                        cases_batch = list(pending_cases_query[:batch_size])
                        
                        if cases_batch:
                            logger.info(f"⚡ Обробляю батч з {len(cases_batch)} справ...")
                            
                            # Запускаємо пошук для батчу
                            search_thread = threading.Thread(
                                target=self._search_batch_cases, 
                                args=(cases_batch,)
                            )
                            search_thread.daemon = True
                            search_thread.start()
                            
                            # Чекаємо завершення батчу
                            search_thread.join()
                            
                            logger.info(f"✅ Батч завершено. Залишилось: {pending_count - len(cases_batch)} справ")
                        
                        # Мінімальна пауза між батчами тільки для стабільності системи
                        time.sleep(2)
                        
                    else:
                        logger.info("🎉 ВСІ СПРАВИ ОБРОБЛЕНО! Пошук судових рішень завершено повністю.")
                        logger.info("⏳ Чекаю на нові справи або зміни статусу...")
                        # Чекаємо 5 хвилин перед повторною перевіркою
                        time.sleep(5 * 60)
                        
                except Exception as e:
                    logger.error(f"❌ Помилка в безперервному пошуку: {str(e)}")
                    # При помилці чекаємо 1 хвилину і продовжуємо
                    time.sleep(60)
        
        # Запускаємо в окремому потоці
        continuous_thread = threading.Thread(target=continuous_search)
        continuous_thread.daemon = True
        continuous_thread.start()
        logger.info("🚀 БЕЗПЕРЕРВНИЙ ПОШУК СУДОВИХ РІШЕНЬ АКТИВОВАНО!")

    def _search_batch_cases(self, cases_batch):
        """Пошук судових рішень для батчу справ"""
        try:
            from django.core.management import call_command
            from bankruptcy.models import MonitoringStatistics
            
            batch_size = len(cases_batch)
            logger.info(f"🔍 Починаю обробку батчу з {batch_size} справ...")
            
            MonitoringStatistics.start_processing("search_court_decisions_batch", batch_size)
            
            # Обробляємо кожну справу в батчі
            total_found_in_batch = 0
            for i, tracked_case in enumerate(cases_batch, 1):
                try:
                    from bankruptcy.services import BankruptcyCaseSearchService
                    service = BankruptcyCaseSearchService()
                    
                    case_num = tracked_case.bankruptcy_case.case_number
                    logger.info(f"🔍 [{i}/{batch_size}] Шукаю рішення для справи: {case_num}")
                    found_decisions = service.search_and_save_court_decisions(tracked_case)

                    # Оновлюємо поле search_decisions_found
                    tracked_case.search_decisions_found = found_decisions
                    tracked_case.search_decisions_status = "completed"
                    tracked_case.save(update_fields=['search_decisions_found', 'search_decisions_status'])

                    logger.info(f"🔍 [{i}/{batch_size}] {case_num}: завершено пошук - {found_decisions} рішень")

                    if found_decisions > 0:
                        total_found_in_batch += found_decisions
                        logger.info(f"🎯 [{i}/{batch_size}] {case_num}: ЗНАЙДЕНО {found_decisions} рішень! (всього в батчі: {total_found_in_batch})")
                    
                    # Прогрес кожні 100 справ
                    if i % 100 == 0:
                        logger.info(f"📊 Прогрес батчу: {i}/{batch_size} справ оброблено, знайдено {total_found_in_batch} рішень")
                    
                    # Мінімальна затримка між справами
                    time.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"❌ Помилка обробки справи {tracked_case.bankruptcy_case.case_number}: {e}")
                    continue
            
            MonitoringStatistics.finish_processing("search_court_decisions_batch")
            logger.info(f"✅ Батч з {batch_size} справ завершено! 🎯 ВСЬОГО ЗНАЙДЕНО: {total_found_in_batch} рішень!")
            
        except Exception as e:
            try:
                from bankruptcy.models import MonitoringStatistics
                MonitoringStatistics.finish_processing("search_court_decisions_batch")
            except:
                pass
            logger.error(f"❌ Критична помилка обробки батчу: {e}")

    def _start_continuous_extraction(self):
        """Запуск БЕЗПЕРЕРВНОГО витягування резолютивних частин до повного завершення"""
        def continuous_extraction():
            logger.info("🚀 БЕЗПЕРЕРВНЕ ВИТЯГУВАННЯ РЕЗОЛЮТИВНИХ ЧАСТИН ЗАПУЩЕНО - БЕЗ ЛІМІТІВ ТА ПЕРЕРВ!")
            
            while self.observer and self.observer.is_alive():
                try:
                    from bankruptcy.models import TrackedCourtDecision
                    from django.db import models
                    
                    # Знаходимо ВСІ рішення що потребують витягування (з RTF посиланнями)
                    pending_query = TrackedCourtDecision.objects.filter(
                        models.Q(resolution_text__isnull=True) | 
                        models.Q(resolution_text="") | 
                        models.Q(resolution_text="Резолютивна частина не знайдена") |
                        models.Q(resolution_text="Не вдалося завантажити документ"),
                        doc_url__isnull=False
                    ).exclude(doc_url="").exclude(doc_url="nan").order_by("-tracked_case__created_at")
                    
                    pending_count = pending_query.count()
                    
                    if pending_count > 0:
                        logger.info(f"🔍 БЕЗПЕРЕРВНЕ ВИТЯГУВАННЯ: залишилось {pending_count} рішень для обробки")
                        
                        # Обробляємо ПОМІРНИМИ батчами для стабільності з"єднань PostgreSQL
                        batch_size = 500
                        decisions_batch = list(pending_query[:batch_size])
                        
                        if decisions_batch:
                            logger.info(f"⚡ Витягую резолютивні частини з {len(decisions_batch)} рішень...")
                            
                            # Запускаємо ШВИДКЕ витягування для батчу (використовуємо наш оптимізований алгоритм)
                            self._extract_batch_resolutions_fast(decisions_batch)
                            
                            logger.info(f"✅ Батч витягування завершено. Залишилось: {pending_count - len(decisions_batch)} рішень")
                        
                        # Мікропауза між батчами для максимальної швидкості
                        time.sleep(1)
                        
                    else:
                        logger.info("🎉 ВСІ РЕЗОЛЮТИВНІ ЧАСТИНИ ВИТЯГНУТО! Витягування завершено повністю.")
                        logger.info("⏳ Чекаю на нові рішення...")
                        # Чекаємо 10 хвилин перед повторною перевіркою
                        time.sleep(10 * 60)
                        
                except Exception as e:
                    logger.error(f"❌ Помилка в безперервному витягуванні: {str(e)}")
                    # При помилці чекаємо 2 хвилини і продовжуємо
                    time.sleep(2 * 60)
        
        # Запускаємо в окремому потоці
        continuous_thread = threading.Thread(target=continuous_extraction)
        continuous_thread.daemon = True
        continuous_thread.start()
        logger.info("🚀 БЕЗПЕРЕРВНЕ ВИТЯГУВАННЯ РЕЗОЛЮТИВНИХ ЧАСТИН АКТИВОВАНО!")

    def _extract_batch_resolutions_fast(self, decisions_batch):
        """Швидке витягування резолютивних частин для батчу рішень (використовує FastResolutionExtractor)"""
        try:
            from bankruptcy.models import MonitoringStatistics
            from bankruptcy.utils.fast_resolution_extractor import FastResolutionExtractor
            
            batch_size = len(decisions_batch)
            logger.info(f"⚡ ШВИДКЕ витягування резолютивних частин з {batch_size} рішень...")
            
            MonitoringStatistics.start_processing("extract_resolutions_batch_fast", batch_size)
            
            # Створюємо швидкий екстрактор з оптимізованими налаштуваннями
            extractor = FastResolutionExtractor()
            
            # Використовуємо нашу багатопоточну обробку
            start_time = time.time()
            result = extractor.extract_resolutions_batch_custom(decisions_batch)
            duration = time.time() - start_time
            
            if result.get("success", False):
                success_count = result["successful"]
                rate = success_count/duration if duration > 0 else 0
                logger.info(f"🎉 ШВИДКЕ витягування ЗАВЕРШЕНО за {duration:.2f} сек! "
                          f"Успішно: {success_count}/{batch_size} рішень "
                          f"({rate:.1f} рішень/сек)")
            else:
                logger.error(f"❌ Помилка при швидкому витягуванні: {result.get("error", "Невідома помилка")}")
            
            MonitoringStatistics.finish_processing("extract_resolutions_batch_fast")
            
        except Exception as e:
            try:
                from bankruptcy.models import MonitoringStatistics
                MonitoringStatistics.finish_processing("extract_resolutions_batch_fast")
            except:
                pass
            logger.error(f"Помилка при швидкому витягуванні резолютивних частин: {str(e)}")

    def _periodic_rtf_check(self):
        """Періодична перевірка та оновлення RTF посилань"""
        try:
            # Перевіряємо кількість рішень без RTF посилань
            from bankruptcy.models import TrackedCourtDecision
            from django.db import models
            
            without_rtf_count = TrackedCourtDecision.objects.filter(
                models.Q(doc_url__isnull=True) | models.Q(doc_url="")
            ).count()
            
            # Якщо є рішення без RTF та пройшло достатньо часу
            if without_rtf_count > 0:
                # Перевіряємо чи не занадто часто запускаємо
                if not hasattr(self, "_last_rtf_check"):
                    self._last_rtf_check = 0
                
                current_time = time.time()
                # Запускаємо тільки раз на 5 хвилин
                if current_time - self._last_rtf_check > 300:  # 300 секунд = 5 хвилин
                    self._last_rtf_check = current_time
                    
                    # Перевіряємо глобальний стан системи
                    if not self._check_global_system_state():
                        logger.info("Періодичне оновлення RTF пропущено - активний примусовий процес")
                        return
                    
                    logger.info(f"🔗 Періодична перевірка RTF: знайдено {without_rtf_count} рішень без посилань")
                    
                    try:
                        from bankruptcy.models import MonitoringStatistics
                        
                        # Позначаємо початок процесу оновлення RTF
                        MonitoringStatistics.start_processing("periodic_rtf_update", without_rtf_count)
                        
                        # Оновлюємо невелику кількість RTF посилань
                        call_command("update_rtf_links", limit=100, batch_size=25)
                        
                        # Позначаємо завершення процесу
                        MonitoringStatistics.finish_processing("periodic_rtf_update")
                        
                        logger.info(f"🔗 Періодичне оновлення RTF посилань завершено")
                        
                        # НОВЕ: Після оновлення RTF посилань витягуємо резолютивні частини
                        self._extract_resolutions_for_new_rtf()
                        
                    except Exception as rtf_error:
                        # Завершуємо процес навіть при помилці
                        try:
                            from bankruptcy.models import MonitoringStatistics
                            MonitoringStatistics.finish_processing("periodic_rtf_update")
                        except:
                            pass
                        logger.error(f"Помилка при періодичному оновленні RTF: {str(rtf_error)}")
            else:
                # Навіть якщо немає рішень без RTF, перевіряємо нові RTF що потребують витягування  
                self._check_for_new_rtf_to_extract()
                        
        except Exception as e:
            logger.error(f"Помилка при періодичній перевірці RTF: {str(e)}")

    def _extract_resolutions_for_new_rtf(self):
        """Витягування резолютивних частин для рішень з новими RTF посиланнями"""
        try:
            from bankruptcy.models import TrackedCourtDecision
            from django.db import models
            
            # Знаходимо рішення з RTF посиланнями, але без резолютивних частин
            decisions_with_new_rtf = TrackedCourtDecision.objects.filter(
                doc_url__isnull=False,  # Є RTF посилання
                resolution_text__isnull=True,  # Немає резолютивної частини
            ).exclude(
                models.Q(doc_url__exact="") | models.Q(doc_url__exact="nan")
            )[:50]  # Обмежуємо до 50 рішень за раз
            
            count = decisions_with_new_rtf.count()
            
            if count > 0:
                logger.info(f"🆕 Знайдено {count} рішень з новими RTF посиланнями для витягування резолютивних частин")
                
                # Використовуємо швидкий екстрактор
                self._extract_batch_resolutions_fast(list(decisions_with_new_rtf))
                
                logger.info(f"✅ Витягування резолютивних частин з нових RTF завершено")
                
        except Exception as e:
            logger.error(f"Помилка при витягуванні резолютивних частин з нових RTF: {str(e)}")
    
    def _check_for_new_rtf_to_extract(self):
        """Періодична перевірка рішень з RTF що потребують витягування резолютивних частин"""
        try:
            # Перевіряємо тільки раз на 10 хвилин для цього процесу
            if not hasattr(self, "_last_new_rtf_check"):
                self._last_new_rtf_check = 0
            
            current_time = time.time()
            # 600 секунд = 10 хвилин
            if current_time - self._last_new_rtf_check > 600:
                self._last_new_rtf_check = current_time
                
                from bankruptcy.models import TrackedCourtDecision
                from django.db import models
                
                # Рахуємо кількість рішень з RTF, але без резолютивних частин
                pending_rtf_count = TrackedCourtDecision.objects.filter(
                    doc_url__isnull=False,
                    resolution_text__isnull=True
                ).exclude(
                    models.Q(doc_url__exact="") | models.Q(doc_url__exact="nan")
                ).count()
                
                if pending_rtf_count > 0:
                    logger.info(f"🔍 Знайдено {pending_rtf_count} рішень з RTF що потребують витягування резолютивних частин")
                    
                    # Витягуємо невелику кількість за раз
                    self._extract_resolutions_for_new_rtf()
                    
        except Exception as e:
            logger.error(f"Помилка при перевірці нових RTF для витягування: {str(e)}")

    def reset_monitoring_state(self):
        """Скидає стан моніторингу (час останньої модифікації)"""
        self.last_modified_time = 0
        self.documents_last_modified = {}
        self._save_last_modified_time()
        self._save_documents_state()
        logger.info("Стан моніторингу скинуто")

    def __del__(self):
        """Деструктор для автоматичної зупинки моніторингу"""
        try:
            self.stop_monitoring()
        except:
            pass


# Глобальний екземпляр сервісу
monitor_service = FileMonitorService()