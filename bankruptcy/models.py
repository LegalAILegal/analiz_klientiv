from django.db import models
from django.core.validators import FileExtensionValidator
from django.utils import timezone
from datetime import datetime
import os


class SystemProcessControl(models.Model):
    """Модель для управління глобальними процесами системи"""
    
    PROCESS_TYPES = [
        ("court_search", "Пошук судових рішень"),
        ("resolution_extraction", "Витягування резолютивних частин"),
        ("file_monitoring", "Моніторинг файлів"),
    ]
    
    PROCESS_STATUSES = [
        ("idle", "Очікування"),
        ("running", "Виконується"),
        ("forced_running", "Примусово запущений"),
        ("paused", "Призупинений"),
        ("stopped", "Зупинений"),
        ("error", "Помилка"),
    ]
    
    process_type = models.CharField(
        max_length=50, 
        choices=PROCESS_TYPES, 
        unique=True,
        verbose_name="Тип процесу"
    )
    status = models.CharField(
        max_length=20, 
        choices=PROCESS_STATUSES, 
        default="idle",
        verbose_name="Статус процесу"
    )
    is_forced = models.BooleanField(
        default=False,
        verbose_name="Примусовий режим"
    )
    force_stop_others = models.BooleanField(
        default=False,
        verbose_name="Зупинити інші процеси"
    )
    started_at = models.DateTimeField(
        null=True, 
        blank=True,
        verbose_name="Час запуску"
    )
    finished_at = models.DateTimeField(
        null=True, 
        blank=True,
        verbose_name="Час завершення"
    )
    progress_current = models.IntegerField(
        default=0,
        verbose_name="Поточний прогрес"
    )
    progress_total = models.IntegerField(
        default=0,
        verbose_name="Загальна кількість"
    )
    last_message = models.TextField(
        blank=True,
        verbose_name="Останнє повідомлення"
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Створено"
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        verbose_name="Оновлено"
    )
    
    class Meta:
        verbose_name = "Управління процесом"
        verbose_name_plural = "Управління процесами"
        db_table = "bankruptcy_system_process_control"
    
    def __str__(self):
        return f"{self.get_process_type_display()} - {self.get_status_display()}"
    
    @classmethod
    def is_any_process_forced(cls):
        """Перевіряє чи є хоча б один процес в примусовому режимі"""
        return cls.objects.filter(is_forced=True, status="forced_running").exists()
    
    @classmethod
    def get_forced_process(cls):
        """Повертає процес який працює в примусовому режимі"""
        return cls.objects.filter(is_forced=True, status="forced_running").first()
    
    def start_forced(self):
        """Запускає процес в примусовому режимі"""
        # Спочатку зупиняємо всі інші процеси якщо потрібно
        if self.force_stop_others:
            SystemProcessControl.objects.exclude(pk=self.pk).update(
                status="paused",
                is_forced=False
            )
        
        # Запускаємо поточний процес
        self.status = "forced_running"
        self.is_forced = True
        self.started_at = timezone.now()
        self.finished_at = None
        self.save()
    
    def stop_forced(self):
        """Зупиняє примусовий режим"""
        self.status = "idle"
        self.is_forced = False
        self.force_stop_others = False
        self.finished_at = timezone.now()
        self.progress_current = 0
        self.progress_total = 0
        self.save()
        
        # Відновлюємо інші процеси до штатного режиму
        SystemProcessControl.objects.filter(status="paused").update(
            status="idle"
        )
    
    def update_progress(self, current, total, message=""):
        """Оновлює прогрес процесу"""
        self.progress_current = current
        self.progress_total = total
        if message:
            self.last_message = message
        self.save()
    
    @property
    def progress_percentage(self):
        """Повертає прогрес у відсотках"""
        if self.progress_total > 0:
            return (self.progress_current / self.progress_total) * 100
        return 0


class Court(models.Model):
    name = models.CharField(max_length=255, unique=True, verbose_name="Назва суду")
    
    class Meta:
        verbose_name = "Суд"
        verbose_name_plural = "Суди"
        db_table = "bankruptcy_court"
    
    def __str__(self):
        return self.name


class Company(models.Model):
    edrpou = models.CharField(max_length=20, unique=True, verbose_name="Код ЄДРПОУ")
    name = models.TextField(verbose_name="Назва підприємства")
    
    class Meta:
        verbose_name = "Підприємство"
        verbose_name_plural = "Підприємства"
        db_table = "bankruptcy_company"
    
    def __str__(self):
        return f"{self.edrpou} - {self.name[:50]}..."


class BankruptcyCase(models.Model):
    number = models.IntegerField(verbose_name="Порядковий номер", unique=True)
    date = models.DateField(verbose_name="Дата провадження")
    type = models.TextField(verbose_name="Тип провадження")
    company = models.ForeignKey(
        Company, 
        on_delete=models.CASCADE, 
        related_name="bankruptcy_cases",
        verbose_name="Підприємство"
    )
    case_number = models.CharField(max_length=50, verbose_name="Номер справи")
    start_date_auc = models.DateField(
        null=True, 
        blank=True, 
        verbose_name="Дата початку аукціону"
    )
    end_date_auc = models.DateField(
        null=True, 
        blank=True, 
        verbose_name="Дата закінчення аукціону"
    )
    court = models.ForeignKey(
        Court,
        on_delete=models.CASCADE,
        related_name="bankruptcy_cases",
        verbose_name="Суд"
    )
    end_registration_date = models.DateField(
        null=True, 
        blank=True, 
        verbose_name="Дата закінчення реєстрації"
    )
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="Створено")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="Оновлено")
    
    class Meta:
        verbose_name = "Справа про банкрутство"
        verbose_name_plural = "Справи про банкрутство"
        ordering = ["-number"]
        indexes = [
            models.Index(fields=["date"]),
            models.Index(fields=["case_number"]),
            models.Index(fields=["number"]),
        ]
    
    def __str__(self):
        return f"{self.case_number} - {self.company.name[:30]}..."


class Region(models.Model):
    code = models.CharField(max_length=10, unique=True, verbose_name="Код регіону")
    name = models.CharField(max_length=255, verbose_name="Назва регіону")
    
    class Meta:
        verbose_name = "Регіон"
        verbose_name_plural = "Регіони"
    
    def __str__(self):
        return self.name


class Instance(models.Model):
    code = models.CharField(max_length=10, unique=True, verbose_name="Код інстанції")
    name = models.CharField(max_length=255, verbose_name="Назва інстанції")
    
    class Meta:
        verbose_name = "Інстанція"
        verbose_name_plural = "Інстанції"
        db_table = "bankruptcy_instances"
    
    def __str__(self):
        return self.name


class JusticeKind(models.Model):
    code = models.CharField(max_length=10, unique=True, verbose_name="Код виду судочинства")
    name = models.CharField(max_length=255, verbose_name="Назва виду судочинства")
    
    class Meta:
        verbose_name = "Вид судочинства"
        verbose_name_plural = "Види судочинства"
        db_table = "bankruptcy_justice_kinds"
    
    def __str__(self):
        return self.name


class JudgmentForm(models.Model):
    code = models.CharField(max_length=10, unique=True, verbose_name="Код форми судового рішення")
    name = models.CharField(max_length=255, verbose_name="Назва форми судового рішення")
    
    class Meta:
        verbose_name = "Форма судового рішення"
        verbose_name_plural = "Форми судових рішень"
        db_table = "bankruptcy_judgment_forms"
    
    def __str__(self):
        return self.name


class CauseCategory(models.Model):
    code = models.CharField(max_length=10, unique=True, verbose_name="Код категорії справи")
    name = models.CharField(max_length=255, verbose_name="Назва категорії справи")
    
    class Meta:
        verbose_name = "Категорія справи"
        verbose_name_plural = "Категорії справ"
        db_table = "bankruptcy_cause_categories"
    
    def __str__(self):
        return self.name


class CourtDecisionDatabase(models.Model):
    IMPORT_STATUS_CHOICES = [
        ("pending", "Очікування"),
        ("importing", "Імпорт"),
        ("completed", "Завершено"),
        ("failed", "Помилка"),
    ]
    
    year = models.IntegerField(unique=True, verbose_name="Рік")
    csv_file_path = models.CharField(max_length=512, verbose_name="Шлях до CSV файлу", blank=True)
    csv_file_size = models.BigIntegerField(default=0, verbose_name="Розмір CSV файлу")
    csv_last_modified = models.DateTimeField(null=True, blank=True, verbose_name="Дата зміни CSV")
    
    documents_count = models.IntegerField(default=0, verbose_name="Кількість документів")
    
    import_status = models.CharField(
        max_length=20, 
        choices=IMPORT_STATUS_CHOICES, 
        default="pending",
        verbose_name="Статус імпорту"
    )
    import_started_at = models.DateTimeField(null=True, blank=True, verbose_name="Початок імпорту")
    import_completed_at = models.DateTimeField(null=True, blank=True, verbose_name="Завершення імпорту")
    import_error_message = models.TextField(blank=True, verbose_name="Повідомлення про помилку")
    
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="Створено")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="Оновлено")
    
    class Meta:
        verbose_name = "База даних судових рішень"
        verbose_name_plural = "Бази даних судових рішень"
        ordering = ["-year"]
    
    def __str__(self):
        return f"Судові рішення {self.year} року ({self.documents_count} документів)"
    
    def get_csv_filename(self):
        if self.year >= 2000:
            short_year = self.year - 2000
        else:
            short_year = self.year - 1900
        return f"documents_{short_year:02d}.csv"
    
    def needs_import(self):
        if self.import_status in ["pending", "failed"]:
            return True
        
        if self.csv_file_path and os.path.exists(self.csv_file_path):
            current_size = os.path.getsize(self.csv_file_path)
            current_modified = os.path.getmtime(self.csv_file_path)
            
            if (self.csv_file_size != current_size or 
                not self.csv_last_modified or
                abs(self.csv_last_modified.timestamp() - current_modified) > 1):
                return True
        
        return False


class TrackedBankruptcyCase(models.Model):
    """
    Модель для відстеження справ банкрутства з автоматичним пошуком судових рішень
    Аналогічна до Case з еталонного проекту
    """
    STATUS_CHOICES = [
        ("active", "Активна"),
        ("completed", "Завершена"),
        ("suspended", "Призупинена"),
        ("archived", "Архівована"),
    ]
    
    bankruptcy_case = models.ForeignKey(
        BankruptcyCase,
        on_delete=models.CASCADE,
        related_name="tracked_cases",
        verbose_name="Справа про банкрутство"
    )
    
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default="active",
        verbose_name="Статус відстеження"
    )
    
    # Статуси фонових задач пошуку судових рішень
    search_decisions_status = models.CharField(
        max_length=20,
        choices=[
            ("pending", "Очікування"),
            ("running", "Виконується"),
            ("completed", "Завершено"),
            ("failed", "Помилка")
        ],
        default="pending",
        verbose_name="Статус пошуку рішень"
    )
    search_decisions_found = models.IntegerField(default=0, verbose_name="Знайдено рішень")
    search_decisions_completed_at = models.DateTimeField(null=True, blank=True, verbose_name="Пошук завершено")
    
    # Пріоритет для обробки (нові справи мають вищий пріоритет)
    priority = models.IntegerField(default=0, verbose_name="Пріоритет", help_text="Вищий номер = вищий пріоритет")
    
    # Часові мітки
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="Створено")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="Оновлено")
    
    class Meta:
        verbose_name = "Відстежувана справа банкрутства"
        verbose_name_plural = "Відстежувані справи банкрутства"
        ordering = ["-priority", "-created_at"]
        unique_together = ["bankruptcy_case"]
        indexes = [
            models.Index(fields=["status", "priority"]),
            models.Index(fields=["search_decisions_status"]),
            models.Index(fields=["-created_at"]),
        ]
    
    def __str__(self):
        return f"Відстеження {self.bankruptcy_case.case_number}"
    
    def needs_decisions_search(self):
        """Перевіряє чи потрібен пошук судових рішень для цієї справи"""
        if self.search_decisions_status in ["pending", "failed"]:
            return True
        
        if self.search_decisions_status == "completed" and self.search_decisions_found == 0:
            if not self.tracked_court_decisions.exists():
                return True
        
        return False
    
    def trigger_background_search_decisions(self):
        """Позначає справу для фонового пошуку судових рішень (виконується через management команди)"""
        import logging
        
        logger = logging.getLogger(__name__)
        
        if not self.needs_decisions_search():
            logger.info(f"Пошук судових рішень для справи {self.bankruptcy_case.case_number} не потрібен")
            return
        
        # Просто позначаємо справу як таку, що потребує пошуку
        # Фактичний пошук буде виконано через management команди
        if self.search_decisions_status not in ["pending", "running"]:
            TrackedBankruptcyCase.objects.filter(id=self.id).update(
                search_decisions_status="pending"
            )
            
        logger.info(f"Справа {self.bankruptcy_case.case_number} позначена для фонового пошуку судових рішень")


class TrackedCourtDecision(models.Model):
    """
    Модель для судових рішень знайдених для відстежуваних справ банкрутства
    Аналогічна до CaseSearchResult з еталонного проекту
    """
    tracked_case = models.ForeignKey(
        TrackedBankruptcyCase,
        on_delete=models.CASCADE,
        related_name="tracked_court_decisions",
        verbose_name="Відстежувана справа"
    )
    
    doc_id = models.CharField(max_length=255, verbose_name="ID документа")
    court_code = models.CharField(max_length=100, verbose_name="Код суду", blank=True)
    judgment_code = models.CharField(max_length=100, verbose_name="Код рішення", blank=True)
    justice_kind = models.CharField(max_length=100, verbose_name="Вид судочинства", blank=True)
    category_code = models.CharField(max_length=100, verbose_name="Код категорії", blank=True)
    cause_num = models.CharField(max_length=255, verbose_name="Номер справи", blank=True)
    adjudication_date = models.CharField(max_length=50, verbose_name="Дата рішення", blank=True)
    receipt_date = models.CharField(max_length=50, verbose_name="Дата надходження", blank=True)
    judge = models.CharField(max_length=500, verbose_name="Суддя", blank=True)
    doc_url = models.URLField(verbose_name="URL документа", blank=True)
    status = models.CharField(max_length=100, verbose_name="Статус", blank=True)
    date_publ = models.CharField(max_length=50, verbose_name="Дата публікації", blank=True)
    database_source = models.CharField(max_length=200, verbose_name="База даних", blank=True)
    
    # Розшифровані назви
    court_name = models.CharField(max_length=500, verbose_name="Назва суду", blank=True)
    judgment_name = models.CharField(max_length=200, verbose_name="Назва рішення", blank=True)
    justice_kind_name = models.CharField(max_length=200, verbose_name="Назва виду судочинства", blank=True)
    category_name = models.CharField(max_length=200, verbose_name="Назва категорії", blank=True)
    
    # Витягнута резолютивна частина
    resolution_text = models.TextField(verbose_name="Резолютивна частина", blank=True)
    resolution_extracted_at = models.DateTimeField(
        null=True, 
        blank=True,
        verbose_name="Дата витягування резолютивної частини"
    )
    
    # Тригерні слова та маркування для мовної моделі
    has_trigger_words = models.BooleanField(
        default=False, 
        verbose_name="Має тригерні слова",
        help_text="Чи містить резолютивна частина тригерні слова для мовної моделі"
    )
    trigger_words_found = models.JSONField(
        default=list, 
        blank=True,
        verbose_name="Знайдені тригерні слова",
        help_text="Список знайдених тригерних слів"
    )
    trigger_types = models.JSONField(
        default=list, 
        blank=True,
        verbose_name="Типи тригерів",
        help_text="Типи тригерів: resolution, judgment_type, critical"
    )
    is_critical_decision = models.BooleanField(
        default=False,
        verbose_name="Критичне рішення",
        help_text="Чи містить критичні тригери для особливої уваги мовної моделі"
    )
    
    found_at = models.DateTimeField(auto_now_add=True, verbose_name="Знайдено")
    
    class Meta:
        verbose_name = "Відстежене судове рішення"
        verbose_name_plural = "Відстежені судові рішення"
        unique_together = ["tracked_case", "doc_id"]
        ordering = ["-found_at"]
        indexes = [
            models.Index(fields=["doc_id"]),
            models.Index(fields=["cause_num"]),
            models.Index(fields=["adjudication_date"]),
        ]
    
    def __str__(self):
        return f"Рішення {self.doc_id} для справи {self.tracked_case.bankruptcy_case.case_number}"
    
    def get_yedr_url(self):
        """Повертає посилання на ЄДРСР"""
        if self.doc_id:
            return f"https://reyestr.court.gov.ua/Review/{self.doc_id}"
        return None
    
    def has_rtf_document(self):
        """Перевіряє чи є RTF документ"""
        return self.doc_url and self.doc_url.endswith(".rtf")
    
    def _format_date(self, date_str):
        """Форматує дату з формату "2025-07-14 00:00:00+03" в "14.07.2025" """
        from datetime import datetime
        
        if not date_str:
            return ""
            
        try:
            if "+" in date_str and " " in date_str:
                date_part = date_str.split(" ")[0]
                parsed_date = datetime.strptime(date_part, "%Y-%m-%d")
                return parsed_date.strftime("%d.%m.%Y")
            elif "-" in date_str and len(date_str) >= 8:
                parsed_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
                return parsed_date.strftime("%d.%m.%Y")
            elif "." in date_str and len(date_str) >= 8:
                return date_str
            else:
                return date_str
        except:
            return date_str
    
    def save(self, *args, **kwargs):
        """При збереженні автоматично заповнюємо назви з довідників та форматуємо дати"""
        is_new = self._state.adding
        
        try:
            # Форматуємо дату ухвалення
            if self.adjudication_date:
                self.adjudication_date = self._format_date(self.adjudication_date)
            
            # Заповнюємо назви з довідників якщо є коди
            if self.court_code and not self.court_name:
                try:
                    from .models import Court
                    court = Court.objects.filter(code=self.court_code).first()
                    if court:
                        self.court_name = court.name
                except:
                    pass
            
            if self.judgment_code and not self.judgment_name:
                try:
                    from .models import JudgmentForm
                    judgment = JudgmentForm.objects.filter(code=self.judgment_code).first()
                    if judgment:
                        self.judgment_name = judgment.name
                except:
                    pass
            
            if self.justice_kind and not self.justice_kind_name:
                try:
                    from .models import JusticeKind
                    justice = JusticeKind.objects.filter(code=self.justice_kind).first()
                    if justice:
                        self.justice_kind_name = justice.name
                except:
                    pass
            
            if self.category_code and not self.category_name:
                try:
                    from .models import CauseCategory
                    category = CauseCategory.objects.filter(code=self.category_code).first()
                    if category:
                        self.category_name = category.name
                except:
                    pass
            
        except Exception as e:
            print(f"Помилка заповнення довідкових даних: {e}")
        
        super().save(*args, **kwargs)
    
    def needs_resolution_extraction(self):
        """Перевіряє чи потрібно витягнути резолютивну частину"""
        return (
            self.doc_url and 
            self.doc_url != "nan" and 
            self.doc_url.startswith("http") and
            (not self.resolution_text or self.resolution_text in [
                "", 
                "Резолютивна частина не знайдена",
                "Не вдалося завантажити документ",
                "URL документа відсутній або некоректний"
            ])
        )
    
    def analyze_trigger_words(self):
        """Аналізує тригерні слова у резолютивній частині"""
        from .trigger_words import has_trigger_words
        
        if not self.resolution_text:
            return
        
        # КРИТИЧНО ВАЖЛИВО: Очищуємо NUL bytes перед аналізом та збереженням
        self.resolution_text = self.resolution_text.replace("\x00", "")
            
        analysis = has_trigger_words(self.resolution_text)
        
        # Оновлюємо поля на основі аналізу
        self.has_trigger_words = analysis["has_triggers"]
        self.trigger_words_found = analysis["found_triggers"]
        self.trigger_types = analysis["trigger_types"]
        self.is_critical_decision = analysis["is_critical"]
        
        # Зберігаємо зміни
        self.save(update_fields=[
            "has_trigger_words", 
            "trigger_words_found", 
            "trigger_types", 
            "is_critical_decision"
        ])
    
    def get_trigger_display_color(self):
        """Повертає колір для відображення на основі типів тригерів"""
        from .trigger_words import get_trigger_color
        return get_trigger_color(self.trigger_types)



class CourtDecisionImportLog(models.Model):
    database = models.ForeignKey(
        CourtDecisionDatabase, 
        on_delete=models.CASCADE, 
        related_name="import_logs",
        verbose_name="База даних"
    )
    
    csv_file_path = models.CharField(max_length=512, verbose_name="Шлях до CSV")
    csv_records_total = models.IntegerField(default=0, verbose_name="Всього записів у CSV")
    records_imported = models.IntegerField(default=0, verbose_name="Імпортовано записів")
    records_updated = models.IntegerField(default=0, verbose_name="Оновлено записів")
    records_skipped = models.IntegerField(default=0, verbose_name="Пропущено записів")
    
    started_at = models.DateTimeField(auto_now_add=True, verbose_name="Початок")
    completed_at = models.DateTimeField(null=True, blank=True, verbose_name="Завершення")
    
    is_successful = models.BooleanField(default=False, verbose_name="Успішно")
    error_message = models.TextField(blank=True, verbose_name="Помилка")
    
    class Meta:
        verbose_name = "Лог імпорту судових рішень"
        verbose_name_plural = "Логи імпорту судових рішень"
        ordering = ["-started_at"]
    
    def __str__(self):
        status = "Успішно" if self.is_successful else "З помилками"
        return f"Імпорт {self.database.year} - {status} ({self.started_at.strftime("%d.%m.%Y %H:%M")})"
    
    @property
    def duration(self):
        if self.completed_at:
            return self.completed_at - self.started_at
        return None


class CourtDecisionStatistics(models.Model):
    """Кешована статистика судових рішень для швидкого відображення"""
    STAT_TYPE_CHOICES = [
        ("general", "Загальна статистика"),
        ("yearly", "Статистика по роках"),
        ("courts", "Статистика по судах"),
        ("categories", "Статистика по категоріях"),
        ("justice_kinds", "Статистика по видах судочинства"),
        ("recent", "Останні рішення"),
    ]
    
    stat_type = models.CharField(
        max_length=20,
        choices=STAT_TYPE_CHOICES,
        verbose_name="Тип статистики",
        db_index=True
    )
    stat_key = models.CharField(
        max_length=100, 
        verbose_name="Ключ статистики", 
        blank=True,
        db_index=True,
        help_text="Додатковий ключ для групування (наприклад, рік, код суду)"
    )
    stat_data = models.JSONField(
        verbose_name="Дані статистики",
        help_text="JSON з статистичними даними"
    )
    
    # Метадані
    records_count = models.BigIntegerField(
        default=0,
        verbose_name="Кількість записів",
        help_text="Загальна кількість записів, на основі яких розрахована статистика"
    )
    calculation_time = models.DurationField(
        null=True,
        blank=True,
        verbose_name="Час розрахунку",
        help_text="Час, потрачений на розрахунок статистики"
    )
    
    # Часові мітки
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Створено",
        db_index=True
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        verbose_name="Оновлено",
        db_index=True
    )
    
    # Параметри кешування
    cache_expires_at = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name="Кеш діє до",
        help_text="Після цього часу статистика буде перерахована"
    )
    is_valid = models.BooleanField(
        default=True,
        verbose_name="Кеш валідний",
        help_text="False, якщо статистика потребує оновлення"
    )
    
    class Meta:
        verbose_name = "Статистика судових рішень"
        verbose_name_plural = "Статистики судових рішень"
        ordering = ["-updated_at"]
        indexes = [
            models.Index(fields=["stat_type", "stat_key"]),
            models.Index(fields=["stat_type", "is_valid", "cache_expires_at"]),
            models.Index(fields=["is_valid", "updated_at"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["stat_type", "stat_key"],
                name="unique_stat_type_key"
            )
        ]
    
    def __str__(self):
        key_part = f" ({self.stat_key})" if self.stat_key else ""
        return f"{self.get_stat_type_display()}{key_part} - {self.updated_at.strftime("%d.%m.%Y %H:%M")}"
    
    def is_expired(self):
        """Перевіряє, чи застаріла статистика"""
        if not self.is_valid:
            return True
        if self.cache_expires_at and timezone.now() > self.cache_expires_at:
            return True
        return False
    
    def invalidate(self):
        """Позначає статистику як застарілу"""
        self.is_valid = False
        self.save(update_fields=["is_valid", "updated_at"])
    
    @classmethod
    def get_cached_stat(cls, stat_type, stat_key=""):
        """Отримує кешовану статистику, якщо вона валідна"""
        try:
            stat = cls.objects.get(
                stat_type=stat_type,
                stat_key=stat_key,
                is_valid=True
            )
            if not stat.is_expired():
                return stat.stat_data
        except cls.DoesNotExist:
            pass
        return None
    
    @classmethod
    def set_cached_stat(cls, stat_type, stat_data, stat_key="", 
                       records_count=0, calculation_time=None, cache_hours=24):
        """Зберігає статистику в кеш"""
        from datetime import timedelta
        
        cache_expires_at = timezone.now() + timedelta(hours=cache_hours)
        
        stat, created = cls.objects.update_or_create(
            stat_type=stat_type,
            stat_key=stat_key,
            defaults={
                "stat_data": stat_data,
                "records_count": records_count,
                "calculation_time": calculation_time,
                "cache_expires_at": cache_expires_at,
                "is_valid": True,
            }
        )
        return stat
    
    @classmethod
    def invalidate_all(cls, stat_type=None):
        """Інвалідує всі статистики або статистики певного типу"""
        queryset = cls.objects.all()
        if stat_type:
            queryset = queryset.filter(stat_type=stat_type)
        queryset.update(is_valid=False)


class MonitoringStatistics(models.Model):
    """
    Модель для зберігання статистики моніторингу в реальному часі
    """
    STAT_TYPES = [
        ("search_progress", "Прогрес пошуку судових рішень"),
        ("extraction_progress", "Прогрес витягування резолютивних частин"),
        ("general_stats", "Загальна статистика"),
    ]
    
    stat_type = models.CharField(max_length=50, choices=STAT_TYPES, verbose_name="Тип статистики")
    
    # Основні лічильники
    total_cases = models.IntegerField(default=0, verbose_name="Загальна кількість справ")
    cases_with_decisions = models.IntegerField(default=0, verbose_name="Справи з рішеннями")
    total_decisions = models.IntegerField(default=0, verbose_name="Загальна кількість рішень")
    decisions_with_resolutions = models.IntegerField(default=0, verbose_name="Рішення з резолютивними частинами")
    decisions_without_rtf = models.IntegerField(default=0, verbose_name="Рішення без RTF файлів")
    
    # Прогрес обробки
    currently_processing = models.BooleanField(default=False, verbose_name="В обробці зараз")
    processing_type = models.CharField(max_length=100, blank=True, verbose_name="Тип поточної обробки")
    processed_count = models.IntegerField(default=0, verbose_name="Оброблено за поточний цикл")
    total_to_process = models.IntegerField(default=0, verbose_name="Загально до обробки")
    
    # Часові мітки
    last_updated = models.DateTimeField(auto_now=True, verbose_name="Останнє оновлення")
    last_search_run = models.DateTimeField(null=True, blank=True, verbose_name="Останній запуск пошуку")
    last_extraction_run = models.DateTimeField(null=True, blank=True, verbose_name="Останнє витягування резолютивних частин")
    
    # Додаткові дані в JSON форматі
    additional_data = models.JSONField(default=dict, blank=True, verbose_name="Додаткові дані")
    
    class Meta:
        verbose_name = "Статистика моніторингу"
        verbose_name_plural = "Статистика моніторингу"
        db_table = "bankruptcy_monitoring_statistics"
        unique_together = ["stat_type"]
    
    def __str__(self):
        return f"{self.get_stat_type_display()} - {self.last_updated}"
    
    @classmethod
    def get_current_stats(cls):
        """Отримує поточну статистику"""
        stats, created = cls.objects.get_or_create(
            stat_type="general_stats",
            defaults={
                "total_cases": 0,
                "cases_with_decisions": 0,
                "total_decisions": 0,
                "decisions_with_resolutions": 0,
            }
        )
        return stats
    
    @classmethod
    def update_general_stats(cls):
        """Оновлює загальну статистику"""
        from django.db import connection
        
        stats = cls.get_current_stats()
        
        # Підраховуємо справи
        total_cases = BankruptcyCase.objects.count()
        cases_with_decisions = BankruptcyCase.objects.filter(
            id__in=TrackedCourtDecision.objects.values("tracked_case__bankruptcy_case_id").distinct()
        ).count()
        
        # Підраховуємо рішення
        total_decisions = TrackedCourtDecision.objects.count()
        
        # Рішення з резолютивними частинами
        decisions_with_resolutions = TrackedCourtDecision.objects.filter(
            resolution_text__isnull=False
        ).exclude(resolution_text="").count()
        
        # Рішення без RTF файлів
        from django.db import models as db_models
        decisions_without_rtf = TrackedCourtDecision.objects.filter(
            db_models.Q(doc_url__isnull=True) | 
            db_models.Q(doc_url="") | 
            db_models.Q(doc_url="nan")
        ).count()
        
        # Оновлюємо статистику
        stats.total_cases = total_cases
        stats.cases_with_decisions = cases_with_decisions
        stats.total_decisions = total_decisions
        stats.decisions_with_resolutions = decisions_with_resolutions
        stats.decisions_without_rtf = decisions_without_rtf
        stats.save()
        
        return stats
    
    @classmethod
    def start_processing(cls, processing_type, total_to_process=0):
        """Розпочинає відслідковування процесу обробки"""
        stats = cls.get_current_stats()
        stats.currently_processing = True
        stats.processing_type = processing_type
        stats.processed_count = 0
        stats.total_to_process = total_to_process
        
        if processing_type.startswith("search"):
            stats.last_search_run = timezone.now()
        elif processing_type.startswith("extract"):
            stats.last_extraction_run = timezone.now()
            
        stats.save()
        return stats
    
    @classmethod
    def update_processing_progress(cls, processed_count):
        """Оновлює прогрес обробки"""
        stats = cls.get_current_stats()
        stats.processed_count = processed_count
        stats.save()
        return stats
    
    @classmethod
    def finish_processing(cls, processing_type=None):
        """Завершує відслідковування процесу обробки"""
        stats = cls.get_current_stats()
        stats.currently_processing = False
        stats.processing_type = ""
        stats.processed_count = 0
        stats.total_to_process = 0
        stats.save()
        
        # Оновлюємо загальну статистику після завершення
        cls.update_general_stats()
        return stats


class Creditor(models.Model):
    """Модель для кредиторів"""
    name = models.CharField(
        max_length=500,
        verbose_name="Назва кредитора",
        db_index=True
    )
    normalized_name = models.CharField(
        max_length=500,
        verbose_name="Нормалізована назва",
        help_text="Назва без ТОВ, ПАТ тощо для групування",
        db_index=True
    )
    
    # Статистика
    total_cases = models.PositiveIntegerField(
        default=0,
        verbose_name="Кількість справ"
    )
    total_amount_1st_queue = models.FloatField(
        default=0.0,
        verbose_name="Загальна сума 1 черга"
    )
    total_amount_2nd_queue = models.FloatField(
        default=0.0,
        verbose_name="Загальна сума 2 черга"
    )
    total_amount_3rd_queue = models.FloatField(
        default=0.0,
        verbose_name="Загальна сума 3 черга"
    )
    total_amount_4th_queue = models.FloatField(
        default=0.0,
        verbose_name="Загальна сума 4 черга"
    )
    total_amount_5th_queue = models.FloatField(
        default=0.0,
        verbose_name="Загальна сума 5 черга"
    )
    total_amount_6th_queue = models.FloatField(
        default=0.0,
        verbose_name="Загальна сума 6 черга"
    )
    total_amount_all = models.FloatField(
        default=0.0,
        verbose_name="Загальна сума всього"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = "Кредитор"
        verbose_name_plural = "Кредитори"
        unique_together = ["name", "normalized_name"]
    
    def __str__(self):
        return self.name
    
    def update_statistics(self):
        """Оновлює статистику кредитора"""
        claims = self.creditor_claims.all()
        
        self.total_cases = claims.values("case").distinct().count()
        self.total_amount_1st_queue = sum(claim.amount_1st_queue or 0 for claim in claims)
        self.total_amount_2nd_queue = sum(claim.amount_2nd_queue or 0 for claim in claims)
        self.total_amount_3rd_queue = sum(claim.amount_3rd_queue or 0 for claim in claims)
        self.total_amount_4th_queue = sum(claim.amount_4th_queue or 0 for claim in claims)
        self.total_amount_5th_queue = sum(claim.amount_5th_queue or 0 for claim in claims)
        self.total_amount_6th_queue = sum(claim.amount_6th_queue or 0 for claim in claims)
        self.total_amount_all = (
            self.total_amount_1st_queue + self.total_amount_2nd_queue + 
            self.total_amount_3rd_queue + self.total_amount_4th_queue + 
            self.total_amount_5th_queue + self.total_amount_6th_queue
        )
        self.save()


class CreditorClaim(models.Model):
    """Модель для вимог кредиторів у справах банкрутства"""
    
    case = models.ForeignKey(
        BankruptcyCase,
        on_delete=models.CASCADE,
        related_name="creditor_claims",
        verbose_name="Справа про банкрутство"
    )
    creditor = models.ForeignKey(
        Creditor,
        on_delete=models.CASCADE,
        related_name="creditor_claims",
        verbose_name="Кредитор"
    )
    
    # Суми за чергами (float для точності)
    amount_1st_queue = models.FloatField(
        null=True, blank=True,
        verbose_name="Сума 1 черга",
        help_text="Грошові вимоги 1 черги"
    )
    amount_2nd_queue = models.FloatField(
        null=True, blank=True,
        verbose_name="Сума 2 черга",
        help_text="Грошові вимоги 2 черги"
    )
    amount_3rd_queue = models.FloatField(
        null=True, blank=True,
        verbose_name="Сума 3 черга",
        help_text="Грошові вимоги 3 черги"
    )
    amount_4th_queue = models.FloatField(
        null=True, blank=True,
        verbose_name="Сума 4 черга",
        help_text="Грошові вимоги 4 черги"
    )
    amount_5th_queue = models.FloatField(
        null=True, blank=True,
        verbose_name="Сума 5 черга",
        help_text="Грошові вимоги 5 черги"
    )
    amount_6th_queue = models.FloatField(
        null=True, blank=True,
        verbose_name="Сума 6 черга",
        help_text="Грошові вимоги 6 черги"
    )
    
    # Розрахункові поля
    total_amount = models.FloatField(
        default=0.0,
        verbose_name="Загальна сума",
        help_text="Сума всіх черг"
    )
    
    # Метадані
    source_resolution_texts = models.TextField(
        blank=True,
        verbose_name="Джерельні резолютивні частини",
        help_text="ID резолютивних частин, з яких витягнуто дані"
    )
    llm_analysis_result = models.JSONField(
        null=True, blank=True,
        verbose_name="Результат аналізу LLM",
        help_text="Повний результат аналізу мовної моделі"
    )
    confidence_score = models.FloatField(
        null=True, blank=True,
        verbose_name="Оцінка достовірності",
        help_text="Оцінка від 0 до 1"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = "Вимога кредитора"
        verbose_name_plural = "Вимоги кредиторів"
        unique_together = ["case", "creditor"]
    
    def __str__(self):
        return f"{self.creditor.name} - Справа {self.case.number}"
    
    def save(self, *args, **kwargs):
        # Автоматично рахуємо загальну суму
        self.total_amount = sum(filter(None, [
            self.amount_1st_queue, self.amount_2nd_queue, self.amount_3rd_queue,
            self.amount_4th_queue, self.amount_5th_queue, self.amount_6th_queue
        ]))
        super().save(*args, **kwargs)
        
        # Оновлюємо статистику кредитора
        self.creditor.update_statistics()


class LLMAnalysisLog(models.Model):
    """Лог аналізу мовної моделі"""
    
    ANALYSIS_TYPES = [
        ("creditor_extraction", "Витягування даних кредиторів"),
        ("test_query", "Тестовий запит"),
        ("batch_processing", "Пакетна обробка"),
    ]
    
    ANALYSIS_STATUS = [
        ("pending", "Очікування"),
        ("processing", "Обробка"),
        ("completed", "Завершено"),
        ("failed", "Помилка"),
    ]
    
    case = models.ForeignKey(
        BankruptcyCase,
        on_delete=models.CASCADE,
        null=True, blank=True,
        verbose_name="Справа"
    )
    analysis_type = models.CharField(
        max_length=50,
        choices=ANALYSIS_TYPES,
        verbose_name="Тип аналізу"
    )
    status = models.CharField(
        max_length=20,
        choices=ANALYSIS_STATUS,
        default="pending",
        verbose_name="Статус"
    )
    
    input_text = models.TextField(
        verbose_name="Вхідний текст"
    )
    output_text = models.TextField(
        blank=True,
        verbose_name="Результат"
    )
    
    # Метрики
    processing_time_seconds = models.FloatField(
        null=True, blank=True,
        verbose_name="Час обробки (сек)"
    )
    token_count_input = models.PositiveIntegerField(
        null=True, blank=True,
        verbose_name="Кількість токенів (вхід)"
    )
    token_count_output = models.PositiveIntegerField(
        null=True, blank=True,
        verbose_name="Кількість токенів (вихід)"
    )
    
    error_message = models.TextField(
        blank=True,
        verbose_name="Повідомлення про помилку"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    
    class Meta:
        verbose_name = "Лог аналізу LLM"
        verbose_name_plural = "Логи аналізу LLM"
        ordering = ["-created_at"]
    
    def __str__(self):
        return f"{self.analysis_type} - {self.status} - {self.created_at}"


class DeduplicationProcessStats(models.Model):
    """Статистика процесу дедуплікації (другий процес)"""

    # Загальна статистика
    total_cases_processed = models.IntegerField(default=0, verbose_name="Справ опрацьовано")
    total_creditors_added = models.IntegerField(default=0, verbose_name="Кредиторів додано")
    total_duplicates_removed = models.IntegerField(default=0, verbose_name="Дублікатів видалено")
    total_claims_updated = models.IntegerField(default=0, verbose_name="Вимог оновлено")

    # Статистика по типах документів
    initial_documents_processed = models.IntegerField(default=0, verbose_name="Вступних частин оброблено")
    full_documents_processed = models.IntegerField(default=0, verbose_name="Повних документів оброблено")
    summary_documents_processed = models.IntegerField(default=0, verbose_name="Підсумкових документів оброблено")

    # Статистика помилок
    api_errors = models.IntegerField(default=0, verbose_name="API помилки")
    parsing_errors = models.IntegerField(default=0, verbose_name="Помилки парсингу")
    database_errors = models.IntegerField(default=0, verbose_name="Помилки БД")

    # Статистика продуктивності
    avg_processing_time = models.FloatField(default=0.0, verbose_name="Середній час обробки (сек)")
    last_processed_case_id = models.IntegerField(null=True, blank=True, verbose_name="Останній оброблений ID")

    # Статус процесу
    is_running = models.BooleanField(default=False, verbose_name="Процес запущений")
    last_run_at = models.DateTimeField(null=True, blank=True, verbose_name="Останній запуск")
    last_error = models.TextField(blank=True, verbose_name="Остання помилка")

    # Часові мітки
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="Створено")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="Оновлено")

    class Meta:
        verbose_name = "Статистика процесу дедуплікації"
        verbose_name_plural = "Статистика процесів дедуплікації"
        db_table = "bankruptcy_dedup_process_stats"

    def __str__(self):
        return f"Дедуплікація: {self.total_cases_processed} справ, {self.total_creditors_added} кредиторів додано"

    @classmethod
    def get_current_stats(cls):
        """Отримує поточну статистику (створює якщо не існує)"""
        stats, created = cls.objects.get_or_create(
            pk=1,  # Завжди один запис статистики
            defaults={
                "total_cases_processed": 0,
                "total_creditors_added": 0,
                "total_duplicates_removed": 0,
                "total_claims_updated": 0,
            }
        )
        return stats

    def update_stats(self, cases_processed=0, creditors_added=0, duplicates_removed=0,
                    claims_updated=0, doc_type="", processing_time=0.0, error=None):
        """Оновлює статистику"""
        from django.utils import timezone

        self.total_cases_processed += cases_processed
        self.total_creditors_added += creditors_added
        self.total_duplicates_removed += duplicates_removed
        self.total_claims_updated += claims_updated

        # Оновлюємо статистику по типах документів
        if doc_type == "initial":
            self.initial_documents_processed += 1
        elif doc_type == "full":
            self.full_documents_processed += 1
        elif doc_type == "summary":
            self.summary_documents_processed += 1

        # Оновлюємо середній час обробки
        if processing_time > 0:
            total_time = self.avg_processing_time * (self.total_cases_processed - cases_processed)
            total_time += processing_time * cases_processed
            self.avg_processing_time = total_time / max(self.total_cases_processed, 1)

        # Обробляємо помилки
        if error:
            if "API" in str(error) or "429" in str(error):
                self.api_errors += 1
            elif "JSON" in str(error) or "parse" in str(error).lower():
                self.parsing_errors += 1
            else:
                self.database_errors += 1
            self.last_error = str(error)[:500]  # Обмежуємо довжину

        self.save()

    def start_processing(self):
        """Позначає початок обробки"""
        from django.utils import timezone
        self.is_running = True
        self.last_run_at = timezone.now()
        self.save()

    def stop_processing(self):
        """Позначає завершення обробки"""
        self.is_running = False
        self.save()

    @property
    def deduplication_rate(self):
        """Відсоток дублікатів"""
        total_processed = self.total_creditors_added + self.total_duplicates_removed
        if total_processed > 0:
            return (self.total_duplicates_removed / total_processed) * 100
        return 0


class DeduplicationLog(models.Model):
    """Детальний лог операцій дедуплікації"""

    OPERATION_TYPES = [
        ("creditor_added", "Кредитор додано"),
        ("duplicate_removed", "Дублікат видалено"),
        ("claim_updated", "Вимога оновлена"),
        ("case_processed", "Справа оброблена"),
    ]

    DOCUMENT_TYPES = [
        ("initial", "Початкова ухвала"),
        ("full", "Повний документ"),
        ("summary", "Підсумкова ухвала"),
        ("unknown", "Невідомий тип"),
    ]

    # Основні поля
    case = models.ForeignKey(
        BankruptcyCase,
        on_delete=models.CASCADE,
        verbose_name="Справа",
        related_name="deduplication_logs"
    )
    operation_type = models.CharField(
        max_length=20,
        choices=OPERATION_TYPES,
        verbose_name="Тип операції"
    )
    document_type = models.CharField(
        max_length=10,
        choices=DOCUMENT_TYPES,
        default="unknown",
        verbose_name="Тип документа"
    )

    # Деталі операції
    creditor_name = models.CharField(
        max_length=500,
        blank=True,
        verbose_name="Назва кредитора"
    )
    old_amount = models.DecimalField(
        max_digits=15,
        decimal_places=2,
        null=True,
        blank=True,
        verbose_name="Стара сума"
    )
    new_amount = models.DecimalField(
        max_digits=15,
        decimal_places=2,
        null=True,
        blank=True,
        verbose_name="Нова сума"
    )
    queue_type = models.CharField(
        max_length=20,
        blank=True,
        verbose_name="Тип черги"
    )

    # Технічні деталі
    decision_doc_id = models.CharField(
        max_length=50,
        blank=True,
        verbose_name="ID судового рішення"
    )
    processing_time = models.FloatField(
        null=True,
        blank=True,
        verbose_name="Час обробки (сек)"
    )
    details = models.JSONField(
        null=True,
        blank=True,
        verbose_name="Додаткові деталі"
    )

    # Часові мітки
    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Створено",
        db_index=True
    )

    class Meta:
        verbose_name = "Лог дедуплікації"
        verbose_name_plural = "Логи дедуплікації"
        db_table = "bankruptcy_deduplication_log"
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["case", "operation_type"]),
            models.Index(fields=["operation_type", "created_at"]),
            models.Index(fields=["document_type", "created_at"]),
        ]

    def __str__(self):
        return f"{self.get_operation_type_display()} - {self.case.case_number} ({self.created_at.strftime('%d.%m.%Y %H:%M')})"