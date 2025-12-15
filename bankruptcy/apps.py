from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)

class BankruptcyConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "bankruptcy"
    verbose_name = "Справи про банкрутство"
    
    def ready(self):
        """Викликається коли додаток готовий до роботи"""
        # Тимчасово відключаємо моніторинг під час міграції
        pass