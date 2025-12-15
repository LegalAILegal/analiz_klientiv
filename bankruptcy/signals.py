"""
Django сигнали для автоматичного оновлення кешу статистики
при зміні даних в базі даних судових рішень
"""

from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from .models import CourtDecisionStatistics
import logging

logger = logging.getLogger(__name__)

# Список таблиць судових рішень для моніторингу
COURT_DECISION_TABLES = [
    f"court_decisions_{year}" for year in range(2007, 2026)
]

def invalidate_court_statistics_cache():
    """Інвалідує кеш статистики судових рішень"""
    try:
        # Помічаємо всю статистику як застарілу
        invalidated_count = CourtDecisionStatistics.objects.filter(
            is_valid=True
        ).update(is_valid=False)
        
        if invalidated_count > 0:
            logger.info(f"Інвалідовано {invalidated_count} записів кешу статистики через зміни в даних")
    
    except Exception as e:
        logger.error(f"Помилка при інвалідації кешу статистики: {e}")

# Можна підключити до моделей, які впливають на статистику
# Наприклад, якщо є модель CourtDecision:

# @receiver([post_save, post_delete], sender=CourtDecision)
# def invalidate_cache_on_court_decision_change(sender, **kwargs):
#     """Інвалідує кеш при зміні судових рішень"""
#     invalidate_court_statistics_cache()

# Функція для ручної інвалідації кешу
def force_invalidate_cache():
    """Примусова інвалідація всього кешу статистики"""
    CourtDecisionStatistics.objects.all().update(is_valid=False)
    logger.info("Примусово інвалідовано весь кеш статистики")