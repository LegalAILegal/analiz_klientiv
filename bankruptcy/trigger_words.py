# -*- coding: utf-8 -*-
"""
Тригерні слова та фрази для позначення резолютивних частин судових рішень
що потребують особливої уваги мовної моделі.

Основано на аналізі еталонних проєктів SR_AI та project_courtagent.pro_production
"""

# Основні тригери для резолютивних частин (червоний колір в Excel)
RESOLUTION_TRIGGERS = [
    # Комбінований тригер за вимогою користувача
    "визнати",
    "грошові вимоги"
]

# Комбіновані тригери - обидві фрази мають бути присутні одночасно
COMBINED_TRIGGERS = [
    ["визнати", "грошові вимоги"]  # Обидві фрази мають бути в тексті
]

# Тригери для типів рішень (ВИМКНЕНО - використовуємо тільки основні тригери банкрутства)
JUDGMENT_TYPE_TRIGGERS = [
    # Вимкнено - використовуємо тільки RESOLUTION_TRIGGERS
]

# Критичні тригери (ВИМКНЕНО - використовуємо тільки основні тригери банкрутства)
CRITICAL_TRIGGERS = [
    # Вимкнено - використовуємо тільки RESOLUTION_TRIGGERS
]

# Об'єднаний список всіх тригерів для швидкого пошуку
ALL_TRIGGERS = RESOLUTION_TRIGGERS + JUDGMENT_TYPE_TRIGGERS + CRITICAL_TRIGGERS

def has_trigger_words(text):
    """
    Перевіряє наявність тригерних слів у тексті резолютивної частини.
    НОВА ЛОГІКА: Тригер спрацьовує ТІЛЬКИ якщо "визнати" + "грошові вимоги" в ОДНОМУ реченні
    
    Args:
        text (str): Текст резолютивної частини
        
    Returns:
        dict: {
            "has_triggers": bool,
            "found_triggers": list,
            "trigger_types": list,
            "is_critical": bool
        }
    """
    if not text:
        return {
            "has_triggers": False,
            "found_triggers": [],
            "trigger_types": [],
            "is_critical": False
        }
    
    # ТІЛЬКИ ОДНА УМОВА: обидва тригери в одному реченні
    if has_both_triggers_in_same_sentence(text):
        return {
            "has_triggers": True,
            "found_triggers": ["визнати", "грошові вимоги"],
            "trigger_types": ["combined_resolution_same_sentence"],
            "is_critical": True
        }
    
    # Якщо не знайдено обидва тригери в одному реченні - немає тригерів
    return {
        "has_triggers": False,
        "found_triggers": [],
        "trigger_types": [],
        "is_critical": False
    }

def get_trigger_color(trigger_types):
    """
    Повертає колір для відображення в інтерфейсі на основі типів тригерів.
    Червоний колір для комбінації "визнати" + "грошові вимоги"
    
    Args:
        trigger_types (list): Список типів тригерів
        
    Returns:
        str: CSS клас або hex колір
    """
    if "combined_resolution" in trigger_types:
        return "#FF0000"  # ЧЕРВОНИЙ для комбінованих тригерів "визнати" + "грошові вимоги"
    elif "critical" in trigger_types:
        return "#FF4444"  # Червоний для критичних
    elif "resolution" in trigger_types:
        return "#FF6B6B"  # Світло-червоний для резолютивних
    elif "judgment_type" in trigger_types:
        return "#FFEB3B"  # Жовтий для типів рішень
    else:
        return "#E0E0E0"  # Сірий за замовчуванням

def should_highlight_red(text):
    """
    Перевіряє чи потрібно підсвічувати текст червоним кольором.
    Повертає True якщо текст містить комбінацію "визнати" + "грошові вимоги" В ОДНОМУ РЕЧЕННІ
    
    Args:
        text (str): Текст резолютивної частини
        
    Returns:
        bool: True якщо потрібно червоне підсвічування
    """
    if not text:
        return False
    
    return has_both_triggers_in_same_sentence(text)

def has_both_triggers_in_same_sentence(text):
    """
    Перевіряє чи знаходяться обидва тригери "визнати" та "грошові вимоги" в одному реченні.
    
    Args:
        text (str): Текст резолютивної частини
        
    Returns:
        bool: True якщо обидва тригери в одному реченні
    """
    if not text:
        return False
    
    # Розділяємо текст на речення (по крапці, знаку оклику, знаку питання)
    import re
    sentences = re.split(r"[.!?]", text)
    
    # Перевіряємо кожне речення
    for sentence in sentences:
        sentence_lower = sentence.lower()
        if "визнати" in sentence_lower and "грошові вимоги" in sentence_lower:
            return True
    
    return False