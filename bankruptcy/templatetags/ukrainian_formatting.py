from django import template
from decimal import Decimal

register = template.Library()

@register.filter
def ukrainian_currency(value):
    """
    Форматує суму у український формат:
    - Пробіли для розрядності (1 234 567)
    - Кома для копійок (1 234 567,89)
    """
    if value is None or value == 0:
        return "-"

    try:
        # Конвертуємо в Decimal для точної роботи з копійками
        if isinstance(value, str):
            value = Decimal(value)
        elif isinstance(value, (int, float)):
            value = Decimal(str(value))

        # Розділяємо на цілу частину та копійки
        integer_part = int(value)
        decimal_part = value % 1

        # Форматуємо цілу частину з пробілами
        integer_str = f"{integer_part:,}".replace(",", " ")

        # Завжди додаємо копійки через кому (навіть якщо 00)
        kopiyky = round(decimal_part * 100)
        return f"{integer_str},{kopiyky:02d}"

    except (ValueError, TypeError, AttributeError):
        return str(value)

@register.filter
def ukrainian_currency_with_unit(value):
    """
    Форматує суму у український формат з одиницею виміру
    """
    formatted = ukrainian_currency(value)
    if formatted == "-":
        return "-"
    return f"{formatted} грн"