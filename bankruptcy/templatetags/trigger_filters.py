# -*- coding: utf-8 -*-
"""
Template filters для роботи з тригерними словами в резолютивних частинах
"""

from django import template
from ..trigger_words import should_highlight_red, has_both_triggers_in_same_sentence

register = template.Library()

@register.filter
def has_trigger_highlight(text):
    """
    Перевіряє чи потрібно підсвічувати резолютивну частину червоним.
    Повертає True тільки якщо "визнати" та "грошові вимоги" в одному реченні.
    
    Usage: {% if decision.resolution_text|has_trigger_highlight %}highlight-critical-combination{% endif %}
    """
    return should_highlight_red(text)

@register.filter
def same_sentence_triggers(text):
    """
    Перевіряє чи знаходяться обидва тригери в одному реченні.
    
    Usage: {% if decision.resolution_text|same_sentence_triggers %}...{% endif %}
    """
    return has_both_triggers_in_same_sentence(text)