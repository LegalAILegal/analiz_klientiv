from django.contrib import admin
from .models import Court, Company, BankruptcyCase


@admin.register(Court)
class CourtAdmin(admin.ModelAdmin):
    list_display = ["name"]
    search_fields = ["name"]


@admin.register(Company)
class CompanyAdmin(admin.ModelAdmin):
    list_display = ["edrpou", "name"]
    search_fields = ["edrpou", "name"]
    list_filter = ["edrpou"]


@admin.register(BankruptcyCase)
class BankruptcyCaseAdmin(admin.ModelAdmin):
    list_display = [
        "number", "date", "case_number", 
        "company", "court", "type"
    ]
    list_filter = ["date", "type", "court"]
    search_fields = [
        "number", "case_number", "company__name", 
        "company__edrpou", "court__name"
    ]
    date_hierarchy = "date"
    readonly_fields = ["created_at", "updated_at"]
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related("company", "court")