from django.shortcuts import render, get_object_or_404
from django.core.paginator import Paginator
from django.db.models import Q, Count, Min, Max, Sum, F
from django.db import connection
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime, date, timedelta
from django.utils import timezone
from dateutil.relativedelta import relativedelta
from django.core.serializers.json import DjangoJSONEncoder
from .models import BankruptcyCase, Company, Court, CourtDecisionStatistics, JusticeKind, Instance, JudgmentForm, TrackedBankruptcyCase, TrackedCourtDecision, SystemProcessControl, CreditorClaim, Creditor, LLMAnalysisLog
from .trigger_words import should_highlight_red
import json
import time


def json_serialize_dates(obj):
    """Конвертує дати у JSON-сумісний формат"""
    if isinstance(obj, dict):
        return {key: json_serialize_dates(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [json_serialize_dates(item) for item in obj]
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat() if obj else None
    else:
        return obj


def order_case_types(case_types_list):
    """Впорядковує типи провадження у потрібному порядку"""
    order = [
        "Повідомлення про відкриття провадження у справі про банкрутство",
        "Повідомлення про визнання боржника банкрутом і відкриття ліквідаційної процедури",
        "Повідомлення про введення процедури санації",
        "Оголошення про відкриття провадження у справі про неплатоспроможність",
        "Повідомлення про визнання боржника банкрутом і введення процедури погашення боргів боржника",
        "Повідомлення про прийняття до розгляду заяви про затвердження плану санації до відкриття провадження у справі про банкрутство",
        "Повідомлення про відкриття процедури превентивної реструктуризації",
        "Оголошення про проведення загальних зборів кредиторів",
    ]
    
    # Створюємо словник для швидкого доступу
    case_types_dict = {item["type"]: item for item in case_types_list}
    
    # Впорядковуємо згідно з order
    ordered_list = []
    for case_type in order:
        if case_type in case_types_dict:
            ordered_list.append(case_types_dict[case_type])
    
    # Додаємо решту типів, яких немає в order
    for item in case_types_list:
        if item["type"] not in order:
            ordered_list.append(item)
    
    return ordered_list


def index(request):
    """Головна сторінка з детальною статистикою"""
    # Поточна дата для всіх розрахунків
    current_date = date.today()
    
    # Основні показники
    total_cases = BankruptcyCase.objects.count()
    total_companies = Company.objects.count()
    total_courts = Court.objects.count()
    
    # Статистика по датах
    date_stats = BankruptcyCase.objects.aggregate(
        min_date=Min("date"),
        max_date=Max("date")
    )
    
    # Статистика за поточний рік
    current_year = current_date.year
    current_year_cases = BankruptcyCase.objects.filter(date__year=current_year).count()
    prev_year_cases = BankruptcyCase.objects.filter(date__year=current_year-1).count()
    
    # Статистика за останні 30 днів
    from datetime import timedelta
    thirty_days_ago = current_date - timedelta(days=30)
    recent_cases = BankruptcyCase.objects.filter(date__gte=thirty_days_ago).count()
    
    # Фільтрація судів за періодом
    courts_period = request.GET.get("courts_period", "all")
    courts_date_from = request.GET.get("courts_date_from")
    courts_date_to = request.GET.get("courts_date_to")
    
    # Базовий queryset для судів
    courts_queryset = Court.objects.all()
    bankruptcy_cases_filter = Q()
    
    # Застосовуємо фільтр за періодом
    if courts_period == "1_year":
        one_year_ago = current_date - relativedelta(years=1)
        bankruptcy_cases_filter = Q(bankruptcy_cases__date__gte=one_year_ago)
    elif courts_period == "2_years":
        two_years_ago = current_date - relativedelta(years=2)
        bankruptcy_cases_filter = Q(bankruptcy_cases__date__gte=two_years_ago)
    elif courts_period == "3_years":
        three_years_ago = current_date - relativedelta(years=3)
        bankruptcy_cases_filter = Q(bankruptcy_cases__date__gte=three_years_ago)
    elif courts_period == "5_years":
        five_years_ago = current_date - relativedelta(years=5)
        bankruptcy_cases_filter = Q(bankruptcy_cases__date__gte=five_years_ago)
    elif courts_period == "10_years":
        ten_years_ago = current_date - relativedelta(years=10)
        bankruptcy_cases_filter = Q(bankruptcy_cases__date__gte=ten_years_ago)
    elif courts_period == "15_years":
        fifteen_years_ago = current_date - relativedelta(years=15)
        bankruptcy_cases_filter = Q(bankruptcy_cases__date__gte=fifteen_years_ago)
    elif courts_period == "20_years":
        twenty_years_ago = current_date - relativedelta(years=20)
        bankruptcy_cases_filter = Q(bankruptcy_cases__date__gte=twenty_years_ago)
    elif courts_period == "custom" and courts_date_from and courts_date_to:
        try:
            date_from_obj = datetime.strptime(courts_date_from, "%Y-%m-%d").date()
            date_to_obj = datetime.strptime(courts_date_to, "%Y-%m-%d").date()
            bankruptcy_cases_filter = Q(bankruptcy_cases__date__gte=date_from_obj) & Q(bankruptcy_cases__date__lte=date_to_obj)
        except ValueError:
            pass
    
    # Топ судів за кількістю справ з фільтрацією (беремо всі для каруселі)
    top_courts_base = courts_queryset.filter(bankruptcy_cases_filter).annotate(
        cases_count=Count("bankruptcy_cases", filter=bankruptcy_cases_filter)
    ).order_by("-cases_count")
    
    # Додаємо розгорнуту статистику для кожного суду
    top_courts = []
    for court in top_courts_base:
        court_cases_filter = bankruptcy_cases_filter & Q(bankruptcy_cases__court=court)
        court_types = BankruptcyCase.objects.filter(court=court)
        
        # Застосовуємо той самий фільтр періоду до типів
        if courts_period == "1_year":
            court_types = court_types.filter(date__gte=one_year_ago)
        elif courts_period == "2_years":
            court_types = court_types.filter(date__gte=two_years_ago)
        elif courts_period == "3_years":
            court_types = court_types.filter(date__gte=three_years_ago)
        elif courts_period == "5_years":
            court_types = court_types.filter(date__gte=five_years_ago)
        elif courts_period == "10_years":
            court_types = court_types.filter(date__gte=ten_years_ago)
        elif courts_period == "15_years":
            court_types = court_types.filter(date__gte=fifteen_years_ago)
        elif courts_period == "20_years":
            court_types = court_types.filter(date__gte=twenty_years_ago)
        elif courts_period == "custom" and courts_date_from and courts_date_to:
            try:
                court_types = court_types.filter(date__gte=date_from_obj, date__lte=date_to_obj)
            except:
                pass
        
        court_types_stats = order_case_types(list(court_types.values("type").annotate(
            count=Count("type")
        ).order_by("-count")))
        
        top_courts.append({
            "court": court,
            "cases_count": court.cases_count,
            "types": court_types_stats
        })
    
    # Загальна статистика за типами провадження
    total_case_types_stats = order_case_types(list(BankruptcyCase.objects.values("type").annotate(
        count=Count("type")
    ).order_by("-count")))
    
    # Статистика за типами для поточного року
    current_year_case_types = order_case_types(list(BankruptcyCase.objects.filter(
        date__year=current_year
    ).values("type").annotate(
        count=Count("type")
    ).order_by("-count")))
    
    # Статистика за типами для попереднього року
    prev_year_case_types = order_case_types(list(BankruptcyCase.objects.filter(
        date__year=current_year-1
    ).values("type").annotate(
        count=Count("type")
    ).order_by("-count")))
    
    # Статистика за типами для останніх 30 днів
    recent_case_types = order_case_types(list(BankruptcyCase.objects.filter(
        date__gte=thirty_days_ago
    ).values("type").annotate(
        count=Count("type")
    ).order_by("-count")))
    
    # Статистика за останні 6 місяців
    last_6_months_stats = []
    
    for i in range(6):
        # Починаємо з поточного місяця і йдемо назад
        month_date = current_date - relativedelta(months=i)
        
        month_types = BankruptcyCase.objects.filter(
            date__year=month_date.year,
            date__month=month_date.month
        ).values("type").annotate(
            count=Count("type")
        ).order_by("-count")
        
        month_types_ordered = order_case_types(list(month_types))
        month_total = sum(item["count"] for item in month_types_ordered)
        
        month_names = ["Січ", "Лют", "Бер", "Кві", "Тра", "Чер", 
                      "Лип", "Сер", "Вер", "Жов", "Лис", "Гру"]
        
        last_6_months_stats.append({
            "month": month_names[month_date.month-1],
            "year": month_date.year,
            "count": month_total,
            "types": month_types_ordered
        })
    
    # Статистика компаній з більше ніж одною справою
    companies_with_multiple = Company.objects.annotate(
        cases_count=Count("bankruptcy_cases")
    ).filter(cases_count__gt=1).count()
    
    context = {
        "total_cases": total_cases,
        "total_companies": total_companies,
        "total_courts": total_courts,
        "current_year_cases": current_year_cases,
        "prev_year_cases": prev_year_cases,
        "recent_cases_count": recent_cases,
        "date_range": date_stats,
        "top_courts": top_courts,
        "total_case_types_stats": total_case_types_stats,
        "current_year_case_types": current_year_case_types,
        "prev_year_case_types": prev_year_case_types,
        "recent_case_types": recent_case_types,
        "last_6_months_stats": last_6_months_stats,
        "companies_with_multiple": companies_with_multiple,
        "current_year": current_year,
        "courts_period": courts_period,
        "courts_date_from": courts_date_from,
        "courts_date_to": courts_date_to,
    }
    return render(request, "bankruptcy/index.html", context)


def case_list(request):
    """Список справ з фільтрацією та пагінацією"""
    cases = BankruptcyCase.objects.select_related("company", "court").all()
    
    # Фільтрація за судом
    court_filter = request.GET.get("court")
    if court_filter and court_filter.strip():
        cases = cases.filter(court_id=court_filter.strip())
    
    # Фільтрація за типом провадження
    case_type_filter = request.GET.get("type")
    if case_type_filter and case_type_filter.strip():
        cases = cases.filter(type=case_type_filter.strip())
    
    # Фільтрація за номером справи
    case_number_filter = request.GET.get("case_number")
    if case_number_filter and case_number_filter.strip():
        cases = cases.filter(case_number__icontains=case_number_filter.strip())
    
    # Фільтрація за ЄДРПОУ
    edrpou_filter = request.GET.get("edrpou")
    if edrpou_filter and edrpou_filter.strip():
        cases = cases.filter(company__edrpou__icontains=edrpou_filter.strip())
    
    # Фільтрація за назвою підприємства
    company_filter = request.GET.get("company")
    if company_filter and company_filter.strip():
        cases = cases.filter(company__name__icontains=company_filter.strip())
    
    # Фільтрація за датою
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")
    exact_date = request.GET.get("exact_date")
    
    if exact_date and exact_date.strip():
        try:
            exact_date_obj = datetime.strptime(exact_date.strip(), "%Y-%m-%d").date()
            cases = cases.filter(date=exact_date_obj)
        except ValueError:
            pass
    else:
        if date_from and date_from.strip():
            try:
                date_from_obj = datetime.strptime(date_from.strip(), "%Y-%m-%d").date()
                cases = cases.filter(date__gte=date_from_obj)
            except ValueError:
                pass
        
        if date_to and date_to.strip():
            try:
                date_to_obj = datetime.strptime(date_to.strip(), "%Y-%m-%d").date()
                cases = cases.filter(date__lte=date_to_obj)
            except ValueError:
                pass
    
    # Підрахунок загальної кількості записів (перед пагінацією)
    total_filtered_records = cases.count()
    
    # Пагінація
    paginator = Paginator(cases, 100)  # 100 записів на сторінці
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)
    
    # Унікальні типи провадження для фільтру
    case_types = BankruptcyCase.objects.values_list("type", flat=True).distinct().order_by("type")
    
    context = {
        "page_obj": page_obj,
        "total_filtered_records": total_filtered_records,
        "courts": Court.objects.all(),
        "case_types": case_types,
        "selected_court": court_filter,
        "selected_type": case_type_filter,
        "selected_case_number": case_number_filter,
        "selected_edrpou": edrpou_filter,
        "selected_company": company_filter,
        "selected_date_from": date_from,
        "selected_date_to": date_to,
        "selected_exact_date": exact_date,
    }
    return render(request, "bankruptcy/case_list.html", context)


def case_detail(request, case_id):
    """Детальна інформація про справу з відстеженими судовими рішеннями"""
    # case_id може бути як ID так і number - спробуємо обидва варіанти
    try:
        # Спочатку пробуємо як ID
        case = BankruptcyCase.objects.select_related("company", "court").get(id=case_id)
    except BankruptcyCase.DoesNotExist:
        # Якщо не знайдено по ID, пробуємо по number
        case = get_object_or_404(
            BankruptcyCase.objects.select_related("company", "court"),
            number=case_id
        )
    
    # Перевіряємо чи справа вже відстежується
    try:
        tracked_case = TrackedBankruptcyCase.objects.get(bankruptcy_case=case)
        is_tracked = True
        
        # Отримуємо відстежені судові рішення для цієї справи
        tracked_decisions = TrackedCourtDecision.objects.filter(
            tracked_case=tracked_case
        ).order_by("-found_at")
        
        court_decisions = []
        for decision in tracked_decisions:
            court_decisions.append({
                "doc_id": decision.doc_id,
                "court_code": decision.court_code,
                "court_name": decision.court_name or decision.court_code,
                "judgment_code": decision.judgment_code,
                "judgment_name": decision.judgment_name or decision.judgment_code,
                "justice_kind": decision.justice_kind,
                "justice_kind_name": decision.justice_kind_name or decision.justice_kind,
                "category_code": decision.category_code,
                "category_name": decision.category_name or decision.category_code,
                "cause_num": decision.cause_num,
                "adjudication_date": decision.adjudication_date,
                "receipt_date": decision.receipt_date,
                "judge": decision.judge,
                "doc_url": decision.doc_url,
                "status": decision.status,
                "date_publ": decision.date_publ,
                "database_source": decision.database_source,
                "resolution_text": decision.resolution_text,
                "should_highlight_red": should_highlight_red(decision.resolution_text),
                "yedr_url": decision.get_yedr_url(),
                "has_rtf_document": decision.has_rtf_document(),
                "found_at": decision.found_at,
            })
        
        # Якщо рішень немає і пошук ще не проводився, запускаємо його
        if not court_decisions and tracked_case.needs_decisions_search():
            # Запускаємо фоновий пошук
            tracked_case.trigger_background_search_decisions()
            search_status = "starting"
        else:
            search_status = tracked_case.search_decisions_status
            
    except TrackedBankruptcyCase.DoesNotExist:
        # Справа ще не відстежується, створюємо запис відстеження
        from bankruptcy.services import BankruptcyAutoTrackingService
        
        service = BankruptcyAutoTrackingService()
        tracked_case = service.add_case_to_tracking(case, priority=100)  # Вищий пріоритет для ручно відкритих справ
        
        is_tracked = True
        court_decisions = []
        search_status = "pending"
        
        # Запускаємо фоновий пошук
        tracked_case.trigger_background_search_decisions()
    
    # Отримуємо дані реєстру кредиторів для цієї справи
    creditor_claims = CreditorClaim.objects.filter(case=case).select_related("creditor").order_by("-total_amount")
    
    context = {
        "case": case,
        "court_decisions": court_decisions,
        "decisions_count": len(court_decisions),
        "is_tracked": is_tracked,
        "search_status": search_status,
        "tracked_case": tracked_case if is_tracked else None,
        "creditor_claims": creditor_claims,
        "creditor_claims_count": creditor_claims.count(),
    }
    
    return render(request, "bankruptcy/case_detail.html", context)


def creditor_statistics(request):
    """Сторінка статистики кредиторів"""
    
    # Отримуємо статистику кредиторів з сумами по чергах
    creditors = Creditor.objects.annotate(
        total_cases_count=Count("creditor_claims", distinct=True),
        total_1st_queue=Sum("creditor_claims__amount_1st_queue"),
        total_2nd_queue=Sum("creditor_claims__amount_2nd_queue"),
        total_3rd_queue=Sum("creditor_claims__amount_3rd_queue"),
        total_4th_queue=Sum("creditor_claims__amount_4th_queue"),
        total_5th_queue=Sum("creditor_claims__amount_5th_queue"),
        total_6th_queue=Sum("creditor_claims__amount_6th_queue"),
        total_all_queues=Sum(
            F("creditor_claims__amount_1st_queue") +
            F("creditor_claims__amount_2nd_queue") +
            F("creditor_claims__amount_3rd_queue") +
            F("creditor_claims__amount_4th_queue") +
            F("creditor_claims__amount_5th_queue") +
            F("creditor_claims__amount_6th_queue")
        )
    ).filter(creditor_claims__isnull=False).order_by("-total_all_queues")
    
    # Загальна статистика
    total_creditors = creditors.count()
    total_claims = CreditorClaim.objects.count()
    total_analyzed_cases = CreditorClaim.objects.values("case").distinct().count()
    
    # Сума по всіх чергах
    all_amounts = CreditorClaim.objects.aggregate(
        sum_1st=Sum("amount_1st_queue"),
        sum_2nd=Sum("amount_2nd_queue"),
        sum_3rd=Sum("amount_3rd_queue"),
        sum_4th=Sum("amount_4th_queue"),
        sum_5th=Sum("amount_5th_queue"),
        sum_6th=Sum("amount_6th_queue"),
    )
    
    # Топ-5 кредиторів
    top_creditors = creditors[:5]
    
    context = {
        "creditors": creditors,
        "total_creditors": total_creditors,
        "total_claims": total_claims,
        "total_analyzed_cases": total_analyzed_cases,
        "all_amounts": all_amounts,
        "top_creditors": top_creditors,
    }
    
    return render(request, "bankruptcy/creditor_statistics.html", context)


def search_court_decisions_for_case(case_number):
    """Пошук судових рішень для конкретної справи у всіх таблицях"""
    # Отримуємо список доступних таблиць судових рішень
    tables = get_court_decision_tables()
    
    if not tables:
        return []
    
    # Різні варіанти номера справи для пошуку
    search_patterns = [
        case_number,                    # Точний номер
        case_number.replace("/", "\\/"),  # З екрануванням слешу
        case_number.split("/")[0] if "/" in case_number else case_number,  # Тільки перша частина
    ]
    
    all_decisions = []
    
    for table_name in tables:
        for pattern in search_patterns:
            decisions = search_decisions_in_table(table_name, pattern)
            if decisions:
                for decision in decisions:
                    decision["source_table"] = table_name
                    decision["yedr_url"] = f"https://reyestr.court.gov.ua/Review/{decision["doc_id"]}" if decision["doc_id"] else ""
                    all_decisions.append(decision)
    
    # Видаляємо дублікати по doc_id
    unique_decisions = {}
    for decision in all_decisions:
        doc_id = decision["doc_id"]
        if doc_id not in unique_decisions:
            unique_decisions[doc_id] = decision
    
    # Сортуємо за датою рішення (новіші спочатку)
    sorted_decisions = sorted(
        unique_decisions.values(), 
        key=lambda x: x["adjudication_date"] or date.min, 
        reverse=True
    )
    
    return sorted_decisions


def get_court_decision_tables():
    """Отримання списку таблиць судових рішень"""
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name LIKE 'court_decisions_%'
            ORDER BY table_name DESC
        """)

        return [row[0] for row in cursor.fetchall()]


def search_decisions_in_table(table_name, case_pattern):
    """Пошук судових рішень в конкретній таблиці"""
    with connection.cursor() as cursor:
        search_sql = f"""
            SELECT doc_id, court_code, judgment_code, cause_num, 
                   adjudication_date, judge, doc_url, status,
                   court_name, judgment_name, resolution_text
            FROM {table_name}
            WHERE cause_num ILIKE %s
            ORDER BY adjudication_date DESC NULLS LAST
            LIMIT 100
        """
        
        try:
            cursor.execute(search_sql, [f"%{case_pattern}%"])
            
            columns = [desc[0] for desc in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                result_dict = dict(zip(columns, row))
                results.append(result_dict)
            
            return results
            
        except Exception as e:
            # Логуємо помилку але продовжуємо роботу
            print(f"Помилка пошуку в таблиці {table_name}: {e}")
            return []


def court_decisions_dashboard(request):
    """Дашборд судових рішень з кешованою статистикою для швидкого відображення"""
    # Отримуємо базову інформацію
    tables = get_court_decision_tables()
    years_coverage = [table.replace("court_decisions_", "") for table in tables]
    
    # Намагаємося отримати кешовані дані
    total_stats = CourtDecisionStatistics.get_cached_stat("general")
    yearly_stats = CourtDecisionStatistics.get_cached_stat("yearly")
    top_courts_stats = CourtDecisionStatistics.get_cached_stat("courts")
    recent_decisions = CourtDecisionStatistics.get_cached_stat("recent")
    
    # Якщо кеш порожній, розраховуємо статистику та зберігаємо в кеш
    if not total_stats:
        start_time = time.time()
        total_stats = get_court_decisions_total_stats(tables)
        calculation_time = timedelta(seconds=time.time() - start_time)
        CourtDecisionStatistics.set_cached_stat(
            "general", 
            json_serialize_dates(total_stats),
            records_count=total_stats.get("total_decisions", 0),
            calculation_time=calculation_time,
            cache_hours=6  # Кеш на 6 годин
        )
    
    if not yearly_stats:
        start_time = time.time()
        yearly_stats = get_court_decisions_yearly_stats(tables)
        calculation_time = timedelta(seconds=time.time() - start_time)
        CourtDecisionStatistics.set_cached_stat(
            "yearly", 
            json_serialize_dates(yearly_stats),
            calculation_time=calculation_time,
            cache_hours=12  # Кеш на 12 годин
        )
    
    if not top_courts_stats:
        start_time = time.time()
        top_courts_stats = get_top_courts_from_decisions(tables, limit=20)
        calculation_time = timedelta(seconds=time.time() - start_time)
        CourtDecisionStatistics.set_cached_stat(
            "courts", 
            json_serialize_dates(top_courts_stats),
            calculation_time=calculation_time,
            cache_hours=24  # Кеш на 24 години
        )
    
    if not recent_decisions:
        start_time = time.time()
        recent_decisions = get_recent_court_decisions(tables, limit=10)
        calculation_time = timedelta(seconds=time.time() - start_time)
        CourtDecisionStatistics.set_cached_stat(
            "recent", 
            json_serialize_dates(recent_decisions),
            records_count=len(recent_decisions),
            calculation_time=calculation_time,
            cache_hours=1  # Кеш на 1 годину (часто змінюється)
        )
    
    context = {
        "total_stats": total_stats,
        "yearly_stats": yearly_stats,
        "top_courts_stats": top_courts_stats[:10],  # Топ 10 судів
        "recent_decisions": recent_decisions,
        "available_tables": len(tables),
        "years_coverage": years_coverage,
        "using_cache": True,  # Індикатор що використовується кеш
    }
    
    return render(request, "bankruptcy/court_decisions_dashboard.html", context)


def court_decisions_stats_api(request):
    """API для динамічного оновлення статистики судових рішень з кешуванням"""
    stats_type = request.GET.get("type", "general")
    force_refresh = request.GET.get("force_refresh", "false").lower() == "true"
    
    # Якщо потрібне примусове оновлення, очищуємо кеш
    if force_refresh:
        CourtDecisionStatistics.invalidate_all(stats_type)
    
    # Намагаємося отримати з кешу
    data = CourtDecisionStatistics.get_cached_stat(stats_type)
    
    if not data:
        # Якщо кеш порожній, розраховуємо заново
        tables = get_court_decision_tables()
        start_time = time.time()
        
        if stats_type == "yearly":
            data = get_court_decisions_yearly_stats(tables)
            cache_hours = 12
        elif stats_type == "courts":
            limit = int(request.GET.get("limit", 20))
            data = get_top_courts_from_decisions(tables, limit)
            cache_hours = 24
        elif stats_type == "categories":
            data = get_court_categories_stats(tables)
            cache_hours = 24
        elif stats_type == "justice_kinds":
            data = get_justice_kinds_stats(tables)
            cache_hours = 24
        elif stats_type == "recent":
            data = get_recent_court_decisions(tables, limit=10)
            cache_hours = 1
        else:  # general
            data = get_court_decisions_total_stats(tables)
            cache_hours = 6
        
        # Зберігаємо в кеш
        calculation_time = timedelta(seconds=time.time() - start_time)
        records_count = data.get("total_decisions", 0) if isinstance(data, dict) else len(data) if isinstance(data, list) else 0
        
        CourtDecisionStatistics.set_cached_stat(
            stats_type,
            json_serialize_dates(data),
            records_count=records_count,
            calculation_time=calculation_time,
            cache_hours=cache_hours
        )
    
    # Додаємо метадані про кеш
    response_data = {
        "data": data,
        "cached": not force_refresh,
        "timestamp": timezone.now().strftime("%d.%m.%Y %H:%M:%S")  # Київський час
    }
    
    return JsonResponse(response_data, safe=False)


def court_decisions_search_api(request):
    """API для пошуку судових рішень"""
    query = request.GET.get("q", "").strip()
    court_code = request.GET.get("court", "").strip()
    year = request.GET.get("year", "").strip()
    category = request.GET.get("category", "").strip()
    
    if not query and not court_code and not year and not category:
        return JsonResponse({"results": [], "total": 0})
    
    tables = get_court_decision_tables()
    results = []
    
    # Якщо вказано конкретний рік
    if year:
        table_name = f"court_decisions_{year}"
        if table_name in tables:
            tables = [table_name]
    
    for table_name in tables[:5]:  # Обмежуємо пошук 5 таблицями для швидкості
        table_results = search_decisions_advanced(table_name, query, court_code, category)
        for result in table_results:
            result["year"] = table_name.replace("court_decisions_", "")
            result["yedr_url"] = f"https://reyestr.court.gov.ua/Review/{result["doc_id"]}" if result["doc_id"] else ""
            results.append(result)
        
        # Видалено обмеження - показуємо всі знайдені рішення
    
    # Сортуємо за датою
    results.sort(key=lambda x: x["adjudication_date"] or date.min, reverse=True)
    
    return JsonResponse({
        "results": results,  # Показуємо всі результати без обмеження
        "total": len(results)
    })


# Допоміжні функції для статистики судових рішень

def get_court_decisions_total_stats(tables):
    """Загальна статистика по всіх таблицях"""
    if not tables:
        return {
            "total_decisions": 0,
            "total_courts": 0,
            "date_range": None,
            "avg_per_year": 0
        }
    
    with connection.cursor() as cursor:
        # Загальна кількість рішень
        total_decisions = 0
        unique_courts = set()
        all_dates = []
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT court_code) FROM {table}")
                count, courts = cursor.fetchone()
                total_decisions += count
                
                cursor.execute(f"SELECT DISTINCT court_code FROM {table} WHERE court_code IS NOT NULL")
                table_courts = cursor.fetchall()
                unique_courts.update([court[0] for court in table_courts])
                
                cursor.execute(f"SELECT MIN(adjudication_date), MAX(adjudication_date) FROM {table} WHERE adjudication_date IS NOT NULL")
                min_date, max_date = cursor.fetchone()
                if min_date and max_date:
                    all_dates.extend([min_date, max_date])
            except Exception as e:
                print(f"Помилка при обробці таблиці {table}: {e}")
                continue
        
        date_range = None
        avg_per_year = 0
        if all_dates:
            min_date = min(all_dates)
            max_date = max(all_dates)
            date_range = {"min": min_date, "max": max_date}
            
            years_diff = max_date.year - min_date.year + 1
            if years_diff > 0:
                avg_per_year = total_decisions // years_diff
        
        return {
            "total_decisions": total_decisions,
            "total_courts": len(unique_courts),
            "date_range": date_range,
            "avg_per_year": avg_per_year
        }


def get_court_decisions_yearly_stats(tables):
    """Статистика за роками"""
    yearly_data = []
    
    with connection.cursor() as cursor:
        for table in tables:
            try:
                year = table.replace("court_decisions_", "")
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                # Статистика за місяцями цього року - конвертуємо в прості значення
                cursor.execute(f"""
                    SELECT EXTRACT(MONTH FROM adjudication_date) as month, COUNT(*)
                    FROM {table}
                    WHERE adjudication_date IS NOT NULL
                    GROUP BY EXTRACT(MONTH FROM adjudication_date)
                    ORDER BY month
                """)
                monthly_data = []
                month_names = ["", "Січ", "Лют", "Бер", "Кві", "Тра", "Чер", 
                              "Лип", "Сер", "Вер", "Жов", "Лис", "Гру"]
                
                for row in cursor.fetchall():
                    month_num = int(row[0]) if row[0] else 0
                    month_name = month_names[month_num] if 0 < month_num < 13 else f"Місяць {month_num}"
                    monthly_data.append({
                        "month": month_num,
                        "month_name": month_name,
                        "count": row[1]
                    })
                
                yearly_data.append({
                    "year": int(year) if year.isdigit() else year,
                    "total": count,
                    "monthly": monthly_data
                })
            except Exception as e:
                print(f"Помилка при обробці річної статистики для {table}: {e}")
                continue
    
    return sorted(yearly_data, key=lambda x: x["year"], reverse=True)


def get_top_courts_from_decisions(tables, limit=10):
    """Топ судів за кількістю рішень"""
    court_stats = {}
    
    with connection.cursor() as cursor:
        for table in tables:
            try:
                cursor.execute(f"""
                    SELECT court_code, COUNT(*) as decision_count
                    FROM {table}
                    WHERE court_code IS NOT NULL AND court_code != ''
                    GROUP BY court_code
                """)
                
                for court_code, count in cursor.fetchall():
                    if court_code not in court_stats:
                        court_stats[court_code] = 0
                    court_stats[court_code] += count
            except Exception as e:
                print(f"Помилка при обробці судів для {table}: {e}")
                continue
    
    # Сортуємо та обмежуємо
    top_courts = sorted(court_stats.items(), key=lambda x: x[1], reverse=True)[:limit]
    
    return [{"court_code": court, "decisions_count": count} for court, count in top_courts]


def get_justice_kinds_stats(tables):
    """Статистика за видами судочинства"""
    justice_stats = {}
    
    with connection.cursor() as cursor:
        for table in tables:
            try:
                cursor.execute(f"""
                    SELECT justice_kind, COUNT(*) as count
                    FROM {table}
                    WHERE justice_kind IS NOT NULL AND justice_kind != ''
                    GROUP BY justice_kind
                """)
                
                for justice_kind, count in cursor.fetchall():
                    if justice_kind not in justice_stats:
                        justice_stats[justice_kind] = 0
                    justice_stats[justice_kind] += count
            except Exception as e:
                print(f"Помилка при обробці видів судочинства для {table}: {e}")
                continue
    
    return sorted([{"kind": kind, "count": count} for kind, count in justice_stats.items()], 
                 key=lambda x: x["count"], reverse=True)


def get_court_categories_stats(tables):
    """Статистика за категоріями справ"""
    category_stats = {}
    
    with connection.cursor() as cursor:
        for table in tables:
            try:
                cursor.execute(f"""
                    SELECT category_code, COUNT(*) as count
                    FROM {table}
                    WHERE category_code IS NOT NULL AND category_code != ''
                    GROUP BY category_code
                """)
                
                for category, count in cursor.fetchall():
                    if category not in category_stats:
                        category_stats[category] = 0
                    category_stats[category] += count
            except Exception as e:
                print(f"Помилка при обробці категорій для {table}: {e}")
                continue
    
    return sorted([{"category": cat, "count": count} for cat, count in category_stats.items()], 
                 key=lambda x: x["count"], reverse=True)


def get_recent_court_decisions(tables, limit=10):
    """Останні судові рішення"""
    all_decisions = []
    
    with connection.cursor() as cursor:
        for table in tables[:3]:  # Беремо тільки останні 3 роки для швидкості
            try:
                cursor.execute(f"""
                    SELECT doc_id, court_code, cause_num, adjudication_date, judge
                    FROM {table}
                    WHERE adjudication_date IS NOT NULL
                    ORDER BY adjudication_date DESC
                    LIMIT 20
                """)
                
                for row in cursor.fetchall():
                    all_decisions.append({
                        "doc_id": row[0],
                        "court_code": row[1],
                        "cause_num": row[2],
                        "adjudication_date": row[3],
                        "judge": row[4],
                        "year": table.replace("court_decisions_", ""),
                        "yedr_url": f"https://reyestr.court.gov.ua/Review/{row[0]}" if row[0] else ""
                    })
            except Exception as e:
                print(f"Помилка при отриманні останніх рішень з {table}: {e}")
                continue
    
    # Сортуємо всі рішення за датою
    all_decisions.sort(key=lambda x: x["adjudication_date"] or date.min, reverse=True)
    
    return all_decisions[:limit]


def search_decisions_advanced(table_name, query, court_code, category):
    """Розширений пошук судових рішень в таблиці"""
    with connection.cursor() as cursor:
        conditions = []
        params = []
        
        if query:
            conditions.append("(cause_num ILIKE %s OR judge ILIKE %s)")
            params.extend([f"%{query}%", f"%{query}%"])
        
        if court_code:
            conditions.append("court_code = %s")
            params.append(court_code)
        
        if category:
            conditions.append("category_code = %s")
            params.append(category)
        
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)
        
        search_sql = f"""
            SELECT doc_id, court_code, judgment_code, cause_num, 
                   adjudication_date, judge, doc_url, status, category_code
            FROM {table_name}
            {where_clause}
            ORDER BY adjudication_date DESC NULLS LAST
            LIMIT 20
        """
        
        try:
            cursor.execute(search_sql, params)
            
            columns = [desc[0] for desc in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                result_dict = dict(zip(columns, row))
                results.append(result_dict)
            
            return results
            
        except Exception as e:
            print(f"Помилка розширеного пошуку в таблиці {table_name}: {e}")
            return []


def hierarchical_court_decisions_stats_api(request):
    """API для ієрархічної статистики судових рішень: роки → види судочинства → інстанції → форми рішень"""
    year = request.GET.get("year", "")
    justice_kind = request.GET.get("justice_kind", "")
    instance_code = request.GET.get("instance_code", "")
    judgment_form = request.GET.get("judgment_form", "")
    force_refresh = request.GET.get("force_refresh", "false").lower() == "true"
    
    tables = get_court_decision_tables()
    
    # Створюємо ключ для кешування
    cache_key_parts = ["hierarchical"]
    if year: cache_key_parts.append(f"year_{year}")
    if justice_kind: cache_key_parts.append(f"jk_{justice_kind}")
    if instance_code: cache_key_parts.append(f"inst_{instance_code}")
    if judgment_form: cache_key_parts.append(f"jf_{judgment_form}")
    
    cache_key = "_".join(cache_key_parts)
    
    # Якщо потрібне примусове оновлення, очищуємо кеш
    if force_refresh:
        CourtDecisionStatistics.objects.filter(
            stat_type="hierarchical",
            stat_key=cache_key
        ).update(is_valid=False)
    
    # Намагаємося отримати з кешу
    cached_data = CourtDecisionStatistics.get_cached_stat("hierarchical", cache_key)
    if cached_data:
        return JsonResponse(cached_data, safe=False)
    
    try:
        start_time = time.time()
        
        # Рівень 1: Статистика за роками
        if not year:
            data = get_yearly_stats_cached(tables)
            print(f"[DEBUG] get_yearly_stats_cached повернув {len(data['data'])} років")
            print(f"[DEBUG] Роки: {[item['year'] for item in data['data']]}")
            cache_hours = 12
        # Рівень 2: Статистика за видами судочинства для конкретного року
        elif not justice_kind:
            data = get_justice_kinds_for_year_cached(tables, year)
            cache_hours = 6
        # Рівень 3: Статистика за інстанціями для виду судочинства
        elif not instance_code:
            data = get_instances_for_justice_kind_cached(tables, year, justice_kind)
            cache_hours = 6
        # Рівень 4: Статистика за формами рішень для інстанції
        elif not judgment_form:
            data = get_judgment_forms_for_instance_cached(tables, year, justice_kind, instance_code)
            cache_hours = 6
        # Детальна статистика
        else:
            data = get_detailed_stats_cached(tables, year, justice_kind, instance_code, judgment_form)
            cache_hours = 6
        
        # Зберігаємо в кеш
        calculation_time = timedelta(seconds=time.time() - start_time)
        records_count = len(data.get("data", [])) if isinstance(data, dict) and "data" in data else 0
        
        CourtDecisionStatistics.set_cached_stat(
            "hierarchical",
            json_serialize_dates(data),
            stat_key=cache_key,
            records_count=records_count,
            calculation_time=calculation_time,
            cache_hours=cache_hours
        )
        
        return JsonResponse(data, safe=False)
        
    except Exception as e:
        return JsonResponse({"error": f"Помилка: {str(e)}"}, status=500)


def get_yearly_stats_cached(tables):
    """Отримання статистики за роками з розбивкою по видах судочинства"""
    yearly_stats = []

    with connection.cursor() as cursor:
        for table in tables:
            try:
                year_str = table.replace("court_decisions_", "")
                
                # Виправляємо відображення року: конвертуємо 25 -> 2025, 24 -> 2024
                if year_str.isdigit():
                    year_num = int(year_str)
                    if len(year_str) == 4:  # Повний рік вже
                        full_year = year_num
                    elif year_num < 50:  # 00-49 це 2000-2049
                        full_year = 2000 + year_num
                    else:  # 50-99 це 1950-1999
                        full_year = 1900 + year_num
                else:
                    full_year = year_str
                
                # Загальна кількість рішень за рік
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                total_count = cursor.fetchone()[0]
                
                # Статистика по видах судочинства
                cursor.execute(f"""
                    SELECT justice_kind, COUNT(*) as count
                    FROM {table}
                    WHERE justice_kind IS NOT NULL AND justice_kind != ''
                    GROUP BY justice_kind
                    ORDER BY 
                        CASE justice_kind
                            WHEN '3' THEN 1  -- Господарське
                            WHEN '2' THEN 2  -- Кримінальне
                            WHEN '1' THEN 3  -- Цивільне
                            WHEN '4' THEN 4  -- Адміністративне
                            WHEN '5' THEN 5  -- Адмінправопорушення
                            ELSE 6
                        END
                """)
                
                justice_kinds_stats = []
                for row in cursor.fetchall():
                    justice_kind_code = row[0]
                    count = row[1]
                    
                    # Отримуємо назву виду судочинства з довідника
                    try:
                        justice_kind_obj = JusticeKind.objects.get(code=justice_kind_code)
                        justice_kind_name = justice_kind_obj.name
                    except:
                        justice_kind_name = f"Невідомий вид ({justice_kind_code})"
                    
                    justice_kinds_stats.append({
                        "code": justice_kind_code,
                        "name": justice_kind_name,
                        "count": count
                    })
                
                yearly_stats.append({
                    "year": full_year,
                    "year_short": year_str,  # Для URL параметрів
                    "total_count": total_count,
                    "justice_kinds": justice_kinds_stats
                })
                
            except Exception as e:
                print(f"Помилка при обробці таблиці {table}: {e}")
                continue
    
    return {
        "level": "years",
        "data": sorted(yearly_stats, key=lambda x: x["year"], reverse=True)
    }


def get_justice_kinds_for_year_cached(tables, year):
    """Отримання статистики по видах судочинства для конкретного року"""
    table_name = f"court_decisions_{year}"
    
    if table_name not in tables:
        return {"error": f"Таблиця для {year} року не знайдена"}
    
    with connection.cursor() as cursor:
        try:
            cursor.execute(f"""
                SELECT justice_kind, COUNT(*) as count
                FROM {table_name}
                WHERE justice_kind IS NOT NULL AND justice_kind != ''
                GROUP BY justice_kind
                ORDER BY 
                    CASE justice_kind 
                        WHEN '3' THEN 1  -- Господарське
                        WHEN '2' THEN 2  -- Кримінальне
                        WHEN '1' THEN 3  -- Цивільне
                        WHEN '4' THEN 4  -- Адміністративне
                        WHEN '5' THEN 5  -- Адмінправопорушення
                        ELSE 6
                    END
            """)
            
            justice_kinds_stats = []
            for row in cursor.fetchall():
                justice_kind_code = row[0]
                count = row[1]
                
                # Отримуємо назву виду судочинства з довідника
                try:
                    justice_kind_obj = JusticeKind.objects.get(code=justice_kind_code)
                    justice_kind_name = justice_kind_obj.name
                except:
                    justice_kind_name = f"Невідомий вид ({justice_kind_code})"
                
                # Тільки відображаємо ті, для яких є дані
                if count > 0:
                    # Отримуємо статистику по інстанціях для цього виду судочинства
                    cursor.execute(f"""
                        SELECT c.instance_code, COUNT(*) as count
                        FROM {table_name} d
                        LEFT JOIN bankruptcy_courts_ref c ON d.court_code = c.court_code
                        WHERE d.justice_kind = %s 
                        AND d.court_code IS NOT NULL 
                        AND d.court_code != ''
                        GROUP BY c.instance_code
                        HAVING c.instance_code IS NOT NULL
                        ORDER BY 
                            CASE c.instance_code 
                                WHEN '1' THEN 1  -- Касаційна
                                WHEN '2' THEN 2  -- Апеляційна
                                WHEN '3' THEN 3  -- Перша
                                WHEN '4' THEN 4  -- Господарський суд області
                                WHEN '5' THEN 5  -- Місцевий суд
                                WHEN '9' THEN 6  -- Інша інстанція
                                WHEN '0' THEN 7  -- Невідома інстанція
                                ELSE 8
                            END
                    """, [justice_kind_code])
                    
                    instances = []
                    for inst_row in cursor.fetchall():
                        instance_code = inst_row[0]
                        instance_count = inst_row[1]
                        
                        # Отримуємо назву інстанції з довідника
                        try:
                            instance_obj = Instance.objects.get(code=instance_code)
                            instance_name = instance_obj.name
                        except:
                            instance_name = f"Невідома інстанція ({instance_code})"
                        
                        instances.append({
                            "code": instance_code,
                            "name": instance_name,
                            "count": instance_count
                        })
                    
                    justice_kinds_stats.append({
                        "code": justice_kind_code,
                        "name": justice_kind_name,
                        "count": count,
                        "instances": instances
                    })
            
            return {
                "level": "justice_kinds",
                "year": year,
                "data": justice_kinds_stats
            }
            
        except Exception as e:
            return {"error": f"Помилка запиту: {str(e)}"}


def get_instances_for_justice_kind_cached(tables, year, justice_kind):
    """Отримання статистики по інстанціях для виду судочинства"""
    table_name = f"court_decisions_{year}"
    
    if table_name not in tables:
        return {"error": f"Таблиця для {year} року не знайдена"}
    
    with connection.cursor() as cursor:
        try:
            # Отримаємо всі рішення для цього виду судочинства
            # і візьмемо код інстанції з довідника судів
            cursor.execute(f"""
                SELECT c.instance_code, COUNT(*) as count
                FROM {table_name} d
                LEFT JOIN bankruptcy_courts_ref c ON d.court_code = c.court_code
                WHERE d.justice_kind = %s 
                AND d.court_code IS NOT NULL 
                AND d.court_code != ''
                GROUP BY c.instance_code
                HAVING c.instance_code IS NOT NULL
                ORDER BY 
                    CASE c.instance_code 
                        WHEN '1' THEN 1  -- Касаційна
                        WHEN '2' THEN 2  -- Апеляційна
                        WHEN '3' THEN 3  -- Перша
                        WHEN '4' THEN 4  -- Господарський суд області
                        WHEN '5' THEN 5  -- Місцевий суд
                        WHEN '9' THEN 6  -- Інша інстанція
                        WHEN '0' THEN 7  -- Невідома інстанція
                        ELSE 8
                    END
            """, [justice_kind])
            
            instances_stats = []
            for row in cursor.fetchall():
                instance_code = row[0]
                count = row[1]
                
                # Отримуємо назву інстанції з довідника
                try:
                    instance_obj = Instance.objects.get(code=instance_code)
                    instance_name = instance_obj.name
                except:
                    instance_name = f"Невідома інстанція ({instance_code})"
                
                # Тільки відображаємо ті, для яких є дані
                if count > 0:
                    # Отримуємо статистику по формах рішень для цієї інстанції
                    cursor.execute(f"""
                        SELECT d.judgment_code, COUNT(*) as count
                        FROM {table_name} d
                        LEFT JOIN bankruptcy_courts_ref c ON d.court_code = c.court_code
                        WHERE d.justice_kind = %s 
                        AND c.instance_code = %s
                        AND d.judgment_code IS NOT NULL 
                        AND d.judgment_code != ''
                        AND d.judgment_code IN ('1', '2', '3', '4', '5', '6', '7', '10')
                        GROUP BY d.judgment_code
                        ORDER BY 
                            CASE d.judgment_code 
                                WHEN '2' THEN 1  -- Постанова
                                WHEN '1' THEN 2  -- Вирок
                                WHEN '5' THEN 3  -- Ухвала
                                WHEN '3' THEN 4  -- Рішення
                                WHEN '7' THEN 5  -- Додаткове рішення
                                WHEN '6' THEN 6  -- Окрема ухвала
                                WHEN '10' THEN 7 -- Окрема думка
                                WHEN '4' THEN 8  -- Судовий наказ
                                ELSE 9 
                            END
                    """, [justice_kind, instance_code])
                    
                    judgment_forms = []
                    for jf_row in cursor.fetchall():
                        judgment_code = jf_row[0]
                        judgment_count = jf_row[1]
                        
                        # Отримуємо назву форми рішення з довідника
                        try:
                            judgment_form_obj = JudgmentForm.objects.get(code=judgment_code)
                            judgment_form_name = judgment_form_obj.name
                        except:
                            judgment_form_name = f"Невідома форма ({judgment_code})"
                        
                        judgment_forms.append({
                            "code": judgment_code,
                            "name": judgment_form_name,
                            "count": judgment_count
                        })
                    
                    instances_stats.append({
                        "code": instance_code,
                        "name": instance_name,
                        "count": count,
                        "judgment_forms": judgment_forms
                    })
            
            return {
                "level": "instances",
                "year": year,
                "justice_kind": justice_kind,
                "data": instances_stats
            }
            
        except Exception as e:
            return {"error": f"Помилка запиту: {str(e)}"}


def get_judgment_forms_for_instance_cached(tables, year, justice_kind, instance_code):
    """Отримання статистики по формах рішень для інстанції"""
    table_name = f"court_decisions_{year}"
    
    if table_name not in tables:
        return {"error": f"Таблиця для {year} року не знайдена"}
    
    with connection.cursor() as cursor:
        try:
            cursor.execute(f"""
                SELECT d.judgment_code, COUNT(*) as count
                FROM {table_name} d
                LEFT JOIN bankruptcy_courts_ref c ON d.court_code = c.court_code
                WHERE d.justice_kind = %s 
                AND c.instance_code = %s
                AND d.judgment_code IS NOT NULL 
                AND d.judgment_code != ''
                AND d.judgment_code IN ('1', '2', '3', '4', '5', '6', '7', '10')
                GROUP BY d.judgment_code
                ORDER BY 
                    CASE d.judgment_code 
                        WHEN '2' THEN 1  -- Постанова
                        WHEN '1' THEN 2  -- Вирок
                        WHEN '5' THEN 3  -- Ухвала
                        WHEN '3' THEN 4  -- Рішення
                        WHEN '7' THEN 5  -- Додаткове рішення
                        WHEN '6' THEN 6  -- Окрема ухвала
                        WHEN '10' THEN 7 -- Окрема думка
                        WHEN '4' THEN 8  -- Судовий наказ
                        ELSE 9 
                    END
            """, [justice_kind, instance_code])
            
            judgment_forms_stats = []
            for row in cursor.fetchall():
                judgment_code = row[0]
                count = row[1]
                
                # Отримуємо назву форми рішення з довідника
                try:
                    judgment_form_obj = JudgmentForm.objects.get(code=judgment_code)
                    judgment_form_name = judgment_form_obj.name
                except:
                    judgment_form_name = f"Невідома форма ({judgment_code})"
                
                # Тільки відображаємо ті, для яких є дані
                if count > 0:
                    judgment_forms_stats.append({
                        "code": judgment_code,
                        "name": judgment_form_name,
                        "count": count
                    })
            
            return {
                "level": "judgment_forms",
                "year": year,
                "justice_kind": justice_kind,
                "instance_code": instance_code,
                "data": judgment_forms_stats
            }
            
        except Exception as e:
            return {"error": f"Помилка запиту: {str(e)}"}


def get_detailed_stats_cached(tables, year, justice_kind, instance_code, judgment_form):
    """Детальна статистика для конкретної комбінації параметрів - повертає конкретні судові рішення"""
    table_name = f"court_decisions_{year}"

    if table_name not in tables:
        return {"error": f"Таблиця для {year} року не знайдена"}

    with connection.cursor() as cursor:
        try:
            # Використовуємо функцію пагінації для отримання судових рішень
            result = get_court_decisions_paginated(year, justice_kind, instance_code, judgment_form, page=1, per_page=100)

            if "error" in result:
                return {"error": result["error"]}

            return {
                "level": "documents",
                "year": year,
                "justice_kind": justice_kind,
                "instance_code": instance_code,
                "judgment_form": judgment_form,
                "data": result["decisions"],
                "pagination": result["pagination"]
            }

        except Exception as e:
            return {"error": f"Помилка запиту: {str(e)}"}


def get_court_decisions_paginated(year, justice_kind, instance_code, judgment_code, page=1, per_page=100, filters=None):
    """Отримання конкретних судових рішень з пагінацією"""
    table_name = f"court_decisions_{year}"

    # Перевіряємо чи існує таблиця
    tables = get_court_decision_tables()
    if table_name not in tables:
        return {"error": f"Таблиця для {year} року не знайдена"}

    offset = (page - 1) * per_page

    # Будуємо умови фільтрації
    filter_conditions = []
    filter_params = [justice_kind, instance_code, judgment_code]

    if filters:
        # Фільтр по точній даті
        if filters.get('exact_date'):
            filter_conditions.append("d.adjudication_date = %s")
            filter_params.append(filters['exact_date'])

        # Фільтр по періоду
        if filters.get('date_from'):
            filter_conditions.append("d.adjudication_date >= %s")
            filter_params.append(filters['date_from'])
        if filters.get('date_to'):
            filter_conditions.append("d.adjudication_date <= %s")
            filter_params.append(filters['date_to'])

        # Фільтр по номеру справи (частковий пошук)
        if filters.get('case_number'):
            filter_conditions.append("d.cause_num ILIKE %s")
            filter_params.append(f"%{filters['case_number']}%")

        # Фільтр по судді (частковий пошук)
        if filters.get('judge'):
            filter_conditions.append("d.judge ILIKE %s")
            filter_params.append(f"%{filters['judge']}%")

        # Фільтр по суду (точний пошук по коду або частковий по назві)
        if filters.get('court'):
            court_value = filters['court']
            # Якщо це числовий код суду, шукаємо точно по court_code
            if court_value.isdigit():
                filter_conditions.append("d.court_code = %s")
                filter_params.append(court_value)
            else:
                # Інакше шукаємо по назві суду (частковий пошук)
                filter_conditions.append("(d.court_name ILIKE %s OR c.name ILIKE %s)")
                filter_params.extend([f"%{court_value}%", f"%{court_value}%"])

    additional_where = ""
    if filter_conditions:
        additional_where = "AND " + " AND ".join(filter_conditions)

    with connection.cursor() as cursor:
        try:
            # Підрахунок загальної кількості записів
            cursor.execute(f"""
                SELECT COUNT(*) as total
                FROM {table_name} d
                LEFT JOIN bankruptcy_courts_ref c ON d.court_code = c.court_code
                WHERE d.justice_kind = %s
                AND c.instance_code = %s
                AND d.judgment_code = %s
                AND d.court_code IS NOT NULL
                AND d.court_code != ''
                {additional_where}
            """, filter_params)

            total_count = cursor.fetchone()[0]
            total_pages = (total_count + per_page - 1) // per_page

            # Отримання судових рішень для поточної сторінки
            data_params = filter_params + [per_page, offset]
            cursor.execute(f"""
                SELECT d.doc_id, d.court_code, d.cause_num, d.adjudication_date,
                       d.judge, d.judgment_code, d.doc_url, d.status,
                       d.court_name, jf.name as judgment_form_name,
                       c.name as court_full_name, c.region_code, c.instance_code
                FROM {table_name} d
                LEFT JOIN bankruptcy_courts_ref c ON d.court_code = c.court_code
                LEFT JOIN bankruptcy_judgment_forms jf ON d.judgment_code = jf.code
                WHERE d.justice_kind = %s
                AND c.instance_code = %s
                AND d.judgment_code = %s
                AND d.court_code IS NOT NULL
                AND d.court_code != ''
                {additional_where}
                ORDER BY d.adjudication_date DESC, d.doc_id DESC
                LIMIT %s OFFSET %s
            """, data_params)

            decisions = []
            for row in cursor.fetchall():
                decision = {
                    'doc_id': row[0],
                    'court_code': row[1],
                    'case_number': row[2],
                    'adjudication_date': row[3].isoformat() if row[3] else None,
                    'judge': row[4],
                    'judgment_code': row[5],
                    'doc_url': row[6],
                    'status': row[7],
                    'court_name': row[10] or row[8],  # Використовуємо з довідника першочергово
                    'judgment_form_name': row[9],
                    'court_full_name': row[10],
                    'region_code': row[11],
                    'instance_code': row[12]
                }
                decisions.append(decision)

            return {
                'decisions': decisions,
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total_count': total_count,
                    'total_pages': total_pages,
                    'has_next': page < total_pages,
                    'has_prev': page > 1
                },
                'filters': {
                    'year': year,
                    'justice_kind': justice_kind,
                    'instance_code': instance_code,
                    'judgment_code': judgment_code
                }
            }

        except Exception as e:
            return {"error": f"Помилка запиту: {str(e)}"}


def court_decisions_list_api(request):
    """API для отримання списку судових рішень з пагінацією та фільтрами"""
    year = request.GET.get('year')
    justice_kind = request.GET.get('justice_kind')
    instance_code = request.GET.get('instance_code')
    judgment_code = request.GET.get('judgment_code')
    page = int(request.GET.get('page', 1))
    per_page = int(request.GET.get('per_page', 100))

    # Фільтри
    filters = {
        'exact_date': request.GET.get('exact_date'),
        'date_from': request.GET.get('date_from'),
        'date_to': request.GET.get('date_to'),
        'case_number': request.GET.get('case_number'),
        'judge': request.GET.get('judge'),
        'court': request.GET.get('court')
    }

    if not all([year, justice_kind, instance_code, judgment_code]):
        return JsonResponse({'error': 'Відсутні обов\'язкові параметри'}, status=400)

    try:
        result = get_court_decisions_paginated(
            year=year,
            justice_kind=justice_kind,
            instance_code=instance_code,
            judgment_code=judgment_code,
            page=page,
            per_page=per_page,
            filters=filters
        )
        return JsonResponse(result, safe=False)
    except Exception as e:
        return JsonResponse({'error': f'Помилка: {str(e)}'}, status=500)


def courts_list_api(request):
    """API для отримання списку судів за параметрами фільтрації"""
    year = request.GET.get('year')
    justice_kind = request.GET.get('justice_kind')
    instance_code = request.GET.get('instance_code')
    judgment_code = request.GET.get('judgment_code')

    if not all([year, justice_kind, instance_code, judgment_code]):
        return JsonResponse({'error': 'Відсутні обов\'язкові параметри'}, status=400)

    table_name = f"court_decisions_{year}"

    # Перевіряємо чи існує таблиця
    tables = get_court_decision_tables()
    if table_name not in tables:
        return JsonResponse({'error': f'Таблиця для {year} року не знайдена'}, status=404)

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT DISTINCT
                    d.court_code,
                    COALESCE(c.name, d.court_name) as court_name,
                    c.region_code,
                    COUNT(*) as decisions_count
                FROM {table_name} d
                LEFT JOIN bankruptcy_courts_ref c ON d.court_code = c.court_code
                WHERE d.justice_kind = %s
                AND c.instance_code = %s
                AND d.judgment_code = %s
                AND d.court_code IS NOT NULL
                AND d.court_code != ''
                GROUP BY d.court_code, c.name, d.court_name, c.region_code
                HAVING COUNT(*) > 0
                ORDER BY court_name, d.court_code
            """, [justice_kind, instance_code, judgment_code])

            courts = []
            for row in cursor.fetchall():
                courts.append({
                    'court_code': row[0],
                    'court_name': row[1] or f'Суд {row[0]}',
                    'region_code': row[2],
                    'decisions_count': row[3]
                })

            return JsonResponse({'courts': courts})

    except Exception as e:
        return JsonResponse({'error': f'Помилка: {str(e)}'}, status=500)


def monitoring_stats_api(request):
    """
    API для отримання статистики моніторингу в реальному часі
    """
    try:
        from .models import MonitoringStatistics
        
        # Оновлюємо загальну статистику
        stats = MonitoringStatistics.update_general_stats()
        
        # Формуємо відповідь
        response_data = {
            "status": "success",
            "data": {
                "total_cases": stats.total_cases,
                "cases_with_decisions": stats.cases_with_decisions,
                "cases_without_decisions": stats.total_cases - stats.cases_with_decisions,
                "total_decisions": stats.total_decisions,
                "decisions_with_resolutions": stats.decisions_with_resolutions,
                "decisions_without_resolutions": stats.total_decisions - stats.decisions_with_resolutions,
                "decisions_without_rtf": stats.decisions_without_rtf,
                "currently_processing": stats.currently_processing,
                "processing_type": stats.processing_type,
                "processed_count": stats.processed_count,
                "total_to_process": stats.total_to_process,
                "progress_percentage": (
                    round((stats.processed_count / stats.total_to_process) * 100, 1) 
                    if stats.total_to_process > 0 else 0
                ),
                "last_updated": stats.last_updated.isoformat() if stats.last_updated else None,
                "last_search_run": stats.last_search_run.isoformat() if stats.last_search_run else None,
                "last_extraction_run": stats.last_extraction_run.isoformat() if stats.last_extraction_run else None,
            },
            "timestamp": timezone.now().isoformat()
        }
        
        return JsonResponse(response_data)
        
    except Exception as e:
        return JsonResponse({
            "status": "error",
            "message": f"Помилка отримання статистики: {str(e)}",
            "timestamp": timezone.now().isoformat()
        }, status=500)


def system_control_panel(request):
    """Панель управління системними процесами"""
    # Ініціалізуємо записи управління процесами якщо їх немає
    process_types = ["court_search", "resolution_extraction", "file_monitoring"]
    
    for process_type in process_types:
        SystemProcessControl.objects.get_or_create(
            process_type=process_type,
            defaults={
                "status": "idle",
                "is_forced": False,
            }
        )
    
    # Отримуємо всі процеси
    processes = SystemProcessControl.objects.all().order_by("process_type")
    
    # Перевіряємо стан системи
    forced_process = SystemProcessControl.get_forced_process()
    
    # Додаємо статистику
    try:
        # Статистика справ
        total_cases = BankruptcyCase.objects.count()
        tracked_cases = TrackedBankruptcyCase.objects.count()
        untracked_cases = total_cases - tracked_cases
        
        # Статистика судових рішень
        total_decisions = TrackedCourtDecision.objects.count()
        decisions_with_resolutions = TrackedCourtDecision.objects.filter(
            resolution_text__isnull=False
        ).exclude(resolution_text="").count()
        decisions_without_resolutions = total_decisions - decisions_with_resolutions
        
        # Статистика з різних років
        current_year_decisions = 0
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM court_decisions_2024")
                current_year_decisions = cursor.fetchone()[0]
        except:
            current_year_decisions = 0
        
        # Додаткова статистика для витягування резолютивних частин
        decisions_with_triggers = TrackedCourtDecision.objects.filter(
            has_trigger_words=True
        ).count()
        decisions_with_rtf = TrackedCourtDecision.objects.filter(
            doc_url__isnull=False
        ).exclude(doc_url="").exclude(doc_url="nan").count()
        decisions_without_rtf = total_decisions - decisions_with_rtf
        
        # Статистика за останню добу
        from datetime import timedelta
        yesterday = timezone.now() - timedelta(days=1)
        recent_extractions = TrackedCourtDecision.objects.filter(
            resolution_extracted_at__gte=yesterday
        ).count()
        
        # Відсоток завершення витягування
        extraction_percentage = (decisions_with_resolutions / total_decisions * 100) if total_decisions > 0 else 0
        trigger_percentage = (decisions_with_triggers / decisions_with_resolutions * 100) if decisions_with_resolutions > 0 else 0
        rtf_availability_percentage = (decisions_with_rtf / total_decisions * 100) if total_decisions > 0 else 0
        
        # Додаткова детальна статистика
        today = timezone.now().date()
        week_ago = timezone.now() - timedelta(days=7)
        month_ago = timezone.now() - timedelta(days=30)
        
        today_extractions = TrackedCourtDecision.objects.filter(
            resolution_extracted_at__date=today
        ).count()
        
        week_extractions = TrackedCourtDecision.objects.filter(
            resolution_extracted_at__gte=week_ago
        ).count()
        
        month_extractions = TrackedCourtDecision.objects.filter(
            resolution_extracted_at__gte=month_ago
        ).count()
        
        # Швидкість витягування (рішень за годину на основі останніх 24 годин)
        extraction_speed = recent_extractions if recent_extractions > 0 else 0
        
        # Пікова година активності (наразі заглушка, можна додати реальну логіку)
        peak_extraction_hour = 14  # 14:00 - найактивніша година
        
        # Якість витягування
        successful_extractions = decisions_with_resolutions
        failed_extractions = TrackedCourtDecision.objects.filter(
            resolution_text__isnull=True,
            doc_url__isnull=False
        ).exclude(doc_url="").exclude(doc_url="nan").count()
        retry_extractions = 0  # Поки заглушка
        
        # Середня довжина резолютивних частин
        from django.db.models import Avg, F
        avg_resolution_length = TrackedCourtDecision.objects.filter(
            resolution_text__isnull=False
        ).exclude(resolution_text="").aggregate(
            avg_length=Avg(F("resolution_text__length"))
        ).get("avg_length", 0) or 0
        
        statistics = {
            "total_cases": total_cases,
            "tracked_cases": tracked_cases,
            "untracked_cases": untracked_cases,
            "total_decisions": total_decisions,
            "decisions_with_resolutions": decisions_with_resolutions,
            "decisions_without_resolutions": decisions_without_resolutions,
            "current_year_decisions": current_year_decisions,
            # Додаткова статистика витягування
            "decisions_with_triggers": decisions_with_triggers,
            "decisions_with_rtf": decisions_with_rtf,
            "decisions_without_rtf": decisions_without_rtf,
            "recent_extractions": recent_extractions,
            "extraction_percentage": extraction_percentage,
            "trigger_percentage": trigger_percentage,
            "rtf_availability_percentage": rtf_availability_percentage,
            # Детальна статистика витягування
            "today_extractions": today_extractions,
            "week_extractions": week_extractions,
            "month_extractions": month_extractions,
            "extraction_speed": extraction_speed,
            "peak_extraction_hour": peak_extraction_hour,
            "successful_extractions": successful_extractions,
            "failed_extractions": failed_extractions,
            "retry_extractions": retry_extractions,
            "avg_resolution_length": avg_resolution_length,
        }
        
    except Exception as e:
        statistics = {
            "error": f"Помилка отримання статистики: {str(e)}"
        }
    
    context = {
        "processes": processes,
        "forced_process": forced_process,
        "is_any_forced": SystemProcessControl.is_any_process_forced(),
        "stats": statistics,
    }
    
    return render(request, "bankruptcy/system_control.html", context)


def start_forced_process(request):
    """API для запуску примусового процесу"""
    if request.method != "POST":
        return JsonResponse({"error": "Тільки POST запити"}, status=405)
    
    try:
        process_type = request.POST.get("process_type")
        if not process_type:
            return JsonResponse({"error": "Не вказано тип процесу"}, status=400)
        
        # Перевіряємо чи немає вже запущеного примусового процесу
        if SystemProcessControl.is_any_process_forced():
            existing_process = SystemProcessControl.get_forced_process()
            return JsonResponse({
                "error": f"Вже запущено примусовий процес: {existing_process.get_process_type_display()}"
            }, status=400)
        
        # Отримуємо процес
        try:
            process_control = SystemProcessControl.objects.get(process_type=process_type)
        except SystemProcessControl.DoesNotExist:
            return JsonResponse({"error": "Невідомий тип процесу"}, status=400)
        
        # Запускаємо примусовий режим
        process_control.start_forced()
        
        # Запускаємо відповідну команду в фоні
        import subprocess
        import threading
        
        if process_type == "court_search":
            command = ["python", "manage.py", "force_court_search", "--all-cases"]
        elif process_type == "resolution_extraction":
            command = ["python", "manage.py", "force_resolution_extract", "--missing-only"]
        else:
            return JsonResponse({"error": "Не підтримується автозапуск для цього типу процесу"}, status=400)
        
        def run_command():
            try:
                subprocess.run(command, cwd="/home/ruslan/PYTHON/analiz_klientiv")
            except Exception as e:
                logger.error(f"Помилка запуску команди {process_type}: {e}")
        
        thread = threading.Thread(target=run_command)
        thread.daemon = True
        thread.start()
        
        return JsonResponse({
            "success": True,
            "message": f"Запущено примусовий процес: {process_control.get_process_type_display()}"
        })
        
    except Exception as e:
        return JsonResponse({"error": f"Помилка запуску процесу: {str(e)}"}, status=500)


def stop_forced_process(request):
    """API для зупинки примусового процесу"""
    if request.method != "POST":
        return JsonResponse({"error": "Тільки POST запити"}, status=405)
    
    try:
        forced_process = SystemProcessControl.get_forced_process()
        if not forced_process:
            return JsonResponse({"error": "Немає активного примусового процесу"}, status=400)
        
        # Зупиняємо примусовий режим
        process_name = forced_process.get_process_type_display()
        forced_process.stop_forced()
        
        return JsonResponse({
            "success": True,
            "message": f"Зупинено примусовий процес: {process_name}"
        })
        
    except Exception as e:
        return JsonResponse({"error": f"Помилка зупинки процесу: {str(e)}"}, status=500)


def process_status_api(request):
    """API для отримання статусу процесів"""
    try:
        import subprocess
        from django.utils import timezone
        from datetime import timedelta
        
        processes = SystemProcessControl.objects.all()
        
        processes_data = []
        for process in processes:
            # 🚀 АВТОМАТИЧНА ПЕРЕВІРКА ЗАСТРЯГЛИХ ПРОЦЕСІВ
            if (process.status == "running" and process.process_type == "resolution_extraction" and 
                process.updated_at and 
                timezone.now() - process.updated_at > timedelta(minutes=10)):  # 10 хвилин без оновлень
                
                # Перевіряємо чи процес дійсно працює
                try:
                    ps_result = subprocess.run([
                        "ps", "aux", "--no-headers"
                    ], capture_output=True, text=True)
                    
                    process_exists = False
                    if ps_result.returncode == 0:
                        for line in ps_result.stdout.strip().split("\n"):
                            if ("extract_resolutions_fast" in line or 
                                "extract_resolution_texts" in line) and "python manage.py" in line:
                                process_exists = True
                                break
                    
                    if not process_exists:
                        # Процес не працює - оновлюємо статус
                        process.status = "failed"
                        process.last_message = "❌ Процес аварійно завершився (автоматично виявлено)"
                        process.finished_at = timezone.now()
                        process.save()
                        
                except Exception as e:
                    # Помилка перевірки - також вважаємо процес завершеним
                    process.status = "failed"
                    process.last_message = f"❌ Помилка перевірки процесу: {str(e)}"
                    process.finished_at = timezone.now()
                    process.save()
            
            processes_data.append({
                "id": process.id,
                "type": process.process_type,
                "type_display": process.get_process_type_display(),
                "status": process.status,
                "status_display": process.get_status_display(),
                "is_forced": process.is_forced,
                "force_stop_others": process.force_stop_others,
                "progress_current": process.progress_current,
                "progress_total": process.progress_total,
                "progress_percentage": process.progress_percentage,
                "last_message": process.last_message,
                "started_at": process.started_at.isoformat() if process.started_at else None,
                "updated_at": process.updated_at.isoformat() if process.updated_at else None,
            })
        
        return JsonResponse({
            "success": True,
            "processes": processes_data,
            "is_any_forced": SystemProcessControl.is_any_process_forced(),
            "timestamp": timezone.now().isoformat()
        })
        
    except Exception as e:
        return JsonResponse({
            "success": False,
            "error": f"Помилка отримання статусу: {str(e)}",
            "timestamp": timezone.now().isoformat()
        }, status=500)


def start_incremental_extraction(request):
    """API для запуску інкрементального витягування резолютивних частин"""
    if request.method != "POST":
        return JsonResponse({"error": "Тільки POST запити"}, status=405)
    
    try:
        import json
        import subprocess
        import threading
        
        # Перевіряємо чи немає примусового процесу
        if SystemProcessControl.is_any_process_forced():
            forced_process = SystemProcessControl.get_forced_process()
            return JsonResponse({
                "error": f"Заблоковано примусовим процесом: {forced_process.get_process_type_display()}"
            }, status=400)
        
        # Отримуємо параметри запиту
        try:
            body = json.loads(request.body) if request.body else {}
        except json.JSONDecodeError:
            body = {}
        
        missing_only = body.get("missing_only", True)
        auto_incremental = body.get("auto_incremental", True)
        
        # Підготовляємо команду
        command = ["python", "manage.py", "extract_resolution_texts"]
        if missing_only:
            command.append("--missing-only")
        if auto_incremental:
            command.append("--auto-incremental")
        
        # Оновлюємо статус процесу
        try:
            process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
            process_control.update_progress(0, 0, "Запуск інкрементального витягування...")
            process_control.status = "running"
            process_control.save()
        except SystemProcessControl.DoesNotExist:
            pass
        
        def run_incremental_extraction():
            """Запуск витягування в окремому потоці"""
            try:
                result = subprocess.run(
                    command, 
                    cwd="/home/ruslan/PYTHON/analiz_klientiv",
                    capture_output=True,
                    text=True,
                    timeout=3600  # 1 година таймаут
                )
                
                # Оновлюємо статус після завершення
                try:
                    process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
                    if result.returncode == 0:
                        process_control.update_progress(
                            process_control.progress_total, 
                            process_control.progress_total,
                            "Інкрементальне витягування завершено успішно"
                        )
                        process_control.status = "idle"
                    else:
                        process_control.update_progress(
                            process_control.progress_current,
                            process_control.progress_total,
                            f"Помилка інкрементального витягування: {result.stderr[:200]}"
                        )
                        process_control.status = "error"
                    process_control.save()
                except SystemProcessControl.DoesNotExist:
                    pass
                    
            except subprocess.TimeoutExpired:
                # Таймаут
                try:
                    process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
                    process_control.update_progress(
                        process_control.progress_current,
                        process_control.progress_total,
                        "Інкрементальне витягування перервано через таймаут"
                    )
                    process_control.status = "error"
                    process_control.save()
                except SystemProcessControl.DoesNotExist:
                    pass
            except Exception as e:
                # Інша помилка
                try:
                    process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
                    process_control.update_progress(
                        process_control.progress_current,
                        process_control.progress_total,
                        f"Критична помилка: {str(e)[:200]}"
                    )
                    process_control.status = "error"
                    process_control.save()
                except SystemProcessControl.DoesNotExist:
                    pass
        
        # Запускаємо в окремому потоці
        thread = threading.Thread(target=run_incremental_extraction)
        thread.daemon = True
        thread.start()
        
        return JsonResponse({
            "success": True,
            "message": "Інкрементальне витягування розпочато",
            "parameters": {
                "missing_only": missing_only,
                "auto_incremental": auto_incremental
            }
        })
        
    except Exception as e:
        return JsonResponse({
            "error": f"Помилка запуску інкрементального витягування: {str(e)}"
        }, status=500)


def start_small_batch_extraction(request):
    """API для запуску тестового витягування невеликої кількості рішень"""
    if request.method != "POST":
        return JsonResponse({"error": "Тільки POST запити"}, status=405)
    
    try:
        import json
        import subprocess
        import threading
        
        # Перевіряємо чи немає примусового процесу
        if SystemProcessControl.is_any_process_forced():
            forced_process = SystemProcessControl.get_forced_process()
            return JsonResponse({
                "error": f"Заблоковано примусовим процесом: {forced_process.get_process_type_display()}"
            }, status=400)
        
        # Отримуємо параметри запиту
        try:
            body = json.loads(request.body) if request.body else {}
        except json.JSONDecodeError:
            body = {}
        
        limit = body.get("limit", 10)
        missing_only = body.get("missing_only", True)
        
        # Підготовляємо команду
        command = ["python", "manage.py", "extract_resolution_texts", "--limit", str(limit)]
        if missing_only:
            command.append("--missing-only")
        
        # Оновлюємо статус процесу
        try:
            process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
            process_control.update_progress(0, limit, f"Запуск тестового витягування {limit} рішень...")
            process_control.status = "running"
            process_control.save()
        except SystemProcessControl.DoesNotExist:
            pass
        
        def run_small_batch_extraction():
            """Запуск тестового витягування в окремому потоці"""
            try:
                result = subprocess.run(
                    command, 
                    cwd="/home/ruslan/PYTHON/analiz_klientiv",
                    capture_output=True,
                    text=True,
                    timeout=600  # 10 хвилин таймаут
                )
                
                # Оновлюємо статус після завершення
                try:
                    process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
                    if result.returncode == 0:
                        # Парсимо результат для отримання кількості оброблених
                        processed_count = limit  # За замовчуванням
                        if "Оброблено:" in result.stdout:
                            import re
                            match = re.search(r"Оброблено:\s*(\d+)", result.stdout)
                            if match:
                                processed_count = int(match.group(1))
                        
                        process_control.update_progress(
                            processed_count,
                            limit,
                            f"Тестове витягування завершено успішно: {processed_count} рішень"
                        )
                        process_control.status = "idle"
                    else:
                        process_control.update_progress(
                            process_control.progress_current,
                            limit,
                            f"Помилка тестового витягування: {result.stderr[:200]}"
                        )
                        process_control.status = "error"
                    process_control.save()
                except SystemProcessControl.DoesNotExist:
                    pass
                    
            except subprocess.TimeoutExpired:
                # Таймаут
                try:
                    process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
                    process_control.update_progress(
                        process_control.progress_current,
                        limit,
                        "Тестове витягування перервано через таймаут"
                    )
                    process_control.status = "error"
                    process_control.save()
                except SystemProcessControl.DoesNotExist:
                    pass
            except Exception as e:
                # Інша помилка
                try:
                    process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
                    process_control.update_progress(
                        process_control.progress_current,
                        limit,
                        f"Критична помилка: {str(e)[:200]}"
                    )
                    process_control.status = "error"
                    process_control.save()
                except SystemProcessControl.DoesNotExist:
                    pass
        
        # Запускаємо в окремому потоці
        thread = threading.Thread(target=run_small_batch_extraction)
        thread.daemon = True
        thread.start()
        
        return JsonResponse({
            "success": True,
            "message": f"Тестове витягування {limit} рішень розпочато",
            "parameters": {
                "limit": limit,
                "missing_only": missing_only
            }
        })
        
    except Exception as e:
        return JsonResponse({
            "error": f"Помилка запуску тестового витягування: {str(e)}"
        }, status=500)


def start_ultra_fast_extraction(request):
    """API для запуску ультра-швидкого режиму витягування"""
    if request.method != "POST":
        return JsonResponse({"error": "Тільки POST запити"}, status=405)
    
    try:
        import json
        import subprocess
        import threading
        import signal
        import os
        
        # 🚀 КРИТИЧНО: Примусово зупиняємо всі фонові процеси витягування перед ультра-режимом
        try:
            # Знаходимо всі процеси Django команд витягування
            ps_result = subprocess.run([
                "ps", "aux", "--no-headers"
            ], capture_output=True, text=True)
            
            if ps_result.returncode == 0:
                processes_to_kill = []
                for line in ps_result.stdout.strip().split("\n"):
                    if ("extract_resolutions_fast" in line or 
                        "extract_resolution_texts" in line) and "python manage.py" in line:
                        parts = line.split()
                        if len(parts) >= 2:
                            try:
                                pid = int(parts[1])
                                processes_to_kill.append(pid)
                            except ValueError:
                                continue
                
                # Зупиняємо знайдені процеси
                for pid in processes_to_kill:
                    try:
                        os.kill(pid, signal.SIGTERM)
                        print(f"🔴 Зупинено фоновий процес витягування PID {pid}")
                    except ProcessLookupError:
                        # Процес вже завершений
                        pass
                    except Exception as e:
                        print(f"⚠️ Помилка зупинення процесу {pid}: {e}")
                        
                # Оновлюємо статуси в базі даних
                SystemProcessControl.objects.filter(
                    process_type="resolution_extraction",
                    status="running"
                ).update(
                    status="stopped",
                    last_message="🔴 Зупинено для ультра-швидкого режиму"
                )
                        
        except Exception as e:
            print(f"⚠️ Помилка при зупиненні фонових процесів: {e}")
        
        # Перевіряємо чи немає примусового процесу
        if SystemProcessControl.is_any_process_forced():
            forced_process = SystemProcessControl.get_forced_process()
            return JsonResponse({
                "error": f"Заблоковано примусовим процесом: {forced_process.get_process_type_display()}"
            }, status=400)
        
        # Отримуємо параметри запиту
        try:
            body = json.loads(request.body) if request.body else {}
        except json.JSONDecodeError:
            body = {}
        
        ultra_mode = body.get("ultra_mode", True)
        limit = body.get("limit", 0)  # 0 = без ліміту, обробляти всі
        missing_only = body.get("missing_only", True)
        
        # Будуємо команду для ультра-швидкого режиму
        command = [
            "python", "manage.py", "extract_resolution_texts_ultra_fast"
        ]
        
        # Додаємо ліміт тільки якщо він не 0
        if limit > 0:
            command.extend(["--limit", str(limit)])
        else:
            # Без ліміту - обробляємо ВСІ рішення
            pass
        
        if ultra_mode:
            command.append("--ultra-mode")
        if missing_only:
            command.append("--missing-only")
        
        # Оновлюємо статус процесу
        try:
            process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
            if limit > 0:
                progress_message = f"🚀 УЛЬТРА-ШВИДКИЙ РЕЖИМ: {limit} рішень..."
                total_count = limit
            else:
                # Підраховуємо кількість рішень без резолютивних частин (як в команді)
                from django.db import models
                missing_count = TrackedCourtDecision.objects.filter(
                    models.Q(resolution_text__isnull=True) | models.Q(resolution_text__exact="")
                ).filter(
                    doc_url__isnull=False
                ).exclude(
                    doc_url__exact=""
                ).exclude(
                    doc_url__exact="nan"
                ).count()
                progress_message = f"🚀 УЛЬТРА-ШВИДКИЙ РЕЖИМ: ВСІ {missing_count:,} рішень..."
                total_count = missing_count
            
            process_control.update_progress(0, total_count, progress_message)
            process_control.status = "running"
            process_control.save()
        except SystemProcessControl.DoesNotExist:
            pass
        
        def run_ultra_fast_extraction():
            """Запуск ультра-швидкого витягування в окремому потоці"""
            try:
                result = subprocess.run(
                    command, 
                    cwd="/home/ruslan/PYTHON/analiz_klientiv",
                    capture_output=True,
                    text=True,
                    timeout=1800  # 30 хвилин таймаут для великих обсягів
                )
                
                # Оновлюємо статус після завершення
                try:
                    process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
                    if result.returncode == 0:
                        # Парсимо результат для отримання успішно оброблених
                        processed_count = 0
                        successful_count = 0
                        
                        if "Оброблено рішень:" in result.stdout:
                            import re
                            processed_match = re.search(r"Оброблено рішень:\s*(\d+)", result.stdout)
                            successful_match = re.search(r"Успішно витягнуто:\s*(\d+)", result.stdout)
                            
                            if processed_match:
                                processed_count = int(processed_match.group(1))
                            if successful_match:
                                successful_count = int(successful_match.group(1))
                        
                        process_control.update_progress(
                            processed_count,
                            limit,
                            f"🚀 УЛЬТРА-РЕЖИМ завершено: {successful_count}/{processed_count} успішно"
                        )
                        process_control.status = "idle"
                    else:
                        process_control.update_progress(
                            process_control.progress_current,
                            limit,
                            f"❌ УЛЬТРА-РЕЖИМ: помилка - {result.stderr[:150]}"
                        )
                        process_control.status = "error"
                    process_control.save()
                except SystemProcessControl.DoesNotExist:
                    pass
                    
            except subprocess.TimeoutExpired:
                # Таймаут
                try:
                    process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
                    process_control.update_progress(
                        process_control.progress_current,
                        limit,
                        "⏰ УЛЬТРА-РЕЖИМ: перервано через таймаут (30 хв)"
                    )
                    process_control.status = "error"
                    process_control.save()
                except SystemProcessControl.DoesNotExist:
                    pass
            except Exception as e:
                # Інша помилка
                try:
                    process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
                    process_control.update_progress(
                        process_control.progress_current,
                        limit,
                        f"💥 УЛЬТРА-РЕЖИМ: критична помилка - {str(e)[:150]}"
                    )
                    process_control.status = "error"
                    process_control.save()
                except SystemProcessControl.DoesNotExist:
                    pass
        
        # Запускаємо в окремому потоці
        thread = threading.Thread(target=run_ultra_fast_extraction)
        thread.daemon = True
        thread.start()
        
        return JsonResponse({
            "success": True,
            "message": f"🚀 УЛЬТРА-ШВИДКИЙ РЕЖИМ запущено для {limit} рішень",
            "parameters": {
                "ultra_mode": ultra_mode,
                "limit": limit,
                "missing_only": missing_only,
                "estimated_time": f"{limit // 50} хвилин (швидкість ~50 рішень/хв)"
            }
        })
        
    except Exception as e:
        return JsonResponse({
            "error": f"Помилка запуску ультра-швидкого режиму: {str(e)}"
        }, status=500)


def extraction_stats_api(request):
    """API для отримання детальної статистики витягування резолютивних частин"""
    try:
        from datetime import timedelta
        from django.db.models import Avg, F, Count
        try:
            from django.db.models.functions import TruncHour
        except ImportError:
            TruncHour = None
        
        # Основна статистика
        total_decisions = TrackedCourtDecision.objects.count()
        decisions_with_resolutions = TrackedCourtDecision.objects.filter(
            resolution_text__isnull=False
        ).exclude(resolution_text="").count()
        decisions_without_resolutions = total_decisions - decisions_with_resolutions
        
        decisions_with_triggers = TrackedCourtDecision.objects.filter(
            has_trigger_words=True
        ).count()
        
        decisions_with_rtf = TrackedCourtDecision.objects.filter(
            doc_url__isnull=False
        ).exclude(doc_url="").exclude(doc_url="nan").count()
        
        # Часові проміжки
        now = timezone.now()
        today = now.date()
        yesterday = now - timedelta(days=1)
        week_ago = now - timedelta(days=7)
        month_ago = now - timedelta(days=30)
        
        # Статистика за періоди
        today_extractions = TrackedCourtDecision.objects.filter(
            resolution_extracted_at__date=today
        ).count()
        
        yesterday_extractions = TrackedCourtDecision.objects.filter(
            resolution_extracted_at__gte=yesterday
        ).count()
        
        week_extractions = TrackedCourtDecision.objects.filter(
            resolution_extracted_at__gte=week_ago
        ).count()
        
        month_extractions = TrackedCourtDecision.objects.filter(
            resolution_extracted_at__gte=month_ago
        ).count()
        
        # Швидкість витягування за останні 24 години
        last_24h_extractions = TrackedCourtDecision.objects.filter(
            resolution_extracted_at__gte=yesterday
        ).count()
        extraction_speed_24h = last_24h_extractions  # За 24 години
        
        # Активність по годинах за останні 24 години
        try:
            if TruncHour:
                hourly_activity = TrackedCourtDecision.objects.filter(
                    resolution_extracted_at__gte=yesterday
                ).annotate(
                    hour=TruncHour("resolution_extracted_at")
                ).values("hour").annotate(
                    count=Count("id")
                ).order_by("hour")
                
                hourly_activity_list = list(hourly_activity)
                
                # Знаходимо пікову годину
                peak_hour_data = max(hourly_activity_list, key=lambda x: x["count"]) if hourly_activity_list else None
                peak_extraction_hour = peak_hour_data["hour"].hour if peak_hour_data else 14
            else:
                # Fallback без TruncHour
                peak_extraction_hour = 14
                hourly_activity_list = []
        except Exception as e:
            peak_extraction_hour = 14
            hourly_activity_list = []
        
        # Якість витягування
        successful_extractions = decisions_with_resolutions
        failed_extractions = TrackedCourtDecision.objects.filter(
            resolution_text__isnull=True,
            doc_url__isnull=False
        ).exclude(doc_url="").exclude(doc_url="nan").count()
        
        # Середня довжина резолютивних частин
        try:
            avg_resolution_length = TrackedCourtDecision.objects.filter(
                resolution_text__isnull=False
            ).exclude(resolution_text="").aggregate(
                avg_length=Avg(F("resolution_text__length"))
            ).get("avg_length", 0) or 0
        except Exception as e:
            avg_resolution_length = 0
        
        # Відсотки
        extraction_percentage = (decisions_with_resolutions / total_decisions * 100) if total_decisions > 0 else 0
        trigger_percentage = (decisions_with_triggers / decisions_with_resolutions * 100) if decisions_with_resolutions > 0 else 0
        rtf_availability_percentage = (decisions_with_rtf / total_decisions * 100) if total_decisions > 0 else 0
        
        # Формування активності по годинах для графіку
        activity_chart_data = []
        try:
            for i in range(24):
                hour_start = yesterday + timedelta(hours=i)
                hour_end = hour_start + timedelta(hours=1)
                hour_count = TrackedCourtDecision.objects.filter(
                    resolution_extracted_at__gte=hour_start,
                    resolution_extracted_at__lt=hour_end
                ).count()
                activity_chart_data.append({
                    "hour": hour_start.hour,
                    "count": hour_count
                })
        except Exception as e:
            # В разі помилки створюємо порожні дані
            activity_chart_data = [{"hour": i, "count": 0} for i in range(24)]
        
        # Тенденції (порівняння з попередніми періодами)
        prev_week_extractions = TrackedCourtDecision.objects.filter(
            resolution_extracted_at__gte=now - timedelta(days=14),
            resolution_extracted_at__lt=week_ago
        ).count()
        
        week_trend = ((week_extractions - prev_week_extractions) / prev_week_extractions * 100) if prev_week_extractions > 0 else 0
        
        stats = {
            # Основні показники
            "total_decisions": total_decisions,
            "decisions_with_resolutions": decisions_with_resolutions,
            "decisions_without_resolutions": decisions_without_resolutions,
            "decisions_with_triggers": decisions_with_triggers,
            "decisions_with_rtf": decisions_with_rtf,
            
            # Часові показники
            "today_extractions": today_extractions,
            "yesterday_extractions": yesterday_extractions,
            "week_extractions": week_extractions,
            "month_extractions": month_extractions,
            "last_24h_extractions": last_24h_extractions,
            
            # Швидкість та активність
            "extraction_speed_24h": extraction_speed_24h,
            "peak_extraction_hour": peak_extraction_hour,
            "activity_chart_data": activity_chart_data,
            
            # Якість
            "successful_extractions": successful_extractions,
            "failed_extractions": failed_extractions,
            "avg_resolution_length": round(avg_resolution_length, 0),
            
            # Відсотки
            "extraction_percentage": round(extraction_percentage, 1),
            "trigger_percentage": round(trigger_percentage, 1),
            "rtf_availability_percentage": round(rtf_availability_percentage, 1),
            
            # Тенденції
            "week_trend": round(week_trend, 1),
            
            # Час оновлення
            "updated_at": now.isoformat()
        }
        
        return JsonResponse({
            "success": True,
            "stats": stats
        })
        
    except Exception as e:
        return JsonResponse({
            "success": False,
            "error": f"Помилка отримання статистики витягування: {str(e)}"
        }, status=500)


def creditors_list(request):
    """Сторінка списку кредиторів"""

    # Статистика
    total_creditors = Creditor.objects.count()
    total_claims = CreditorClaim.objects.count()
    total_amount = CreditorClaim.objects.aggregate(Sum("total_amount"))["total_amount__sum"] or 0
    processed_cases = CreditorClaim.objects.values("case").distinct().count()

    # AJAX запит на оновлення статистики
    if request.GET.get('ajax') == 'stats':
        # Розраховуємо статистику обробки для AJAX
        processing_stats = LLMAnalysisLog.objects.aggregate(
            total_processed=Count("id"),
            successful=Count("id", filter=Q(status="completed")),
            failed=Count("id", filter=Q(status="failed"))
        )
        success_percentage = 0
        if processing_stats["total_processed"] > 0:
            success_percentage = (processing_stats["successful"] * 100.0) / processing_stats["total_processed"]

        return JsonResponse({
            'success': True,
            'stats': {
                'total_creditors': total_creditors,
                'total_claims': total_claims,
                'total_amount': float(total_amount),
                'processed_cases': processed_cases,
                'success_percentage': success_percentage
            }
        })

    # Статистика за чергами
    queue_stats = CreditorClaim.objects.aggregate(
        queue_1=Sum("amount_1st_queue"),
        queue_2=Sum("amount_2nd_queue"),
        queue_3=Sum("amount_3rd_queue"),
        queue_4=Sum("amount_4th_queue"),
        queue_5=Sum("amount_5th_queue"),
        queue_6=Sum("amount_6th_queue")
    )

    # Статистика обробки
    processing_stats = LLMAnalysisLog.objects.aggregate(
        total_processed=Count("id"),
        successful=Count("id", filter=Q(status="completed")),
        failed=Count("id", filter=Q(status="failed"))
    )

    # Розрахунок відсотка успішності
    success_percentage = 0
    if processing_stats["total_processed"] > 0:
        success_percentage = (processing_stats["successful"] * 100.0) / processing_stats["total_processed"]

    # Всі кредитори в одній таблиці
    search_query = request.GET.get("search", "")
    creditors_query = Creditor.objects.all()

    if search_query:
        creditors_query = creditors_query.filter(name__icontains=search_query)

    creditors = creditors_query.annotate(
        claims_count=Count("creditor_claims"),
        total_sum=Sum("creditor_claims__total_amount"),
        total_1st_queue=Sum("creditor_claims__amount_1st_queue"),
        total_2nd_queue=Sum("creditor_claims__amount_2nd_queue"),
        total_3rd_queue=Sum("creditor_claims__amount_3rd_queue"),
        total_4th_queue=Sum("creditor_claims__amount_4th_queue"),
        total_5th_queue=Sum("creditor_claims__amount_5th_queue"),
        total_6th_queue=Sum("creditor_claims__amount_6th_queue")
    ).order_by("-total_sum").prefetch_related("creditor_claims__case")

    # ТОП-10 кредиторів
    top_creditors = Creditor.objects.annotate(
        total_sum=Sum("creditor_claims__total_amount"),
        claims_count=Count("creditor_claims")
    ).order_by("-total_sum")[:10]

    context = {
        "creditors": creditors,
        "top_creditors": top_creditors,
        "search_query": search_query,
        "stats": {
            "total_creditors": total_creditors,
            "total_claims": total_claims,
            "total_amount": total_amount,
            "processed_cases": processed_cases,
            "queue_stats": queue_stats,
            "processing_stats": processing_stats,
            "success_percentage": success_percentage
        }
    }

    return render(request, "bankruptcy/creditors.html", context)


@csrf_exempt
def start_creditor_extraction(request):
    """AJAX endpoint для запуску витягування кредиторів"""
    if request.method != "POST":
        return JsonResponse({"success": False, "error": "Тільки POST запити"})

    try:
        import subprocess
        import os
        from django.conf import settings

        # Запускаємо команду витягування кредиторів в циклічному режимі
        cmd = [
            os.path.join(settings.BASE_DIR, "venv", "bin", "python"),
            "manage.py",
            "analyze_resolutions_mistral",
            "--continuous",  # Циклічна обробка
            "--limit", "5"  # Обробляти по 5 справ за цикл (швидша модель)
        ]

        subprocess.Popen(
            cmd,
            cwd=settings.BASE_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        return JsonResponse({
            "success": True,
            "message": "Запущено циклічне витягування кредиторів (по 5 справ за цикл, модель ministral-8b)"
        })

    except Exception as e:
        return JsonResponse({
            "success": False,
            "error": f"Помилка запуску: {str(e)}"
        })


@csrf_exempt
def start_deduplication_process(request):
    """AJAX endpoint для запуску процесу дедуплікації"""
    if request.method != "POST":
        return JsonResponse({"success": False, "error": "Тільки POST запити"})

    try:
        import subprocess
        import os
        from django.conf import settings

        # Запускаємо команду дедуплікації в циклічному режимі
        cmd = [
            os.path.join(settings.BASE_DIR, "venv", "bin", "python"),
            "manage.py",
            "analyze_resolutions_dedup",
            "--continuous",  # Циклічна обробка
            "--limit", "3"  # Обробляти по 3 справи за цикл (менше для дедуплікації)
        ]

        subprocess.Popen(
            cmd,
            cwd=settings.BASE_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        return JsonResponse({
            "success": True,
            "message": "Запущено процес дедуплікації (по 3 справи за цикл, другий API)"
        })

    except Exception as e:
        return JsonResponse({
            "success": False,
            "error": f"Помилка запуску дедуплікації: {str(e)}"
        })


@csrf_exempt
def stop_deduplication_process(request):
    """AJAX endpoint для зупинки процесу дедуплікації"""
    if request.method != "POST":
        return JsonResponse({"success": False, "error": "Тільки POST запити"})

    try:
        import subprocess
        import signal
        import os

        # Знаходимо та зупиняємо процеси analyze_resolutions_dedup
        try:
            # Шукаємо процеси Python з командою analyze_resolutions_dedup
            result = subprocess.run([
                "pgrep", "-f", "analyze_resolutions_dedup"
            ], capture_output=True, text=True)

            if result.stdout.strip():
                pids = result.stdout.strip().split("\n")
                killed_count = 0
                for pid in pids:
                    if pid.strip():
                        try:
                            os.kill(int(pid.strip()), signal.SIGTERM)
                            killed_count += 1
                        except (OSError, ValueError):
                            continue

                if killed_count > 0:
                    # Оновлюємо статистику
                    from bankruptcy.models import DeduplicationProcessStats
                    stats = DeduplicationProcessStats.get_current_stats()
                    stats.stop_processing()

                    return JsonResponse({
                        "success": True,
                        "message": f"Зупинено {killed_count} процесів дедуплікації"
                    })
                else:
                    return JsonResponse({
                        "success": False,
                        "error": "Не вдалося зупинити процеси"
                    })
            else:
                return JsonResponse({
                    "success": True,
                    "message": "Процеси дедуплікації не запущені"
                })

        except subprocess.CalledProcessError:
            return JsonResponse({
                "success": True,
                "message": "Процеси дедуплікації не знайдені"
            })

    except Exception as e:
        return JsonResponse({
            "success": False,
            "error": f"Помилка зупинки дедуплікації: {str(e)}"
        })


def deduplication_stats_api(request):
    """API для отримання статистики процесу дедуплікації"""
    try:
        from bankruptcy.models import DeduplicationProcessStats

        stats = DeduplicationProcessStats.get_current_stats()

        return JsonResponse({
            "success": True,
            "stats": {
                "total_cases_processed": stats.total_cases_processed,
                "total_creditors_added": stats.total_creditors_added,
                "total_duplicates_removed": stats.total_duplicates_removed,
                "total_claims_updated": stats.total_claims_updated,
                "initial_documents_processed": stats.initial_documents_processed,
                "full_documents_processed": stats.full_documents_processed,
                "summary_documents_processed": stats.summary_documents_processed,
                "api_errors": stats.api_errors,
                "parsing_errors": stats.parsing_errors,
                "database_errors": stats.database_errors,
                "avg_processing_time": stats.avg_processing_time,
                "is_running": stats.is_running,
                "last_run_at": stats.last_run_at.isoformat() if stats.last_run_at else None,
                "last_error": stats.last_error,
                "deduplication_rate": stats.deduplication_rate,
                "updated_at": stats.updated_at.isoformat() if stats.updated_at else None,
            }
        })

    except Exception as e:
        return JsonResponse({
            "success": False,
            "error": f"Помилка отримання статистики дедуплікації: {str(e)}"
        }, status=500)


@csrf_exempt
def stop_creditor_extraction(request):
    """AJAX endpoint для зупинки витягування кредиторів"""
    if request.method != "POST":
        return JsonResponse({"success": False, "error": "Тільки POST запити"})

    try:
        import subprocess
        import signal

        # Знаходимо та вбиваємо процеси analyze_resolutions_mistral
        try:
            # Шукаємо процеси Python з командою analyze_resolutions_mistral
            result = subprocess.run([
                "pkill", "-f", "analyze_resolutions_mistral"
            ], capture_output=True, text=True)

            if result.returncode == 0:
                return JsonResponse({
                    "success": True,
                    "message": "Витягування кредиторів зупинено"
                })
            else:
                return JsonResponse({
                    "success": True,
                    "message": "Процеси витягування не знайдено (можливо вже завершені)"
                })
        except Exception as e:
            return JsonResponse({
                "success": False,
                "error": f"Помилка зупинки процесів: {str(e)}"
            })

    except Exception as e:
        return JsonResponse({
            "success": False,
            "error": f"Помилка зупинки: {str(e)}"
        })


def deduplication_detail(request):
    """Детальна сторінка статистики дедуплікації з логами операцій"""
    from bankruptcy.models import DeduplicationLog, DeduplicationProcessStats
    from django.core.paginator import Paginator
    from django.db.models import Q

    # Фільтри
    operation_type = request.GET.get('operation_type', '')
    document_type = request.GET.get('document_type', '')
    search = request.GET.get('search', '')

    # Базовий запит
    logs_query = DeduplicationLog.objects.select_related('case').all()

    # Застосовуємо фільтри
    if operation_type:
        logs_query = logs_query.filter(operation_type=operation_type)
    if document_type:
        logs_query = logs_query.filter(document_type=document_type)
    if search:
        logs_query = logs_query.filter(
            Q(creditor_name__icontains=search) |
            Q(case__case_number__icontains=search) |
            Q(case__company__name__icontains=search)
        )

    # Пагінація
    paginator = Paginator(logs_query, 50)  # 50 записів на сторінку
    page_number = request.GET.get('page')
    logs = paginator.get_page(page_number)

    # Загальна статистика
    stats = DeduplicationProcessStats.get_current_stats()

    # Статистика по операціях
    operation_stats = {}
    for op_type, op_label in DeduplicationLog.OPERATION_TYPES:
        count = DeduplicationLog.objects.filter(operation_type=op_type).count()
        operation_stats[op_type] = {'label': op_label, 'count': count}

    # Статистика по типах документів
    document_stats = {}
    for doc_type, doc_label in DeduplicationLog.DOCUMENT_TYPES:
        count = DeduplicationLog.objects.filter(document_type=doc_type).count()
        document_stats[doc_type] = {'label': doc_label, 'count': count}

    context = {
        'logs': logs,
        'stats': stats,
        'operation_stats': operation_stats,
        'document_stats': document_stats,
        'operation_types': DeduplicationLog.OPERATION_TYPES,
        'document_types': DeduplicationLog.DOCUMENT_TYPES,
        'current_filters': {
            'operation_type': operation_type,
            'document_type': document_type,
            'search': search,
        }
    }

    return render(request, 'bankruptcy/deduplication_detail.html', context)