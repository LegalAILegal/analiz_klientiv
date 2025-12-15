"""
Команда для зупинки процесу витягування резолютивних частин
"""

from django.core.management.base import BaseCommand
from django.utils import timezone
from bankruptcy.models import SystemProcessControl

class Command(BaseCommand):
    help = "Зупиняє процес витягування резолютивних частин"

    def handle(self, *args, **options):
        try:
            # Знаходимо процес витягування резолютивних частин
            process_control = SystemProcessControl.objects.get(process_type="resolution_extraction")
            
            if process_control.status in ["running", "forced_running"]:
                process_control.status = "stopped"
                process_control.finished_at = timezone.now()
                process_control.last_message = "⏹️ Процес зупинено адміністратором - система оптимізована"
                process_control.save()
                
                self.stdout.write(
                    self.style.SUCCESS(
                        "✅ Процес витягування резолютивних частин ЗУПИНЕНО\n"
                        "   Система переведена в режим економії ресурсів"
                    )
                )
            else:
                self.stdout.write(
                    self.style.WARNING(
                        f"⚠️ Процес вже в статусі: {process_control.get_status_display()}"
                    )
                )
                
        except SystemProcessControl.DoesNotExist:
            self.stdout.write(
                self.style.ERROR("❌ Процес витягування не знайдено")
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ Помилка: {e}")
            )