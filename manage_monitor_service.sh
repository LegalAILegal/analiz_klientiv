#!/bin/bash

SERVICE_NAME="analiz-court-monitor"
SERVICE_FILE="/home/ruslan/PYTHON/analiz_klientiv/analiz-court-monitor.service"

case "$1" in
    install)
        echo "📦 Встановлення сервісу моніторингу..."
        sudo cp "$SERVICE_FILE" /etc/systemd/system/
        sudo systemctl daemon-reload
        sudo systemctl enable $SERVICE_NAME
        echo "✅ Сервіс встановлено та увімкнено для автозапуску"
        ;;
    start)
        echo "▶️ Запуск сервісу моніторингу..."
        sudo systemctl start $SERVICE_NAME
        echo "✅ Сервіс запущено"
        ;;
    stop)
        echo "⏹️ Зупинка сервісу моніторингу..."
        sudo systemctl stop $SERVICE_NAME
        echo "✅ Сервіс зупинено"
        ;;
    status)
        echo "📊 Статус сервісу моніторингу:"
        sudo systemctl status $SERVICE_NAME
        ;;
    logs)
        echo "📝 Логи сервісу моніторингу:"
        sudo journalctl -u $SERVICE_NAME -f
        ;;
    restart)
        echo "🔄 Перезапуск сервісу моніторингу..."
        sudo systemctl restart $SERVICE_NAME
        echo "✅ Сервіс перезапущено"
        ;;
    uninstall)
        echo "🗑️ Видалення сервісу моніторингу..."
        sudo systemctl stop $SERVICE_NAME
        sudo systemctl disable $SERVICE_NAME
        sudo rm /etc/systemd/system/$SERVICE_NAME.service
        sudo systemctl daemon-reload
        echo "✅ Сервіс видалено"
        ;;
    test)
        echo "🧪 Тестовий запуск моніторингу (один раз)..."
        cd /home/ruslan/PYTHON/analiz_klientiv
        source venv/bin/activate
        python manage.py auto_update_statistics --run-once --verbose
        ;;
    *)
        echo "Використання: $0 {install|start|stop|status|logs|restart|uninstall|test}"
        echo ""
        echo "Команди:"
        echo "  install   - Встановити сервіс та увімкнути автозапуск"
        echo "  start     - Запустити сервіс"
        echo "  stop      - Зупинити сервіс" 
        echo "  status    - Показати статус сервісу"
        echo "  logs      - Показати логи сервісу (в реальному часі)"
        echo "  restart   - Перезапустити сервіс"
        echo "  uninstall - Видалити сервіс повністю"
        echo "  test      - Тестовий запуск (виконати один раз)"
        exit 1
        ;;
esac