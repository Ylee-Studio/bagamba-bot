#!/usr/bin/env python3
"""
CLI для управления Redis уведомлениями
"""

import argparse
import sys
import json
from datetime import datetime
from dotenv import load_dotenv
import redis
from config import Config

# Загружаем переменные окружения
load_dotenv()


def connect_redis():
    """Подключается к Redis"""
    try:
        client = redis.Redis.from_url(Config.REDIS_URL, db=Config.REDIS_DB)
        client.ping()  # Проверяем подключение
        return client
    except Exception as e:
        print(f"❌ Ошибка подключения к Redis: {e}")
        sys.exit(1)


def clear_all_notifications():
    """Очищает все уведомления"""
    client = connect_redis()

    try:
        # Очищаем очередь уведомлений
        queue_deleted = client.delete("notifications:queue")

        # Очищаем ключи уведомлений
        notification_keys = client.keys("notification:*")
        keys_deleted = 0
        if notification_keys:
            keys_deleted = client.delete(*notification_keys)

        print("✅ Очистка завершена:")
        print(f"  - Удалено очередей: {queue_deleted}")
        print(f"  - Удалено ключей уведомлений: {keys_deleted}")

    except Exception as e:
        print(f"❌ Ошибка при очистке: {e}")


def clear_notifications_for_ticket(ticket_key):
    """Очищает уведомления для конкретного тикета"""
    client = connect_redis()

    try:
        # Ищем ключи для данного тикета
        notification_pattern = f"notification:{ticket_key}:*"
        notification_keys = client.keys(notification_pattern)

        keys_deleted = 0
        if notification_keys:
            keys_deleted = client.delete(*notification_keys)

        print(f"✅ Очистка уведомлений для {ticket_key}:")
        print(f"  - Удалено ключей: {keys_deleted}")

    except Exception as e:
        print(f"❌ Ошибка при очистке уведомлений для {ticket_key}: {e}")


def show_stats():
    """Показывает статистику Redis"""
    client = connect_redis()

    try:
        # Статистика уведомлений
        notification_count = len(client.keys("notification:*"))
        queue_length = client.llen("notifications:queue")

        print("📊 Статистика Redis уведомлений:")
        print(f"  - Всего ключей уведомлений: {notification_count}")
        print(f"  - Длина очереди: {queue_length}")
        print("  - Redis подключен: ✅")

        # Показываем активные уведомления
        if queue_length > 0:
            print("\n🔔 Уведомления в очереди:")
            notifications = client.lrange("notifications:queue", 0, -1)
            for i, notification in enumerate(notifications):
                try:
                    data = json.loads(notification)
                    scheduled_time = datetime.fromisoformat(data["scheduled_time"])
                    print(
                        f"  {i + 1}. {data['ticket_key']} - {scheduled_time.strftime('%H:%M:%S')}"
                    )
                except Exception as e:
                    print(f"  {i + 1}. Ошибка парсинга: {e}")

        # Показываем ключи уведомлений
        if notification_count > 0:
            print("\n📋 Ключи уведомлений:")
            notification_keys = client.keys("notification:*")
            for key in notification_keys[:10]:  # Показываем первые 10
                try:
                    data = client.get(key)
                    if data:
                        notification_data = json.loads(data)
                        scheduled_time = datetime.fromisoformat(
                            notification_data["scheduled_time"]
                        )
                        print(
                            f"  - {key.decode('utf-8')} - {scheduled_time.strftime('%H:%M:%S')}"
                        )
                except Exception as e:
                    print(f"  - {key.decode('utf-8')} - Ошибка: {e}")

            if len(notification_keys) > 10:
                print(f"  ... и еще {len(notification_keys) - 10} ключей")

    except Exception as e:
        print(f"❌ Ошибка при получении статистики: {e}")


def show_notification_details(ticket_key):
    """Показывает детали уведомления для тикета"""
    client = connect_redis()

    try:
        # Ищем ключи для данного тикета
        notification_pattern = f"notification:{ticket_key}:*"
        notification_keys = client.keys(notification_pattern)

        if not notification_keys:
            print(f"ℹ️ Уведомления для {ticket_key} не найдены")
            return

        print(f"📋 Детали уведомлений для {ticket_key}:")

        for key in notification_keys:
            try:
                data = client.get(key)
                if data:
                    notification_data = json.loads(data)
                    scheduled_time = datetime.fromisoformat(
                        notification_data["scheduled_time"]
                    )
                    created_at = datetime.fromisoformat(notification_data["created_at"])

                    print(f"\n  Ключ: {key.decode('utf-8')}")
                    print(f"  Тип: {notification_data['notification_type']}")
                    print(
                        f"  Запланировано: {scheduled_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    print(f"  Создано: {created_at.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"  Интервал: {notification_data['interval_minutes']} минут")

            except Exception as e:
                print(f"  Ошибка при чтении {key.decode('utf-8')}: {e}")

    except Exception as e:
        print(f"❌ Ошибка при получении деталей для {ticket_key}: {e}")


def test_redis_connection():
    """Тестирует подключение к Redis"""
    try:
        client = connect_redis()
        info = client.info()
        print("✅ Подключение к Redis успешно")
        print(f"  - Версия Redis: {info.get('redis_version', 'неизвестно')}")
        print(f"  - Используемая память: {info.get('used_memory_human', 'неизвестно')}")
        print(
            f"  - Подключенные клиенты: {info.get('connected_clients', 'неизвестно')}"
        )

    except Exception as e:
        print(f"❌ Ошибка подключения к Redis: {e}")


def main():
    """Главная функция CLI"""
    parser = argparse.ArgumentParser(
        description="CLI для управления Redis уведомлениями",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python redis_cli.py stats                    # Показать статистику
  python redis_cli.py clear                    # Очистить все уведомления
  python redis_cli.py clear BACK-123           # Очистить уведомления для BACK-123
  python redis_cli.py details BACK-123         # Показать детали уведомлений для BACK-123
  python redis_cli.py test                     # Тестировать подключение к Redis
        """,
    )

    parser.add_argument(
        "command",
        choices=["stats", "clear", "details", "test"],
        help="Команда для выполнения",
    )

    parser.add_argument(
        "ticket_key", nargs="?", help="Ключ тикета (для команд clear и details)"
    )

    args = parser.parse_args()

    if args.command == "stats":
        show_stats()
    elif args.command == "clear":
        if args.ticket_key:
            clear_notifications_for_ticket(args.ticket_key)
        else:
            clear_all_notifications()
    elif args.command == "details":
        if not args.ticket_key:
            print("❌ Для команды 'details' необходимо указать ticket_key")
            sys.exit(1)
        show_notification_details(args.ticket_key)
    elif args.command == "test":
        test_redis_connection()


if __name__ == "__main__":
    main()
