#!/usr/bin/env python3
"""
Скрипт для управления базой данных инцидентов
Позволяет просматривать, очищать и управлять данными
"""

import asyncio
import argparse
from datetime import datetime
from database import Database, IncidentStatus


async def show_incidents(db: Database, status_filter: str = None):
    """Показывает список инцидентов"""
    if status_filter:
        try:
            status = IncidentStatus(status_filter)
            incidents = await db.get_incidents_by_status(status)
        except ValueError:
            print(f"❌ Неверный статус: {status_filter}")
            print(f"Доступные статусы: {[s.value for s in IncidentStatus]}")
            return
    else:
        incidents = await db.get_all_incidents()

    if not incidents:
        print("📋 Инциденты не найдены")
        return

    print(f"📋 Найдено инцидентов: {len(incidents)}")
    print("-" * 80)

    for incident in incidents:
        print(f"🎫 Тикет: {incident.ticket_key}")
        print(f"📅 Создан: {incident.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Статус: {incident.status.value}")
        print(f"👤 Автор: {incident.author_id}")
        if incident.assigned_to:
            print(f"👨‍💼 Ответственный: {incident.assigned_to}")
        print(f"💬 Канал: {incident.channel_id}")
        print(f"🧵 Тред: {incident.thread_ts}")
        if incident.last_notification:
            print(
                f"🔔 Последнее уведомление: {incident.last_notification.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        print("-" * 80)


async def show_stats(db: Database):
    """Показывает статистику по инцидентам"""
    all_incidents = await db.get_all_incidents()
    active_incidents = await db.get_active_incidents()

    print("📊 СТАТИСТИКА ИНЦИДЕНТОВ")
    print("=" * 40)
    print(f"📋 Всего инцидентов: {len(all_incidents)}")
    print(f"🟢 Активных инцидентов: {len(active_incidents)}")
    print()

    # Статистика по статусам
    status_counts = {}
    for incident in all_incidents:
        status = incident.status.value
        status_counts[status] = status_counts.get(status, 0) + 1

    print("📊 По статусам:")
    for status, count in status_counts.items():
        print(f"  {status}: {count}")
    print()

    # Статистика по дням
    if all_incidents:
        today = datetime.now().date()
        today_count = sum(1 for i in all_incidents if i.created_at.date() == today)
        print(f"📅 Создано сегодня: {today_count}")


async def cleanup_old_incidents(db: Database, days: int):
    """Удаляет старые закрытые инциденты"""
    all_incidents = await db.get_all_incidents()
    cutoff_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff_date = cutoff_date.replace(day=cutoff_date.day - days)

    old_closed = [
        incident
        for incident in all_incidents
        if incident.status in [IncidentStatus.CLOSED, IncidentStatus.FROZEN]
        and incident.created_at < cutoff_date
    ]

    if not old_closed:
        print("🧹 Старые закрытые инциденты не найдены")
        return

    print(f"🧹 Найдено старых закрытых инцидентов: {len(old_closed)}")
    print("Удаление:")

    for incident in old_closed:
        print(f"  🗑️ {incident.ticket_key} ({incident.created_at.strftime('%Y-%m-%d')})")
        await db.delete_incident(incident.ticket_key)

    print(f"✅ Удалено {len(old_closed)} инцидентов")


async def main():
    parser = argparse.ArgumentParser(description="Управление базой данных инцидентов")
    parser.add_argument("--db", default="incidents.db", help="Путь к файлу базы данных")

    subparsers = parser.add_subparsers(dest="command", help="Доступные команды")

    # Команда show
    show_parser = subparsers.add_parser("show", help="Показать инциденты")
    show_parser.add_argument("--status", help="Фильтр по статусу")

    # Команда stats
    subparsers.add_parser("stats", help="Показать статистику")

    # Команда cleanup
    cleanup_parser = subparsers.add_parser("cleanup", help="Очистить старые инциденты")
    cleanup_parser.add_argument(
        "--days", type=int, default=30, help="Удалить инциденты старше N дней"
    )

    # Команда init
    subparsers.add_parser("init", help="Инициализировать базу данных")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    db = Database(args.db)

    if args.command == "init":
        await db.init_db()
        print("✅ База данных инициализирована")

    elif args.command == "show":
        await show_incidents(db, args.status)

    elif args.command == "stats":
        await show_stats(db)

    elif args.command == "cleanup":
        await cleanup_old_incidents(db, args.days)


if __name__ == "__main__":
    asyncio.run(main())
