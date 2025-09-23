#!/usr/bin/env python3
"""
Скрипт для получения ID пользователей Slack
Используйте этот скрипт, чтобы узнать ID пользователей для настройки ALLOWED_BUTTON_USERS
"""

import os
from slack_sdk import WebClient
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()


def get_user_ids():
    """Получает список всех пользователей и их ID"""
    bot_token = os.getenv("SLACK_BOT_TOKEN")

    if not bot_token:
        print("❌ Ошибка: SLACK_BOT_TOKEN не найден в переменных окружения")
        print("Убедитесь, что файл .env существует и содержит SLACK_BOT_TOKEN")
        return

    try:
        client = WebClient(token=bot_token)

        print("👥 ПОЛЬЗОВАТЕЛИ SLACK:")
        print("-" * 60)

        response = client.users_list(limit=1000)

        for user in response["members"]:
            # Пропускаем ботов и удаленных пользователей
            if user.get("is_bot") or user.get("deleted"):
                continue

            user_id = user["id"]
            real_name = user.get("real_name", "Нет имени")
            display_name = user.get("profile", {}).get("display_name", "")
            email = user.get("profile", {}).get("email", "Нет email")

            print(f"👤 {real_name}")
            if display_name and display_name != real_name:
                print(f"   Отображаемое имя: {display_name}")
            print(f"   ID: {user_id}")
            print(f"   Email: {email}")
            print()

        print("💡 Для настройки бота добавьте в .env:")
        print("ALLOWED_BUTTON_USERS=U1234567890,U0987654321")
        print("(замените на нужные ID пользователей через запятую)")
        print()
        print(
            "🔒 Если ALLOWED_BUTTON_USERS пустой, все пользователи могут нажимать кнопки"
        )

    except Exception as e:
        print(f"❌ Ошибка при получении списка пользователей: {e}")
        print("Проверьте правильность SLACK_BOT_TOKEN и права бота")


if __name__ == "__main__":
    get_user_ids()
