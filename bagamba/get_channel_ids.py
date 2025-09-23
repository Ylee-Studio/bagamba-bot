#!/usr/bin/env python3
"""
Скрипт для получения ID каналов Slack
Используйте этот скрипт, чтобы узнать ID каналов для настройки ALLOWED_CHANNELS
"""

import os
from slack_sdk import WebClient
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()


def get_channel_ids():
    """Получает список всех каналов и их ID"""
    bot_token = os.getenv("SLACK_BOT_TOKEN")

    if not bot_token:
        print("❌ Ошибка: SLACK_BOT_TOKEN не найден в переменных окружения")
        print("Убедитесь, что файл .env существует и содержит SLACK_BOT_TOKEN")
        return

    try:
        client = WebClient(token=bot_token)

        # Получаем список публичных каналов
        print("📋 Публичные каналы:")
        print("-" * 50)

        response = client.conversations_list(
            types="public_channel", exclude_archived=True, limit=1000
        )

        for channel in response["channels"]:
            print(f"#{channel['name']:<20} ID: {channel['id']}")

        # Получаем список приватных каналов (если бот имеет доступ)
        print("\n🔒 Приватные каналы:")
        print("-" * 50)

        response = client.conversations_list(
            types="private_channel", exclude_archived=True, limit=1000
        )

        for channel in response["channels"]:
            print(f"#{channel['name']:<20} ID: {channel['id']}")

        print("\n💡 Для настройки бота добавьте в .env:")
        print("ALLOWED_CHANNELS=C1234567890,C0987654321")
        print("(замените на нужные ID каналов через запятую)")

    except Exception as e:
        print(f"❌ Ошибка при получении списка каналов: {e}")
        print("Проверьте правильность SLACK_BOT_TOKEN и права бота")


if __name__ == "__main__":
    get_channel_ids()
