#!/usr/bin/env python3
"""
Скрипт для получения информации о проекте Jira
Показывает доступные типы задач, статусы, переходы и другую информацию
"""

import os
from jira import JIRA
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()


def get_jira_info():
    """Получает информацию о проекте Jira"""

    # Проверяем переменные окружения
    jira_url = os.getenv("JIRA_URL")
    jira_username = os.getenv("JIRA_USERNAME")
    jira_token = os.getenv("JIRA_API_TOKEN")
    project_key = os.getenv("JIRA_PROJECT_KEY", "INC")
    default_issue_type = os.getenv("JIRA_ISSUE_TYPE", "Task")

    if not all([jira_url, jira_username, jira_token]):
        print("❌ Ошибка: Не все переменные окружения Jira настроены")
        print("Убедитесь, что в .env есть:")
        print("- JIRA_URL")
        print("- JIRA_USERNAME")
        print("- JIRA_API_TOKEN")
        return

    try:
        # Подключаемся к Jira
        jira = JIRA(server=jira_url, basic_auth=(jira_username, jira_token))

        print(f"🔗 Подключение к Jira: {jira_url}")
        print(f"📋 Проект: {project_key}")
        print("=" * 60)

        # Получаем информацию о проекте
        try:
            project = jira.project(project_key)
            print(f"📁 Название проекта: {project.name}")
            print(f"📝 Описание: {project.description or 'Нет описания'}")
            print()
        except Exception as e:
            print(f"⚠️ Не удалось получить информацию о проекте: {e}")
            print()

        # Получаем типы задач
        print("🎯 ДОСТУПНЫЕ ТИПЫ ЗАДАЧ:")
        print("-" * 40)
        try:
            issue_types = jira.project(project_key).issueTypes
            for issue_type in issue_types:
                print(f"• {issue_type.name} (ID: {issue_type.id})")
                if hasattr(issue_type, "description") and issue_type.description:
                    print(f"  Описание: {issue_type.description}")
            print()
        except Exception as e:
            print(f"❌ Ошибка получения типов задач: {e}")
            print()

        # Получаем статусы
        print("📊 ДОСТУПНЫЕ СТАТУСЫ:")
        print("-" * 40)
        try:
            statuses = jira.statuses()
            for status in statuses:
                print(f"• {status.name} (ID: {status.id})")
            print()
        except Exception as e:
            print(f"❌ Ошибка получения статусов: {e}")
            print()

        # Получаем приоритеты
        print("⚡ ДОСТУПНЫЕ ПРИОРИТЕТЫ:")
        print("-" * 40)
        try:
            priorities = jira.priorities()
            for priority in priorities:
                print(f"• {priority.name} (ID: {priority.id})")
            print()
        except Exception as e:
            print(f"❌ Ошибка получения приоритетов: {e}")
            print()

        # Создаем тестовую задачу для получения переходов
        print("🔄 ПЕРЕХОДЫ ДЛЯ ТЕСТОВОЙ ЗАДАЧИ:")
        print("-" * 40)
        try:
            # Создаем тестовую задачу
            test_issue_dict = {
                "project": {"key": project_key},
                "summary": "TEST - Удалить эту задачу",
                "description": "Тестовая задача для получения информации о переходах",
                "issuetype": {"name": default_issue_type},
            }
            print(test_issue_dict)

            test_issue = jira.create_issue(fields=test_issue_dict)
            print(f"✅ Создана тестовая задача: {test_issue.key}")

            # Получаем переходы
            transitions = jira.transitions(test_issue)
            print("Доступные переходы:")
            for transition in transitions:
                print(f"• {transition['name']} (ID: {transition['id']})")

            # Удаляем тестовую задачу
            jira.delete_issue(test_issue.key)
            print(f"🗑️ Тестовая задача {test_issue.key} удалена")
            print()

        except Exception as e:
            print(f"❌ Ошибка при работе с тестовой задачей: {e}")
            print()

        # Показываем примеры конфигурации
        print("⚙️ ПРИМЕРЫ КОНФИГУРАЦИИ:")
        print("-" * 40)
        if issue_types:
            print(f"# Для типа задачи '{issue_types[0].name}':")
            print(f"JIRA_ISSUE_TYPE={issue_types[0].name}")
            print()

        print("# Для приоритета 'High':")
        print("JIRA_PRIORITY=High")
        print()

        print("💡 Добавьте эти переменные в .env файл")

    except Exception as e:
        print(f"❌ Ошибка подключения к Jira: {e}")
        print("Проверьте:")
        print("1. Правильность URL Jira")
        print("2. Правильность логина и API токена")
        print("3. Права доступа к проекту")
        print("4. Доступность Jira из сети")


if __name__ == "__main__":
    get_jira_info()
