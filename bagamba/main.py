import logging
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient

from config import Config
from jira_client import JiraClient
from incident_manager import IncidentManager
from database import Database, IncidentStatus
from duty_manager import DutyManager
from bot import PermissionsChecker, Bot
from redis_scheduler import RedisNotificationScheduler, RedisClient
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Инициализация приложения Slack
app = App(token=Config.SLACK_BOT_TOKEN, signing_secret=Config.SLACK_SIGNING_SECRET)

# Инициализация клиентов
jira_client = JiraClient()
incident_manager = IncidentManager(
    db=Database(),
    notification_manager=RedisNotificationScheduler(
        redis_client=RedisClient(url=Config.REDIS_URL, db=Config.REDIS_DB),
    ),
)
slack_client = WebClient(token=Config.SLACK_BOT_TOKEN)

permission_checker = PermissionsChecker(
    allowed_button_users=Config.ALLOWED_BUTTON_USERS,
    allowed_channels=Config.ALLOWED_CHANNELS,
)
duty_manager = DutyManager(
    google_sheets_url=Config.GOOGLE_SHEET_URL,
    credentials_path=Config.GOOGLE_CREDENTIALS_PATH,
    sheet_range=Config.GOOGLE_SHEET_RANGE,
)
IncidentBot = Bot(
    slack_client=slack_client,
    permissions_checker=permission_checker,
    duty_manager=duty_manager,
    incident_manager=incident_manager,
    default_responsible_user_id=Config.RESPONSIBLE_USER_ID,
)


@app.event("message")
def handle_message_events(event, say, client):
    """Обрабатывает сообщения в каналах и личных сообщениях"""
    # Игнорируем сообщения от ботов
    if IncidentBot.is_bot_message(event):
        return

    # Получаем информацию о пользователе
    user_id = event.get("user")
    if not user_id:
        return

    channel_id = event.get("channel")
    channel_type = event.get("channel_type")

    # Обрабатываем личные сообщения (команды боту)
    if channel_type == "im":
        IncidentBot.handle_dm_command(event, say)
        return

    # Обрабатываем сообщения в тредах
    if event.get("thread_ts"):
        IncidentBot.handle_thread_message(event, say, client)
        return

    # Проверяем, разрешен ли канал
    if not IncidentBot.permissions_checker.is_channel_allowed(channel_id):
        logger.info(f"Игнорируем сообщение из неразрешенного канала: {channel_id}")
        return

    user_name = IncidentBot.get_user_name(user_id)
    message_text = event.get("text", "")

    logger.info(f"Новое сообщение от {user_name} в канале {channel_id}: {message_text}")

    # Создаем тикет в Jira
    try:
        # Используем первые 300 символов сообщения в качестве названия
        title = message_text[:150] if len(message_text) > 150 else message_text
        if len(message_text) > 150:
            title += "..."
        title =title.replace("\n", " ").replace("\r", " ")

        # Создаем ссылку на тред
        thread_url = f"https://instoriesworkspace.slack.com/archives/{channel_id}/p{event['ts'].replace('.', '')}"

        ticket_key = jira_client.create_incident_ticket(
            title=title,
            description=message_text,
            reporter=user_name,
            thread_url=thread_url,
        )

        # Создаем инцидент
        incident = incident_manager.create_incident(
            ticket_key=ticket_key,
            channel_id=channel_id,
            thread_ts=event["ts"],
            author_id=user_id,
        )

        if not incident:
            say(
                channel=channel_id,
                thread_ts=event["ts"],
                text="❌ Ошибка при создании инцидента в базе данных",
            )
            return

        # Отправляем первое сообщение - информация об инциденте
        info_blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"🚨 *Инцидент зарегистрирован*\n\n*Ссылка на Jira:* {jira_client.get_ticket_url(ticket_key)}",
                },
            }
        ]

        say(channel=channel_id, thread_ts=event["ts"], blocks=info_blocks)

        # Отправляем второе сообщение - кнопки управления
        control_blocks = IncidentBot.create_incident_buttons(incident)
        if control_blocks:
            say(channel=channel_id, thread_ts=event["ts"], blocks=control_blocks)

        # Запускаем уведомления через Redis
        incident_data = {
            "ticket_key": incident.ticket_key,
            "channel_id": incident.channel_id,
            "thread_ts": incident.thread_ts,
            "author_id": incident.author_id,
            "status": incident.status.value,
            "assigned_to": incident.assigned_to,
            "created_at": (
                incident.created_at.isoformat()
                if incident.created_at
                else datetime.now().isoformat()
            ),
            "last_notification": (
                incident.last_notification.isoformat()
                if incident.last_notification
                else None
            ),
        }

        # Запускаем уведомления
        incident_manager.start_notification_task(
            ticket_key, incident_data, Config.NOTIFICATION_INTERVAL_MINUTES, "default"
        )

    except Exception as e:
        logger.error(f"Ошибка при создании инцидента: {e}")
        say(
            channel=channel_id,
            thread_ts=event["ts"],
            text=f"❌ Ошибка при создании инцидента: {str(e)}",
        )


@app.action("take_incident")
def handle_take_incident(ack, body, say):
    """Обрабатывает нажатие кнопки 'Взять в работу'"""
    ack()

    user_id = body["user"]["id"]
    ticket_key = body["actions"][0]["value"]
    channel_id = body["channel"]["id"]
    message_ts = body["message"]["ts"]
    thread_ts = body["message"].get(
        "thread_ts", message_ts
    )  # thread_ts для добавления реакций к исходному сообщению

    # Сразу отключаем кнопки для предотвращения двойного клика
    try:
        slack_client.chat_update(
            channel=channel_id,
            ts=message_ts,
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "⏳ Обработка запроса..."},
                }
            ],
        )
    except Exception as e:
        logger.warning(f"Не удалось обновить сообщение для отключения кнопок: {e}")

    # Проверяем права пользователя
    if not IncidentBot.permissions_checker.is_user_allowed_for_buttons(user_id):
        user_name = IncidentBot.get_user_name(user_id)
        say(
            channel=channel_id,
            thread_ts=message_ts,
            text=f"❌ {user_name}, у вас нет прав для управления инцидентами",
        )
        logger.warning(
            f"Пользователь {user_name} ({user_id}) попытался взять инцидент без прав"
        )

        # Восстанавливаем кнопки для авторизованных пользователей
        try:
            incident = incident_manager.get_incident(ticket_key)
            if incident:
                blocks = IncidentBot.create_incident_buttons(incident)
                slack_client.chat_update(
                    channel=channel_id, ts=message_ts, blocks=blocks
                )
                logger.info(f"✅ Кнопки восстановлены для инцидента {ticket_key}")
        except Exception as e:
            logger.error(f"❌ Ошибка при восстановлении кнопок: {e}")

        return

    # Проверяем, не взят ли уже инцидент в работу
    incident = incident_manager.get_incident(ticket_key)
    if not incident:
        logger.error(f"Инцидент {ticket_key} не найден в базе данных")
        return

    if incident.status != IncidentStatus.CREATED:
        user_name = IncidentBot.get_user_name(user_id)
        say(
            channel=channel_id,
            thread_ts=message_ts,
            text=f"❌ {user_name}, инцидент {ticket_key} уже обрабатывается (статус: {incident.status.value})",
        )
        logger.warning(
            f"Пользователь {user_name} ({user_id}) попытался взять уже обработанный инцидент {ticket_key}"
        )

        # Восстанавливаем кнопки для авторизованных пользователей
        try:
            blocks = IncidentBot.create_incident_buttons(incident)
            slack_client.chat_update(channel=channel_id, ts=message_ts, blocks=blocks)
            logger.info(f"✅ Кнопки восстановлены для инцидента {ticket_key}")
        except Exception as e:
            logger.error(f"❌ Ошибка при восстановлении кнопок: {e}")
        return

    user_name = IncidentBot.get_user_name(user_id)
    user_email = IncidentBot.get_user_email(user_id)

    # Назначаем инцидент в системе
    assigned = incident_manager.take_incident_in_progress(ticket_key, user_id)

    if assigned:
        # Назначаем задачу в Jira
        if user_email:
            jira_client.assign_ticket(ticket_key, user_email)

        incident = incident_manager.get_incident(ticket_key)

        # Обновляем сообщение с новыми кнопками
        status_text = "👀 Взято в работу"
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": status_text}}]
        blocks.extend(IncidentBot.create_incident_buttons(incident))

        # Обновляем сообщение с кнопками (то же сообщение, которое мы изменили на "Обработка запроса")
        slack_client.chat_update(channel=channel_id, ts=message_ts, blocks=blocks)

        # Останавливаем уведомления о назначении
        incident_manager.stop_notification_task(ticket_key)

        # Добавляем эмодзи глазки к исходному сообщению (thread_ts - это время исходного сообщения)
        IncidentBot.add_reaction(channel_id, thread_ts, "eyes")

        logger.info(f"Инцидент {ticket_key} взят в работу пользователем {user_name}")
    else:
        # Ошибка назначения
        say(
            channel=channel_id,
            thread_ts=message_ts,
            text=f"❌ Ошибка при назначении инцидента {ticket_key}",
        )


@app.action("awaiting_response")
def handle_awaiting_response(ack, body, say):
    """Обрабатывает нажатие кнопки 'Ожидаю ответ'"""
    ack()

    user_id = body["user"]["id"]
    ticket_key = body["actions"][0]["value"]
    channel_id = body["channel"]["id"]
    message_ts = body["message"]["ts"]
    thread_ts = body["message"].get("thread_ts", message_ts)
    # Сразу отключаем кнопки для предотвращения двойного клика
    try:
        slack_client.chat_update(
            channel=channel_id,
            ts=message_ts,
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "⏳ Обработка запроса..."},
                }
            ],
        )
    except Exception as e:
        logger.warning(f"Не удалось обновить сообщение для отключения кнопок: {e}")

    logger.info(
        f"🔍 Обработка кнопки 'Ожидаю ответ' для тикета {ticket_key} пользователем {user_id}"
    )

    # Проверяем права пользователя
    if not IncidentBot.permissions_checker.is_user_allowed_for_buttons(user_id):
        user_name = IncidentBot.get_user_name(user_id)
        say(
            channel=channel_id,
            thread_ts=message_ts,
            text=f"❌ {user_name}, у вас нет прав для управления инцидентами",
        )
        logger.warning(
            f"Пользователь {user_name} ({user_id}) попытался изменить статус инцидента без прав"
        )

        # Восстанавливаем кнопки для авторизованных пользователей
        try:
            incident = incident_manager.get_incident(ticket_key)
            if incident:
                blocks = IncidentBot.create_incident_buttons(incident)
                slack_client.chat_update(
                    channel=channel_id, ts=message_ts, blocks=blocks
                )
                logger.info(f"✅ Кнопки восстановлены для инцидента {ticket_key}")
        except Exception as e:
            logger.error(f"❌ Ошибка при восстановлении кнопок: {e}")

        return

    # Проверяем текущий статус инцидента
    incident = incident_manager.get_incident(ticket_key)
    if not incident:
        logger.error(f"Инцидент {ticket_key} не найден в базе данных")
        return

    if incident.status not in (IncidentStatus.ASSIGNED, IncidentStatus.FROZEN):
        user_name = IncidentBot.get_user_name(user_id)
        say(
            channel=channel_id,
            thread_ts=message_ts,
            text=f"❌ {user_name}, нельзя установить ожидание ответа для инцидента в статусе {incident.status.value}",
        )
        logger.warning(
            f"Пользователь {user_name} ({user_id}) попытался установить ожидание ответа для инцидента {ticket_key} в неподходящем статусе {incident.status.value}"
        )

        # Восстанавливаем кнопки для авторизованных пользователей
        try:
            blocks = IncidentBot.create_incident_buttons(incident)
            slack_client.chat_update(channel=channel_id, ts=message_ts, blocks=blocks)
            logger.info(f"✅ Кнопки восстановлены для инцидента {ticket_key}")
        except Exception as e:
            logger.error(f"❌ Ошибка при восстановлении кнопок: {e}")
        return

    logger.info(
        f"✅ Права пользователя проверены, устанавливаем статус ожидания ответа для {ticket_key}"
    )

    # Устанавливаем статус ожидания ответа
    awaiting_set = incident_manager.set_awaiting_response(ticket_key)
    logger.info(f"📝 Результат установки статуса ожидания ответа: {awaiting_set}")

    if awaiting_set:
        logger.info(
            f"✅ Статус успешно установлен, получаем обновленный инцидент {ticket_key}"
        )
        incident = incident_manager.get_incident(ticket_key)
        logger.info(
            f"📋 Получен инцидент: статус={incident.status if incident else 'None'}"
        )

        if incident:
            logger.info(f"📝 Обновляем сообщение для инцидента {ticket_key}")
            # Обновляем сообщение
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"🧘 Ожидание ответа от <@{incident.author_id}>",
                    },
                }
            ]
            blocks.extend(IncidentBot.create_incident_buttons(incident))

            # Обновляем сообщение с кнопками (то же сообщение, которое мы изменили на "Обработка запроса")
            slack_client.chat_update(channel=channel_id, ts=message_ts, blocks=blocks)
            IncidentBot.add_reaction(channel_id, thread_ts, "person_in_lotus_position")
            logger.info(f"✅ Сообщение обновлено в канале {channel_id}")

            # Запускаем уведомления автору через Redis
            incident_data = {
                "ticket_key": incident.ticket_key,
                "channel_id": incident.channel_id,
                "thread_ts": incident.thread_ts,
                "author_id": incident.author_id,
                "status": incident.status.value,
                "assigned_to": incident.assigned_to,
                "created_at": (
                    incident.created_at.isoformat()
                    if incident.created_at
                    else datetime.now().isoformat()
                ),
                "last_notification": (
                    incident.last_notification.isoformat()
                    if incident.last_notification
                    else None
                ),
            }

            logger.info(f"🔔 Запускаем уведомления автору для {ticket_key}")
            # Запускаем уведомления автору
            incident_manager.start_notification_task(
                ticket_key,
                incident_data,
                Config.AWAITING_RESPONSE_INTERVAL_MINUTES,
                "awaiting_response",
            )

            logger.info(
                f"✅ Инцидент {ticket_key} переведен в статус 'ожидание ответа'"
            )
        else:
            logger.error(
                f"❌ Не удалось получить инцидент {ticket_key} для обновления сообщения"
            )
    else:
        logger.error(
            f"❌ Не удалось установить статус ожидания ответа для {ticket_key}"
        )


@app.action("close_incident")
def handle_close_incident(ack, body, say):
    """Обрабатывает нажатие кнопки 'Закрыто'"""
    ack()

    user_id = body["user"]["id"]
    ticket_key = body["actions"][0]["value"]
    channel_id = body["channel"]["id"]
    message_ts = body["message"]["ts"]
    thread_ts = body["message"].get(
        "thread_ts", message_ts
    )  # thread_ts для добавления реакций к исходному сообщению

    # Сразу отключаем кнопки для предотвращения двойного клика
    try:
        slack_client.chat_update(
            channel=channel_id,
            ts=message_ts,
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "⏳ Обработка запроса..."},
                }
            ],
        )
    except Exception as e:
        logger.warning(f"Не удалось обновить сообщение для отключения кнопок: {e}")

    logger.info(
        f"DEBUG: message_ts={message_ts}, thread_ts={thread_ts}, channel_id={channel_id}"
    )

    # Проверяем права пользователя
    if not IncidentBot.permissions_checker.is_user_allowed_for_buttons(user_id):
        user_name = IncidentBot.get_user_name(user_id)
        say(
            channel=channel_id,
            thread_ts=message_ts,
            text=f"❌ {user_name}, у вас нет прав для управления инцидентами",
        )
        logger.warning(
            f"Пользователь {user_name} ({user_id}) попытался закрыть инцидент без прав"
        )

        # Восстанавливаем кнопки для авторизованных пользователей
        try:
            incident = incident_manager.get_incident(ticket_key)
            if incident:
                blocks = IncidentBot.create_incident_buttons(incident)
                slack_client.chat_update(
                    channel=channel_id, ts=message_ts, blocks=blocks
                )
                logger.info(f"✅ Кнопки восстановлены для инцидента {ticket_key}")
        except Exception as e:
            logger.error(f"❌ Ошибка при восстановлении кнопок: {e}")

        return

    # Проверяем текущий статус инцидента
    incident = incident_manager.get_incident(ticket_key)
    if not incident:
        logger.error(f"Инцидент {ticket_key} не найден в базе данных")
        return

    if incident.status == IncidentStatus.CLOSED:
        user_name = IncidentBot.get_user_name(user_id)
        say(
            channel=channel_id,
            thread_ts=message_ts,
            text=f"❌ {user_name}, инцидент {ticket_key} уже закрыт",
        )
        logger.warning(
            f"Пользователь {user_name} ({user_id}) попытался закрыть уже закрытый инцидент {ticket_key}"
        )

        # Восстанавливаем кнопки для авторизованных пользователей
        try:
            blocks = IncidentBot.create_incident_buttons(incident)
            slack_client.chat_update(channel=channel_id, ts=message_ts, blocks=blocks)
            logger.info(f"✅ Кнопки восстановлены для инцидента {ticket_key}")
        except Exception as e:
            logger.error(f"❌ Ошибка при восстановлении кнопок: {e}")
        return

    # Закрываем тикет в Jira
    logger.info(f"Попытка закрыть тикет {ticket_key} в Jira")
    jira_closed = jira_client.close_incident_ticket(ticket_key)
    logger.info(f"Результат закрытия тикета в Jira: {jira_closed}")

    if jira_closed:
        # Закрываем инцидент
        incident_manager.close_incident(ticket_key)

        # Обновляем второе сообщение
        blocks = [
            {"type": "section", "text": {"type": "mrkdwn", "text": "✅ *Решено!*\n\n"}}
        ]

        # Обновляем сообщение с кнопками (то же сообщение, которое мы изменили на "Обработка запроса")
        slack_client.chat_update(channel=channel_id, ts=message_ts, blocks=blocks)

        # Добавляем зеленую галочку к исходному сообщению (thread_ts - это время исходного сообщения)
        logger.info(
            f"Попытка добавить реакцию white_check_mark к сообщению {thread_ts} в канале {channel_id}"
        )
        IncidentBot.add_reaction(channel_id, thread_ts, "white_check_mark")

        logger.info(f"Инцидент {ticket_key} закрыт")
    else:
        logger.error(f"Не удалось закрыть тикет {ticket_key} в Jira")
        say(
            channel=channel_id,
            thread_ts=message_ts,
            text=f"❌ Ошибка при закрытии тикета {ticket_key} в Jira",
        )


@app.action("freeze_incident")
def handle_freeze_incident(ack, body, say):
    """Обрабатывает нажатие кнопки 'Не напоминать'"""
    ack()

    user_id = body["user"]["id"]
    ticket_key = body["actions"][0]["value"]
    channel_id = body["channel"]["id"]
    message_ts = body["message"]["ts"]
    thread_ts = body["message"].get(
        "thread_ts", message_ts
    )  # thread_ts для добавления реакций к исходному сообщению

    # Сразу отключаем кнопки для предотвращения двойного клика
    try:
        slack_client.chat_update(
            channel=channel_id,
            ts=message_ts,
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "⏳ Обработка запроса..."},
                }
            ],
        )
    except Exception as e:
        logger.warning(f"Не удалось обновить сообщение для отключения кнопок: {e}")

    # Проверяем права пользователя
    if not IncidentBot.permissions_checker.is_user_allowed_for_buttons(user_id):
        user_name = IncidentBot.get_user_name(user_id)
        say(
            channel=channel_id,
            thread_ts=message_ts,
            text=f"❌ {user_name}, у вас нет прав для управления инцидентами",
        )
        logger.warning(
            f"Пользователь {user_name} ({user_id}) попытался заморозить инцидент без прав"
        )

        # Восстанавливаем кнопки для авторизованных пользователей
        try:
            incident = incident_manager.get_incident(ticket_key)
            if incident:
                blocks = IncidentBot.create_incident_buttons(incident)
                slack_client.chat_update(
                    channel=channel_id, ts=message_ts, blocks=blocks
                )
                logger.info(f"✅ Кнопки восстановлены для инцидента {ticket_key}")
        except Exception as e:
            logger.error(f"❌ Ошибка при восстановлении кнопок: {e}")

        return

    # Проверяем текущий статус инцидента
    incident = incident_manager.get_incident(ticket_key)
    if not incident:
        logger.error(f"Инцидент {ticket_key} не найден в базе данных")
        return

    if incident.status == IncidentStatus.FROZEN:
        user_name = IncidentBot.get_user_name(user_id)
        say(
            channel=channel_id,
            thread_ts=message_ts,
            text=f"❌ {user_name}, инцидент {ticket_key} уже заморожен",
        )
        logger.warning(
            f"Пользователь {user_name} ({user_id}) попытался заморозить уже замороженный инцидент {ticket_key}"
        )

        # Восстанавливаем кнопки для авторизованных пользователей
        try:
            blocks = IncidentBot.create_incident_buttons(incident)
            slack_client.chat_update(channel=channel_id, ts=message_ts, blocks=blocks)
            logger.info(f"✅ Кнопки восстановлены для инцидента {ticket_key}")
        except Exception as e:
            logger.error(f"❌ Ошибка при восстановлении кнопок: {e}")
        return

    # Замораживаем инцидент
    frozen = incident_manager.freeze_incident(ticket_key)

    if frozen:
        # Получаем обновленный инцидент
        incident = incident_manager.get_incident(ticket_key)

        # Обновляем второе сообщение
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "🤫 Принято"}}]
        blocks.extend(IncidentBot.create_incident_buttons(incident))

        # Обновляем сообщение с кнопками (то же сообщение, которое мы изменили на "Обработка запроса")
        slack_client.chat_update(channel=channel_id, ts=message_ts, blocks=blocks)

    # Добавляем эмодзи снежинки к исходному сообщению (thread_ts - это время исходного сообщения)
    IncidentBot.add_reaction(channel_id, thread_ts, "snowflake")

    logger.info(f"Инцидент {ticket_key} заморожен")


def main():
    """Основная функция запуска бота"""
    logger.info("Запуск Slack бота для работы с инцидентами...")

    # Инициализируем базу данных
    incident_manager.init()

    # Инициализируем менеджер дежурных
    duty_manager.init()

    # Запускаем бота
    handler = SocketModeHandler(app, Config.SLACK_APP_TOKEN)
    handler.start()


if __name__ == "__main__":
    main()
