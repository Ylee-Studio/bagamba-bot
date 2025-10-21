import logging
from dataclasses import dataclass, field
from slack_sdk import WebClient
from database import IncidentStatus
from duty_manager import DutyManager
from incident_manager import IncidentManager

logger = logging.getLogger(__name__)


@dataclass
class PermissionsChecker:
    allowed_channels: list = field(default_factory=list)
    allowed_button_users: list = field(default_factory=list)

    def is_user_allowed_for_buttons(self, user_id: str) -> bool:
        logger.info(f"🔐 Проверка прав доступа для пользователя {user_id}")
        logger.info(f"🔐 ALLOWED_BUTTON_USERS: {self.allowed_button_users}")
        logger.info(f"🔐 Количество разрешенных пользователей: {len(self.allowed_button_users)}")

        if not len(self.allowed_button_users):
            logger.info(
                f"🔐 Список разрешенных пользователей пустой, разрешаем доступ для {user_id}"
            )
            return True

        is_allowed = user_id in self.allowed_button_users
        logger.info(
            f"🔐 Пользователь {user_id} {'разрешен' if is_allowed else 'запрещен'}"
        )
        return is_allowed

    def is_channel_allowed(self, channel_id: str) -> bool:
        """Проверяет, разрешен ли канал для работы бота"""
        logger.info(f"🔐 Проверка канала {channel_id}")
        logger.info(f"🔐 ALLOWED_CHANNELS: {self.allowed_channels}")
        logger.info(f"🔐 Количество разрешенных каналов: {len(self.allowed_channels)}")
        
        if not len(self.allowed_channels):
            logger.info("🔐 Список разрешенных каналов пустой, разрешаем доступ")
            return True
        is_allowed = channel_id in self.allowed_channels
        logger.info(f"🔐 Канал {channel_id} {'разрешен' if is_allowed else 'запрещен'}")
        return is_allowed


@dataclass
class Bot:
    slack_client: WebClient
    permissions_checker: PermissionsChecker
    duty_manager: DutyManager
    incident_manager: IncidentManager
    default_responsible_user_id: str
    bot_id: str = "B09F0M5V5T9"

    def get_duty_manager(
        self,
    ) -> str:
        duty_slot = self.duty_manager.get_current_duty_person()
        logger.info(
            f"🔍 В create_incident_buttons получен дежурный: {duty_slot.name if duty_slot else 'None'} ({duty_slot.slack_id if duty_slot else 'None'})"
        )
        # Используем дежурного, если он найден, иначе используем ответственного из конфига
        duty_user_id = (
            duty_slot.slack_id if duty_slot else self.default_responsible_user_id
        )
        duty_name = duty_slot.name if duty_slot else "ответственный"
        logger.info(
            f"🔍 В create_incident_buttons будет использован: {duty_name} ({duty_user_id})"
        )
        return duty_user_id

    def is_bot_message(self, event) -> bool:
        """Проверяет, является ли сообщение от бота"""
        return event.get("bot_id") is not None or event.get("subtype") == "bot_message"

    def get_user_name(self, user_id: str) -> str:
        """Получает имя пользователя по ID"""
        try:
            response = self.slack_client.users_info(user=user_id)
            return response["user"]["real_name"] or response["user"]["name"]
        except Exception as e:
            logger.error(f"Ошибка получения имени пользователя {user_id}: {e}")
            return f"<@{user_id}>"

    def get_user_email(self, user_id: str) -> str | None:
        """Получает email пользователя по ID"""
        try:
            response = self.slack_client.users_info(user=user_id)
            return response["user"]["profile"]["email"]
        except Exception as e:
            logger.error(f"Ошибка получения email пользователя {user_id}: {e}")
            return None

    def add_reaction(self, channel_id: str, message_ts: str, emoji: str) -> bool:
        """Добавляет реакцию к сообщению"""
        try:
            self.slack_client.reactions_add(
                channel=channel_id, timestamp=message_ts, name=emoji
            )
            logger.info(f"Добавлена реакция {emoji} к сообщению {message_ts}")
            return True
        except Exception as e:
            logger.error(
                f"Ошибка при добавлении реакции {emoji} к сообщению {message_ts}: {e}"
            )
            return False

    def remove_reaction(self, channel_id: str, message_ts: str, emoji: str) -> bool:
        """Удаляет реакцию с сообщения"""
        try:
            self.slack_client.reactions_remove(
                channel=channel_id, timestamp=message_ts, name=emoji
            )
            logger.info(f"Удалена реакция {emoji} с сообщения {message_ts}")
            return True
        except Exception as e:
            logger.error(
                f"Ошибка при удалении реакции {emoji} с сообщения {message_ts}: {e}"
            )
            return False

    def find_and_update_control_message(
        self, channel_id: str, thread_ts: str, blocks: list, ticket_key: str
    ):
        try:
            response = self.slack_client.conversations_replies(
                channel=channel_id, ts=thread_ts
            )
            # Ищем сообщения бота с кнопками
            bot_messages = []
            for message in response["messages"]:
                if (
                    message.get("bot_id") == self.bot_id
                    and message.get("blocks")
                    and any(
                        block.get("type") == "actions"
                        for block in message.get("blocks", [])
                    )
                ):
                    bot_messages.append(message)

            # Берем последнее сообщение с кнопками (второе сообщение)
            if bot_messages:
                message_to_update = bot_messages[-1]
                self.slack_client.chat_update(
                    channel=channel_id, ts=message_to_update["ts"], blocks=blocks
                )
                logger.info(
                    f"✅ Обновлено сообщение управления для инцидента {ticket_key}"
                )
                return True
            else:
                logger.warning(
                    f"⚠️ Не найдено сообщение с кнопками для инцидента {ticket_key}"
                )
                return False
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении сообщения управления: {e}")
            return False

    def create_incident_buttons(self, incident, user_id=None):
        if user_id and not self.permissions_checker.is_user_allowed_for_buttons(
            user_id
        ):
            return []
        duty_user_id = self.get_duty_manager()

        if incident.status == IncidentStatus.CREATED:
            return [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"<@{duty_user_id}> Пожалуйста, возьмите инцидент в работу",
                    },
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Взять в работу"},
                            "action_id": "take_incident",
                            "value": incident.ticket_key,
                            "style": "primary",
                        }
                    ],
                },
            ]
        elif incident.status == IncidentStatus.ASSIGNED:
            return [
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Ожидаю ответ"},
                            "action_id": "awaiting_response",
                            "value": incident.ticket_key,
                            "style": "primary",
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Решено"},
                            "action_id": "close_incident",
                            "value": incident.ticket_key,
                            "style": "danger",
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Не напоминать"},
                            "action_id": "freeze_incident",
                            "value": incident.ticket_key,
                        },
                    ],
                }
            ]
        elif incident.status == IncidentStatus.AWAITING_RESPONSE:
            return [
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Решено"},
                            "action_id": "close_incident",
                            "value": incident.ticket_key,
                            "style": "danger",
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Не напоминать"},
                            "action_id": "freeze_incident",
                            "value": incident.ticket_key,
                        },
                    ],
                }
            ]
        elif incident.status == IncidentStatus.FROZEN:
            return [
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Ожидаю ответ"},
                            "action_id": "awaiting_response",
                            "value": incident.ticket_key,
                            "style": "primary",
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Решено"},
                            "action_id": "close_incident",
                            "value": incident.ticket_key,
                            "style": "danger",
                        },
                    ],
                }
            ]
        return []

    def handle_dm_command(self, event, say):
        """Обрабатывает команды в личных сообщениях"""
        user_id = event.get("user")
        text = event.get("text", "").strip().lower()
        channel_id = event.get("channel")

        logger.info(f"💬 Обрабатываем DM команду: user={user_id}, text='{text}', channel={channel_id}")

        if not user_id or not text:
            logger.warning("⚠️ Пустая команда или отсутствует user_id")
            return

        logger.info(f"📩 Получена команда в личку от {user_id}: {text}")

        # Команда обновления расписания дежурных
        if text in ["обновить расписание", "update schedule", "refresh"]:
            self._handle_update_schedule_command(channel_id, say)
        elif text in ["расписание", "schedule", "дежурные"]:
            self._handle_show_schedule_command(channel_id, say)
        else:
            # Показываем доступные команды
            self._show_available_commands(channel_id, say)

    def _handle_update_schedule_command(self, channel_id: str, say):
        """Обрабатывает команду обновления расписания"""
        try:
            logger.info("🔄 Принудительное обновление расписания дежурных")
            
            # Принудительно обновляем расписание
            self.duty_manager.update_duty_schedule()
            
            # Получаем информацию о текущем расписании
            schedule_info = self.duty_manager.get_duty_schedule_info()
            
            say(
                channel=channel_id,
                text=f"✅ Расписание дежурных обновлено!\n\n{schedule_info}"
            )
            
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении расписания: {e}")
            say(
                channel=channel_id,
                text=f"❌ Ошибка при обновлении расписания: {str(e)}"
            )

    def _handle_show_schedule_command(self, channel_id: str, say):
        """Обрабатывает команду показа расписания"""
        try:
            schedule_info = self.duty_manager.get_duty_schedule_info()
            
            # Получаем текущего дежурного
            current_duty = self.duty_manager.get_current_duty_person()
            if current_duty:
                current_info = f"\n🕐 Сейчас дежурный: {current_duty.name} ({current_duty.start_time}-{current_duty.end_time})"
            else:
                current_info = "\n🕐 Сейчас нет активного дежурного"
            
            say(
                channel=channel_id,
                text=f"{schedule_info}{current_info}"
            )
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении расписания: {e}")
            say(
                channel=channel_id,
                text=f"❌ Ошибка при получении расписания: {str(e)}"
            )

    def _show_available_commands(self, channel_id: str, say):
        """Показывает доступные команды"""
        commands_text = """
🤖 Доступные команды:

• `обновить расписание` - принудительно обновить расписание дежурных
• `расписание` - показать текущее расписание дежурных
• `help` - показать эту справку

Просто напишите команду в этом чате.
        """
        
        say(channel=channel_id, text=commands_text)

    def handle_thread_message(self, event, say, client):
        """Обрабатывает сообщения в тредах инцидентов"""
        channel_id = event.get("channel")
        thread_ts = event.get("thread_ts")
        user_id = event.get("user")

        if not channel_id or not thread_ts or not user_id:
            return

        # Проверяем, есть ли инцидент для этого треда
        incident = self.incident_manager.get_incident_by_thread(channel_id, thread_ts)

        if incident and incident.status == IncidentStatus.AWAITING_RESPONSE:
            logger.info(
                f"📝 Получено сообщение в тред инцидента {incident.ticket_key} в статусе 'ожидание ответа'"
            )

            # Переводим инцидент обратно в статус "назначен"
            incident.status = IncidentStatus.ASSIGNED
            updated = self.incident_manager.db.update_incident(incident)

            if updated:
                # Останавливаем уведомления автору
                self.incident_manager.stop_notification_task(
                    incident.ticket_key, "awaiting_response"
                )

                # Обновляем сообщение бота с новыми кнопками
                blocks = [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "👀 Продолжаем работу (ответ получен)",
                        },
                    }
                ]
                blocks.extend(self.create_incident_buttons(incident))

                # Обновляем второе сообщение бота (с кнопками управления)
                self.find_and_update_control_message(
                    channel_id, thread_ts, blocks, incident.ticket_key
                )
                self.remove_reaction(
                    channel_id=channel_id,
                    message_ts=thread_ts,
                    emoji="person_in_lotus_position",
                )

                logger.info(
                    f"✅ Инцидент {incident.ticket_key} переведен обратно в статус 'назначен' после получения ответа"
                )

    def send_notification_sync(self, incident, notification_type: str = "default"):
        """Отправляет уведомление о инциденте (синхронная версия)"""
        try:
            logger.info(
                f"🔍 Проверка уведомления: тип={notification_type}, статус={incident.status}, тикет={incident.ticket_key}"
            )
            duty_user_id = self.get_duty_manager()
            if (
                notification_type == "default"
                and incident.status == IncidentStatus.CREATED
            ):
                # Получаем текущего дежурного

                # Уведомляем дежурного
                message = f"<@{duty_user_id}> Инцидент {incident.ticket_key} ожидает назначения!"
                self.slack_client.chat_postMessage(
                    channel=incident.channel_id,
                    thread_ts=incident.thread_ts,
                    text=message,
                )
                logger.info(
                    f"🔔 Отправлено уведомление о инциденте {incident.ticket_key} (CREATED) - пинг дежурного {duty_user_id}"
                )
            elif (
                notification_type == "awaiting_response"
                and incident.status == IncidentStatus.AWAITING_RESPONSE
            ):
                # Уведомляем автора вопроса
                message = f"<@{incident.author_id}> Ожидаем ваш ответ по инциденту {incident.ticket_key}"
                self.slack_client.chat_postMessage(
                    channel=incident.channel_id,
                    thread_ts=incident.thread_ts,
                    text=message,
                )
                logger.info(
                    f"🔔 Отправлено уведомление о инциденте {incident.ticket_key} (AWAITING_RESPONSE) - пинг автора"
                )
            else:
                logger.warning(
                    f"⚠️ Уведомление не отправлено: тип={notification_type}, статус={incident.status}"
                )
        except Exception as e:
            logger.error(f"❌ Ошибка отправки уведомления: {e}")
