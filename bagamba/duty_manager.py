import logging
import os
from datetime import datetime, timedelta
from typing import Any, Optional, List
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dataclasses import dataclass, field
import re


logger = logging.getLogger(__name__)


@dataclass
class DutyPerson:
    """Информация о дежурном"""

    name: str
    slack_id: str
    start_date: datetime
    end_date: datetime
    week_number: int


@dataclass
class DutyManager:
    """Менеджер дежурных"""

    google_sheets_url: str
    credentials_path: str
    service: Any | None = None
    duty_schedule: list[DutyPerson] = field(default_factory=list)
    last_update: datetime | None = datetime.now() - timedelta(days=2)
    sheet_id: str | None = None
    update_interval_days: int = 2

    def __post_init__(
        self,
    ):
        self._extract_sheet_id()
        self.init()

    def _extract_sheet_id(
        self,
    ) -> None:
        """Извлекает ID документа из URL Google Sheets"""

        # Извлекаем ID документа из URL
        # Примеры URL:
        # https://docs.google.com/spreadsheets/d/1ABC123/edit#gid=0
        # https://docs.google.com/spreadsheets/d/1ABC123/edit?gid=0#gid=0

        # Ищем ID документа в URL
        match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", self.google_sheets_url)
        if not match:
            logger.error(
                f"❌ Не удалось извлечь ID документа из URL: {self.google_sheets_url}"
            )
            return None

        self.sheet_id = match.group(1)
        logger.info(f"✅ Извлечен ID таблицы: {self.sheet_id}")

    def _init_google_sheets_service(self):
        """Инициализирует Google Sheets API сервис"""
        if not self.credentials_path:
            logger.error(
                "❌ Путь к файлу учетных данных Google не установлен (GOOGLE_CREDENTIALS_PATH)"
            )
            return False

        if not os.path.exists(self.credentials_path):
            logger.error(f"❌ Файл учетных данных не найден: {self.credentials_path}")
            return False

        try:
            # Загружаем учетные данные из файла
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
            )

            # Создаем сервис
            self.service = build("sheets", "v4", credentials=credentials)
            logger.info("✅ Google Sheets API сервис инициализирован")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации Google Sheets API: {e}")
            return False

    def init(self):
        """Инициализация менеджера дежурных"""
        logger.info("🚀 Инициализация менеджера дежурных...")
        if self._init_google_sheets_service():
            logger.info("✅ Google Sheets API инициализирован, загружаем данные...")
            self.update_duty_schedule()
            logger.info(
                f"📊 После загрузки: {len(self.duty_schedule)} записей в расписании"
            )
        else:
            logger.warning(
                "⚠️ Google Sheets API не инициализирован, система дежурных не будет работать"
            )
        logger.info("✅ Менеджер дежурных инициализирован")

    def update_duty_schedule(self):
        """Обновляет расписание дежурных из Google Sheets"""
        if not self.service or not self.sheet_id:
            logger.error(
                "❌ Google Sheets API не инициализирован или ID таблицы не найден"
            )
            return

        logger.info(
            f"🔄 Загружаем расписание дежурных из Google Sheets (ID: {self.sheet_id})"
        )

        try:
            # Получаем данные из Google Sheets
            range_name = "A:F"  # Колонки A-F (номер недели, дата начала, дата окончания, дежурный, slackId, капасити дней)
            result = (
                self.service.spreadsheets()
                .values()
                .get(spreadsheetId=self.sheet_id, range=range_name)
                .execute()
            )

            values = result.get("values", [])

            if not values:
                logger.warning("⚠️ Таблица пуста или не найдена")
                return

            # Парсим данные
            self._parse_sheet_data(values)
            self.last_update = datetime.now()
            logger.info(
                f"✅ Расписание дежурных обновлено. Загружено {len(self.duty_schedule)} записей"
            )

        except HttpError as e:
            logger.error(f"❌ Ошибка Google Sheets API: {e}")
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении расписания дежурных: {e}")

    def _parse_sheet_data(self, values: List[List[str]]):
        """Парсит данные из Google Sheets"""
        self.duty_schedule = []

        # Пропускаем заголовки (первая строка)
        for row in values[1:]:
            if len(row) < 6:
                continue

            try:
                week_number = int(row[0].strip()) if row[0].strip() else 0
                start_date_str = row[1].strip()
                end_date_str = row[2].strip()
                name = row[3].strip()
                slack_id = row[4].strip()

                # Пропускаем записи с #N/A в slack_id
                if slack_id == "#N/A" or not slack_id or slack_id.lower() == "n/a":
                    logger.debug(
                        f"Пропускаем запись для {name} (неделя {week_number}) из-за отсутствующего Slack ID."
                    )
                    continue

                # Удаляем @ из Slack ID, если он есть
                if slack_id.startswith("@"):
                    slack_id = slack_id[1:]

                # Парсим даты в разных форматах
                start_date = self._parse_date(start_date_str)
                end_date = self._parse_date(end_date_str)

                if not start_date or not end_date:
                    logger.warning(
                        f"⚠️ Не удалось распарсить даты для {name} (неделя {week_number}): {start_date_str} - {end_date_str}"
                    )
                    continue

                # Добавляем время к дате окончания, чтобы включить весь день
                end_date = end_date + timedelta(days=1, microseconds=-1)

                duty_person = DutyPerson(
                    name=name,
                    slack_id=slack_id,
                    start_date=start_date,
                    end_date=end_date,
                    week_number=week_number,
                )

                self.duty_schedule.append(duty_person)
                logger.debug(
                    f"✅ Добавлен дежурный: {name} ({slack_id}) с {start_date.date()} по {end_date.date()}"
                )

            except (ValueError, IndexError) as e:
                logger.warning(f"⚠️ Ошибка парсинга строки: {row} - {e}")
                continue

        # Сортируем расписание по дате начала
        self.duty_schedule.sort(key=lambda x: x.start_date)
        logger.info(f"📅 Загружено {len(self.duty_schedule)} записей дежурных")

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Парсит дату в различных форматах"""
        if not date_str:
            return None

        # Убираем лишние пробелы
        date_str = date_str.strip()

        # Список возможных форматов дат
        date_formats = [
            "%d.%m.%Y",  # 16.09.2025
            "%d/%m/%Y",  # 16/09/2025
            "%Y-%m-%d",  # 2025-09-16
            "%d-%m-%Y",  # 16-09-2025
            "%d.%m.%y",  # 16.09.25
            "%d/%m/%y",  # 16/09/25
        ]

        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        logger.warning(f"⚠️ Не удалось распарсить дату: {date_str}")
        return None

    def get_current_duty_person(self) -> Optional[DutyPerson]:
        """Возвращает текущего дежурного"""
        logger.info(
            f"🔍 get_current_duty_person вызван. last_update: {self.last_update}, update_interval: {self.update_interval_days}"
        )

        # Проверяем, нужно ли обновить данные
        if self.last_update is None or datetime.now() - self.last_update > timedelta(
            days=self.update_interval_days
        ):
            logger.info("🔄 Обновляем расписание дежурных...")
            self.update_duty_schedule()
        else:
            logger.info("📋 Используем кэшированное расписание дежурных")

        current_date = datetime.now()
        logger.info(f"📅 Текущая дата: {current_date.strftime('%d.%m.%Y %H:%M:%S')}")
        logger.info(f"📋 Количество записей в расписании: {len(self.duty_schedule)}")

        for duty_person in self.duty_schedule:
            logger.info(
                f"🔍 Проверяем дежурного: {duty_person.name} ({duty_person.start_date.strftime('%d.%m.%Y')} - {duty_person.end_date.strftime('%d.%m.%Y')})"
            )
            if duty_person.start_date <= current_date <= duty_person.end_date:
                logger.info(
                    f"✅ Текущий дежурный найден: {duty_person.name} ({duty_person.slack_id})"
                )
                return duty_person

        logger.warning("⚠️ Не найден дежурный на текущую дату")
        return None

    async def get_duty_person_by_week(self, week_number: int) -> Optional[DutyPerson]:
        """Возвращает дежурного по номеру недели"""
        for duty_person in self.duty_schedule:
            if duty_person.week_number == week_number:
                return duty_person
        return None

    def get_duty_schedule_info(self) -> str:
        """Возвращает информацию о расписании дежурных"""
        if not self.duty_schedule:
            return "Расписание дежурных не загружено"

        info = f"Расписание дежурных (обновлено: {self.last_update.strftime('%d.%m.%Y %H:%M') if self.last_update else 'никогда'}):\n"

        for duty_person in self.duty_schedule[:5]:  # Показываем только первые 5 записей
            info += f"• Неделя {duty_person.week_number}: {duty_person.name} ({duty_person.start_date.strftime('%d.%m.%y')} - {duty_person.end_date.strftime('%d.%m.%y')})\n"

        if len(self.duty_schedule) > 5:
            info += f"... и еще {len(self.duty_schedule) - 5} записей"

        return info
