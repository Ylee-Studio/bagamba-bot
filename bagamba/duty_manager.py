import logging
import os
from datetime import datetime, timedelta
from typing import Any, Optional, List
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dataclasses import dataclass, field
import re
import pytz

logger = logging.getLogger(__name__)


@dataclass
class DutySlot:
    """Информация о временном слоте дежурства"""

    start_time: str  # Время начала в формате "07:00"
    end_time: str    # Время окончания в формате "12:00"
    name: str        # Имя дежурного
    slack_id: str    # Slack ID дежурного


@dataclass
class DutyManager:
    """Менеджер дежурных"""

    google_sheets_url: str
    credentials_path: str
    sheet_range: str = "A:D"  # По умолчанию колонки A-D (будет переопределено из конфигурации)
    service: Any | None = None
    duty_slots: list[DutySlot] = field(default_factory=list)
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
                f"📊 После загрузки: {len(self.duty_slots)} записей в расписании"
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
            logger.info(f"📊 Загружаем данные из листа: {self.sheet_range}")
            result = (
                self.service.spreadsheets()
                .values()
                .get(spreadsheetId=self.sheet_id, range=self.sheet_range)
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
                f"✅ Расписание дежурных обновлено. Загружено {len(self.duty_slots)} записей"
            )

        except HttpError as e:
            logger.error(f"❌ Ошибка Google Sheets API: {e}")
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении расписания дежурных: {e}")

    def _parse_sheet_data(self, values: List[List[str]]):
        """Парсит данные из Google Sheets"""
        self.duty_slots = []

        # Пропускаем заголовки (первая строка)
        for row in values[1:]:
            if len(row) < 4:
                continue

            try:
                duty_slot = self._parse_single_row(row)
                if duty_slot:
                    self.duty_slots.append(duty_slot)
            except (ValueError, IndexError) as e:
                logger.warning(f"⚠️ Ошибка парсинга строки: {row} - {e}")
                continue

        # Сортируем слоты по времени начала
        self.duty_slots.sort(key=lambda x: x.start_time)
        logger.info(f"📅 Загружено {len(self.duty_slots)} временных слотов")

    def _parse_single_row(self, row: List[str]) -> Optional[DutySlot]:
        """Парсит одну строку из Google Sheets"""
        start_time = row[0].strip()
        end_time = row[1].strip()
        name = row[2].strip()
        slack_id = row[3].strip()

        # Пропускаем записи с #N/A в slack_id
        if slack_id == "#N/A" or not slack_id or slack_id.lower() == "n/a":
            logger.debug(
                f"Пропускаем запись для {name} ({start_time}-{end_time}) из-за отсутствующего Slack ID."
            )
            return None

        # Удаляем @ из Slack ID, если он есть
        if slack_id.startswith("@"):
            slack_id = slack_id[1:]

        # Пропускаем пустые строки
        if not name or not slack_id:
            return None

        # Пытаемся распарсить даты и преобразовать их в время
        start_time_parsed = self._parse_time_from_date(start_time)
        end_time_parsed = self._parse_time_from_date(end_time)
        
        if not start_time_parsed or not end_time_parsed:
            logger.warning(
                f"⚠️ Не удалось распарсить время для {name}: {start_time} - {end_time}"
            )
            return None

        duty_slot = DutySlot(
            start_time=start_time_parsed,
            end_time=end_time_parsed,
            name=name,
            slack_id=slack_id,
        )

        logger.debug(
            f"✅ Добавлен слот: {name} ({slack_id}) с {start_time_parsed} по {end_time_parsed}"
        )
        return duty_slot

    def _validate_time_format(self, time_str: str) -> bool:
        """Проверяет формат времени HH:MM"""
        try:
            datetime.strptime(time_str, "%H:%M")
            return True
        except ValueError:
            return False

    def _parse_time_from_date(self, date_str: str) -> Optional[str]:
        """Парсит время из строки даты или возвращает время в формате HH:MM"""
        if not date_str:
            return None

        # Убираем лишние пробелы
        date_str = date_str.strip()

        # Если это уже время в формате HH:MM, возвращаем как есть
        if self._validate_time_format(date_str):
            return date_str

        # Пытаемся распарсить как дату
        parsed_date = self._parse_date(date_str)
        if parsed_date:
            # Возвращаем время в формате HH:MM
            return parsed_date.strftime("%H:%M")

        # Если это формат "число - дата" (например "5 - 27.01.25")
        if " - " in date_str:
            parts = date_str.split(" - ")
            if len(parts) == 2:
                # Берем первое число как час
                try:
                    hour = int(parts[0].strip())
                    if 0 <= hour <= 23:
                        return f"{hour:02d}:00"
                except ValueError:
                    pass

        # Если это просто число (например "5")
        try:
            hour = int(date_str)
            if 0 <= hour <= 23:
                return f"{hour:02d}:00"
        except ValueError:
            pass

        logger.warning(f"⚠️ Не удалось распарсить время из: {date_str}")
        return None

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

    def get_current_duty_person(self) -> Optional[DutySlot]:
        """Возвращает текущего дежурного по времени"""
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

        # Получаем московское время
        
        moscow_tz = pytz.timezone('Europe/Moscow')
        current_time = datetime.now(moscow_tz).strftime("%H:%M")
        logger.info(f"📅 Текущее время (Москва): {current_time}")
        logger.info(f"📋 Количество слотов в расписании: {len(self.duty_slots)}")

        for duty_slot in self.duty_slots:
            logger.info(
                f"🔍 Проверяем слот: {duty_slot.name} ({duty_slot.start_time} - {duty_slot.end_time})"
            )
            if duty_slot.start_time <= current_time <= duty_slot.end_time:
                logger.info(
                    f"✅ Текущий дежурный найден: {duty_slot.name} ({duty_slot.slack_id})"
                )
                return duty_slot

        logger.warning("⚠️ Не найден дежурный на текущее время")
        return None

    def get_duty_schedule_info(self) -> str:
        """Возвращает информацию о расписании дежурных"""
        if not self.duty_slots:
            return "Расписание дежурных не загружено"

        info = f"Расписание дежурных (обновлено: {self.last_update.strftime('%d.%m.%Y %H:%M') if self.last_update else 'никогда'}):\n"

        for duty_slot in self.duty_slots:
            info += f"• {duty_slot.start_time}-{duty_slot.end_time}: {duty_slot.name} ({duty_slot.slack_id})\n"

        return info
