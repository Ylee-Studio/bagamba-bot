from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Slack Configuration
    SLACK_BOT_TOKEN: str
    SLACK_APP_TOKEN: str
    SLACK_SIGNING_SECRET: str

    # Jira Configuration
    JIRA_URL: str
    JIRA_USERNAME: str
    JIRA_API_TOKEN: str
    JIRA_PROJECT_KEY: str
    JIRA_ISSUE_TYPE: str
    JIRA_PRIORITY: str = "High"

    # Bot Configuration
    RESPONSIBLE_USER_ID: str
    NOTIFICATION_INTERVAL_MINUTES: int = 2
    AWAITING_RESPONSE_INTERVAL_MINUTES: int = 2

    # Channel Configuration
    ALLOWED_CHANNELS: list[str] = Field(default_factory=list)
    # Если ALLOWED_CHANNELS пустой, бот работает во всех каналах

    # User Permissions Configuration
    ALLOWED_BUTTON_USERS: list[str] = Field(default_factory=list)
    # Если ALLOWED_BUTTON_USERS пустой, все пользователи могут нажимать кнопки

    # Redis Configuration
    REDIS_URL: str = "redis://localhost:6379"
    REDIS_DB: int = 0
    GOOGLE_SHEET_URL: str | None = None
    GOOGLE_CREDENTIALS_PATH: str | None = None


Config = Config()
