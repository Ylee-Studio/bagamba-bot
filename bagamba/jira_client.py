import logging
from jira import JIRA
from config import Config

logger = logging.getLogger(__name__)


class JiraClient:
    def __init__(self):
        self.jira = JIRA(
            server=Config.JIRA_URL,
            basic_auth=(Config.JIRA_USERNAME, Config.JIRA_API_TOKEN),
        )
        self.project_key = Config.JIRA_PROJECT_KEY
        self.close_transition = {"id": 91, "name": "Done"}
        self.in_progress_transition = {"id": 111, "name": "In Progress"}

    def create_incident_ticket(
        self, title: str, description: str, reporter: str, thread_url: str = None
    ) -> str:
        """Создает новый тикет инцидента в Jira"""
        try:
            thread_info = f"\n**Ссылка на тред:** {thread_url}" if thread_url else ""

            issue_dict = {
                "project": {"key": Config.JIRA_PROJECT_KEY},
                "summary": title,
                "description": f"""
**Описание инцидента:**
{description}

**Сообщил:** {reporter}
**Источник:** Slack Bot{thread_info}
                """.strip(),
                "issuetype": {"name": Config.JIRA_ISSUE_TYPE},
            }

            issue = self.jira.create_issue(fields=issue_dict)
            logger.info(f"Создан тикет Jira: {issue.key}")
            return issue.key

        except Exception as e:
            logger.error(f"Ошибка при создании тикета Jira: {e}")
            raise

    def close_incident_ticket(
        self, ticket_key: str, resolution: str = "Resolved"
    ) -> bool:
        """Закрывает тикет инцидента в Jira"""
        try:
            issue = self.jira.issue(ticket_key)
            self.jira.transition_issue(issue, self.close_transition["id"])
            logger.info(f"Тикет {ticket_key} закрыт")
            return True

        except Exception as e:
            logger.error(f"Ошибка при закрытии тикета {ticket_key}: {e}")
            return False

    def assign_ticket(self, ticket_key: str, assignee_email: str) -> bool:
        """Назначает тикет пользователю и переводит в статус In Progress"""
        try:
            # Сначала пытаемся найти пользователя по email
            account_id = None
            try:
                users = self.jira.search_users(query=assignee_email)
                if len(users) == 1:
                    account_id = users[0].accountId
                else:
                    logger.info(f"Finding Users: {users}")
                    for user in users:
                        logger.info(f"User email {getattr(user, 'email')}")
                        if (
                            user.emailAddress
                            and user.emailAddress.lower() == assignee_email.lower()
                        ):
                            account_id = user.accountId
                            logger.info(
                                f"Найден пользователь {user.displayName} с email {assignee_email}"
                            )
                            break
            except Exception as e:
                logger.warning(
                    f"Не удалось найти пользователя по email {assignee_email}: {e}"
                )

            if not account_id:
                logger.warning(
                    f"Пользователь с email {assignee_email} не найден в Jira. Назначение пропущено."
                )
                return False

            issue = self.jira.issue(ticket_key)
            issue.update(assignee={"accountId": account_id})
            self.jira.transition_issue(issue, self.in_progress_transition["id"])
            logger.info(
                f"Тикет {ticket_key} назначен пользователю {assignee_email} (ID: {account_id}) и переведен в статус In Progress"
            )
            return True
        except Exception as e:
            logger.error(
                f"Ошибка при назначении тикета {ticket_key} пользователю {assignee_email}: {e}"
            )
            return False

    def add_comment(self, ticket_key: str, comment: str, author: str) -> bool:
        """Добавляет комментарий к тикету"""
        try:
            formatted_comment = f"**{author}:** {comment}"
            self.jira.add_comment(ticket_key, formatted_comment)
            logger.info(f"Добавлен комментарий к тикету {ticket_key}")
            return True
        except Exception as e:
            logger.error(
                f"Ошибка при добавлении комментария к тикету {ticket_key}: {e}"
            )
            return False

    def transition_to_in_progress(self, ticket_key: str) -> bool:
        """Переводит тикет в статус In Progress"""
        try:
            issue = self.jira.issue(ticket_key)
            self.jira.transition_issue(issue, self.in_progress_transition["id"])
            logger.info(f"Тикет {ticket_key} переведен в статус In Progress")

        except Exception as e:
            logger.error(
                f"Ошибка при переводе тикета {ticket_key} в статус In Progress: {e}"
            )
            return False

    def get_ticket_url(self, ticket_key: str) -> str:
        """Возвращает URL тикета в Jira"""
        return f"{Config.JIRA_URL}/browse/{ticket_key}"
