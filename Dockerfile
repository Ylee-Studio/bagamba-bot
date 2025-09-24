FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
RUN pip install --upgrade pip
COPY bagamba/uv.lock ./uv.lock
COPY bagamba/pyproject.toml ./pyproject.toml
RUN uv sync --locked --python-preference system

COPY bagamba/ .

CMD ["uvicorn", "server:api", "--host", "0.0.0.0", "--port", "8001", "--proxy-headers"]
