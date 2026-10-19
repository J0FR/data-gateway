FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
# Server only: the Presto/Hive drivers are not needed here.
RUN pip install --no-cache-dir ".[server]"
ENV RELAY_PORT=8080
EXPOSE 8080
CMD ["python", "-m", "data_gateway.server"]
