FROM python:3.10-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements_api.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements_api.txt

COPY src ./src
COPY data ./data
COPY models ./models

EXPOSE 8000

CMD ["uvicorn", "src.api.flight_ops_api:app", "--host", "0.0.0.0", "--port", "8000"]