FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app
WORKDIR /app

# 1) кэш-бастер
ARG CACHEBUST=0
RUN echo "CACHEBUST=${CACHEBUST}"

# 2) зависимости
COPY requirements.txt ./ 
RUN pip install -U pip && pip install --no-cache-dir -r requirements.txt

# 3) код
COPY . .

# 4) подчистить старые байткоды
RUN find /app -name '*.pyc' -delete

CMD ["python", "bot.py"]
