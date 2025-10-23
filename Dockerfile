FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Сначала зависимости (кэш эффективнее)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir -r /app/requirements.txt \
 && python -c "import aiogram; print('aiogram version in image:', aiogram.__version__)"

# Потом код
COPY . /app

EXPOSE 8080
CMD ["python", "bot.py"]
