FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

ARG CACHEBUST=0

COPY requirements.txt ./
RUN echo "CACHEBUST=${CACHEBUST}" && \
    RUN pip install --no-cache-dir -U pip \
    && pip install --no-cache-dir aiogram==3.6.0 aiohttp aiosqlite python-dotenv aiolimiter==1.1.0
    python -c "import aiogram; print('aiogram version in image:', aiogram.__version__)"

COPY . .

CMD ["python", "bot.py"]
