# ARG Trading Desk - pasos 1 a 4

Incluye:
1. Alertas en tiempo real (consola + Telegram opcional)
2. Persistencia SQLite
3. Filtro profesional de señales
4. Paper trading

## Local
```bash
python -m venv venv
venv\\Scripts\\activate
pip install -r requirements.txt
set USE_IOL=0
set TEST_PERTURB=1
python -m uvicorn app:app --reload
```

## Railway
Variables recomendadas para prueba:
- USE_IOL=0
- TEST_PERTURB=1
- CAPITAL=25000000
- ENABLE_CONSOLE_ALERTS=1
- SQLITE_PATH=data/trading_desk.db

Variables opcionales Telegram:
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID
