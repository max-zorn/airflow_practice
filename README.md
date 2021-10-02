# airflow_practice

It is necessary to determine:

Variables
- BOT_TOKEN = telegram bot token (receive from https://t.me/BotFather)
- MAX_ZORN_CHAT_ID = your personal chat ID in telegram

Connections
- dog_api = https://api.thedogapi.com
- cat_api = https://api.thecatapi.com

PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:- telebot pyTelegramBotAPI}
