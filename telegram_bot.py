import requests

def send_telegram_message(bot_token, chat_id, text):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = {"chat_id": chat_id, "text": text}
    response = requests.post(url, data=data)
    return response.json()

if __name__ == "__main__":
    BOT_TOKEN = "7747935597:AAHjm45dio5SauGNyzlsx2YXWoRQxO6SmYQ"
    CHAT_ID = "221067200"
    MESSAGE = "Ciao dal tuo bot Telegram!"
    
    result = send_telegram_message(BOT_TOKEN, CHAT_ID, MESSAGE)
    print(result)