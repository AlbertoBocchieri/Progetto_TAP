FROM python:3.9-slim

WORKDIR /app

# Installare le dipendenze
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copiare il codice sorgente
COPY . .

# Comando di avvio
CMD ["python", "main.py"]
