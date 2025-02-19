FROM python:3.11

WORKDIR /app

# Installare le dipendenze
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copiare il codice sorgente
COPY . .

# Comando di avvio
CMD ["python", "main.py"]
