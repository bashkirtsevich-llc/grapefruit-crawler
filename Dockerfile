FROM python:3.6.3

RUN mkdir -p /app WORKDIR /app
COPY requirements.txt ./ RUN pip install -r requirements.txt

COPY . .

CMD [ "python", "./app.py" ]