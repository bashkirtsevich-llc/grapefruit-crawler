FROM python:3.6.3

WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

ENV MONGODB_URL=mongodb://mongodb:27017/grapefruit

CMD [ "python", "./app.py" ]