FROM python:3.6.3

LABEL maintainer="bashkirtsevich@gmail.com"

WORKDIR /usr/src/app
COPY . .
RUN pip install -r requirements.txt

CMD [ "python", "app.py" ]
