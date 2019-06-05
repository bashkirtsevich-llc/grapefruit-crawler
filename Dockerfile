FROM python:3.6.3-alpine3.7

MAINTAINER Bashkirtsev D.A. <bashkirtsevich@gmail.com>
LABEL maintainer="bashkirtsevich@gmail.com"

WORKDIR /usr/src/app
ENV CRAWLER_WRITER=file
COPY . .
RUN apk update && apk upgrade && \
    apk add --no-cache git && \
    pip install -r requirements.txt

CMD [ "python", "app.py" ]
