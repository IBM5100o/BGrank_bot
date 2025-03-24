# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.12.0

FROM python:${PYTHON_VERSION}-slim

WORKDIR /code

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["python", "app.py"]
