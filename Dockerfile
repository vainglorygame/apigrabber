FROM python:3.5-alpine
ADD requirements.txt /
RUN apk add --no-cache ca-certificates gcc linux-headers build-base
RUN pip install -r /requirements.txt
CMD ["python3", "/apps/api/api.py"]
