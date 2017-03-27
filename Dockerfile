FROM python:3.6-alpine
ADD requirements.txt /code/requirements.txt
WORKDIR /code
RUN pip install -r requirements.txt
CMD ["python", "worker.py"]
