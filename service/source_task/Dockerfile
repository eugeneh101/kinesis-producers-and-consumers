FROM python:3.10-alpine

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY app.py app.py

ENTRYPOINT ["python3", "app.py"]