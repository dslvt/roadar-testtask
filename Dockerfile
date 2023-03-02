FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY controller . 
COPY manipulator . 
COPY sensor . 