FROM python:3.8
RUN mkdir app
COPY . /app
WORKDIR app
RUN pip3 install -r requirements.txt
CMD ["python3", "prefect_102.py"]