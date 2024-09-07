# Use an official Python runtime as a parent image
FROM python:3.12-slim-bookworm

RUN python3 -m pip install --no-cache-dir --upgrade pip && \
    python3 -m pip install --no-cache-dir \
    azure-identity \
    azure-mgmt-appcontainers \
    azure-mgmt-monitor \ 
    azure-mgmt-loganalytics \
    azure-mgmt-resource \
    pandas \
    ipython


WORKDIR /app
# Add the current directory contents into the container at /app
ADD . /app

CMD ["/app/monitor.py"]
