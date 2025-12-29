FROM python:3.9

WORKDIR /app

# Install system dependencies for snappy compression
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install -r requirements.txt

CMD ["python","src/FinnhubProducer.py"]

