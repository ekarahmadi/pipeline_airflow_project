# Menggunakan image dasar Airflow
FROM apache/airflow:2.7.0

# Set environment agar instalasi non-interaktif
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies sistem tambahan yang diperlukan untuk pip install
USER root
RUN apt-get update \
       && apt-get install -y \
       gcc \
       libpq-dev \
       && rm -rf /var/lib/apt/lists/*

# Menyalin file requirements.txt ke dalam container
COPY requirements.txt /requirements.txt

# Kembali ke user airflow sebelum menjalankan pip install
USER airflow

# Install dependencies Python dari requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set direktori kerja
WORKDIR /opt/airflow
