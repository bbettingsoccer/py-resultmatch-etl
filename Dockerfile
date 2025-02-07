# Pull de la imagen base oficial
FROM python:3.11-slim


# setup del directorio de trabajo
WORKDIR /app

# Configuración de las variables de ambiente
# ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV ENVIRONMENT_TYPE=DEV

RUN apt-get update && apt-get install -y --no-install-recommends \
    net-tools \
    iputils-ping \
    wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


RUN pip install --upgrade pip
COPY requirements.txt /app/
RUN pip install -r requirements.txt
COPY . /app

CMD ["spark-submit", "--master", "spark://master:7077", "/app/__main__.py"]
