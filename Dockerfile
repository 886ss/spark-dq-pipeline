# Docker镜像定义：基于apache/spark-py(v3.4.0 JVM+Python3.9)，安装7个Python依赖，复制src+config到/app
FROM apache/spark-py:latest
USER root
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir pyspark==3.4.0 pandas==2.1.1 numpy==1.26.0 pyyaml==6.0.1 tabulate==0.9.0 psycopg2-binary==2.9.9 scipy==1.11.3
COPY ./src ./src
COPY ./config ./config
RUN mkdir -p /app/data /app/reports
