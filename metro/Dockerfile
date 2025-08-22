# Start from Airflow official image
FROM apache/airflow:2.10.2-python3.11

USER root

# Install basic utilities
RUN apt-get update && apt-get install -y \
        wget \
        tar \
        curl \
        unzip \
        ca-certificates \
        && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install JDK 11 from Adoptium
RUN wget https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.21+9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.21_9.tar.gz \
    && tar -xzf OpenJDK11U-jdk_x64_linux_hotspot_11.0.21_9.tar.gz -C /opt/ \
    && rm OpenJDK11U-jdk_x64_linux_hotspot_11.0.21_9.tar.gz

# Install Hadoop 3.3.6
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz \
    && tar -xzf hadoop-3.3.6.tar.gz -C /opt/ \
    && mv /opt/hadoop-3.3.6 /opt/hadoop \
    && rm hadoop-3.3.6.tar.gz

# Set environment variables for Java and Hadoop (for all users)
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
ENV JAVA_HOME=/opt/jdk-11.0.21+9
ENV PATH=$JAVA_HOME/bin:$PATH
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH

# Switch to airflow user
USER airflow

# Copy and install Python dependencies
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Expose Airflow webserver and Flower ports
EXPOSE 8080 8793
