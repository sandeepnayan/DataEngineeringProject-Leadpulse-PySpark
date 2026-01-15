# Base image with Java 17 (good for Spark)
FROM eclipse-temurin:17-jdk-jammy

# Avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Set working directory
WORKDIR /app

# Install Python & system dependencies
RUN apt-get update && \
    apt-get install -y \
        python3 \
        python3-pip \
        curl \
        unzip && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="$JAVA_HOME/bin:$PATH"

# Upgrade pip & install Jupyter (single layer)
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir jupyter notebook

# Copy only requirements first (better caching)
COPY requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app

# Expose Jupyter port
EXPOSE 8888

# Start Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--no-browser"]
