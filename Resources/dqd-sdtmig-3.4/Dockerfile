FROM --platform=linux/amd64 ubuntu:22.04

ENV TZ=America/New_York
ENV DEBIAN_FRONTEND=noninteractive

# Install Python 3.12
RUN apt-get update && \
    apt-get install -y software-properties-common curl git && \
    add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get update -y && \
    apt-get install -y python3.12 python3.12-dev python3-pip && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12

# Set working directory
WORKDIR /app

RUN git clone -b v0.13.0 https://github.com/cdisc-org/cdisc-rules-engine.git /app

# Install dependencies exactly
RUN python3.12 -m pip install --upgrade pip && \
    pip install setuptools wheel twine && \
    pip install --no-deps -r requirements.txt && \
    pip install -r requirements.txt --no-cache-dir

# Build binary
RUN pyinstaller --onedir \
    --contents-directory "." \
    core.py \
    --dist ./dist/output/core-ubuntu-22.04 \
    --collect-submodules pyreadstat \
    --add-data="resources/cache:resources/cache" \
    --add-data="resources/templates:resources/templates" \
    --add-data="resources/schema:resources/schema" \
    --add-data="tests/resources/datasets:tests/resources/datasets"

# Set permissions and verify
RUN chmod +x /app/dist/output/core-ubuntu-22.04/core/core && \
    file /app/dist/output/core-ubuntu-22.04/core/core && \
    echo "Executable built successfully"

ENTRYPOINT while true; do python3 /server/server.py; sleep 3; done
