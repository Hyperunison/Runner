FROM python:3.9.16

RUN mkdir /var/www

WORKDIR /app
RUN chown -R www-data /app /var/www

RUN apt-get update
RUN apt-get install -y apt-transport-https gnupg sudo libsasl2-dev libsasl2-modules psmisc

# Install kubectl
RUN curl -L "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" -o /bin/kubectl
RUN chmod +x /bin/kubectl

# Install AWS
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && unzip awscliv2.zip && sudo ./aws/install
RUN rm -rf awscliv2.zip aws

RUN pip install "xport==3.6.1" \
     "numpy==1.26.4" \
     "pip==23.3" \
     "click==8.1.3" \
     "certifi>=2024.7.4" \
     "frozendict~=2.3.4" \
     "python-dateutil~=2.7.0" \
     "setuptools>=65.5.1<81" \
     "typing_extensions~=4.3.0" \
     "urllib3~=2.2.2" \
     "pyyaml==6.0" \
     "setproctitle==1.3.2" \
     "sqlalchemy>=2.0.0" \
     "psycopg2" \
     "sentry-sdk" \
     "labkey==2.6.1" \
     "python-json-logger" \
     "sqlglot===25.24.5" \
     "PyMySQL===1.1.1" \
     "pyhive==0.7.0" \
     "pure-sasl==0.6.2" \
     "thrift==0.21.0" \
     "thrift-sasl==0.4.3" \
     "psutil==7.0.0"

# Install docker
RUN apt-get --yes install ca-certificates curl
RUN install -m 0755 -d /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
RUN chmod a+r /etc/apt/keyrings/docker.asc
RUN echo  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update
RUN apt-get install --yes docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Clean cache
RUN apt-get clean
RUN pip cache purge

# ENTRYPOINT pip install -r requirements.txt; (while true; do echo 1; sleep 1; done) | xargs -P 5 -I {} python main.py
ENTRYPOINT pip install -r requirements.txt; while true; do python main.py; sleep 3; done
