FROM python:3.9.16

#RUN cp -R . /app
RUN mkdir /var/www

WORKDIR /app
RUN chown -R www-data /app /var/www

# Install kubectl
RUN apt-get install -y apt-transport-https gnupg
#RUN curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
#RUN echo "deb https://apt.kubernetes.io/ kubernetes-xenial main" | tee -a /etc/apt/sources.list.d/kubernetes.list
RUN curl -L "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" -o /bin/kubectl
RUN chmod +x  /bin/kubectl

RUN apt-get update
#RUN apt-get install -y kubectl
RUN apt-get install -y sudo

# Install AWS
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN sudo ./aws/install
RUN rm -rf awscliv2.zip aws

RUN pip install "pip==23.3"
RUN pip install "click==8.1.3"
RUN pip install "certifi>=2024.7.4"
RUN pip install "frozendict~=2.3.4"
RUN pip install "python-dateutil~=2.7.0"
RUN pip install "setuptools>=65.5.1"
RUN pip install "typing_extensions~=4.3.0"
RUN pip install "urllib3~=2.2.2"
RUN pip install "pyyaml==6.0"
RUN pip install "setproctitle==1.3.2"
RUN pip install "sqlalchemy>=2.0.0"
RUN pip install "psycopg2"
RUN pip install "sentry-sdk"
RUN pip install "labkey==2.6.1"
RUN pip install "python-json-logger"
RUN pip install "sqlglot===25.24.5"
RUN pip install "PyMySQL===1.1.1"

RUN apt-get install psmisc

RUN apt-get install -y libsasl2-dev  libsasl2-modules
RUN pip install "pyhive[hive]===1.1.1"


RUN wget https://github.com/kubernetes-sigs/aws-iam-authenticator/releases/download/v0.6.14/aws-iam-authenticator_0.6.14_linux_amd64 -O /bin/aws-iam-authenticator
RUN chmod +x /bin/aws-iam-authenticator

# Install docker
RUN apt-get --yes install ca-certificates curl
RUN install -m 0755 -d /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
RUN chmod a+r /etc/apt/keyrings/docker.asc
RUN echo  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update
RUN apt-get install --yes docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin


# ENTRYPOINT pip install -r requirements.txt; (while true; do echo 1; sleep 1; done) | xargs -P 5 -I {} python main.py
ENTRYPOINT pip install -r requirements.txt; while true; do python main.py; done