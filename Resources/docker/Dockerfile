FROM python:3.9.16

#RUN cp -R . /app
RUN mkdir /var/www

WORKDIR /app
RUN chown -R www-data /app /var/www

# Install kubectl
RUN apt-get install -y apt-transport-https gnupg
RUN curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN echo "deb https://apt.kubernetes.io/ kubernetes-xenial main" | tee -a /etc/apt/sources.list.d/kubernetes.list
RUN apt-get update
RUN apt-get install -y kubectl
RUN apt-get install -y sudo

# Install AWS
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN sudo ./aws/install
RUN rm -rf awscliv2.zip aws

RUN pip install "click==8.1.3"
RUN pip install "certifi>=14.5.14"
RUN pip install "frozendict~=2.3.4"
RUN pip install "python-dateutil~=2.7.0"
RUN pip install "setuptools>=21.0.0"
RUN pip install "typing_extensions~=4.3.0"
RUN pip install "urllib3~=1.26.7"
RUN pip install "pyyaml==6.0"
RUN pip install "setproctitle==1.3.2"
RUN pip install "sqlalchemy>=2.0.0"
RUN pip install "psycopg2"
RUN pip install "sentry-sdk"

RUN apt-get install psmisc

# ENTRYPOINT pip install -r requirements.txt; (while true; do echo 1; sleep 1; done) | xargs -P 5 -I {} python main.py
ENTRYPOINT pip install -r requirements.txt; while true; do python main.py; done