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


ENTRYPOINT  pip install -r requirements.txt; sleep 1000000