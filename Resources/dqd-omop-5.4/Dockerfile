FROM r-base:4.4.3
RUN echo 'install.packages("remotes")' | r
RUN echo 'remotes::install_github("OHDSI/DataQualityDashboard")' | r
RUN apt-get update
RUN apt-get install -y default-jre default-jdk

RUN echo 'remotes::install_github("OHDSI/DbDiagnostics")' | r
RUN echo 'remotes::install_github("OHDSI/DatabaseConnector")' | r
RUN mkdir /app/
RUN apt-get update
RUN apt-get install -y default-jdk default-jre
RUN java --version
RUN apt-get install -y curl xml2
RUN apt-get install -y libcurl4-openssl-dev libxml2-dev

RUN echo 'install.packages("ParallelLogger")' | r
RUN echo 'install.packages("DBI")' | r
RUN echo 'install.packages("rJava")' | r
RUN echo 'install.packages("RJDBC")' | r
RUN export LD_LIBRARY_PATH=/usr/lib/jvm/java-21-openjdk-amd64/lib/server/; echo 'library("rJava")' | r

RUN echo 'install.packages("Achilles")' | r
RUN echo 'install.packages("bit")' | r
RUN echo 'install.packages("curl")' | r
RUN echo 'install.packages("TTR")' | r
RUN echo 'install.packages("quantmod")' | r
RUN echo 'install.packages("tseries") ' | r

RUN export LD_LIBRARY_PATH=/usr/lib/jvm/java-21-openjdk-amd64/lib/server/; echo 'library("Achilles")' | r
RUN export LD_LIBRARY_PATH=/usr/lib/jvm/java-21-openjdk-amd64/lib/server/; echo 'library("bit")' | r
RUN export LD_LIBRARY_PATH=/usr/lib/jvm/jashiva-21-openjdk-amd64/lib/server/; echo 'library("curl")' | r
RUN export LD_LIBRARY_PATH=/usr/lib/jvm/java-21-openjdk-amd64/lib/server/; echo 'library("TTR")' | r
RUN export LD_LIBRARY_PATH=/usr/lib/jvm/java-21-openjdk-amd64/lib/server/; echo 'library("quantmod")' | r
RUN export LD_LIBRARY_PATH=/usr/lib/jvm/java-21-openjdk-amd64/lib/server/; echo 'library("tseries") ' | r
RUN export LD_LIBRARY_PATH=/usr/lib/jvm/java-21-openjdk-amd64/lib/server/; echo 'install.packages("RSQLite")' | r

RUN echo 'if (!require("DbDiagnostics")){remotes::install_github("OHDSI/DbDiagnostics")}' | r
RUN echo 'if (!require("OHDSI/DatabaseConnector")){remotes::install_github("OHDSI/DatabaseConnector")}' | r
RUN echo 'if (!require("DT")){install.packages("DT")}' | r
#COPY qc-lib.r /app/
#RUN export LD_LIBRARY_PATH=/usr/lib/jvm/java-21-openjdk-amd64/lib/server/; r /app/qc-lib.r


RUN apt-get install -y python3
RUN apt-get clean
WORKDIR /app

ENTRYPOINT while true; do python3 server.py; sleep 3; done