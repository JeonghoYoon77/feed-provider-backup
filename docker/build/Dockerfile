FROM node:21

WORKDIR /home

RUN git config --global credential.helper store

COPY ./.git-credentials /home/.git-credentials
COPY ./.git-credentials /root/.git-credentials
COPY ./run.sh /home/run.sh
RUN apt update -qq
RUN apt install -y curl git unzip
RUN git config --global credential.helper store
RUN git clone https://github.com/Fetching-Korea/feed-provider ./feed-provider
RUN ls
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install
COPY ./.aws/config /root/.aws/config
COPY ./.aws/credentials /root/.aws/credentials
COPY ./.aws/config /home/.aws/config
COPY ./.aws/credentials /home/.aws/credentials
WORKDIR feed-provider
RUN npm install
WORKDIR /home

ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENTRYPOINT [ "/bin/bash", "./run.sh" ]
