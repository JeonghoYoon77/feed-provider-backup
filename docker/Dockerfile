FROM 605438638109.dkr.ecr.ap-northeast-2.amazonaws.com/feed-provider:base
ENV TZ=Asia/Seoul
ENV HOME=/home

RUN git config --global credential.helper store

WORKDIR /home/feed-provider
RUN git pull

RUN aws s3 sync s3://fetching-env/feed-provider .

RUN npm install
RUN npm run build
WORKDIR $HOME
RUN cd $HOME

COPY ./run.sh /home/run.sh

ENTRYPOINT [ "/bin/bash", "./run.sh" ]
