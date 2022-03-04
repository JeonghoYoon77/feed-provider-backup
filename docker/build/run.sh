cd /home/feed-provider

git pull

npm install

aws s3 sync s3://fetching-env/feed-provider .

npm start $*
