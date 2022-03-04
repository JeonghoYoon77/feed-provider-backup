aws ecr get-login-password --region ap-northeast-2 |  docker login --username AWS --password-stdin 605438638109.dkr.ecr.ap-northeast-2.amazonaws.com

docker build -t 605438638109.dkr.ecr.ap-northeast-2.amazonaws.com/feed-provider:latest --push .
