## AWS ECR 로그인

```
aws configure
aws ecr get-login-password --region ap-northeast-2 |  docker login --username AWS --password-stdin 605438638109.dkr.ecr.ap-northeast-2.amazonaws.com
```

## 도커 이미지 작업하기

```
docker pull 605438638109.dkr.ecr.ap-northeast-2.amazonaws.com/feed-provider:latest
```

```
sudo docker run -idt 605438638109.dkr.ecr.ap-northeast-2.amazonaws.com/feed-provider:latest
sudo docker exec -it <컨테이너ID> bash
```

```
docker commit <ID> 605438638109.dkr.ecr.ap-northeast-2.amazonaws.com/feed-provider:latest
```

```
sudo docker build -t 605438638109.dkr.ecr.ap-northeast-2.amazonaws.com/feed-provider:latest
```

```
docker push 605438638109.dkr.ecr.ap-northeast-2.amazonaws.com/feed-provider:latest
```

## AWS Batch에 추가하기

1. 작업 정의 탭으로 이동
2. 작업 생성
3. Fargate로 선택
4. 이미지는 605438638109.dkr.ecr.ap-northeast-2.amazonaws.com/feed-provider:latest
5. 명령은 ...
6. 퍼블릭 IP 할당 활성화
7. CPU나 메모리는 필요에 맞춰 조절
8. 클라우드와치에서 이벤트->규칙 생성
9. 일정/크론 표현식으로 일정에 대한 크론식 정의 (예: 0 9,21 \* _ ? _)
10. 대상은 배치 작업 대기열로 선택, 작업 대기열과 작업 정의의 ARN을 복사해넣어 생성
