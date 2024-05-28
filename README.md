# feed-provider

### 설정파일 위치
- https://www.notion.so/c0bf1b7cde624b20b7701911badeaccf

### 간단 설명
- 네이버, 페이스북, 쿠챠, 자이 등에 피드 파일을 업로드 하는 프로젝트입니다.
- 배치잡은 AWS 배치를 통해 실행되고 있습니다.
    - 잡 설정은 ```AWS Batch > Job definitions``` 에 있습니다.
    - 잡 실행 결과는 ```AWS Batch > Jobs``` 에 있습니다.
    - 일반적으로 크론잡으로 트리거되고 있지만, submit new job 을 통해 직접 트리거 할 수도 있습니다.
- 잡 스케줄링은 AWS EventBridge 를 통해 트리거 되고 있습니다.
    - 크론 설정은 ```AWS EventBridge > Buses > Rules``` 에 있습니다.

### 실행
```shell
npm run start -- -f ${피드 이름} -c ${청크 사이즈}
```

- 또는

```shell
tsc && node --max-old-space-size=16384 dist/src/app.js -f ${피드 이름} -c ${청크 사이즈}
```

