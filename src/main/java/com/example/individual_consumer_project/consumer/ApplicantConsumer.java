package com.example.individual_consumer_project.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Service
public class ApplicantConsumer {

    @KafkaListener(
            topics = "applicant.send",
            groupId = "applicant-send-group",
            concurrency = "3" // 멀티스레드
    )
    @RetryableTopic(
            attempts = "5", // 총 5번에 시도
            backoff = @Backoff(delay = 1000, multiplier = 2)  // 재시도 시간 설정 (delay 1초를 기다리다가 그 다음 multiplier로 인해서 x2로 들어간다. 현업에서는 이렇게 한다.)
            ,  dltTopicSuffix = ".dlt" // 토픽명.dlt

    )
    public void consume(String message){
        System.out.println("Kafka로부터 받아온 메시지 : " + message);

        ApplicantConsumerMessage applicantConsumerMessage = ApplicantConsumerMessage.fromJson(message);

        System.out.println("applicantConsumerMessage : " + applicantConsumerMessage);

        // ... 실제 이메일 발송 로직은 생략 ...
        try {
            Thread.sleep(3000);
        }catch (InterruptedException e){
            throw new RuntimeException("이메일 전송 실패");
        }


        System.out.println("이메일 발송 완료");

    }

}
