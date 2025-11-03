package com.example.individual_consumer_project.consumer.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ApplicantConsumerDltService {

    @KafkaListener(
            topics = "applicant.send.dlt",
            groupId = "applicant-send-dlt-group"
    )
    public void consume(String message){

        // 로그 시스템에 전송
        System.out.println("로그 시스템에 전송 : " + message);

        // 알림 발송
        System.out.println("Slack에 알림 발송");


    }

}
