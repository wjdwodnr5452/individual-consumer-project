package com.example.individual_consumer_project.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
@RequiredArgsConstructor
public class ApplicantConsumer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CopyOnWriteArrayList<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    
    // 서버 요청 상관없이 프론트 웹페이지에 데이터 보내기
    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        emitters.add(emitter);

        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));

        return emitter;
    }


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

        System.out.println("applicantConsumerMessage : " + applicantConsumerMessage.getApplicantId());

        // ✅ SSE로 프론트에 메시지 전송
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name("applicant-message")
                        .data(applicantConsumerMessage, MediaType.APPLICATION_JSON));
            } catch (IOException e) {
                emitter.completeWithError(e);
                emitters.remove(emitter);
            }
        }

        System.out.println("이메일 발송 완료");

    }

}
