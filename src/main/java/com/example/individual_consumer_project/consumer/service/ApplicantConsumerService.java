package com.example.individual_consumer_project.consumer.service;

import com.example.individual_consumer_project.comm.encrypt.EncryptionService;
import com.example.individual_consumer_project.consumer.message.ApplicantConsumerMessage;
import com.example.individual_consumer_project.consumer.repository.UserRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
@RequiredArgsConstructor
@Slf4j
public class ApplicantConsumerService {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<Long, List<SseEmitter>> emitters = new ConcurrentHashMap<>();
    private final UserRepository userRepository;
    private final EncryptionService encryptionService;
    
   // 서버 요청 상관없이 프론트 웹페이지에 데이터 보내기
    public SseEmitter subscribe(Long userId) {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        emitters.computeIfAbsent(userId, k -> new CopyOnWriteArrayList<>()).add(emitter);

        emitter.onCompletion(() -> emitters.getOrDefault(userId, new CopyOnWriteArrayList<>()).remove(emitter));
        emitter.onTimeout(()     -> emitters.getOrDefault(userId, new CopyOnWriteArrayList<>()).remove(emitter));
        emitter.onError(e ->     emitters.getOrDefault(userId, new CopyOnWriteArrayList<>()).remove(emitter));

        // 연결 확인
        try { emitter.send(SseEmitter.event().name("connected").data("ok")); } catch (Exception ignored) {}

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

        ApplicantConsumerMessage applicantConsumerMessage = ApplicantConsumerMessage.fromJson(message);

        log.info("applicantConsumerMessage {} : " , applicantConsumerMessage.getApplicantId());

        // 해당 유저 가져오기
       List<SseEmitter> list = emitters.get(applicantConsumerMessage.getUserId());

       List<SseEmitter> toRemove = new ArrayList<>();

       for (SseEmitter emitter : list) {
           try {
               emitter.send(
                       SseEmitter.event()
                               .name("applicant-message")
                               .data(applicantConsumerMessage, MediaType.APPLICATION_JSON) // Jackson으로 JSON 직렬화
                       // .id(String.valueOf(System.currentTimeMillis())) // (옵션) 유실 복구용
               );
           } catch (Exception e) {
               log.warn("SSE send failed for userId={}, removing emitter. err={}", applicantConsumerMessage.getUserId(), e.toString());
               try { emitter.completeWithError(e); } catch (Exception ignore) {}
               toRemove.add(emitter);
           }

        System.out.println("이메일 발송 완료");

    }

       if (!toRemove.isEmpty()) {
           list.removeAll(toRemove);
       }

       log.info("SSE pushed to userId={} ({} success, {} removed)",
               applicantConsumerMessage.getUserId(), list.size(), toRemove.size());



 }
}
