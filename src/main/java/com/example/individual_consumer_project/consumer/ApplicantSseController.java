package com.example.individual_consumer_project.consumer;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequiredArgsConstructor
public class ApplicantSseController {

    private final ApplicantConsumer applicantConsumer;

    @GetMapping("/subscribe/applicant")
    public SseEmitter subscribe() {
        return applicantConsumer.subscribe();
    }

}
