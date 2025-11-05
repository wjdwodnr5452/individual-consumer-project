package com.example.individual_consumer_project.consumer.controller;

import com.example.individual_consumer_project.SessionConst;
import com.example.individual_consumer_project.consumer.service.ApplicantConsumerService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

@RestController
@RequiredArgsConstructor
@Slf4j
@RequestMapping("/subscribe")
public class ApplicantSseController {

    private final ApplicantConsumerService applicantConsumer;

    @GetMapping("/applicant")
    public SseEmitter subscribe(HttpServletRequest request) {

        HttpSession session = request.getSession(false);
        if (session == null) {
            throw new RuntimeException("로그인이 필요합니다.");
        }

        Map<String, Object> userMap = (Map<String, Object>) session.getAttribute(SessionConst.LOGIN_MEMBER);

        return applicantConsumer.subscribe((Long)userMap.get("id"));
    }

}
