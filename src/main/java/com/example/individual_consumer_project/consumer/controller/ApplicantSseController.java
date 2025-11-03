package com.example.individual_consumer_project.consumer.controller;

import com.example.individual_consumer_project.SessionConst;
import com.example.individual_consumer_project.consumer.entity.User;
import com.example.individual_consumer_project.consumer.service.ApplicantConsumerService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.SessionAttribute;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequiredArgsConstructor
@Slf4j
public class ApplicantSseController {

    private final ApplicantConsumerService applicantConsumer;

    @GetMapping("/subscribe/applicant")
    public SseEmitter subscribe(@SessionAttribute(SessionConst.LOGIN_MEMBER) User user) {

        if(user != null) {

            log.info("user :  {} " , user);

        }else {
            System.out.println("user :  " + "세션값 없음");
        }

        return applicantConsumer.subscribe();
    }

}
