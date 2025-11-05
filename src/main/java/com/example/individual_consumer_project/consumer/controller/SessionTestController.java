package com.example.individual_consumer_project.consumer.controller;


import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequestMapping("/subscribe")
public class SessionTestController {

    @GetMapping("/session")
    public ResponseEntity<String> subscribeSession(HttpServletRequest request) {

        HttpSession session = request.getSession(false);

        if (session != null) {
            String sessionId = session.getId();
            return ResponseEntity.ok(sessionId);
        }else{
            return ResponseEntity.ok("session is null");
        }
    }

}
