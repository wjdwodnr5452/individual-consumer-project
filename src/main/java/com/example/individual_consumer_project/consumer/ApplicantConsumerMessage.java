package com.example.individual_consumer_project.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ApplicantConsumerMessage {
    private Long serviceBoardId;
    private Long userId;
    private Long applicantId;

    public ApplicantConsumerMessage(Long serviceBoardId, Long userId, Long applicantId) {
        this.serviceBoardId = serviceBoardId;
        this.userId = userId;
        this.applicantId = applicantId;
    }

    public static ApplicantConsumerMessage fromJson(String json) {
        try{
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(json, ApplicantConsumerMessage.class);
        }catch(JsonProcessingException e){
            throw new RuntimeException("Json 파싱 실패");
        }
    }


    public Long getServiceBoardId() {
        return serviceBoardId;
    }

    public Long getUserId() {
        return userId;
    }

    public Long getApplicantId() {
        return applicantId;
    }
}
