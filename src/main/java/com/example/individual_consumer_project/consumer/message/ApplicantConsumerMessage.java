package com.example.individual_consumer_project.consumer.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class ApplicantConsumerMessage {
    private final Long serviceBoardId;
    private final Long userId;
    private final Long applicantId;

    @JsonCreator
    public ApplicantConsumerMessage(
            @JsonProperty("serviceBoardId") Long serviceBoardId,
            @JsonProperty("userId") Long userId,
            @JsonProperty("applicantId") Long applicantId
    ) {
        this.serviceBoardId = serviceBoardId;
        this.userId = userId;
        this.applicantId = applicantId;
    }

    public static ApplicantConsumerMessage fromJson(String json) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(json, ApplicantConsumerMessage.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Json 파싱 실패: " + e.getMessage(), e);
        }
    }

    public Long getServiceBoardId() { return serviceBoardId; }
    public Long getUserId() { return userId; }
    public Long getApplicantId() { return applicantId; }
}
