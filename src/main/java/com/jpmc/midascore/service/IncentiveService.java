package com.jpmc.midascore.service;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class IncentiveService {
    private final RestTemplate restTemplate;
    private final String incentiveApiUrl;

    public IncentiveService(@Value("${incentive.api.url:http://localhost:8080/incentive}") String incentiveApiUrl) {
        this.restTemplate = new RestTemplate();
        this.incentiveApiUrl = incentiveApiUrl;
    }

    public float getIncentiveAmount(Transaction transaction) {
        Incentive incentive = restTemplate.postForObject(incentiveApiUrl, transaction, Incentive.class);
        return incentive != null ? incentive.getAmount() : 0;
    }
} 
