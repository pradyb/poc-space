package com.prady.sample.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.prady.sample.dto.RateDTO;

import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Prady
 */
@Slf4j
@Service
public class RateServiceImpl implements RateService {

    @Autowired
    private RestTemplate restTemplate;

    @Value("${rate.service.url}")
    private String rateServiceUrl;

    @Override
    @CircuitBreaker(name = "rateService")
    @Bulkhead(name = "rateService")
    @Retry(name = "rateService", fallbackMethod = "fallback")
    public RateDTO invokeRateService(RateDTO rateDTO) {
        log.info("Invoking Service call {}", rateDTO.getRequestData());
        ResponseEntity<Void> response = restTemplate.exchange(rateServiceUrl, HttpMethod.POST, null, void.class, rateDTO.getRequestData());
        if (response.getStatusCode().is2xxSuccessful()) {
            rateDTO.setResponseData("SUCCESS");
        }
        log.info("Invoked Service call {}", rateDTO.getRequestData());
        return rateDTO;
    }

    private RateDTO fallback(RateDTO rateDTO, Exception e) throws Exception {
        log.info("In Fallback method");
        rateDTO.setResponseData("FAILED");
        // return rateDTO;
        throw e;
    }
}
