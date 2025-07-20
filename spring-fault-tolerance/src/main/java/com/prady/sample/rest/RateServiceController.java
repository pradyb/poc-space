package com.prady.sample.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.prady.sample.dto.RateDTO;
import com.prady.sample.service.RateService;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Prady
 */
@RestController
@RequestMapping("/rates")
@Slf4j
public class RateServiceController {

    @Autowired
    private RateService rateService;

    @PostMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public RateDTO invokeRate(@RequestBody RateDTO rateDTO) {
        log.info("START: Invoking RateService {}", rateDTO.getRequestData());
        RateDTO dto = rateService.invokeRateService(rateDTO);
        log.info("END: Invoked RateService {}", rateDTO.getRequestData());
        return dto;
    }
}
