package com.prady.sample.rest;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.prady.sample.service.K8Service;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Prady
 */
@RestController
@RequestMapping("/k8")
@Slf4j
public class K8Controller {

    @Autowired
    private K8Service k8Service;

    @GetMapping(value = "/info", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> info() {
        return k8Service.info();
    }

}
