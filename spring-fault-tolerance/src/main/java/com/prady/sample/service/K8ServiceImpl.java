package com.prady.sample.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Prady
 */
@Slf4j
@Service
public class K8ServiceImpl implements K8Service {

    @Autowired
    private KubernetesClient fabric8Client;

    //@Autowired
    //private CoreV1Api kubernetesClient;

    //@Autowired
    //private ApiClient apiClient;

    @Override
    public Map<String, Object> info() {
        return fabric8info();
    }

    public Map<String, Object> fabric8info() {
        log.info("In K8 Info");
        log.info("Client: {}", fabric8Client);

        fabric8Client.namespaces().list().getItems().forEach(n -> log.info("Namespaces: {}", n.getMetadata().getName()));

        fabric8Client.services().inAnyNamespace().list().getItems().forEach(s -> log.info("Service: {}", s.getMetadata().getName()));

        Map<String, Object> deployments = new HashMap<>();
        fabric8Client.apps().deployments().inAnyNamespace().list().getItems().forEach(d -> {
            log.info("Deployment: {}", d.getMetadata().getName());
            log.info("Container Name: {}; Image: {}", d.getSpec().getTemplate().getSpec().getContainers().get(0).getName(), d.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());

            deployments.put(d.getMetadata().getName(), d.getSpec());
        });
        return deployments;
    }

    /*public Map<String, Object> k8clientInfo() {
        log.info("In K8 Info");
        log.info("Client: {}", kubernetesClient);

        try {
            V1ServiceList serviceList = kubernetesClient.listServiceForAllNamespaces(Boolean.TRUE, null, null, null, null, null, null, null, null, Boolean.FALSE);

            serviceList.getItems().forEach(s -> {
                log.info("Service: {}", s.getMetadata().getName());
            });

            AppsV1Api appsV1Api = new AppsV1Api(apiClient);
            V1DeploymentList deploymentList = appsV1Api.listDeploymentForAllNamespaces(Boolean.TRUE, null, null, null, null, null, null, null, null, Boolean.FALSE);

            Map<String, Object> deployments = new HashMap<>();
            deploymentList.getItems().forEach(d -> {
                log.info("Deployment: {}", d.getMetadata().getName());
                log.info("Container Name: {}; Image: {}", d.getSpec().getTemplate().getSpec().getContainers().get(0).getName(), d.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());

                deployments.put(d.getMetadata().getName(), d.getSpec());
            });
            return deployments;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }*/

}
