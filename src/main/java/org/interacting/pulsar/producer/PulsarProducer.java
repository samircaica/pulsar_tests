package org.interacting.pulsar.producer;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PulsarProducer {
    PulsarClient client;
    PulsarAdmin admin;

    private java.util.List<String> adminRoles = null;
    private java.util.List<String> allowedClusters = null;
    private String TENANT_NAME = "manning";
    private String NS_NAME = "chapter03";
    private String PULSAR_HOST = "localhost";
    private String HTTP_PORT = "8080";
    private String SERVICE_PORT = "6650";


    public void createTopic() throws PulsarClientException, PulsarAdminException {
        admin = PulsarAdmin.builder()
                .serviceHttpUrl("http://"+PULSAR_HOST+":"+HTTP_PORT)
                .build();

        if(admin.tenants().getTenantInfo(TENANT_NAME) != null) {
            System.out.println("Tenant: "+ TENANT_NAME+" already exists.");
        } else {

            if (adminRoles == null) {
                adminRoles = Collections.emptyList();
            }

            if (allowedClusters == null || allowedClusters.isEmpty()) {
                // Default to all available cluster
                allowedClusters = admin.clusters().getClusters();
            }

            TenantInfoImpl tenantInfo = new TenantInfoImpl(new HashSet<>(adminRoles), new HashSet<>(allowedClusters));
            admin.tenants().createTenant(TENANT_NAME, tenantInfo);
            admin.namespaces().createNamespace(TENANT_NAME+"/"+NS_NAME);
        }
    }

    public void producer() throws PulsarClientException {
        client = PulsarClient.builder()
                .serviceUrl("pulsar://"+PULSAR_HOST+":"+SERVICE_PORT)
                .build();

    }
}
