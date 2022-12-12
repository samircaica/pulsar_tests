package org.interacting.pulsar.executor;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.interacting.pulsar.producer.PulsarProducer;

public class PulsarExecutor {

    public static void main(String[] args) throws PulsarClientException, PulsarAdminException {

        PulsarProducer pProducer = new PulsarProducer();

        pProducer.createTopic();
    }

}
