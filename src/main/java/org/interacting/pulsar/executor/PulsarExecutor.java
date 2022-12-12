package org.interacting.pulsar.executor;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.interacting.pulsar.producer.PulsarProducer;

public class PulsarExecutor {

    public static void main(String[] args) throws PulsarClientException, PulsarAdminException {

        PulsarProducer pProducer = new PulsarProducer();

        try {
            pProducer.createFullTopic();
        } catch (PulsarAdminException e) {
            System.out.println("Alert!: "+e.getHttpError()+" Status code: "+e.getStatusCode());
            switch (e.getStatusCode()) {
                case 409:
                    try {
                        pProducer.createNamespace();
                        pProducer.createTopic();
                    } catch (PulsarAdminException e1) {
                        System.out.println("Alert!: " + e1.getHttpError() + " Status code: " + e1.getStatusCode());
                        switch (e1.getStatusCode()) {
                            case 409:
                                try {
                                    pProducer.createTopic();
                                } catch (PulsarAdminException e2) {
                                    System.out.println("Alert!: " + e2.getHttpError() + " Status code: " + e2.getStatusCode());
                                }
                        }
                    }
            }
        }



        System.exit(0);
    }

}
