package io.github.howinfun.client;


import lombok.Builder;
import lombok.Data;
import org.apache.pulsar.client.api.PulsarClient;

@Data
@Builder
public class CustomerPulsarClient {

    private PulsarClient client;

    private Integer maxPendingMessages;
}


