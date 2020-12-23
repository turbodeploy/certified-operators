package com.vmturbo.mediation.actionstream.kafka;

import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;
import com.vmturbo.platform.sdk.probe.properties.PropertySpec;

/**
 * Properties for ActionStreamKafka probe.
 * TODO OM-64041 figure out list of properties that we need to have for KafkaProducer and KafkaConsumer.
 */
public class ActionStreamKafkaProbeProperties {
    private final IPropertyProvider propertyProvider;

    /**
     * Maximum size of message that can be sent with this producer, in bytes.
     */
    static final IProbePropertySpec<Integer> MAX_REQUEST_SIZE_BYTES =
            new PropertySpec<>("maxRequestSizeBytes", Integer::valueOf, 126976);

    /**
     * Maximum amount of time to block while synchronizing topic metadata before a send.
     */
    static final IProbePropertySpec<Integer> MAX_BLOCK_MS =
            new PropertySpec<>("maxBlockMs", Integer::valueOf, 3600);

    /**
     * The total time given to kafka to complete sending a message.
     * This includes retry attempts.
     */
    static final IProbePropertySpec<Integer> DELIVERY_TIMEOUT_MS =
            new PropertySpec<>("deliveryTimeoutMs", Integer::valueOf, 300000);

    /**
     * Interval between retrials to send the message.
     */
    static final IProbePropertySpec<Integer> RETRY_INTERVAL_MS =
            new PropertySpec<>("retryIntervalMs", Integer::valueOf, 1000);


    /**
     * Constructor.
     *
     * @param propertyProvider provider of probe properties
     */
    public ActionStreamKafkaProbeProperties(IPropertyProvider propertyProvider) {
        this.propertyProvider = propertyProvider;
    }

    /**
     * Maximum size of message that can be sent with this producer.
     *
     * @return max message size in bytes
     */
    public Integer getMaxRequestSizeBytes() {
        return propertyProvider.getProperty(MAX_REQUEST_SIZE_BYTES);
    }

    /**
     * Maximum amount of time to block while synchronizing topic metadata before a send.
     *
     * @return max block time in milliseconds
     */
    public Integer getMaxBlockMs() {
        return propertyProvider.getProperty(MAX_BLOCK_MS);
    }

    /**
     * The total length of time given to kafka to complete sending a message.
     *
     * @return delivery timeout in milliseconds
     */
    public Integer getDeliveryTimeoutMs() {
        return propertyProvider.getProperty(DELIVERY_TIMEOUT_MS);
    }

    /**
     * The interval between retrials to send the message.
     *
     * @return retry interval in milliseconds
     */
    public Integer getRetryIntervalMs() {
        return propertyProvider.getProperty(RETRY_INTERVAL_MS);
    }
}
