package com.vmturbo.components.common.health;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.server.KafkaMessageProducer;

/**
 * KafkaProducerHealthMonitor will track the health of the singleton kafka producer. This is a
 * simple health check that will report unhealthy if the last sent message attempt resulted in
 * an exception.
 *
 * This method implements a new SendMessageCallbackHandler interface that can be used to catch
 * message sent acknowledgements. Another approach considered was to use the kafka
 * ProducerInterceptor class, which does the same thing, but requires wiring in at kafka producer
 * construction time.
 */
public class KafkaProducerHealthMonitor extends SimpleHealthStatusProvider {
    private Logger log = LogManager.getLogger();

    private final KafkaMessageProducer kafkaMessageProducer;

    /**
     * Constructs the kafka producer health monitor.
     *
     * @param kafkaProducer the kafka producer config
     */
    public KafkaProducerHealthMonitor(@Nonnull KafkaMessageProducer kafkaProducer) {
        super("Kafka Producer");

        this.kafkaMessageProducer = Objects.requireNonNull(kafkaProducer);
        reportHealthy("Running");
    }

    /**
     * Dynamically check the kafka producer to determine current health status
     * @return the current health status
     */
    @Override
    public synchronized SimpleHealthStatus getHealthStatus() {
        SimpleHealthStatus lastStatus = super.getHealthStatus();

        // only return a new status object if the health status has changed.
        boolean isNowHealthy = ! kafkaMessageProducer.lastSendFailed();
        if (lastStatus.isHealthy() == isNowHealthy) {
            return lastStatus;
        }

        // else our health status has changed, so generate and return a new one.
        return isNowHealthy ? reportHealthy() : reportUnhealthy("Error sending message");
    }
}
