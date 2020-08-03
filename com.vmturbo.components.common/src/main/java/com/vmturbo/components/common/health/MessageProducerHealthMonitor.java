package com.vmturbo.components.common.health;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.server.IMessageSenderFactory;

/**
 * MessageProducerHealthMonitor will track the health of the singleton message producer. This is a
 * simple health check that will report unhealthy if the last sent message attempt resulted in
 * an exception.
 *
 * <p/>This method implements a new SendMessageCallbackHandler interface that can be used to catch
 * message sent acknowledgements. Another approach considered was to use the kafka
 * ProducerInterceptor class, which does the same thing, but requires wiring in at kafka producer
 * construction time.
 */
public class MessageProducerHealthMonitor extends SimpleHealthStatusProvider {
    private Logger log = LogManager.getLogger();

    private final IMessageSenderFactory messageProducer;

    /**
     * Constructs the producer health monitor.
     *
     * @param messageProducer the kafka producer config
     */
    public MessageProducerHealthMonitor(@Nonnull IMessageSenderFactory messageProducer) {
        super("Kafka Producer");

        this.messageProducer = Objects.requireNonNull(messageProducer);
        reportHealthy("Running");
    }

    /**
     * Dynamically check the sender factory to determine current health status.
     *
     * @return the current health status
     */
    @Override
    public synchronized SimpleHealthStatus getHealthStatus() {
        SimpleHealthStatus lastStatus = super.getHealthStatus();

        // only return a new status object if the health status has changed.
        boolean isNowHealthy = !messageProducer.lastSendFailed();
        if (lastStatus.isHealthy() == isNowHealthy) {
            return lastStatus;
        }

        // else our health status has changed, so generate and return a new one.
        return isNowHealthy ? reportHealthy() : reportUnhealthy("Error sending message");
    }
}
