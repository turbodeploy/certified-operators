package com.vmturbo.components.api.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * Special Spring-related application listener, that starts up all the known
 * {@link KafkaMessageConsumer} instances after application context is started up.
 */
public class KafkaConsumerStarter implements ApplicationListener<ContextRefreshedEvent> {
    private final Logger logger = LogManager.getLogger();

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        final ApplicationContext context = event.getApplicationContext();
        for (KafkaMessageConsumer consumer : context.getBeansOfType(KafkaMessageConsumer.class)
                .values()) {
            try {
                logger.info("Starting Kafka consumer.");
                consumer.start();
            } catch (IllegalStateException ise) {
                // This is a harmless exception in this situation. We expect these from time to time
                // because multiple ContextRefreshedEvents over the course of a component instance
                // is a supported case (i.e. configuration changes can trigger these). They are
                // also the norm in the API component, which has two spring contexts firing events.
                //
                // In other applications using our kafka libs, the consumer.start() may not be tied
                // to spring context lifecycle events, and the redundant start in that kind of case
                // could be a sign of something more troubling. But here, we'll just log a message
                // and continue on our merry way.
                logger.info("Kafka consumer already started.");
            }
        }
    }
}
