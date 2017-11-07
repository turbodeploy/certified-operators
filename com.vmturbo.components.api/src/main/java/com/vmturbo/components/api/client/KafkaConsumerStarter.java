package com.vmturbo.components.api.client;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * Special Spring-related application listener, that starts up all the known
 * {@link KafkaMessageConsumer} instances after application context is started up.
 */
public class KafkaConsumerStarter implements ApplicationListener<ContextRefreshedEvent> {

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        final ApplicationContext context = event.getApplicationContext();
        for (KafkaMessageConsumer consumer : context.getBeansOfType(KafkaMessageConsumer.class)
                .values()) {
            consumer.start();
        }
    }
}
