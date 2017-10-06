package com.vmturbo.market.priceindex;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.vmturbo.priceindex.api.PriceIndexNotificationSender;
import com.vmturbo.priceindex.api.impl.PriceIndexReceiver;

/**
 * Spring configuration to provide the {@link com.vmturbo.priceindex.api}
 * integration.
 */
@Configuration
public class PriceIndexApiConfig {

    /**
     * Constructs the sender thread pool.
     * Requires 1 thread.
     *
     * @return The sender single-threaded thread pool.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService apiSenderThreadPool() {
        // Requires more than 1 thread in order to work (proven empirically).
        return Executors.newCachedThreadPool(threadFactory());
    }

    @Bean
    public ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("priceindex-api-sender-%d").build();
    }

    /**
     * Constructs the sender backend.
     *
     * @return The Sender API backend.
     */
    @Bean
    public PriceIndexNotificationSender priceIndexNotificationSender() {
        return new PriceIndexNotificationSender(apiSenderThreadPool());
    }

    /**
     * This bean configures sender endpoint to bind it to a specific address (path).
     * The Server means Sender in this context.
     * TODO: The underlying API needs to change.
     *
     * @return Sender endpoint registration.
     */
    @Bean
        public ServerEndpointRegistration priceIndexApiEndpointRegistration() {
        return new ServerEndpointRegistration(PriceIndexReceiver.WEBSOCKET_PATH,
                                          priceIndexNotificationSender().getWebsocketEndpoint());
    }

}
