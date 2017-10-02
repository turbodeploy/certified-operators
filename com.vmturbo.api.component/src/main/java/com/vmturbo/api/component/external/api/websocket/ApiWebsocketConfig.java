package com.vmturbo.api.component.external.api.websocket;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@EnableWebSocket
@Configuration
public class ApiWebsocketConfig implements WebSocketConfigurer {

    /**
     * The URL at which the UI listens for websocket notifications.
     */
    public static final String WEBSOCKET_URL = "/vmturbo/messages";

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry webSocketHandlerRegistry) {
        webSocketHandlerRegistry.addHandler(websocketHandler(), WEBSOCKET_URL);
    }

    /**
     * Construct the handler for the websocket connection to the UI.
     *
     * @return The handler.
     */
    @Bean
    public ApiWebsocketHandler websocketHandler() {
        return new ApiWebsocketHandler();
    }
}
