package com.vmturbo.api.component.external.api.websocket;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.support.HttpSessionHandshakeInterceptor;

@EnableWebSocket
@Configuration
public class ApiWebsocketConfig implements WebSocketConfigurer {

    @Value("${wsSessionTimeoutSecs:1800}")
    private int sessionTimeoutSeconds;

    @Value("${wsPingIntervalSecs:120}")
    private int pingIntervalSeconds;

    /**
     * The URL at which the UI listens for websocket notifications.
     */
    public static final String WEBSOCKET_URL = "/ws/messages";

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry webSocketHandlerRegistry) {
        // disabling CORS checks since we are expecting everything to be routed from nginx.
        // The disabling allows WSS -> WS proxying to work. (e.g. Browser -WSS-> Nginx -WS-> API)
        // If the CORS check is in place, the "same origin" check in the spring framework fails
        // because of the scheme (https vs http) and port (80 vs an assumed 443) differences in the
        // request vs origin header.
        //
        // We can make it work with the CORS check,  but it would be easier with a fixed
        // domain name (which would replace '*' in the allowed origin set) or some time to drill
        // into how we could manipulate the proxy headers to make the check to succeed. Finally,
        // there was also a related issue fixed in a newer version of Spring
        // (https://jira.spring.io/browse/SPR-16262) that we might also need to pull in if we want
        // to go the "headers" route.
        webSocketHandlerRegistry.addHandler(websocketHandler(), WEBSOCKET_URL).setAllowedOrigins("*")
            // Add Session Handshake interceptor to attach meta info to the attributes of the
            // websocket object.  This will tie both the HTTP Session ID and the Spring Security
            // Context to the websocket instance.
            .addInterceptors(new HttpSessionHandshakeInterceptor());
    }

    /**
     * Construct the handler for the websocket connection to the UI.
     *
     * @return The handler.
     */
    @Bean
    public ApiWebsocketHandler websocketHandler() {
        return new ApiWebsocketHandler(sessionTimeoutSeconds, pingIntervalSeconds);
    }
}
