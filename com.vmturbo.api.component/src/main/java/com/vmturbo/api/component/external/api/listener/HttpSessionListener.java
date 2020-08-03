package com.vmturbo.api.component.external.api.listener;

import javax.servlet.http.HttpSession;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.security.web.session.HttpSessionDestroyedEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;

import com.vmturbo.api.component.external.api.websocket.ApiWebsocketHandler;

/**
 * Listener Class to provide hooks for when a user session is destroyed so that proper clean up
 * of session managed resources can occur.
 * These events are published via HttpSessionEventPublisher.  The class implements
 * ApplicationListener in order to capture ApplicationEvents in the context of the Spring
 * environment and therefore can have access to other beans in the Spring environment.
 */
@Component
public class HttpSessionListener implements ApplicationListener<ApplicationEvent> {
    private ApiWebsocketHandler websocketHandler;

    private static final String SESSION_END_REASON = "Terminating session due to user logout or timeout";

    /**
     * The logger.
     */
    private final Logger logger_ = LogManager.getLogger();

    /**
     * Instantiate an instance of an {@link HttpSessionListener} with access to a websocketHandler.
     *
     * @param websocketHandler a {@link ApiWebsocketHandler} managing websocket lifecycles.
     */
    public HttpSessionListener(ApiWebsocketHandler websocketHandler) {
        this.websocketHandler = websocketHandler;
    }

    @Override
    public void onApplicationEvent(final ApplicationEvent applicationEvent) {
        // Listen for HTTP Session Destroyed events s.t. all active websocket sessions tied to the
        // ending user session will be closed
        if (applicationEvent instanceof HttpSessionDestroyedEvent) {
            final HttpSession httpSession = ((HttpSessionDestroyedEvent)applicationEvent).getSession();
            if (httpSession == null) {
                logger_.info("The HTTP Session getting destroyed did not have a Session ID, " +
                    "cannot close Websockets.");
                return;
            }

            final String httpSessionId = httpSession.getId();
            if (httpSessionId == null) {
                logger_.info("The HTTP Session getting destroyed did not have a Session ID, " +
                    "cannot close Websockets.");
                return;
            }

            websocketHandler.closeWSSessionsByHttpSessionId(httpSessionId,
                                        CloseStatus.NORMAL.withReason(SESSION_END_REASON));
        }
    }
}
