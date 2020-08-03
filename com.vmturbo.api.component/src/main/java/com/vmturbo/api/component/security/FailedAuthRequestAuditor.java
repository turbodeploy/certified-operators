package com.vmturbo.api.component.security;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.security.access.event.AuthorizationFailureEvent;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerExecutionChain;
import org.springframework.web.servlet.HandlerMapping;

import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;
import com.vmturbo.auth.api.authentication.credentials.SAMLUserUtils;
import com.vmturbo.auth.api.usermgmt.AuthUserDTO;


/**
 * Audit authorization failure actions sent from Spring Framework.
 */
@Component
public class FailedAuthRequestAuditor {
    private static final Logger logger = LogManager.getLogger();
    private ApplicationContext context;

    public FailedAuthRequestAuditor(@Nonnull final ApplicationContext context) {
        this.context = context;
    }

    /**
     * Listen to AuthorizationFailureEvent.
     *
     * @param event authorization failure event
     */
    @EventListener
    public void onAuthorizationFailureEvent(AuthorizationFailureEvent event) {
        final Authentication authentication = event.getAuthentication();
        final AuditAction action = AuditAction.CHECK_AUTHORIZATION;
        // If there is no method name, this actions is a supplement event from Spring and can be ignored.
        getMethodName().ifPresent(targetName -> {
                String detailMessage = event.getAccessDeniedException().getMessage();
                AuthUserDTO authUserDTO = SAMLUserUtils.getAuthUserDTO(authentication);
                AuditLog.newEntry(action, detailMessage, false)
                    .actionInitiator(authUserDTO.getUser())
                    .targetName(targetName)
                    .remoteClientIP(authUserDTO.getIpAddress())
                    .audit();
            }
        );
    }

    /**
     * Get the invoking method.
     *
     * @return the invoking method
     */
    private Optional<String> getMethodName() {
        try {
            final RequestAttributes requestAttributes = RequestContextHolder.getRequestAttributes();
            if (requestAttributes != null && requestAttributes instanceof  ServletRequestAttributes) {
                final HttpServletRequest request = ((ServletRequestAttributes) requestAttributes).getRequest();
                for (HandlerMapping handlerMapping : context.getBeansOfType(HandlerMapping.class).values()) {
                    HandlerExecutionChain handlerExecutionChain = handlerMapping.getHandler(request);
                    if (handlerExecutionChain != null
                        && handlerExecutionChain.getHandler() instanceof HandlerMethod
                        && ((HandlerMethod) handlerExecutionChain.getHandler()).getMethod() != null) {
                        String methodName = ((HandlerMethod) handlerExecutionChain.getHandler())
                            .getMethod()
                            .getName();
                        return Optional.of(methodName);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to get invoking method name", e.getMessage());
        }
        return Optional.empty();
    }
}
