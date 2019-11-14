package com.vmturbo.api.component.security;

import static com.vmturbo.api.component.external.api.service.AuthenticationService.LOGIN_MANAGER;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;

/**
 * Condition to load local, AD and SAML authentication and authorization related beans.
 */
public class UserPolicyCondition extends HeaderAuthenticationCondition {
    private static final Logger logger = LogManager.getLogger();
    private AtomicBoolean writtenTolog = new AtomicBoolean();

    /**
     * Determine if the default authentication and authorization beans should loaded.
     *
     * @param context the condition context
     * @param metadata metadata of the {@link org.springframework.core.type.AnnotationMetadata
     *         class}
     *         or {@link org.springframework.core.type.MethodMetadata method} being checked.
     * @return {@code true} if "headerAuthenticationEnabled" is NOT set to true from environment.
     */
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        final boolean isDefaultUserPolicy = !super.matches(context, metadata);
        if (isDefaultUserPolicy && !writtenTolog.get()) {
            writtenTolog.set(true);
            logger.info("System support local and AD authentication and authorization.");
            AuditLog.newEntry(AuditAction.SET_DEFAULT_AUTH,
                    "Enabled local and AD authentication and authorization.", true)
                    .targetName(LOGIN_MANAGER)
                    .audit();
        }
        return isDefaultUserPolicy;
    }
}