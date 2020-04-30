package com.vmturbo.api.component.security;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Condition to load OpenId authentication and authorization related beans.
 */
public class OpenIdAuthenticationCondition implements Condition {
    @VisibleForTesting
    static final String OPENID_ENABLED = "openIdEnabled";
    private static final String TRUE = "true";
    private static final Logger logger = LogManager.getLogger();

    /**
     * Determine if the OpenId authentication and authorization beans should loaded.
     *
     * @param context the condition context
     * @param metadata metadata of the {@link org.springframework.core.type.AnnotationMetadata
     *         class} or {@link org.springframework.core.type.MethodMetadata method} being checked.
     * @return {@code true} if "openIdEnabled" is set to true from environment.
     */
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        final String enableString = context.getEnvironment().getProperty(OPENID_ENABLED);
        final boolean openIdAuthenticationEnabled = TRUE.equals(enableString);
        if (openIdAuthenticationEnabled) {
            logger.info("OpenID OAuth2 SSO enabled.");
        }
        return openIdAuthenticationEnabled;
    }
}