package com.vmturbo.api.component.security;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Condition to load header authentication and authorization related beans.
 */
public class HeaderAuthenticationCondition implements Condition {
    @VisibleForTesting
    static final String ENABLED = "headerAuthenticationEnabled";
    private static final Logger logger = LogManager.getLogger();
    private static final String TRUE = "true";
    private static final String HEADER_AUTHORIZATION_ENABLED = "Header authorization enabled: ";

    /**
     * Determine if the header authentication and authorization bean should loaded.
     *
     * @param context the condition context
     * @param metadata metadata of the {@link org.springframework.core.type.AnnotationMetadata
     *         class}
     *         or {@link org.springframework.core.type.MethodMetadata method} being checked.
     * @return {@code true} if "headerAuthenticationEnabled" is set to true from environment.
     */
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        final String enableString = context.getEnvironment().getProperty(ENABLED);
        final boolean headerAuthenticationEnabled = TRUE.equals(enableString);
        return headerAuthenticationEnabled;
    }
}