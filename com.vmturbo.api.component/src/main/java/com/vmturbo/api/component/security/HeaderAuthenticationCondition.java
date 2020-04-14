package com.vmturbo.api.component.security;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Condition to load header authentication and authorization related beans.
 */
public class HeaderAuthenticationCondition implements Condition {
    /**
     * The flag to enable header authentication for integration, such as integrating with Cisco Intersight.
     */
    public static final String ENABLED = "headerAuthenticationEnabled";
    private static final String TRUE = "true";

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