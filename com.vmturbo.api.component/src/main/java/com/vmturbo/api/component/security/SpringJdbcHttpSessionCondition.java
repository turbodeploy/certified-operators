package com.vmturbo.api.component.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Condition to load header authentication and authorization related beans.
 */
public class SpringJdbcHttpSessionCondition implements Condition {
    /**
     * The flag to enable Spring cluster sessions.
     */
    public static final String ENABLED = "jdbcHttpSessionEnabled";
    private static final String TRUE = "true";
    private static final Logger logger = LogManager.getLogger();

    /**
     * Determine if the Spring cluster sessions bean should loaded.
     *
     * @param context the condition context
     * @param metadata metadata of the {@link org.springframework.core.type.AnnotationMetadata
     *         class}
     *         or {@link org.springframework.core.type.MethodMetadata method} being checked.
     * @return {@code true} if "jdbcHttpSessionEnabled" is set to true from environment.
     */
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        final String enableString = context.getEnvironment().getProperty(ENABLED);
        final boolean isEnabled = TRUE.equals(enableString);
        if (isEnabled) {
            logger.info("Spring cluster sessions is enabled.");
        }
        return isEnabled;
    }
}