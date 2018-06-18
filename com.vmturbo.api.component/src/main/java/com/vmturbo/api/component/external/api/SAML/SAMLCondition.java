package com.vmturbo.api.component.external.api.SAML;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Condition to load SAML related beans.
 */
public class SAMLCondition implements Condition {
    private static final Logger logger = LogManager.getLogger();
    private static final String TRUE = "true";
    private static final String SAML_ENABLED = "SAML enabled: ";
    private static final String ENABLED = "samlEnabled";

    /**
     * Determine if the SAML bean should loaded.
     *
     * @param context  the condition context
     * @param metadata metadata of the {@link org.springframework.core.type.AnnotationMetadata class}
     *                 or {@link org.springframework.core.type.MethodMetadata method} being checked.
     * @return {@code true} if "samlEnabled" is set to true in Consul.
     */
    @Override
    public boolean matches(ConditionContext context,
                           AnnotatedTypeMetadata metadata) {
        final String samlEnabledStr = context.getEnvironment().getProperty(ENABLED);
        final boolean samlEnabled = TRUE.equals(samlEnabledStr);
        logger.debug(SAML_ENABLED + samlEnabled);
        return samlEnabled;
    }
}