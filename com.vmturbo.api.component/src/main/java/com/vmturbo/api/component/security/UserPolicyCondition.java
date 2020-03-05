package com.vmturbo.api.component.security;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Condition to load local, AD and SAML authentication and authorization related beans.
 */
public class UserPolicyCondition extends HeaderAuthenticationCondition {

    /**
     * Determine if the default authentication and authorization beans should loaded.
     *
     * @param context  the condition context
     * @param metadata metadata of the {@link org.springframework.core.type.AnnotationMetadata
     *                 class} or {@link org.springframework.core.type.MethodMetadata method} being
     *                 checked.
     * @return {@code true} if "headerAuthenticationEnabled" is NOT set to true from environment.
     */
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        return !super.matches(context, metadata);
    }
}