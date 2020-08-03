package com.vmturbo.api.component.security;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Condition to load local, AD and SAML authentication and authorization related beans.
 */
public class UserPolicyCondition implements Condition {

    private static final Set<Condition> AUTHENTICATION_CONDITIONS =
            ImmutableSet.of(new OpenIdAuthenticationCondition(), new SamlAuthenticationCondition(), new HeaderAuthenticationCondition());

    /**
     * Determine if the header authentication and authorization bean should loaded.
     *
     * @param context  the condition context
     * @param metadata metadata of the {@link org.springframework.core.type.AnnotationMetadata
     *                 class} or {@link org.springframework.core.type.MethodMetadata method} being
     *                 checked.
     * @return {@code true} if all conditions in AUTHENTICATION_CONDITIONS are false.
     */
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        return AUTHENTICATION_CONDITIONS.stream()
                .allMatch(condition -> !condition.matches(context, metadata));
    }
}