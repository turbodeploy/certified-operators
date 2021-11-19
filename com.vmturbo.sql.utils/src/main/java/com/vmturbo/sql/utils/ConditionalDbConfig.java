package com.vmturbo.sql.utils;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import com.vmturbo.components.common.featureflags.FeatureFlags;

/**
 * Conditions used to decide whether to use {@link DbEndpoint} or legacy {@link SQLDatabaseConfig}.
 * If FF postgresPrimaryDB is enabled, then {@link DbEndpoint} is used; otherwise
 * {@link SQLDatabaseConfig} is used.
 */
public class ConditionalDbConfig {

    /**
     * Condition to load component SQL config based on {@link DbEndpoint}.
     */
    public static class DbEndpointCondition implements Condition {
        /**
         * Determine if {@link DbEndpoint} should be used in components for interaction with db.
         *
         * @param context the condition context
         * @param metadata metadata of the {@link org.springframework.core.type.AnnotationMetadata
         *         class} or {@link org.springframework.core.type.MethodMetadata method} being checked.
         * @return true if "postgresPrimaryDB" is set to true from environment.
         */
        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled();
        }
    }

    /**
     * Condition to load component SQL config based on {@link SQLDatabaseConfig}.
     */
    public static class SQLDatabaseConfigCondition implements Condition {
        /**
         * Determine if {@link SQLDatabaseConfig} should be used in components for interaction with db.
         *
         * @param context the condition context
         * @param metadata metadata of the {@link org.springframework.core.type.AnnotationMetadata
         *         class} or {@link org.springframework.core.type.MethodMetadata method} being checked
         * @return true if "postgresPrimaryDB" is set to false from environment
         */
        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return !FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled();
        }
    }
}
