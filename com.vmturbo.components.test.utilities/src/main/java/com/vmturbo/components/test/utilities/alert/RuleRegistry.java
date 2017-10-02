package com.vmturbo.components.test.utilities.alert;

import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * A rule registry is used to maintain the rule used to generate alerts for a particular
 * {@link com.vmturbo.components.test.utilities.alert.AlertManager.AlertContext}.
 */
public abstract class RuleRegistry {

    public AlertRule getAlertRule(@Nonnull final String metricName) {
        return overrideAlertRule(metricName).orElse(AlertRule.newBuilder().build());
    }

    protected abstract Optional<AlertRule> overrideAlertRule(@Nonnull final String metricName);

    public static class DefaultRuleRegistry extends RuleRegistry {

        @Override
        protected Optional<AlertRule> overrideAlertRule(@Nonnull final String metricName) {
            return Optional.empty();
        }
    }
}
