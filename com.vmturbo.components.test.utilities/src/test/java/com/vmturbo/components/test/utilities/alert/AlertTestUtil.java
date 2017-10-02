package com.vmturbo.components.test.utilities.alert;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

public class AlertTestUtil {

    static final String METRIC_NAME = "metric";
    static final String METRIC_NAME_2 = "metric2";
    static final AlertMetricId METRIC = AlertMetricId.fromString(METRIC_NAME);

    static final long CUSTOM_BASELINE_MS = 30;
    static final long CUSTOM_COMPARISON_MS = 10;
    static final double CUSTOM_STD_DEVIATIONS = 3;

    @Nonnull
    static Alert makeAlert(@Nonnull final String... metrics) {
        return makeAlert(metrics, metrics);
    }

    @Nonnull
    static Alert makeAlert(@Nonnull final String[] value,
                           @Nonnull final String[] metrics) {
        return makeAlert(value, metrics, TestRuleRegistry.class);
    }

    @Nonnull
    static Alert makeAlert(@Nonnull final Class<? extends RuleRegistry> registryClass,
                           @Nonnull final String... metrics) {
        return makeAlert(metrics, metrics, registryClass);
    }

    @Nonnull
    static Alert makeAlert(@Nonnull final String[] value,
                           @Nonnull final String[] metrics,
                           @Nonnull final Class<? extends RuleRegistry> registry) {
        return new Alert() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return Alert.class;
            }

            @Override
            public String[] value() {
                return value;
            }

            @Override
            public String[] metrics() {
                return metrics;
            }

            @Override
            public Class<? extends RuleRegistry> ruleRegistry() {
                return registry;
            }

            @Override
            public boolean equals(Object other) {
                if (other instanceof Alert) {
                    Alert alert = (Alert)other;
                    return Arrays.equals(alert.metrics(), metrics) &&
                        Arrays.equals(alert.value(), value) &&
                        alert.ruleRegistry().equals(ruleRegistry());
                } else {
                    return false;
                }
            }
        };
    }

    static class TestRuleRegistry extends RuleRegistry {

        @Override
        protected Optional<AlertRule> overrideAlertRule(@Nonnull final String metricName) {
            return metricName.equals(METRIC_NAME) || metricName.equals(METRIC_NAME_2) ?
                    Optional.of(AlertRule.newBuilder()
                            .setBaselineTime(CUSTOM_BASELINE_MS, TimeUnit.MILLISECONDS)
                            .setComparisonTime(CUSTOM_COMPARISON_MS, TimeUnit.MILLISECONDS)
                            .setStandardDeviations(CUSTOM_STD_DEVIATIONS)
                            .build()) : Optional.empty();
        }
    }
}
