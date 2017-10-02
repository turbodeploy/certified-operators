package com.vmturbo.components.test.utilities.alert;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.components.test.utilities.alert.AlertRule.MetricEvaluationResult;
import com.vmturbo.components.test.utilities.alert.RuleRegistry.DefaultRuleRegistry;

/**
 * The {@link AlertManager} is responsible for keeping track of and triggering alerts,
 * as configured by {@link Alert} annotations on performance test methods.
 */
public class AlertManager {

    private final Logger logger = LogManager.getLogger();

    private final Map<String, AlertContext> alertsByTest = new HashMap<>();

    private final MetricsStore metricsStore;

    @VisibleForTesting
    AlertManager(@Nonnull final MetricsStore metricsStore) {
        this.metricsStore = metricsStore;
    }

    AlertManager() {
        this(new InfluxDBMetricsStore());
    }

    void addAlert(@Nonnull final String key,
                  @Nonnull final String methodName,
                  @Nonnull final String testClassName,
                  @Nonnull final AlertAnnotationsForTest alertAnnotations) {
        final Set<AlertMetricId> ids = alertAnnotations.getMetricIds();
        if (!ids.isEmpty()) {
            alertsByTest.put(key, new AlertContext(methodName, testClassName,
                    alertAnnotations.getRegistry(), ids));
        }
    }

    void removeAlert(@Nonnull final String key) {
        alertsByTest.remove(key);
    }

    @VisibleForTesting
    int numAlerts() {
        return alertsByTest.size();
    }

    @Nonnull
    public List<MetricEvaluationResult> evaluateAlerts() {
        if (!metricsStore.isAvailable()) {
            logger.warn("Metrics store of type " + metricsStore.getClass().getSimpleName()
                + " is unavailable. Skipping alert evaluation.");
            return Collections.emptyList();
        }

        return alertsByTest.values().stream()
            .flatMap(alertContext -> alertContext.evaluateAlert(metricsStore))
            .collect(Collectors.toList());
    }

    /**
     * An object representing all {@link Alert} annotations that apply to a specific
     * performance test.
     */
    public static class AlertAnnotationsForTest {
        /**
         * The alert on the actual test method (if any).
         */
        private final Optional<Alert> testAlert;

        /**
         * The alert on the class to which the test method belongs (if any).
         */
        private final Optional<Alert> testClassAlert;

        AlertAnnotationsForTest(@Nullable final Alert testAlert,
                                @Nullable final Alert testClassAlert) {
            this.testAlert = Optional.ofNullable(testAlert);
            this.testClassAlert = Optional.ofNullable(testClassAlert);
        }

        @Nonnull
        public Class<? extends RuleRegistry> getRegistry() {
            Class<? extends RuleRegistry> registry = DefaultRuleRegistry.class;
            if (testClassAlert.isPresent()) {
                registry = testClassAlert.get().ruleRegistry();
            }
            // The test-level rule registry (if it's non-default) overrides the class-level
            // rule registry.
            if (testAlert.isPresent() &&
                !testAlert.get().ruleRegistry().equals(DefaultRuleRegistry.class)) {
                registry = testAlert.get().ruleRegistry();
            }
            return registry;
        }

        @Nonnull
        public Set<AlertMetricId> getMetricIds() {
            return Stream.of(testClassAlert, testAlert)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .flatMap(alert -> Stream.of(alert.metrics().length > 0 ?
                            alert.metrics() : alert.value()))
                    .map(AlertMetricId::fromString)
                    .collect(Collectors.toSet());
        }

        @VisibleForTesting
        Optional<Alert> getTestAlert() {
            return testAlert;
        }

        @VisibleForTesting
        Optional<Alert> getTestClassAlert() {
            return testClassAlert;
        }
    }

    /**
     * Information about alerts configured for a specific test.
     * <p>
     * This is a helper object to assist the {@link AlertManager} in tracking tests that have
     * alerting configured.
     */
    private static class AlertContext {
        private final String testName;
        private final String testClassName;
        private final Map<AlertMetricId, AlertRule> metricsAndRules;

        private AlertContext(@Nonnull final String testName,
                             @Nonnull final String testClassName,
                             @Nonnull final Class<? extends RuleRegistry> registryClass,
                             @Nonnull final Set<AlertMetricId> metrics) {
            this.testName = testName;
            this.testClassName = testClassName;
            try {
                final Constructor<? extends RuleRegistry> constructor =
                        registryClass.getDeclaredConstructor();
                constructor.setAccessible(true);
                final RuleRegistry registry = constructor.newInstance();
                this.metricsAndRules = metrics.stream()
                        .collect(Collectors.toMap(Function.identity(),
                                metric -> registry.getAlertRule(metric.getMetricName())));
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException("Registry class " + registryClass.getSimpleName()
                        + " must have a default constructor.");
            } catch (InvocationTargetException | IllegalAccessException | InstantiationException e) {
                throw new IllegalStateException("Failed to instantiate registry.", e);
            }
        }

        @Nonnull
        private Stream<MetricEvaluationResult> evaluateAlert(@Nonnull final MetricsStore metricsStore) {
            return metricsAndRules.entrySet().stream()
                .map(metricEntry -> {
                    final AlertMetricId metric = metricEntry.getKey();
                    final AlertRule alertRule = metricEntry.getValue();
                    return alertRule.evaluate(metric, testName, testClassName, metricsStore);
                })
                .filter(Optional::isPresent)
                .map(Optional::get);
        }
    }
}
