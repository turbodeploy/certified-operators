package com.vmturbo.components.test.utilities.alert;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

/**
 * A rule signature consists of a specific test class with a set of associated metrics being
 * evaluated by alerting rules.
 */
public class RuleSignature {
    public final String testClassName;
    public final List<AlertMetricId> testMetricNames;

    public RuleSignature(@Nonnull final String testClassName,
                         @Nonnull final AlertMetricId... testMetrics) {
        this.testClassName = Objects.requireNonNull(testClassName);
        this.testMetricNames = Arrays.asList(Objects.requireNonNull(testMetrics));
    }

    @Override
    public String toString() {
        return testClassName + " " + metricsDescription();
    }

    /**
     * Construct a description for the metrics that are part of the rule signature.
     * For a single metric: "metric Foo"
     * For multiple metrics: "metrics [Foo, Bar, Baz]"
     *
     * @return A description for the metrics.
     */
    public String metricsDescription() {
        final String metricsStr = "(metric" + (testMetricNames.size() > 1 ? "s: " : ": ");
        final String groupPrefix = testMetricNames.size() > 1 ? "[" : "";
        final String groupSuffix = testMetricNames.size() > 1 ? "]" : "";
        return metricsStr + testMetricNames.stream()
            .map(AlertMetricId::toString)
            .collect(Collectors.joining(", ", groupPrefix, groupSuffix)) + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(testClassName, testMetricNames);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof RuleSignature) && equalTo((RuleSignature) o);
    }

    private boolean equalTo(@Nonnull final RuleSignature series) {
        return Objects.equals(testClassName, series.testClassName) &&
            Objects.equals(testMetricNames, series.testMetricNames);
    }
}
