package com.vmturbo.components.test.utilities.alert;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.vmturbo.components.test.utilities.alert.RuleRegistry.DefaultRuleRegistry;

/**
 * Use this annotation to specify which metrics for a given performance test should trigger
 * alerts when they deviate from the historical average.
 * <p>
 * Example usage:
 *     public class ComponentPerformanceTest {
 *         \@Test
 *         \@Alert({"jvm_memory_bytes_used_max", "op_duration_seconds_sum"})
 *         public void test100kTopology() {
 *             // Stuff...
 *         }
 *     }
 * <p>
 * To filter by labels:
 *     \@Alert({"metric{label1='val1',label2='val2'}", ...})
 * <p>
 * To apply an SLA_VIOLATION:
 *     \@Alert({"op_duration_seconds_sum/6m"})
 * indicates that the metric op_duration_seconds_sum should meet an SLA_VIOLATION of 6 minutes.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface Alert {
    /**
     * The metrics to alert for. This is an alias for {@link Alert#metrics()} to support
     * declarations like: @Alert({"metricName1", "metricName2"}).
     */
    String[] value() default {};

    /**
     * The metrics to alert for.
     */
    String[] metrics() default {};

    /**
     * If set, use this rule registry to look up which rules to use to decide
     * whether or not to generate alerts for the metrics.
     * <p>
     * The default rule registry should be sufficient for most use cases.
     */
    Class<? extends RuleRegistry> ruleRegistry() default DefaultRuleRegistry.class;
}
