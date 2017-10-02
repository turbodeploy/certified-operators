package com.vmturbo.components.test.utilities.alert;

import static com.vmturbo.components.test.utilities.alert.AlertTestUtil.CUSTOM_BASELINE_MS;
import static com.vmturbo.components.test.utilities.alert.AlertTestUtil.CUSTOM_COMPARISON_MS;
import static com.vmturbo.components.test.utilities.alert.AlertTestUtil.CUSTOM_STD_DEVIATIONS;
import static com.vmturbo.components.test.utilities.alert.AlertTestUtil.METRIC_NAME;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.vmturbo.components.test.utilities.alert.AlertTestUtil.TestRuleRegistry;
import com.vmturbo.components.test.utilities.alert.RuleRegistry.DefaultRuleRegistry;

public class RuleRegistryTest {

    @Test
    public void testDefaultRegistry() {
        AlertRule rule = new DefaultRuleRegistry().getAlertRule("blah");
        assertEquals(TimeUnit.DAYS.toMillis(AlertRule.DEFAULT_BASELINE_DAYS),
                rule.getBaselineTime().millis());
        assertEquals(TimeUnit.DAYS.toMillis(AlertRule.DEFAULT_COMPARISON_DAYS),
                rule.getComparisonTime().millis());
        assertEquals(AlertRule.DEFAULT_STD_DEVIATIONS, rule.getStandardDeviations(), 0);
    }

    @Test
    public void testCustomRegistryOverride() {
        AlertRule rule = new TestRuleRegistry().getAlertRule(METRIC_NAME);
        assertEquals(CUSTOM_BASELINE_MS, rule.getBaselineTime().millis());
        assertEquals(CUSTOM_COMPARISON_MS, rule.getComparisonTime().millis());
        assertEquals(CUSTOM_STD_DEVIATIONS, rule.getStandardDeviations(), 0);
    }

    @Test
    public void testCustomRegistryFallback() {
        AlertRule rule = new TestRuleRegistry().getAlertRule("blah");
        assertEquals(TimeUnit.DAYS.toMillis(AlertRule.DEFAULT_BASELINE_DAYS),
                rule.getBaselineTime().millis());
        assertEquals(TimeUnit.DAYS.toMillis(AlertRule.DEFAULT_COMPARISON_DAYS),
                rule.getComparisonTime().millis());
        assertEquals(AlertRule.DEFAULT_STD_DEVIATIONS, rule.getStandardDeviations(), 0);
    }
}
