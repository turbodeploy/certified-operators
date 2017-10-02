package com.vmturbo.components.test.utilities.alert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

public class RuleSignatureTest {
    @Test
    public void testRuleSignatureEquals() {
        final RuleSignature signatureA = new RuleSignature("Foo", AlertMetricId.fromString("metric"));
        final RuleSignature signatureB = new RuleSignature("Foo", AlertMetricId.fromString("metric"));

        assertEquals(signatureA, signatureB);
    }

    @Test
    public void testRuleSignatureNotEquals() {
        final RuleSignature signatureA = new RuleSignature("Foo", AlertMetricId.fromString("metricA"));
        final RuleSignature signatureB = new RuleSignature("Foo", AlertMetricId.fromString("metricB"));

        assertNotEquals(signatureA, signatureB);
    }

    @Test
    public void testSignatureDescriptionSingleMetric() {
        final RuleSignature signature = new RuleSignature("Foo", AlertMetricId.fromString("metricA"));

        assertEquals("(metric: metricA)", signature.metricsDescription());
    }

    @Test
    public void testSignatureDescriptionMultipleMetrics() {
        final RuleSignature signature = new RuleSignature("Foo",
            AlertMetricId.fromString("metricA"), AlertMetricId.fromString("metricB"));

        assertEquals("(metrics: [metricA, metricB])", signature.metricsDescription());
    }
}