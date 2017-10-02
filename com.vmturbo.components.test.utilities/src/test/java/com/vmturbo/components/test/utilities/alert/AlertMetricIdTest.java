package com.vmturbo.components.test.utilities.alert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AlertMetricIdTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCreateNoLabels() {
        AlertMetricId metric = AlertMetricId.fromString("metric");
        assertEquals("metric", metric.getMetricName());
        assertTrue(metric.getLabels().isEmpty());
        assertFalse(metric.getSlaThreshold().isPresent());
    }

    @Test
    public void testCreateSingleLabel() {
        AlertMetricId metric = AlertMetricId.fromString("metric{label='value'}");
        assertEquals("metric", metric.getMetricName());
        assertEquals(1, metric.getLabels().size());
        assertEquals("value", metric.getLabels().get("label"));
    }

    @Test
    public void testCreateLabelWithSpaces() {
        AlertMetricId metric = AlertMetricId.fromString("metric{ label = 'value' }");
        assertEquals("metric", metric.getMetricName());
        assertEquals(1, metric.getLabels().size());
        assertEquals("value", metric.getLabels().get("label"));
    }

    @Test
    public void testCreateTwoLabels() {
        AlertMetricId metric = AlertMetricId.fromString("metric{tag1='val1',tag2='val2'}");
        assertEquals("metric", metric.getMetricName());
        assertEquals(2, metric.getLabels().size());
        assertEquals("val1", metric.getLabels().get("tag1"));
        assertEquals("val2", metric.getLabels().get("tag2"));
    }

    @Test
    public void testCreateTwoLabelsWithSpaces() {
        AlertMetricId metric = AlertMetricId.fromString("metric{tag1 = 'val1' , tag2 = 'val2'}");
        assertEquals("metric", metric.getMetricName());
        assertEquals(2, metric.getLabels().size());
        assertEquals("val1", metric.getLabels().get("tag1"));
        assertEquals("val2", metric.getLabels().get("tag2"));
    }

    @Test
    public void testCreateWithSlaMissingUnits() {
        expectedException.expect(IllegalArgumentException.class);

        AlertMetricId.fromString("metric/1.2");
    }

    @Test
    public void testCreateWithMalformattedSla() {
        expectedException.expect(IllegalArgumentException.class);

        AlertMetricId.fromString("metric/1.2-bar");
    }

    @Test
    public void testCreateWithSlaTimeUnits() {
        AlertMetricId metric = AlertMetricId.fromString("metric/1.2day");
        assertEquals("metric", metric.getMetricName());

        final SlaThreshold slaThreshold = metric.getSlaThreshold().get();
        assertEquals(TimeUnit.DAYS.toSeconds(1) * 1.2, slaThreshold.getRawValue(), 0);
        assertEquals("1.2day", slaThreshold.getDescription());
    }

    @Test
    public void testCreateWithSlaSIUnits() {
        AlertMetricId metric = AlertMetricId.fromString("metric/1.2k");
        assertEquals("metric", metric.getMetricName());
        final SlaThreshold slaThreshold = metric.getSlaThreshold().get();

        assertEquals(1200.0, slaThreshold.getRawValue(), 0);
        assertEquals("1.2k", slaThreshold.getDescription());
    }

    @Test
    public void testCreateWithNegativeSla() {
        AlertMetricId metric = AlertMetricId.fromString("metric/-1.2k");
        assertEquals("metric", metric.getMetricName());
        final SlaThreshold slaThreshold = metric.getSlaThreshold().get();

        assertEquals(-1200.0, slaThreshold.getRawValue(), 0);
        assertEquals("-1.2k", slaThreshold.getDescription());
    }

    @Test
    public void testCreateWithSlaNoDecimal() {
        AlertMetricId metric = AlertMetricId.fromString("metric/55m");
        assertEquals("metric", metric.getMetricName());
        final SlaThreshold slaThreshold = metric.getSlaThreshold().get();

        assertEquals(TimeUnit.MINUTES.toSeconds(55), slaThreshold.getRawValue(), 0);
        assertEquals("55m", slaThreshold.getDescription());
    }

    @Test
    public void testCreateWithSlaSIUnitsAndTag() {
        AlertMetricId metric = AlertMetricId.fromString("metric{ label = 'value' }/1.2k");
        assertEquals("metric", metric.getMetricName());
        assertEquals(1, metric.getLabels().size());
        assertEquals("value", metric.getLabels().get("label"));
        final SlaThreshold slaThreshold = metric.getSlaThreshold().get();

        assertEquals(1200.0, slaThreshold.getRawValue(), 0);
        assertEquals("1.2k", slaThreshold.getDescription());
    }

    @Test
    public void testEquals() {
        assertEquals(AlertMetricId.fromString("metric"), AlertMetricId.fromString("metric"));

        assertEquals(AlertMetricId.fromString("metric{tag1='val1',tag2='val2'}"),
                AlertMetricId.fromString("metric{tag1='val1',tag2='val2'}"));
    }

    @Test
    public void testNotEquals() {
        assertNotEquals(AlertMetricId.fromString("metric1"), AlertMetricId.fromString("metric2"));

        assertNotEquals(AlertMetricId.fromString("metric{tag1='val1',tag2='val3'}"),
                AlertMetricId.fromString("metric{tag1='val1',tag2='val2'}"));

        assertNotEquals(AlertMetricId.fromString("metric"), null);
        assertNotEquals(AlertMetricId.fromString("metric"), "string");
    }

    @Test
    public void testHashcode() {
        assertEquals(AlertMetricId.fromString("metric").hashCode(),
                AlertMetricId.fromString("metric").hashCode());
        assertEquals(AlertMetricId.fromString("metric{tag1='val1'}").hashCode(),
                AlertMetricId.fromString("metric{tag1 = 'val1'}").hashCode());
    }

    @Test
    public void testToString() {
        final AlertMetricId metric = AlertMetricId.fromString("metric{tag1='val1',tag2='val2'}");
        assertEquals(metric, AlertMetricId.fromString(metric.toString()));
    }

    @Test
    public void testToStringWithSla() {
        final AlertMetricId metric = AlertMetricId.fromString("metric{tag1='val1',tag2='val2'}/10s");
        assertEquals(metric, AlertMetricId.fromString(metric.toString()));
    }

    @Test
    public void testToStringSingleTag() {
        final AlertMetricId metric = AlertMetricId.fromString("metric{tag1='val1'}");
        assertEquals("metric{tag1='val1'}", metric.toString());
    }

    @Test
    public void testDoubleQuotes() {
        expectedException.expect(IllegalArgumentException.class);
        AlertMetricId.fromString("metric{tag1=\"val1\"}");
    }

    @Test
    public void testUnterminatedLabelError() {
        expectedException.expect(IllegalArgumentException.class);
        AlertMetricId.fromString("metric{tag1='val1'");
    }

    @Test
    public void testValNoQuotesError() {
        expectedException.expect(IllegalArgumentException.class);
        AlertMetricId.fromString("metric{tag1=val1}");
    }

    @Test
    public void testValUnterminatedQuotesError() {
        expectedException.expect(IllegalArgumentException.class);
        AlertMetricId.fromString("metric{tag1='val1}");
    }

    @Test
    public void testSpaceBetweenNameAndLabelsError() {
        expectedException.expect(IllegalArgumentException.class);
        AlertMetricId.fromString("metric {tag1='val1}");
    }
}
