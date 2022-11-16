package com.vmturbo.components.common;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Test cases for {@link PropertiesHelpers} methods.
 */
public class PropertiesHelpersTest {
    /**
     * Rule to manage feature flag enablement.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule =
            new FeatureFlagTestRule(FeatureFlags.ENABLE_FLEXIBLE_DURATIONS);

    private static final String propertyName = "testProperty";

    private static final ChronoUnit propertyUnit = ChronoUnit.DAYS;

    /**
     * Initialization of each test.
     */
    @Before
    public void setup() {
        featureFlagTestRule.enable(FeatureFlags.ENABLE_FLEXIBLE_DURATIONS);
    }

    /**
     * Test {@link PropertiesHelpers#parseDuration} for a complex duration.
     */
    @Test
    public void testComplexDuration() {
        String propertyValue = "1d12h";
        Duration result =
                PropertiesHelpers.parseDuration(propertyName, propertyValue, propertyUnit);
        Assert.assertEquals(Duration.ofDays(1).plusHours(12), result);
    }

    /**
     * Test {@link PropertiesHelpers#parseDuration} for a duration that contains non-alphanumeric
     * characters.
     */
    @Test
    public void testNonAlphanumericCharacters() {
        String propertyValue = "-5h!";
        Duration result =
                PropertiesHelpers.parseDuration(propertyName, propertyValue, propertyUnit);
        Assert.assertEquals(Duration.ofHours(5), result);
    }

    /**
     * Test {@link PropertiesHelpers#parseDuration} for a normal duration, as it is expected from
     * the CR (raw number without specified time units) and with the Feature Flag disabled.
     */
    @Test
    public void testNormalDuration() {
        featureFlagTestRule.disable(FeatureFlags.ENABLE_FLEXIBLE_DURATIONS);
        String propertyValue = "5";
        Duration result =
                PropertiesHelpers.parseDuration(propertyName, propertyValue, propertyUnit);
        Assert.assertEquals(Duration.ofDays(5), result);
    }

    /**
     * Test {@link PropertiesHelpers#parseDuration} for a duration with a fractional seconds part.
     */
    @Test
    public void testFractionalSeconds() {
        String propertyValue = "1m1.123456789s";
        Duration result = PropertiesHelpers.parseDuration(propertyName, propertyValue,
                propertyUnit);
        Assert.assertEquals(Duration.ofMinutes(1)
                .plus(Duration.ofSeconds(1)).plus(Duration.ofNanos(123456789L)), result);
    }

    /**
     * Test that if a fractional seconds specification with more than nine decimal places fails.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testFractionalSecondsPrecisionLimit() {
        String propertyValue = "1m0.1234567890s";
        Duration result = PropertiesHelpers.parseDuration(propertyName, propertyValue,
                propertyUnit);
    }

    /**
     * Make sure that a fractional part of any unit other seconds fails.
     *
     * <p>Note: this does NOT test an integer with a trailing period, which should technically
     * probably fail. But since the "fractional part" in this case is zero, the duration is
     * actually well-formed. And such durations will in fact work just fine.</p>
     */
    @Test
    public void testFractionalNonSecondUnitsFail() {
        PropertiesHelpers.durationUnits.stream()
                .filter(u -> !u.equals("S"))
                .forEach(u -> {
            try {
                PropertiesHelpers.parseDuration(propertyName, "1.5" + u, propertyUnit);
                Assert.fail("Parser accepted fractional value for duration unit " + u);
            } catch (IllegalArgumentException ignored) {
                // expected
            }
        });
    }

    /**
     * Test {@link PropertiesHelpers#parseDuration} for a flexible duration with the Feature Flag
     * disabled.
     *
     * @throws IllegalArgumentException expected
     */
    @Test(expected = IllegalArgumentException.class)
    public void testDisabledFeatureFlagException() throws IllegalArgumentException {
        featureFlagTestRule.disable(FeatureFlags.ENABLE_FLEXIBLE_DURATIONS);
        String propertyValue = "5d";
        Duration result =
                PropertiesHelpers.parseDuration(propertyName, propertyValue, propertyUnit);
    }

    /**
     * Test {@link PropertiesHelpers#parseDuration} for an illegal duration.
     * @throws IllegalArgumentException expected
     */
    @Test(expected = IllegalArgumentException.class)
    public void testIllegalDuration() throws IllegalArgumentException {
        String propertyValue = "10dh";
        Duration result =
                PropertiesHelpers.parseDuration(propertyName, propertyValue, propertyUnit);
    }
}
