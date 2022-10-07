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
     * Test {@link PropertiesHelpers#parseDuration} for a flexible duration with the Feature Flag
     * disabled.
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
     */
    @Test(expected = IllegalArgumentException.class)
    public void testIllegalDuration() throws IllegalArgumentException {
        String propertyValue = "10dh";
        Duration result =
                PropertiesHelpers.parseDuration(propertyName, propertyValue, propertyUnit);
    }
}
