package com.vmturbo.components.common.utils;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

@NotThreadSafe // must not be run in parallel
public class EnvironmentUtilsTest {

    public static final String TEST_KEY1 = "test-key1";
    // Save System Properties before each test, and restore afterwards
    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();


    public static final String TEST_KEY = "test-key";

    @Test
    public void testParseIntegerFromProperty() throws Exception {
        environmentVariables.set("test-key", "123");
        int value = EnvironmentUtils.parseIntegerFromEnv("test-key");
        assertThat(value, equalTo(123));
    }

    @Test
    public void testParseIntegerFromNullProperty() throws Exception {
        // don't set any property value
        try {
            environmentVariables.set(TEST_KEY, null);
            testExceptionCase(null);
            Assert.fail("Expected exception.");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(TEST_KEY));
        }
    }

    @Test
    public void testParseIntegerFromEmptyProperty() throws Exception {
        testExceptionCase("");
    }

    @Test
    public void testParseIntegerFromCharProperty() throws Exception {
        testExceptionCase("abc");
    }

    /**
     * Set the test property to the given value and run the test method.
     * A {@link NumberFormatException} should result. If the given value is null,
     * do not set the System property, as null values are not allowed.
     *
     * @param value the value to set; if null, then don't set the property at all
     */
    private void testExceptionCase(@Nullable String value) {
        environmentVariables.set(TEST_KEY, value);
        try {
            EnvironmentUtils.parseIntegerFromEnv(TEST_KEY);
        } catch(NumberFormatException e) {
            assertThat(e.getMessage(), containsString("'" + TEST_KEY + "'"));
            assertThat(e.getMessage(), containsString(">" + value + "<"));
            return;
        }
        Assert.fail("expected a NumberFormatException");
    }

    @Test
    public void testParseBooleanFromProperty() throws Exception {
        environmentVariables.set(TEST_KEY1, "true");
        boolean value = EnvironmentUtils.parseBooleanFromEnv(TEST_KEY1);
        assertTrue(value);
        environmentVariables.set(TEST_KEY1, "false");
        value = EnvironmentUtils.parseBooleanFromEnv("test-key");
        assertFalse(value);
        environmentVariables.set(TEST_KEY1, "");
        value = EnvironmentUtils.parseBooleanFromEnv("test-key");
        assertFalse(value);
        environmentVariables.set(TEST_KEY1, "others");
        value = EnvironmentUtils.parseBooleanFromEnv("test-key");
        assertFalse(value);
    }

}