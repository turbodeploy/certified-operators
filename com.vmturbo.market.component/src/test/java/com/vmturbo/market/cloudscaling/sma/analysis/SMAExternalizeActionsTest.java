package com.vmturbo.market.cloudscaling.sma.analysis;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * JUnit to test SMAExternalizeActions methods.
 */
public class SMAExternalizeActionsTest {
    private static long key1 = 1L;
    private static double value1 = 3.5;
    private static long key2 = 10L;
    private static double value2 = 1.25;
    private static long key3 = 100L;
    private static double value3 = 0.25;

    /**
     * Test the computeCouponsUsed method.
     */
    @Test
    public void testComputeCouponsUsed() {
        String type = "source";
        long oid = 1L;
        Map<Long, Double> map = new HashMap<>();

        SMAExternalizeActions externalizedActions = new SMAExternalizeActions();

        // empty map, returns 0f
        float value = externalizedActions.computeCouponsUsed(map, oid, type);
        assertEquals(value, 0.0f, 0.00001f);

        // map with null value, returns 0f
        map.put(key1, null);
        assertEquals(value, 0.0f, 0.00001f);

        // map with one value, returns the value.
        map.put(key1, value1);
        value = externalizedActions.computeCouponsUsed(map, oid, type);
        assertEquals(value, value1, 0.00001f);

        // map with two values, returns the sum of the values
        map.put(key2, value2);
        value = externalizedActions.computeCouponsUsed(map, oid, type);
        assertEquals(value, (value1 + value2), 0.00001f);

        // map with three values, returns the sum of the values
        map.put(key3, value3);
        value = externalizedActions.computeCouponsUsed(map, oid, type);
        assertEquals(value, (value1 + value2 + value3), 0.00001f);

        // map with null key, don't include null key's value.
        map.put(null, value3);
        value = externalizedActions.computeCouponsUsed(map, oid, type);
        assertEquals(value, (value1 + value2 + value3), 0.00001f);
    }
}
