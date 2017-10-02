package com.vmturbo.topology.processor.communication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.google.common.base.Objects;

/**
 * Tests for the {@link PassiveAdjustableExpiringMap} class.
 */
public class PassiveAdjustableExpiringMapTest {

    /**
     * Test implementation of an expiring value.
     */
    private static class TestExpirationValue implements ExpiringValue {

        private final int value;

        public TestExpirationValue(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        @Override
        public long expirationTime() {
            // odd keys expire immediately, even keys never expire
            if (value % 2 == 0) {
                return -1;
            }

            return 0;
        }

        @Override
        public void onExpiration() {

        }

        @Override
        public boolean equals(Object obj) {
            return obj != null &&
                   getClass() == obj.getClass() &&
                   value == ((TestExpirationValue)obj).value;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(value);

        }
    }

    /**
     * An expiring value that lives for an amount of time specified by its time-to-live.
     * The time to live can be updated. Adds itself to a list on expiration.
     */
    private static class TimeToLiveExpirationValue implements ExpiringValue {

        private final String value;
        private long timeToLive;
        private final List<String> expirations;
        private final long refreshTime;

        public TimeToLiveExpirationValue(String value, long timeToLive, @Nonnull List<String> expirations) {
            refreshTime = System.currentTimeMillis();
            this.value = value;
            this.timeToLive = timeToLive;
            this.expirations = expirations;
        }

        @Override
        public long expirationTime() {
            return refreshTime + timeToLive;
        }

        @Override
        public void onExpiration() {
            expirations.add(value);
        }

        public void setTimeToLive(long timeToLive) {
            this.timeToLive = timeToLive;
        }
    }

    private List<String> expirations = new ArrayList<>();

    private Map<Integer, TestExpirationValue> makeDecoratedTestMap() {
        final Map<Integer, TestExpirationValue> m = new HashMap<>();
        m.put(Integer.valueOf(1), new TestExpirationValue(1));
        m.put(Integer.valueOf(2), new TestExpirationValue(2));
        m.put(Integer.valueOf(3), new TestExpirationValue(3));
        m.put(Integer.valueOf(4), new TestExpirationValue(4));
        m.put(Integer.valueOf(5), new TestExpirationValue(5));
        m.put(Integer.valueOf(6), new TestExpirationValue(6));
        return new PassiveAdjustableExpiringMap<>(m, Clock.systemUTC());
    }

    private Map<Integer, TestExpirationValue> makeTestMap() {
        final Map<Integer, TestExpirationValue> m = new PassiveAdjustableExpiringMap<>();
        m.put(Integer.valueOf(1), new TestExpirationValue(1));
        m.put(Integer.valueOf(2), new TestExpirationValue(2));
        m.put(Integer.valueOf(3), new TestExpirationValue(3));
        m.put(Integer.valueOf(4), new TestExpirationValue(4));
        m.put(Integer.valueOf(5), new TestExpirationValue(5));
        m.put(Integer.valueOf(6), new TestExpirationValue(6));
        return m;
    }

    @Test
    public void testConstructors() {
        try {
            final Map<Integer, TestExpirationValue> map = null;
            new PassiveAdjustableExpiringMap<>(map, Clock.systemUTC());
            fail("constructor - exception should have been thrown.");
        } catch (final NullPointerException ex) {
            // success
        }
    }

    @Test
    public void testContainsKey() {
        final Map<Integer, TestExpirationValue> m = makeTestMap();
        assertFalse(m.containsKey(1));
        assertFalse(m.containsKey(3));
        assertFalse(m.containsKey(5));
        assertTrue(m.containsKey(2));
        assertTrue(m.containsKey(4));
        assertTrue(m.containsKey(6));
    }

    @Test
    public void testContainsValue() {
        final Map<Integer, TestExpirationValue> m = makeTestMap();
        assertFalse(m.containsValue(new TestExpirationValue(1)));
        assertFalse(m.containsValue(new TestExpirationValue(3)));
        assertFalse(m.containsValue(new TestExpirationValue(5)));
        assertTrue(m.containsValue(new TestExpirationValue(2)));
        assertTrue(m.containsValue(new TestExpirationValue(4)));
        assertTrue(m.containsValue(new TestExpirationValue(6)));
    }

    @Test
    public void testDecoratedMap() {
        // entries should expire
        final Map<Integer, TestExpirationValue> m = makeDecoratedTestMap();
        assertEquals(3, m.size());
        assertNull(m.get(1));

        // removing a single item shouldn't affect any other items
        assertEquals(2, m.get(2).getValue());
        m.remove(2);
        assertEquals(2, m.size());
        assertNotNull(m.get(4));
        assertNull(m.get(2));

        // adding a single, even item shouldn't affect any other items
        assertNull(m.get(2));
        m.put(2, new TestExpirationValue(2));
        assertEquals(3, m.size());
        assertEquals(4, m.get(4).getValue());
        assertEquals(2, m.get(2).getValue());

        // adding a single, odd item other items
        // the entry expires immediately
        m.put(1, new TestExpirationValue(1));
        assertEquals(3, m.size());
        assertNull(m.get(1));
        assertEquals(2, m.get(2).getValue());
    }

    @Test
    public void testEntrySet() {
        final Map<Integer, TestExpirationValue> m = makeTestMap();
        assertEquals(3, m.entrySet().size());
    }

    @Test
    public void testGet() {
        final Map<Integer, TestExpirationValue> m = makeTestMap();
        assertNull(m.get(1));
        assertEquals(2, m.get(2).getValue());
        assertNull(m.get(3));
        assertEquals(4, m.get(4).getValue());
        assertNull(m.get(5));
        assertEquals(6, m.get(6).getValue());
    }

    @Test
    public void testIsEmpty() {
        Map<Integer, TestExpirationValue> m = makeTestMap();
        assertFalse(m.isEmpty());

        // remove just evens
        m = makeTestMap();
        m.remove(2);
        m.remove(4);
        m.remove(6);
        assertTrue(m.isEmpty());
    }

    @Test
    public void testKeySet() {
        final Map<Integer, TestExpirationValue> m = makeTestMap();
        assertEquals(3, m.keySet().size());
    }

    @Test
    public void testSize() {
        final Map<Integer, TestExpirationValue> m = makeTestMap();
        assertEquals(3, m.size());
    }

    @Test
    public void testValues() {
        final Map<Integer, TestExpirationValue> m = makeTestMap();
        assertEquals(3, m.values().size());
    }

    @Test
    public void testZeroTimeToLive() {
        // item should not be available
        final PassiveAdjustableExpiringMap<String, TimeToLiveExpirationValue> m = new PassiveAdjustableExpiringMap<>();
        m.put("a", new TimeToLiveExpirationValue("b", 0, expirations));
        assertNull(m.get("a"));
        assertTrue(expirations.contains("b"));
    }

    @Test
    public void testExpiration() {
        validateExpiration(new PassiveAdjustableExpiringMap<>(), 100);
        validateExpiration(new PassiveAdjustableExpiringMap<>(), 200);
    }

    @Test
    public void testModifiedExpiration() {
        PassiveAdjustableExpiringMap<String, TimeToLiveExpirationValue> map = new PassiveAdjustableExpiringMap<>();

        TimeToLiveExpirationValue value = new TimeToLiveExpirationValue("b", TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS), expirations);

        // The value should not be expired initially
        map.put("a", value);
        assertNotNull(map.get("a"));

        // Updating the value's time-to-live should cause it to be expired in the map.
        value.setTimeToLive(0);
        assertNull(map.get("a"));
    }

    private void validateExpiration(final Map<String, TimeToLiveExpirationValue> map, long timeout) {
        map.put("a", new TimeToLiveExpirationValue("b", timeout, expirations));

        assertNotNull(map.get("a"));

        try {
            Thread.sleep(2 * timeout);
        } catch (InterruptedException e) {
            fail();
        }

        assertNull(map.get("a"));
        assertTrue(expirations.contains("b"));
    }

}
