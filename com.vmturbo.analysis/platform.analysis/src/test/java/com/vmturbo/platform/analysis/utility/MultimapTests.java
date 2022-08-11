package com.vmturbo.platform.analysis.utility;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * This class contains a number of generic tests, performed by many test classes, on multimaps
 * returned by various methods.
 *
 * <p>
 *  e.g. You may want to test that a returned multimap is unmodifiable or that it implements all
 *  optional operations.
 * </p>
 */
public final class MultimapTests {
    // Methods

    /**
     * Tests whether a given multimap supports all optional operations (which implies it's
     * modifiable).
     *
     * <p>
     *  This is not intended as a thorough test of the correct implementation of Multimap semantics.
     *  Its purpose is to test that all Multimap operations are supported and that they behave
     *  reasonably.
     * </p>
     *
     * @param map The multimap that will be tested.
     * @param key An auxiliary key, not in the multimap, to help test some operations.
     * @param value An auxiliary value, not in the multimap, to help test some operations.
     */
    public static <K,V> void verifyModifiable(@NonNull Multimap<@NonNull K, @NonNull V> map,
                                              @NonNull K key, @NonNull V value) {
        assertNotNull(map.asMap());
        assertTrue(map.equals(map));
        assertTrue(map.get(key).isEmpty());
        assertTrue(map.removeAll(key).isEmpty());
        assertTrue(map.replaceValues(key,map.values()).isEmpty());
        assertFalse(map.containsEntry(key, value));
        assertFalse(map.containsKey(key));
        assertFalse(map.containsValue(value));
        assertNotNull(map.entries());
        assertNotNull(map.keys());
        assertNotNull(map.keySet());
        assertNotNull(map.values());
        assertTrue(0 <= map.size());
        assertTrue(map.put(key, value));
        assertTrue(map.putAll(key, map.values()));
        assertTrue(map.putAll(ArrayListMultimap.create(map)));
        assertTrue(map.remove(key, value));

        map.clear();
        assertTrue(map.isEmpty());
    }

    /**
     * Tests whether a given multimap supports all mandatory operations.
     *
     * <p>
     *  This is intended to be used in conjunction with another method that tests that the optional
     *  operations are not supported. It is not intended to thoroughly test Multimap semantics.
     * </p>
     *
     * @param map The multimap that will be tested.
     * @param key An auxiliary key, not in the multimap, to help test some operations.
     * @param value An auxiliary value, not in the multimap, to help test some operations.
     *
     * @see #verifyUnmodifiableInvalidOperations(Multimap, Object, Object)
     */
    public static <K,V> void verifyUnmodifiableValidOperations(@NonNull Multimap<@NonNull K, @NonNull V> map,
                                                               @NonNull K key, @NonNull V value) {
        assertNotNull(map.asMap());
        assertNotNull(map.entries());
        assertNotNull(map.keys());
        assertNotNull(map.keySet());
        assertNotNull(map.values());
        assertTrue(map.equals(map));
        assertTrue(map.get(key).isEmpty());
        assertFalse(map.containsEntry(key, value));
        assertFalse(map.containsKey(key));
        assertFalse(map.containsValue(value));
        assertTrue(0 <= map.size());
        map.isEmpty();
    }

    /**
     * Tests that a given multimap does not support any of the optional operations. This implies it
     * is unmodifiable.
     *
     * <p>
     *  This is intended to be used in conjunction with another method that tests that the mandatory
     *  operations are supported. It is not intended to thoroughly test Multimap semantics.
     * </p>
     *
     * @param map The multimap that will be tested.
     * @param key An auxiliary key, not in the multimap, to help test some operations.
     * @param value An auxiliary value, not in the multimap, to help test some operations.
     *
     * @see #verifyUnmodifiableValidOperations(Multimap, Object, Object)
     */
    public static <K,V> void verifyUnmodifiableInvalidOperations(@NonNull Multimap<@NonNull K, @NonNull V> map,
                                                                 @NonNull K key, @NonNull V value) {
        try {
            map.put(key, value);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try {
            map.putAll(key, map.values());
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try {
            map.putAll(ArrayListMultimap.create(map));
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try {
            map.remove(key, value);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try {
            map.removeAll(key);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try {
            map.replaceValues(key,map.values());
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try {
            map.clear();
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
    }

} // end MultimapTests class
