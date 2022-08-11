package com.vmturbo.platform.analysis.utility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class contains a number of generic tests, performed by many test classes, on sets returned
 * by various methods.
 *
 * <p>
 *  e.g. You may want to test that a returned set is unmodifiable or that it implements all optional
 *  operations.
 * </p>
 */
public final class SetTests {
    // Methods

    /**
     * Tests whether a given set supports all optional operations (which implies it's modifiable).
     *
     * <p>
     *  This is not intended as a thorough test of the correct implementation of Set semantics.
     *  Its purpose is to test that all Set operations are supported and that they behave
     *  reasonably.
     * </p>
     *
     * @param set The set that will be tested.
     * @param element An auxiliary element, not in the set, to help test some operations.
     */
    public static <E> void verifyModifiable(@NonNull Set<@NonNull E> set, @NonNull E element) {
        assertFalse(set.contains(element));
        assertTrue(set.add(element));
        assertTrue(set.contains(element));
        assertTrue(set.remove(element));
        assertFalse(set.contains(element));
        set.clear();
        assertTrue(set.isEmpty());
    }

    /**
     * Tests whether a given set supports all mandatory operations.
     *
     * <p>
     *  This is intended to be used in conjunction with another method that tests that the optional
     *  operations are not supported. It is not intended to thoroughly test Set semantics.
     * </p>
     *
     * @param set The set that will be tested.
     * @param element An auxiliary element, not in the set, to help test some operations.
     *
     * @see #verifyUnmodifiableInvalidOperations(Set, Object)
     */
    public static <E> void verifyUnmodifiableValidOperations(@NonNull Set<@NonNull E> set, @NonNull E element) {
        CollectionTests.verifyUnmodifiableValidOperations(set, element);

        assertFalse(set.contains(element));
        set.size();
        set.isEmpty();
        set.iterator();
        set.toArray();
    }

    /**
     * Tests that a given set does not support any of the optional operations. This implies it is
     * unmodifiable.
     *
     * <p>
     *  This is intended to be used in conjunction with another method that tests that the mandatory
     *  operations are supported. It is not intended to thoroughly test Set semantics.
     * </p>
     *
     * @param set The set that will be tested.
     * @param element An auxiliary element, not in the set, to help test some operations.
     *
     * @see #verifyUnmodifiableValidOperations(Set, Object)
     */
    public static <E> void verifyUnmodifiableInvalidOperations(@NonNull Set<@NonNull E> set, @NonNull E element) {
        CollectionTests.verifyUnmodifiableInvalidOperations(set, element);

        try{
            set.add(element);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            set.addAll(Arrays.asList(element, element));
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            set.remove(element);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            set.removeAll(Arrays.asList(element, element));
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            set.clear();
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            set.retainAll(Arrays.asList(element, element));
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
    }

} // end SetTests class
