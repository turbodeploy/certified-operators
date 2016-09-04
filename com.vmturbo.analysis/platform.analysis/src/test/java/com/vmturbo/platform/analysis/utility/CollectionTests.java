package com.vmturbo.platform.analysis.utility;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class contains a number of generic tests, performed by many test classes, on collections
 * returned by various methods.
 *
 * <p>
 *  e.g. You may want to test that a returned collection is unmodifiable or that it implements all
 *  optional operations.
 * </p>
 */
public final class CollectionTests {
    // Methods

    /**
     * Tests whether a given collection supports all optional operations (which implies it's
     * modifiable).
     *
     * <p>
     *  This is not intended as a thorough test of the correct implementation of Collection
     *  semantics. Its purpose is to test that all Collection operations are supported and that they
     *  behave reasonably.
     * </p>
     *
     * @param elements The collection that will be tested.
     * @param element An auxiliary element, not in the collection, to help test some operations.
     */
    public static <E> void verifyModifiable(@NonNull Collection<@NonNull E> elements, @NonNull E element) {
        assertFalse(elements.contains(element));
        assertFalse(elements.containsAll(Arrays.asList(element,element)));
        assertTrue(elements.equals(elements));
        elements.isEmpty();
        assertNotNull(elements.iterator());
        assertTrue(0 <= elements.size());
        assertNotNull(elements.toArray());
        assertNotNull(elements.toArray(new Object[elements.size()]));
        assertTrue(elements.add(element));
        assertTrue(elements.addAll(elements));
        assertTrue(elements.remove(element));
        assertTrue(elements.removeAll(elements));
        assertFalse(elements.retainAll(elements));
        elements.clear();
    }

    /**
     * Tests whether a given collection supports all mandatory operations.
     *
     * <p>
     *  This is intended to be used in conjunction with another method that tests that the optional
     *  operations are not supported. It is not intended to thoroughly test Collection semantics.
     * </p>
     *
     * @param elements The collection that will be tested.
     * @param element An auxiliary element, not in the collection, to help test some operations.
     *
     * @see #verifyUnmodifiableInvalidOperations(Collection, Object)
     */
    public static <E> void verifyUnmodifiableValidOperations(@NonNull Collection<@NonNull E> elements, @NonNull E element) {
        assertFalse(elements.contains(element));
        assertFalse(elements.containsAll(Arrays.asList(element,element)));
        assertTrue(elements.equals(elements));
        elements.isEmpty();
        assertNotNull(elements.iterator());
        assertTrue(0 <= elements.size());
        assertNotNull(elements.toArray());
        assertNotNull(elements.toArray(new Object[elements.size()]));
    }

    /**
     * Tests that a given collection does not support any of the optional operations. This implies
     * it is unmodifiable.
     *
     * <p>
     *  This is intended to be used in conjunction with another method that tests that the mandatory
     *  operations are supported. It is not intended to thoroughly test Collection semantics.
     * </p>
     *
     * @param elements The collection that will be tested.
     * @param element An auxiliary element, not in the collection, to help test some operations.
     *
     * @see #verifyUnmodifiableValidOperations(Collection, Object)
     */
    public static <E> void verifyUnmodifiableInvalidOperations(@NonNull Collection<@NonNull E> elements, @NonNull E element) {
        // TODO: may need to modify the test to work in a predictable way on empty collections
        // because the API does not guarantee that this exception will be thrown in some cases.
        try{
            elements.add(element);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            elements.addAll(Arrays.asList(element,element));
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            elements.clear();
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            elements.remove(element);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            elements.removeAll(elements);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            elements.retainAll(elements);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
    }

} // end CollectionTests class
