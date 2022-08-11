package com.vmturbo.platform.analysis.utility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * This class contains a number of generic tests, performed by many test classes, on lists returned
 * by various methods.
 *
 * <p>
 *  e.g. You may want to test that a returned list is unmodifiable or that it implements all
 *  optional operations.
 * </p>
 */
public final class ListTests {
    // Methods

    /**
     * Tests whether a given list supports all optional operations (which implies it's modifiable).
     *
     * <p>
     *  This is not intended as a thorough test of the correct implementation of List semantics.
     *  Its purpose is to test that all List operations are supported and that they behave
     *  reasonably.
     * </p>
     *
     * @param elements The list that will be tested.
     * @param element An auxiliary element, not in the list, to help test some operations.
     */
    public static <E> void verifyModifiable(@NonNull List<@NonNull E> elements, @NonNull E element) {
        CollectionTests.verifyModifiable(elements, element);

        assertEquals(-1, elements.indexOf(null));
        assertEquals(-1, elements.lastIndexOf(null));
        assertNotNull(elements.listIterator());
        assertNotNull(elements.subList(0, 0));
        elements.add(0,element);
        assertNotNull(elements.listIterator(0));
        assertSame(element, elements.get(0));
        assertTrue(elements.addAll(0,elements));
        elements.remove(0);
        assertSame(element, elements.set(0, element));
    }

    /**
     * Tests whether a given list supports all mandatory operations.
     *
     * <p>
     *  This is intended to be used in conjunction with another method that tests that the optional
     *  operations are not supported. It is not intended to thoroughly test List semantics.
     * </p>
     *
     * @param elements The list that will be tested.
     * @param element An auxiliary element, not in the list, to help test some operations.
     *
     * @see #verifyUnmodifiableInvalidOperations(List, Object)
     */
    public static <E> void verifyUnmodifiableValidOperations(@NonNull List<@NonNull E> elements, @NonNull E element) {
        CollectionTests.verifyUnmodifiableValidOperations(elements,element);

        assertEquals(-1, elements.indexOf(null));
        assertEquals(-1, elements.lastIndexOf(null));
        assertNotNull(elements.listIterator());
        assertNotNull(elements.subList(0, 0));
        if (elements.size() > 0) {
            elements.get(0);
            assertNotNull(elements.listIterator(0));
        }
    }

    /**
     * Tests that a given list does not support any of the optional operations. This implies it is
     * unmodifiable.
     *
     * <p>
     *  This is intended to be used in conjunction with another method that tests that the mandatory
     *  operations are supported. It is not intended to thoroughly test List semantics.
     * </p>
     *
     * @param elements The list that will be tested.
     * @param element An auxiliary element, not in the list, to help test some operations.
     *
     * @see #verifyUnmodifiableValidOperations(List, Object)
     */
    public static <E> void verifyUnmodifiableInvalidOperations(@NonNull List<@NonNull E> elements, @NonNull E element) {
        CollectionTests.verifyUnmodifiableInvalidOperations(elements, element);

        // TODO: may need to modify the test to work in a predictable way on empty lists because the
        // API does not guarantee that this exception will be thrown in some cases.
        try{
            elements.add(0,element);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            elements.addAll(0,Arrays.asList(element,element));
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            elements.remove(0);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
        try{
            elements.set(0, element);
            fail();
        } catch(UnsupportedOperationException e) {
            // ignore
        }
    }

} // end ListTests class
