package com.vmturbo.platform.analysis.utilities;

import static org.junit.Assert.*;

import java.util.Map.Entry;
import java.util.Set;

import com.vmturbo.commons.analysis.NumericIDAllocator;
import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.vmturbo.platform.analysis.utility.CollectionTests;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

/**
 * A test case for the {@link NumericIDAllocator} class.
 */
@RunWith(JUnitParamsRunner.class)
public class NumericIDAllocatorTest {
    // Fields

    // Methods

    @Test
    public final void testNumericIDAllocator() {
        @NonNull NumericIDAllocator allocator = new NumericIDAllocator();
        assertEquals(0, allocator.size());
        assertEquals(0, allocator.entrySet().size());
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.allocate({1}) == {2}")
    public final void testAllocate(@NonNull NumericIDAllocator allocator, @NonNull String name, int id) {
        assertEquals(id, allocator.allocate(name));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestAllocate() {
        @NonNull NumericIDAllocator a1 = new NumericIDAllocator();
        a1.allocate("a");
        @NonNull NumericIDAllocator a2 = new NumericIDAllocator();
        a2.allocate("a");
        @NonNull NumericIDAllocator a3 = new NumericIDAllocator();
        a3.allocate("a");
        a3.allocate("b");
        @NonNull NumericIDAllocator a4 = new NumericIDAllocator();
        a4.allocate("a");
        a4.allocate("b");
        @NonNull NumericIDAllocator a5 = new NumericIDAllocator();
        a5.allocate("a");
        a5.allocate("b");

        return new Object[][] {
            {new NumericIDAllocator(), "foo", 0},
            {new NumericIDAllocator(), "bar", 0},
            {a1, "a", 0},
            {a2, "b", 1},
            {a3, "a", 0},
            {a4, "b", 1},
            {a5, "c", 2}
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.getId({1}) == {2}")
    public final void testGetId_NormalInput(@NonNull NumericIDAllocator allocator, @NonNull String name, int id) {
        assertEquals(id, allocator.getId(name));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestGetId_NormalInput() {
        @NonNull NumericIDAllocator a1 = new NumericIDAllocator();
        a1.allocate("a");
        @NonNull NumericIDAllocator a2 = new NumericIDAllocator();
        a2.allocate("a");
        a2.allocate("b");

        return new Object[][] {
            {a1, "a", 0},
            {a2, "a", 0},
            {a2, "b", 1},
        };
    }

    @Test(expected = NullPointerException.class)
    @Parameters
    @TestCaseName("Test #{index}: {0}.getId({1})")
    public final void testGetId_InvalidInput(@NonNull NumericIDAllocator allocator, @NonNull String name) {
        allocator.getId(name);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestGetId_InvalidInput() {
        @NonNull NumericIDAllocator a1 = new NumericIDAllocator();
        a1.allocate("a");
        @NonNull NumericIDAllocator a2 = new NumericIDAllocator();
        a2.allocate("a");
        a2.allocate("b");

        return new Object[][] {
            {new NumericIDAllocator(), "foo"},
            {a1, "b"},
            {a2, "c"},
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.getName({1}) == {2}")
    public final void testGetName_NormalInput(@NonNull NumericIDAllocator allocator, int id, @NonNull String name) {
        assertEquals(name, allocator.getName(id));
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestGetName_NormalInput() {
        @NonNull NumericIDAllocator a1 = new NumericIDAllocator();
        a1.allocate("a");
        @NonNull NumericIDAllocator a2 = new NumericIDAllocator();
        a2.allocate("a");
        a2.allocate("b");

        return new Object[][] {
            {a1, 0, "a"},
            {a2, 0, "a"},
            {a2, 1, "b"},
        };
    }

    @Test(expected = IndexOutOfBoundsException.class)
    @Parameters
    @TestCaseName("Test #{index}: {0}.getName({1})")
    public final void testGetName_InvalidInput(@NonNull NumericIDAllocator allocator, int id) {
        allocator.getName(id);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestGetName_InvalidInput() {
        @NonNull NumericIDAllocator a1 = new NumericIDAllocator();
        a1.allocate("a");
        @NonNull NumericIDAllocator a2 = new NumericIDAllocator();
        a2.allocate("a");
        a2.allocate("b");

        return new Object[][] {
            {new NumericIDAllocator(), 0},
            {a1, -1},
            {a1, 1},
            {a2, -1},
            {a2, 2},
        };
    }

    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}.size() == {1}")
    public final void testSizeAndIsEmpty(@NonNull NumericIDAllocator allocator, int size, boolean empty) {
        assertEquals(size, allocator.size());
        assertEquals(empty, allocator.isEmpty());
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSizeAndIsEmpty() {
        @NonNull NumericIDAllocator a1 = new NumericIDAllocator();
        a1.allocate("a");
        @NonNull NumericIDAllocator a2 = new NumericIDAllocator();
        a2.allocate("a");
        a2.allocate("b");

        return new Object[][] {
            {new NumericIDAllocator(), 0, true},
            {a1, 1, false},
            {a2, 2, false},
        };
    }

    @Test
    public final void testEntrySet() {
        @PolyRead Set<Entry<@NonNull String, @NonNull Integer>> entrySet = new NumericIDAllocator().entrySet();
        Entry<@NonNull String, @NonNull Integer> entry = new Entry<String, Integer>() {
            @Override public String getKey() {return null;}
            @Override public Integer getValue() {return null;}
            @Override public Integer setValue(Integer value) {return null;}};
        CollectionTests.verifyUnmodifiableValidOperations(entrySet,entry); // TODO: test set operations
        CollectionTests.verifyUnmodifiableInvalidOperations(entrySet,entry);
    }

} // end NumericIDAllocatorTest class
