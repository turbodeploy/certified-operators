package com.vmturbo.stitching.journal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Deque;

import org.junit.Test;

public class JsonDiffDescriptionTest {
    @Test
    public void testCreatePathDeque() {
        final JsonDiffDescription description = new JsonDiffDescription();
        description.setPath("/foo/bar/9");

        final Deque<String> pathDeque = description.createPathDeque();
        assertEquals(3, pathDeque.size());
        assertEquals("foo", pathDeque.removeFirst());
        assertEquals("bar", pathDeque.removeFirst());
        assertEquals("9", pathDeque.removeFirst());
    }

    @Test
    public void testCreateFromPathDeque() {
        final JsonDiffDescription description = new JsonDiffDescription();
        description.setFrom("/foo/bar/9");
        description.setPath("/qux/baz/1");

        final Deque<String> pathDeque = description.createPathDeque();
        assertEquals(3, pathDeque.size());
        assertEquals("foo", pathDeque.removeFirst());
        assertEquals("bar", pathDeque.removeFirst());
        assertEquals("9", pathDeque.removeFirst());
    }

    @Test
    public void testNullPathDeque() {
        final JsonDiffDescription description = new JsonDiffDescription();

        assertTrue(description.createPathDeque().isEmpty());
    }

    @Test
    public void testEmptyPathDeque() {
        final JsonDiffDescription description = new JsonDiffDescription();
        description.setPath("");

        assertTrue(description.createPathDeque().isEmpty());
    }
}