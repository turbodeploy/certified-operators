package com.vmturbo.action.orchestrator.action;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit tests for {@link VisibilityLevel} enumeration.
 */
public class VisibilityLevelTest {

    /**
     * Test for {@link VisibilityLevel#ALWAYS_VISIBLE} level.
     */
    @Test
    public void testAlwaysVisible() {
        assertTrue(VisibilityLevel.ALWAYS_VISIBLE.checkVisibility(null));
        assertTrue(VisibilityLevel.ALWAYS_VISIBLE.checkVisibility(true));
        assertFalse(VisibilityLevel.ALWAYS_VISIBLE.checkVisibility(false));
    }

    /**
     * Test for {@link VisibilityLevel#HIDDEN_BY_DEFAULT} level.
     */
    @Test
    public void testHiddenByDefault() {
        assertTrue(VisibilityLevel.HIDDEN_BY_DEFAULT.checkVisibility(null));
        assertFalse(VisibilityLevel.HIDDEN_BY_DEFAULT.checkVisibility(true));
        assertTrue(VisibilityLevel.HIDDEN_BY_DEFAULT.checkVisibility(false));
    }

    /**
     * Test for {@link VisibilityLevel#ALWAYS_HIDDEN} level.
     */
    @Test
    public void testAlwaysHiddenDefault() {
        assertFalse(VisibilityLevel.ALWAYS_HIDDEN.checkVisibility(null));
        assertFalse(VisibilityLevel.ALWAYS_HIDDEN.checkVisibility(true));
        assertFalse(VisibilityLevel.ALWAYS_HIDDEN.checkVisibility(false));
    }
}
