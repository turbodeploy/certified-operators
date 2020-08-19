package com.vmturbo.components.common.health;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.components.common.config.PropertiesConfigSource;

/**
 * Test the "interesting changes" logic in MemoryMonitor.
 */
@RunWith(Parameterized.class)
public class MemoryMonitorInterestingChangesTest {
    /**
     * List of test scenarios.
     * @return list of test scenarios.
     */
    @Parameters
    public static Collection<Object[]> scenarios() {
        // test scenarios for matching scope hash keys
        return Arrays.asList(new Object[][] {
                {0.2, 0.0, false}, // under logging threshold -- not interesting
                {0.50, 0.0, true}, // crosses logging threshold and over min delta -- interesting
                {0.50, 0.49, false}, // crosses logging threshold, but under min delta -- not interesting
                {0.52, 0.51, false}, // over threshold but under min delta -- not interesting
                {0.53, 0.51, true}, // over threshold and equal to delta -- interesting
                {0.49, 0.50, false}, // crossing back under threshold but under delta -- not interesting
                {0.49, 0.51, true}, // crossing under threshold and equal delta -- interesting
        });
    }

    private final double newHeapRatio;
    private final double lastLoggedHeapRatio;
    private final boolean shouldBeInteresting;

    /**
     * Parameterized test runner.
     * @param newHeapRatio new heap ratio to check
     * @param lastLoggedHeapRatio last heap ratio to compare to
     * @param shouldBeInteresting expected result
     */
    public MemoryMonitorInterestingChangesTest(double newHeapRatio, double lastLoggedHeapRatio, boolean shouldBeInteresting) {
        this.newHeapRatio = newHeapRatio;
        this.lastLoggedHeapRatio = lastLoggedHeapRatio;
        this.shouldBeInteresting = shouldBeInteresting;
    }

    /**
     * The actual test.
     */
    @Test
    public void testIsInteresting() {
        PropertiesConfigSource config = new PropertiesConfigSource();
        config.setProperty(MemoryMonitor.PROP_MIN_INTERESTING_HEAP_USED_RATIO_CHANGE, 0.02d);
        config.setProperty(MemoryMonitor.PROP_START_LOGGING_AT_HEAP_USED_RATIO, 0.50d);
        MemoryMonitor memoryMonitor = new MemoryMonitor(config);
        assertEquals(shouldBeInteresting, memoryMonitor.isInterestingChange(newHeapRatio, lastLoggedHeapRatio));
    }
}
