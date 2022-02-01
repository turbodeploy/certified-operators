package com.vmturbo.sql.utils.jooq;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.time.LocalDateTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

/**
 * Test that procedure is scheduled to run at expected pace.
 */
public class PurgeEventTest {

    private static final Runnable noOp = () -> { };
    private final PurgeEvent purgeEvent = spy(new PurgeEvent("test", noOp));
    private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    /**
     * Setup before each test.
     */
    @Before
    public void setup() {
        doReturn(scheduler).when(purgeEvent).getScheduler();
    }

    /**
     * Test that procedure is scheduled to run at 0 o'clock.
     */
    @Test
    public void testScheduledToRunAt0() {
        doReturn(LocalDateTime.of(2022, 1, 20, 20, 0)).when(purgeEvent).getCurrentDateTime();
        purgeEvent.schedule(0, 24);
        verify(scheduler).scheduleAtFixedRate(eq(noOp), eq(14400L), eq(86400L), eq(TimeUnit.SECONDS));
    }

    /**
     * Test that procedure is scheduled to run at 2 o'clock.
     */
    @Test
    public void testScheduledToRunAt2() {
        doReturn(LocalDateTime.of(2022, 1, 20, 0, 0)).when(purgeEvent).getCurrentDateTime();
        purgeEvent.schedule(2, 24);
        verify(scheduler).scheduleAtFixedRate(eq(noOp), eq(93600L), eq(86400L), eq(TimeUnit.SECONDS));
    }
}