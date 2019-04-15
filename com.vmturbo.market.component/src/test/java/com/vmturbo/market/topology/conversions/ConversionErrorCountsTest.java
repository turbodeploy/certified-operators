package com.vmturbo.market.topology.conversions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.market.topology.conversions.ConversionErrorCounts.ErrorCategory;
import com.vmturbo.market.topology.conversions.ConversionErrorCounts.Phase;

public class ConversionErrorCountsTest {

    private ConversionErrorCounts errorCounts = new ConversionErrorCounts();

    @Test
    public void testPhaseErrorCount() {
        errorCounts.startPhase(Phase.CONVERT_FROM_MARKET);
        errorCounts.recordError(ErrorCategory.MAX_QUANTITY_NEGATIVE, "bar");
        errorCounts.recordError(ErrorCategory.MAX_QUANTITY_NEGATIVE, "bar");
        errorCounts.recordError(ErrorCategory.MAX_QUANTITY_NEGATIVE, "bar2");

        assertThat(errorCounts.getCurPhaseDebug(), is(Phase.CONVERT_FROM_MARKET));
        assertThat(errorCounts.getErrorCountsDebug().size(), is(2));
        assertThat(errorCounts.getErrorCountsDebug()
            .get(ErrorCategory.MAX_QUANTITY_NEGATIVE, "bar").getValue(), is(2));
        assertThat(errorCounts.getErrorCountsDebug()
            .get(ErrorCategory.MAX_QUANTITY_NEGATIVE, "bar2").getValue(), is(1));
    }

    @Test
    public void testEndPhaseClears() {
        errorCounts.startPhase(Phase.CONVERT_FROM_MARKET);
        errorCounts.recordError(ErrorCategory.MAX_QUANTITY_NEGATIVE, "bar");
        errorCounts.recordError(ErrorCategory.MAX_QUANTITY_NEGATIVE, "bar");
        errorCounts.recordError(ErrorCategory.MAX_QUANTITY_NEGATIVE, "bar2");
        errorCounts.endPhase();

        assertThat(errorCounts.getCurPhaseDebug(), is(Phase.NONE));
        assertTrue(errorCounts.getErrorCountsDebug().isEmpty());
    }
}
