package com.vmturbo.history.db;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.auth.api.db.DBPasswordUtil;

/**
 * Tests for the stats value clipping function. Clipping ensures that the stats value to be
 * written fits within the DB field.
 */
public class ClipStatsValueTest {

    // min and max for numerical values for the xxx_stats_yyy tables; must fit in DECIMAL(15,3)
    private static final double MIN_STATS_VALUE = -999999999999D;
    private static final double MAX_STATS_VALUE = 999999999999D;

    private static HistorydbIO historydbIO;

    @BeforeClass
    public static void setup() {
        // always return the default DB password for this test
        DBPasswordUtil dbPasswordUtilMock = Mockito.mock(DBPasswordUtil.class);
        when(dbPasswordUtilMock.getSqlDbRootPassword()).thenReturn(DBPasswordUtil.obtainDefaultPW());
        historydbIO = new HistorydbIO(dbPasswordUtilMock, null);
    }

    /**
     * Large Positive value should be clipped to 12 digits.
     */
    @Test
    public void clipValueTest() throws Exception {
        singleClipTest("Large Positive value should be clipped to 12 digits.",
                Double.MAX_VALUE, MAX_STATS_VALUE);
        singleClipTest("Large Negative value should be clipped to 12 digits.",
                -Double.MAX_VALUE, MIN_STATS_VALUE);
        singleClipTest("Normal positive value should be the same.",
                Math.PI, Math.PI);
        singleClipTest("Normal negative value should be the same.",
                -Math.PI, -Math.PI);
        singleClipTest("Tiny positive value should be the same.",
                Double.MIN_VALUE, Double.MIN_VALUE);
        singleClipTest("Normal negative value should be the same.",
                -Double.MIN_VALUE, -Double.MIN_VALUE);
        singleClipTest("Value to clip may be null.",
                null, null);
    }

    private void singleClipTest(String description, Double testValue, Double expectedValue) {
        // act
        Double clipped = historydbIO.clipValue(testValue);
        // assert
        assertThat(description, clipped, is(expectedValue));
    }

}
