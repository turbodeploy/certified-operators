package com.vmturbo.market.runner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.HashMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig.Builder;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;

/**
 * A test case for the {@link AnalysisFactory} class.
 */
@RunWith(Enclosed.class)
public class AnalysisFactoryTest {

    /**
     * A test case for the {@link AnalysisConfig} class.
     */
    @RunWith(JUnitParamsRunner.class)
    public static class AnalysisConfigTest {

        /**
         * Tests that the <b>use quote cache during SNM</b> field can be correctly stored by the
         * {@link Builder} object and retrieved by the built {@link AnalysisConfig} object.
         */
        @Test
        @Parameters({"true", "false"})
        @TestCaseName("Test #{index}: (set|get)UseQuoteCacheDuringSNM({0})")
        public final void testGetSetUseQuoteCacheDuringSNM(boolean useQuoteCacheDuringSNM) {
            Builder builder = AnalysisConfig.newBuilder(0, 0,
                SuspensionsThrottlingConfig.DEFAULT, new HashMap<>());
            assertSame(builder, builder.setUseQuoteCacheDuringSNM(useQuoteCacheDuringSNM));
            assertEquals(useQuoteCacheDuringSNM, builder.build().getUseQuoteCacheDuringSNM());
        }

        /**
         * Tests that the <b>replay provisions during real-time</b> field can be correctly stored by
         * the {@link Builder} object and retrieved by the built {@link AnalysisConfig} object.
         */
        @Test
        @Parameters({"true", "false"})
        @TestCaseName("Test #{index}: (set|get)ReplayProvisionsForRealTime({0})")
        public final void
                        testGetSetReplayProvisionsForRealTime(boolean replayProvisionsForRealTime) {
            Builder builder = AnalysisConfig.newBuilder(0, 0,
                SuspensionsThrottlingConfig.DEFAULT, new HashMap<>());
            assertSame(builder,
                builder.setReplayProvisionsForRealTime(replayProvisionsForRealTime));
            assertEquals(replayProvisionsForRealTime,
                builder.build().getReplayProvisionsForRealTime());
        }
    }
}
