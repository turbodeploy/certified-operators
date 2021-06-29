package com.vmturbo.cloud.common.stat;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudStatGranularity;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.TimeFrameCalculator;

@RunWith(MockitoJUnitRunner.class)
public class CloudGranularityCalculatorTest {

    @Mock
    private TimeFrameCalculator timeFrameCalculator;

    private CloudGranularityCalculator granularityCalculator;

    @Before
    public void setup() {
        granularityCalculator = new CloudGranularityCalculator(timeFrameCalculator);
    }

    @Test
    public void testStartTimeToGranularity() {

        final Instant startTime = Instant.ofEpochMilli(123);

        // setup time frame calculator
        when(timeFrameCalculator.millis2TimeFrame(anyLong())).thenReturn(TimeFrame.MONTH);

        // invoke SUT
        final CloudStatGranularity actualGranularity =
                granularityCalculator.startTimeToGranularity(startTime);

        // verify time frame calculator invocation
        final ArgumentCaptor<Long> startTimeMillisCaptor = ArgumentCaptor.forClass(Long.class);
        verify(timeFrameCalculator).millis2TimeFrame(startTimeMillisCaptor.capture());

        // ASSERTIONS
        assertThat(startTimeMillisCaptor.getValue(), equalTo(123L));
        assertThat(actualGranularity, equalTo(CloudStatGranularity.MONTHLY));
    }
}
