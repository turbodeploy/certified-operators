package com.vmturbo.market.topology.conversions;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;

/**
 * Test the TopologyConversionUtils package.
 **/
public class TopologyConversionUtilsTest {

    @Test
    public void testGetMinDesiredUtilizationOutofRange() {
        TopologyEntityDTO entityDTO =
                TopologyEntityDTO.newBuilder()
                        .setEntityType(1)
                        .setOid(1)
                        .setAnalysisSettings(
                                AnalysisSettings.newBuilder()
                                        .setDesiredUtilizationTarget(20)
                                        .setDesiredUtilizationRange(80)
                                        .build())
                        .build();

        // (20 - (80/2.0))/100
        assertThat(TopologyConversionUtils.getMinDesiredUtilization(entityDTO),
                is(0f));
    }


    @Test
    public void testGetMaxDesiredUtilization() {
        TopologyEntityDTO entityDTO =
                TopologyEntityDTO.newBuilder()
                        .setEntityType(1)
                        .setOid(1)
                        .setAnalysisSettings(
                                AnalysisSettings.newBuilder()
                                        .setDesiredUtilizationTarget(20)
                                        .setDesiredUtilizationRange(80)
                                        .build())
                        .build();

        // (20 + (80/2.0))/100
        assertThat(TopologyConversionUtils.getMaxDesiredUtilization(entityDTO),
                is(0.6f));
    }



    @Test
    public void testGetMaxDesiredUtilizationOutOfRange() {
        TopologyEntityDTO entityDTO =
                TopologyEntityDTO.newBuilder()
                        .setEntityType(1)
                        .setOid(1)
                        .setAnalysisSettings(
                                AnalysisSettings.newBuilder()
                                        .setDesiredUtilizationTarget(80)
                                        .setDesiredUtilizationRange(60)
                                        .build())
                        .build();

        // (80 + (60/2.0))/100
        assertThat(TopologyConversionUtils.getMaxDesiredUtilization(entityDTO),
                is(1f));
    }


    @Test
    public void testLimitFloatRangeLessThanMin() {
        assertThat(TopologyConversionUtils.limitFloatRange(-1.2f, 0f, 1f), is(0f));
    }

    @Test
    public void testLimitFloatRangeGreaterThanMax() {
        assertThat(TopologyConversionUtils.limitFloatRange(100f, 0f, 1f), is(1f));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLimitFloatRangeMinGreaterThanMax() {
        TopologyConversionUtils.limitFloatRange(100f, 10, 0f);
    }

    @Test
    public void testGetMinDesiredUtilization() {
        TopologyEntityDTO entityDTO =
                TopologyEntityDTO.newBuilder()
                        .setEntityType(1)
                        .setOid(1)
                        .setAnalysisSettings(
                                AnalysisSettings.newBuilder()
                                        .setDesiredUtilizationTarget(40)
                                        .setDesiredUtilizationRange(40)
                                        .build())
                        .build();

        // (40 - (40/2.0))/100
        assertThat(TopologyConversionUtils.getMinDesiredUtilization(entityDTO),
                is(0.2f));
    }


    @Test
    public void testLimitFloatRangeWithinRange() {
        assertThat(TopologyConversionUtils.limitFloatRange(1f, 0f, 1f), is(1f));
        assertThat(TopologyConversionUtils.limitFloatRange(0f, 0f, 1f), is(0f));
        assertThat(TopologyConversionUtils.limitFloatRange(0.8f, 0f, 1f), is(0.8f));
    }
}
