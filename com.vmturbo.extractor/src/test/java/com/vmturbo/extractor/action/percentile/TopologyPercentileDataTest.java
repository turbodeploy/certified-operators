package com.vmturbo.extractor.action.percentile;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.extractor.action.percentile.TopologyPercentileData.CommType;

/**
 * Unit tests for {@link TopologyPercentileData}.
 */
public class TopologyPercentileDataTest {

    /**
     * Test putting percentiles and retrieving them.
     */
    @Test
    public void testPutAndGet() {
        TopologyPercentileData percentileData = new TopologyPercentileData();
        percentileData.putSoldPercentile(1L, comm(UICommodityType.MEM, null), 1);
        percentileData.putSoldPercentile(1L, comm(UICommodityType.MEM, "foo"), .2);
        percentileData.putSoldPercentile(1L, comm(UICommodityType.MEM, "foooo"), .2);
        percentileData.putSoldPercentile(2L, comm(UICommodityType.MEM, null), .3);

        assertThat(percentileData.getSoldPercentile(1L, comm(UICommodityType.MEM, null)).get(), closeTo(100.0, 0.0001));
        assertThat(percentileData.getSoldPercentile(1L, comm(UICommodityType.MEM, "foo")).get(), closeTo(20.0, 0.0001));
        assertThat(percentileData.getSoldPercentile(1L, comm(UICommodityType.MEM, "foooo")).get(), closeTo(20.0, 0.0001));
        assertThat(percentileData.getSoldPercentile(2L, comm(UICommodityType.MEM, null)).get(), closeTo(30.0, 0.0001));

        assertThat(percentileData.getSoldPercentile(1L, comm(UICommodityType.MEM, "bar")), is(Optional.empty()));
        assertThat(percentileData.getSoldPercentile(2L, comm(UICommodityType.MEM, "foo")), is(Optional.empty()));
        assertThat(percentileData.getSoldPercentile(1L, comm(UICommodityType.VMEM, null)), is(Optional.empty()));
    }

    /**
     * Test the "toString" method.
     */
    @Test
    public void testToString() {
        TopologyPercentileData percentileData = new TopologyPercentileData();
        percentileData.putSoldPercentile(1L, comm(UICommodityType.MEM, null), 1);
        percentileData.putSoldPercentile(1L, comm(UICommodityType.MEM, "foo"), .2);
        percentileData.putSoldPercentile(1L, comm(UICommodityType.MEM, "foooo"), .2);
        percentileData.putSoldPercentile(2L, comm(UICommodityType.CPU, null), .3);

        String str = percentileData.toString();
        assertThat(str, containsString("2 entities"));
        assertThat(str, containsString("Mem"));
        assertThat(str, containsString("CPU"));
    }

    /**
     * Explicitly test the equality method.
     */
    @Test
    public void testCommTypeEquals() {
        final CommType commType1 = new CommType(1, null);
        final CommType commType11 = new CommType(1, null);
        final CommType commType2 = new CommType(1, "foo");
        final CommType commType22 = new CommType(1, "foo");
        final CommType commType3 = new CommType(2, null);
        final CommType commType33 = new CommType(2, null);
        final CommType commType4 = new CommType(3, "bar");
        final CommType commType44 = new CommType(3, "bar");

        assertThat(commType1, is(commType11));
        assertThat(commType2, is(commType22));
        assertThat(commType3, is(commType33));
        assertThat(commType4, is(commType44));

        assertThat(commType1, not(commType2));
        assertThat(commType1, not(commType3));
        assertThat(commType1, not(commType4));

        assertThat(commType2, not(commType1));
        assertThat(commType3, not(commType1));
        assertThat(commType4, not(commType1));
    }

    @Nonnull
    private CommodityType comm(@Nonnull final UICommodityType commType, @Nullable String key) {
        if (key != null) {
            return CommodityType.newBuilder()
                .setType(commType.typeNumber())
                .setKey(key)
                .build();
        } else {
            return CommodityType.newBuilder()
                .setType(commType.typeNumber())
                .build();
        }

    }
}