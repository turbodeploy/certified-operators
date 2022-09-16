package com.vmturbo.market.topology.conversions;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.market.runner.FakeEntityCreator;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;

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

    /**
     * Test the removal of SLs with fake suppliers
     */
    @Test
    public void testRemoveSLsWithFakeSuppliers() {
        ShoppingListTO clusterSl = ShoppingListTO.newBuilder().setSupplier(1L).setOid(1000L).build();
        ShoppingListTO computeSl = ShoppingListTO.newBuilder().setSupplier(2L).setOid(1001L).build();
        ShoppingListTO storageSl = ShoppingListTO.newBuilder().setSupplier(3L).setOid(1002L).build();
        TraderTO vm1 = TraderTO.newBuilder().setOid(100L)
                .addShoppingLists(clusterSl)
                .addShoppingLists(computeSl)
                .addShoppingLists(storageSl).build();
        TraderTO vm2 = TraderTO.newBuilder().setOid(101L)
                .addShoppingLists(computeSl)
                .addShoppingLists(storageSl).build();

        FakeEntityCreator fakeEntityCreator = mock(FakeEntityCreator.class);
        when(fakeEntityCreator.isFakeComputeClusterOid(1L)).thenReturn(true);
        when(fakeEntityCreator.isFakeComputeClusterOid(2L)).thenReturn(false);
        when(fakeEntityCreator.isFakeComputeClusterOid(3L)).thenReturn(false);
        List<TraderTO> resultTraders = TopologyConversionUtils.removeSLsWithFakeSuppliers(
                Arrays.asList(vm1, vm2), fakeEntityCreator);
        Assert.assertEquals(2, resultTraders.size());
        TraderTO vm1Result = resultTraders.get(0);
        TraderTO vm2Result = resultTraders.get(1);
        Assert.assertEquals(2, vm1Result.getShoppingListsCount());
        Assert.assertEquals(2, vm2Result.getShoppingListsCount());
        List<Long> suppliers = vm1Result.getShoppingListsList().stream()
                .map(ShoppingListTO::getSupplier).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(2L, 3L), suppliers);
        suppliers = vm2Result.getShoppingListsList().stream()
                .map(ShoppingListTO::getSupplier).collect(Collectors.toList());
        Assert.assertEquals(Arrays.asList(2L, 3L), suppliers);
    }
}
