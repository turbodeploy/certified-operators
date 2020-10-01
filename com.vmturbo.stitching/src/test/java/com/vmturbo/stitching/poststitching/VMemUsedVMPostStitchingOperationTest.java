package com.vmturbo.stitching.poststitching;

import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Unit tests for {@link VMemUsedVMPostStitchingOperation}.
 */
public class VMemUsedVMPostStitchingOperationTest {
    private static final CommodityType COMM_TYPE = CommodityType.VMEM;
    private static final CommodityType COMM_TYPE_EX = CommodityType.forNumber(11);
    private static final String COMM_KEY = "Key-1";
    private static final String COMM_KEY_EX = "Key-2";

    private static final EntityType SELLER_TYPE = EntityType.VIRTUAL_MACHINE;

    private final VMemUsedVMPostStitchingOperation stitchOperation
            = new VMemUsedVMPostStitchingOperation();

    private static final List<Double> usedValues = ImmutableList.of(10.0, 5.5, 60.1, 30.6);

    private final IStitchingJournal journal = mock(IStitchingJournal.class);

    /**
     * Test that the sold used value is the sum of bought used values.
     */
    @Test
    public void testUsedValueFromConsumers() {
        TopologyEntity provider = seller(0d, usedValues);
        double expectedResult = usedValues.stream().mapToDouble(Double::doubleValue).sum();

        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        stitchOperation.performOperation(
                Stream.of(provider), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        Assert.assertEquals(expectedResult,
                provider.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getUsed(), 1e-5);
    }

    /**
     * Test that if the used VMEM sold commodity of entity is bigger than the sum of used.
     * commodities by consumers then use it from entity itself.
     */
    @Test
    public void testUsedValueFromEntity() {
        TopologyEntity provider = seller(200d, usedValues);
        double expectedResult = 200.0;

        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        stitchOperation.performOperation(
                Stream.of(provider), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        Assert.assertEquals(expectedResult,
                provider.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getUsed(), 1e-5);
    }

    /**
     * Test that if neither original sold used is set nor any of the consumers have used values
     * then 'used' value won't be set.
     */
    @Test
    public void testHasNoUsedValue() {
        TopologyEntity provider = seller(null, Collections.emptyList());
        double expectedResult = 0;

        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        stitchOperation.performOperation(
                Stream.of(provider), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        Assert.assertEquals(expectedResult,
                provider.getTopologyEntityDtoBuilder().getCommoditySoldList(0).getUsed(), 1e-5);
    }

    /**
     * Create a topology graph with one seller and a few buyers.
     *
     * @param used 'used' value or null if commodity doesn't have it.
     * @param usedValues list of values used by consumers.
     * @return a seller that sells to a few buyers.
     */
    private TopologyEntity seller(Double used, List<Double> usedValues) {
        final long sellerOid = 1L;
        final int buyerType = 21;

        final TopologyEntity.Builder seller =
                PostStitchingTestUtilities.makeTopologyEntityBuilder(sellerOid,
                        SELLER_TYPE.getNumber(), Lists.newArrayList(used != null
                                ? PostStitchingTestUtilities.makeCommoditySold(COMM_TYPE, COMM_KEY,
                                        used)
                                : PostStitchingTestUtilities.makeCommoditySold(COMM_TYPE,
                                COMM_KEY)),
                        Collections.emptyList());

        final CommodityBoughtDTO commBoughtInclude =
                PostStitchingTestUtilities.makeCommodityBought(COMM_TYPE, COMM_KEY);
        final CommodityBoughtDTO commBoughtExcludeByKey =
                PostStitchingTestUtilities.makeCommodityBought(COMM_TYPE, COMM_KEY_EX);
        final CommodityBoughtDTO commBoughtExcludeByType =
                PostStitchingTestUtilities.makeCommodityBought(COMM_TYPE_EX);

        usedValues.forEach(usedValue -> {
            // Each buyer buys 3 commodities from the seller: one has the right
            // type and key, one has the right type but wrong key and one with
            // the wrong type. Only the first should be included in the sum.
            final TopologyEntity.Builder buyer =
                    PostStitchingTestUtilities.makeTopologyEntityBuilder(0,
                            buyerType,
                            Collections.singletonList(
                                    PostStitchingTestUtilities.makeCommoditySold(COMM_TYPE)),
                            ImmutableMap.of(sellerOid, Lists.newArrayList(
                                    commBoughtInclude.toBuilder().setUsed(usedValue).build(),
                                    commBoughtExcludeByKey.toBuilder().setUsed(1000).build(),
                                    commBoughtExcludeByType.toBuilder().setUsed(1000).build())));
            seller.addConsumer(buyer);
            buyer.build();
        });

        return seller.build();
    }
}
