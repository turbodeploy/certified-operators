package com.vmturbo.stitching.poststitching;

import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Unit test for {@link ComputedUsedValuePostStitchingOperation}.
 */
public class ComputedUsedValuePostStitchingTest {

    private static final CommodityType COMM_TYPE = CommodityType.forNumber(10);
    private static final CommodityType COMM_TYPE_EX = CommodityType.forNumber(11);
    private static final String COMM_KEY = "Key-1";
    private static final String COMM_KEY_EX = "Key-2";

    private static final EntityType SELLER_TYPE = EntityType.forNumber(20);

    private final ComputedUsedValuePostStitchingOperation stitchOperation =
                    new ComputedUsedValuePostStitchingOperation(SELLER_TYPE, COMM_TYPE);

    private static final List<Double> usedValues = ImmutableList.of(10.0, 5.5, 60.1, 30.6);

    private final IStitchingJournal journal = mock(IStitchingJournal.class);

    /**
     * Test that the sold used value is the sum of bought used values.
     */
    @Test
    public void testUsedValue() {
        TopologyEntity provider = seller();

        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        stitchOperation.performOperation(
            Stream.of(provider), mock(EntitySettingsCollection.class), resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));

        Assert.assertEquals(usedValues.stream().mapToDouble(Double::doubleValue).sum(),
            provider.getTopologyEntityImpl().getCommoditySoldList(0).getUsed(), 1e-5);
    }

    /**
     * Create a topology graph with one seller and a few buyers.
     * @return a seller that sells to a few buyers
     */
    private TopologyEntity seller() {
        final long sellerOid = 111L;
        final int buyerType = 21;

        final TopologyEntity.Builder seller =
                PostStitchingTestUtilities.makeTopologyEntityBuilder(
                    sellerOid,
                    SELLER_TYPE.getNumber(),
                    Lists.newArrayList(
                        PostStitchingTestUtilities.makeCommoditySold(COMM_TYPE, COMM_KEY)),
                        Collections.emptyList());

        final CommodityBoughtView commBoughtInclude =
                        PostStitchingTestUtilities.makeCommodityBought(COMM_TYPE, COMM_KEY);
        final CommodityBoughtView commBoughtExcludeByKey =
                        PostStitchingTestUtilities.makeCommodityBought(COMM_TYPE, COMM_KEY_EX);
        final CommodityBoughtView commBoughtExcludeByType =
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
                        commBoughtInclude.copy().setUsed(usedValue),
                        commBoughtExcludeByKey.copy().setUsed(1000),
                        commBoughtExcludeByType.copy().setUsed(1000))));
            seller.addConsumer(buyer);
            buyer.build();
        });

        return seller.build();
    }
}
