package com.vmturbo.stitching.poststitching;

import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

/**
 * Unit tests for {@link SetDefaultCommodityCapacityPostStitchingOperation}.
 */
public class SetDefaultCommodityCapacityPostStitchingTest {

    /**
     * The entity type of the consumer.
     */
    private static final EntityType SELLER_TYPE = EntityType.APPLICATION_COMPONENT_SPEC;

    private static SetDefaultCommodityCapacityPostStitchingOperation stitchOperation;

    private static final float DEFAULT_CAPACITY =
            (Float)EntitySettingSpecs.ResponseTimeSLO.getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT_SPEC);
    private final IStitchingJournal journal = mock(IStitchingJournal.class);

    private final EntitySettingsCollection entitySettingsCollection =
            mock(EntitySettingsCollection.class);

    /**
     * Common code before every test.
     */
    @Before
    public void init() {
        stitchOperation = new SetDefaultCommodityCapacityPostStitchingOperation(EntityType.APPLICATION_COMPONENT_SPEC,
                ProbeCategory.CLOUD_MANAGEMENT,
                CommodityType.RESPONSE_TIME,
                DEFAULT_CAPACITY);
    }

    /**
     * Test to check if SetDefaultCommodityCapacityPostStitchingOperation  is able to set
     * default capacity for a given entityType, commodityType combination.
     */
    @Test
    public void testToCheckDefaultCapacity() {
        TopologyEntity provider = seller(1L, Optional.empty(),
                Optional.empty());
        UnitTestResultBuilder resultBuilder = new UnitTestResultBuilder();
        stitchOperation.performOperation(
                Stream.of(provider), entitySettingsCollection, resultBuilder);
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        // check that transaction commodity capacity is set to the default
        final float defaultSLO = (Float)Objects.requireNonNull(
                EntitySettingSpecs.ResponseTimeSLO
                        .getDataStructure().getDefault(EntityType.APPLICATION_COMPONENT_SPEC));
        Assert.assertEquals(defaultSLO,
                provider.getTopologyEntityImpl().getCommoditySoldList(0).getCapacity(),
                1e-5);
    }

    private TopologyEntity seller(long sellerOid, Optional<Double> usedValue,
                                  Optional<Double> capacityValue) {
        final TopologyEntity.Builder seller =
                PostStitchingTestUtilities.makeTopologyEntityBuilder(
                        sellerOid,
                        SELLER_TYPE.getNumber(),
                        Lists.newArrayList(
                                PostStitchingTestUtilities
                                        .makeCommoditySold(CommodityType.RESPONSE_TIME)),
                        Collections.emptyList());
        seller.getTopologyEntityImpl().getCommoditySoldListImplList().forEach(builder -> {
            usedValue.ifPresent(used -> builder.setUsed(used));
            capacityValue.ifPresent(capacity -> builder.setCapacity(capacity));

        });
        return seller.build();
    }
}
