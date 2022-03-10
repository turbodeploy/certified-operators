package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeNumericSetting;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.OverprovisionCapacityPostStitchingOperation.VmmPmMemoryAllocationPostStitchingOperation;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

public class VmmPmMemoryAllocationPostStitchingOpTest {

    private EntityChangesBuilder<TopologyEntity> resultBuilder;

    private final float overprovisionPercentage = 150;
    private final float initialCapacity = 10;
    private final float modifiedCapacity = initialCapacity * (overprovisionPercentage / 100);

    private final double  smallDelta = 0.001;

    private final EntitySettingSpecs overprovisionSettingType = EntitySettingSpecs.MemoryOverprovisionedPercentage;
    private final EntitySettingsCollection settingsMock = mock(EntitySettingsCollection.class);

    @SuppressWarnings("unchecked")
    private final IStitchingJournal<TopologyEntity> stitchingJournal =
        (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    private final VmmPmMemoryAllocationPostStitchingOperation operation =
            new VmmPmMemoryAllocationPostStitchingOperation();

    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();

        final Setting overprovisionSetting = makeNumericSetting(overprovisionPercentage);
        when(settingsMock.getEntitySetting(any(TopologyEntity.class), eq(overprovisionSettingType)))
                .thenReturn(Optional.of(overprovisionSetting));
    }


    @Test
    public void testAllocationCommodity() {

        // create the commodity and the entity
        final CommoditySoldView memAllocationSold = makeCommoditySold(CommodityType.MEM_ALLOCATION, initialCapacity);
        final List<CommoditySoldView> commoditiesSold = Collections.singletonList(memAllocationSold);
        final TopologyEntity testTE = PostStitchingTestUtilities.makeTopologyEntity(commoditiesSold);

        // run operation
        final TopologicalChangelog result =
                operation.performOperation(Stream.of(testTE), settingsMock, resultBuilder);

        // apply changes
        resultBuilder.getChanges().forEach(change -> change.applyChange(stitchingJournal));


        // get the modified commodity
        final Optional<CommoditySoldView> modifiedCommSoldOpt = testTE.getTopologyEntityImpl()
                .getCommoditySoldListList().stream().findFirst();
        assertNotNull(modifiedCommSoldOpt);
        final CommoditySoldView modifiedCommSold = modifiedCommSoldOpt.get();

        // check that the capacity has been overwritten with the expected one
        assertEquals(CommodityType.MEM_ALLOCATION_VALUE, modifiedCommSold.getCommodityType().getType());
        assertEquals(modifiedCapacity, modifiedCommSold.getCapacity(), smallDelta);
    }

}
