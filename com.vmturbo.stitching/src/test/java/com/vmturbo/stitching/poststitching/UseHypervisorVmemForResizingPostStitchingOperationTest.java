package com.vmturbo.stitching.poststitching;

import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeBooleanSetting;
import static com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.makeCommoditySold;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.poststitching.PostStitchingTestUtilities.UnitTestResultBuilder;

public class UseHypervisorVmemForResizingPostStitchingOperationTest {

    private final IStitchingJournal<TopologyEntity> journal =
            (IStitchingJournal<TopologyEntity>)mock(IStitchingJournal.class);

    private final CommoditySoldDTO vmem = makeCommoditySold(CommodityType.VMEM);
    private final List<CommoditySoldDTO> vmCommoditySoldList = ImmutableList.of(vmem);

    private final EntitySettingsCollection settingsCollection = mock(EntitySettingsCollection.class);

    private UnitTestResultBuilder resultBuilder;

    private TopologyEntity vmEntity = PostStitchingTestUtilities.makeTopologyEntityBuilder(12345678L,
            EntityType.VIRTUAL_MACHINE_VALUE, vmCommoditySoldList, Collections.emptyList()).build();

    @Before
    public void setup() {
        resultBuilder = new UnitTestResultBuilder();

        final Setting useHypervisorMetricsForResizingSetting = makeBooleanSetting(false);
        when(settingsCollection.getEntitySetting(any(TopologyEntity.class), eq(EntitySettingSpecs.UseHypervisorMetricsForResizing)))
                .thenReturn(Optional.of(useHypervisorMetricsForResizingSetting));
    }

    @Test
    public void testDisableVmemResizing() {
        UseHypervisorVmemForResizingPostStitchingOperation operation =
                new UseHypervisorVmemForResizingPostStitchingOperation();

        operation.performOperation(Stream.of(vmEntity), settingsCollection, resultBuilder);

        assertEquals(1, resultBuilder.getChanges().size());

        // apply the changes
        resultBuilder.getChanges().forEach(change -> change.applyChange(journal));
        assertFalse(vmEntity.getTopologyEntityDtoBuilder()
                .getCommoditySoldListBuilderList()
                .stream().filter(commodity -> CommonDTO.CommodityDTO.CommodityType.VMEM_VALUE == commodity.getCommodityType().getType())
                .findFirst().get().getIsResizeable());
    }

}
