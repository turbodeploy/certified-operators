package com.vmturbo.extractor.action.percentile;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import it.unimi.dsi.fastutil.ints.IntSets;
import it.unimi.dsi.fastutil.longs.LongSets;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.action.percentile.PercentileSettingsRetriever.PercentileSettings;
import com.vmturbo.extractor.action.percentile.PercentileSettingsRetriever.PercentileSettings.PercentileSetting;
import com.vmturbo.extractor.schema.json.common.CommodityPercentileChange;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.WriterConfig;

/**
 * Unit tests for {@link ActionPercentileDataRetriever}.
 */
public class ActionPercentileDataRetrieverTest {

    private ProjectedTopologyPercentileDataRetriever projectedTopologyDataRetriever = mock(ProjectedTopologyPercentileDataRetriever.class);
    private PercentileSettingsRetriever percentileSettingsRetriever = mock(PercentileSettingsRetriever.class);

    private ActionPercentileDataRetriever dataRetriever =
        new ActionPercentileDataRetriever(projectedTopologyDataRetriever, percentileSettingsRetriever);

    /**
     * Test the workflow for collecting percentile data, combining data from source topology,
     * projected topology, and settings.
     */
    @Test
    public void testCollectPercentileData() {

        final long actionSpecId = 1000;
        final double srcPercentile = .1;
        final double projPercentile = 20;
        final int aggresiveness = 7;
        final int observationPeriod = 30;
        final CommodityType commType = CommodityType.newBuilder()
                .setType(UICommodityType.VMEM.typeNumber())
                .build();
        Consumer<TopologyEntityDTO> srcEntityConsumer = dataRetriever.startTopology(TopologyInfo.getDefaultInstance(), mock(WriterConfig.class), new MultiStageTimer(null));
        TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setOid(1L)
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setDisplayName("vm 1")
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(commType)
                        .setHistoricalUsed(HistoricalValues.newBuilder()
                            .setPercentile(srcPercentile)))
                .build();
        srcEntityConsumer.accept(vm);
        dataRetriever.finish(mock(DataProvider.class));

        final TopologyPercentileData projPercentileData = mock(TopologyPercentileData.class);
        when(projPercentileData.getSoldPercentile(vm.getOid(), commType)).thenReturn(Optional.of(projPercentile));
        when(projectedTopologyDataRetriever.fetchPercentileData(any(), any())).thenReturn(projPercentileData);

        final PercentileSettings percentileSettings = mock(PercentileSettings.class);
        final PercentileSetting entitySetting = mock(PercentileSetting.class);
        when(entitySetting.getAggresiveness()).thenReturn(aggresiveness);
        when(entitySetting.getObservationPeriod()).thenReturn(observationPeriod);

        when(percentileSettings.getEntitySettings(vm.getOid(), ApiEntityType.VIRTUAL_MACHINE)).thenReturn(Optional.of(entitySetting));
        when(percentileSettingsRetriever.getPercentileSettingsData(any())).thenReturn(percentileSettings);

        final List<ActionSpec> actionSpecsById = Collections.singletonList(ActionSpec.newBuilder()
            .setRecommendation(Action.newBuilder()
                .setExplanation(Explanation.getDefaultInstance())
                .setId(actionSpecId)
                .setDeprecatedImportance(1L)
                .setInfo(ActionInfo.newBuilder()
                    .setResize(Resize.newBuilder()
                        .setCommodityType(commType)
                        .setTarget(ActionEntity.newBuilder()
                            .setId(vm.getOid())
                            .setType(vm.getEntityType())))))
            .build());

        ActionPercentileData percentileData = dataRetriever.getActionPercentiles(actionSpecsById);
        CommodityPercentileChange change = percentileData.getChange(vm.getOid(), commType);
        assertThat(change.getBefore(), is(srcPercentile * 100));
        assertThat(change.getAfter(), is(projPercentile));
        assertThat(change.getAggressiveness(), is(aggresiveness));
        assertThat(change.getObservationPeriodDays(), is(observationPeriod));

        verify(percentileSettingsRetriever).getPercentileSettingsData(LongSets.singleton(vm.getOid()));
        verify(projectedTopologyDataRetriever).fetchPercentileData(LongSets.singleton(vm.getOid()),
                IntSets.singleton(commType.getType()));
    }

}