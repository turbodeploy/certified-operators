package com.vmturbo.extractor.action.commodity;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSets;
import it.unimi.dsi.fastutil.shorts.Short2ObjectMaps;

import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.action.commodity.PercentileSettingsRetriever.PercentileSettings;
import com.vmturbo.extractor.action.commodity.PercentileSettingsRetriever.PercentileSettings.PercentileSetting;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.models.Constants;
import com.vmturbo.extractor.schema.json.common.ActionImpactedEntity.ActionCommodity;
import com.vmturbo.extractor.schema.json.common.ActionImpactedEntity.ImpactedMetric;
import com.vmturbo.extractor.schema.json.common.CommodityPercentileChange;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Unit tests for {@link ActionCommodityDataRetriever}.
 */
public class ActionCommodityDataRetrieverTest {

    private final ProjectedTopologyCommodityDataRetriever projectedTopologyDataRetriever = mock(
            ProjectedTopologyCommodityDataRetriever.class);
    private final PercentileSettingsRetriever percentileSettingsRetriever = mock(PercentileSettingsRetriever.class);

    private final Set<Integer> commWhitelist = Constants.REPORTING_DEFAULT_COMMODITY_TYPES_WHITELIST.stream()
            .map(CommodityDTO.CommodityType::getNumber)
            .collect(Collectors.toSet());

    private final ActionCommodityDataRetriever dataRetriever =
        new ActionCommodityDataRetriever(projectedTopologyDataRetriever, percentileSettingsRetriever, commWhitelist);

    /**
     * Test the workflow for collecting commodity and percentile data, combining data from source topology,
     * projected topology, and settings.
     */
    @Test
    public void testCollectCommodityAndPercentileData() {

        final long actionSpecId = 1000;
        final double srcPercentile = .1;
        final double projPercentile = 20;
        final int aggresiveness = 7;
        final int observationPeriod = 30;
        final float used = 1024.0f;
        final float currentCapacity = 2048.0f;
        final float projectedCapacity = currentCapacity * 2;
        final CommodityType commType = CommodityType.newBuilder()
                .setType(UICommodityType.VMEM.typeNumber())
                .build();
        final Consumer<TopologyEntityDTO> srcEntityConsumer = dataRetriever.startTopology(TopologyInfo.getDefaultInstance(), mock(WriterConfig.class), new MultiStageTimer(null));
        final TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setOid(1L)
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setDisplayName("vm 1")
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(commType)
                        .setUsed(used)
                        .setCapacity(currentCapacity)
                        .setHistoricalUsed(HistoricalValues.newBuilder()
                            .setPercentile(srcPercentile)))
                .build();
        srcEntityConsumer.accept(vm);
        dataRetriever.finish(mock(DataProvider.class));

        ActionCommodity actionCommodity = new ActionCommodity();
        actionCommodity.addUsed((float)used);
        actionCommodity.addCapacity((float)projectedCapacity);

        final TopologyActionCommodityData projPercentileData = mock(TopologyActionCommodityData.class);
        when(projPercentileData.getSoldPercentile(vm.getOid(), commType)).thenReturn(Optional.of(projPercentile));
        when(projPercentileData.getSoldCommms(vm.getOid())).thenReturn(Short2ObjectMaps.singleton((short)UICommodityType.VMEM.typeNumber(), actionCommodity));
        when(projectedTopologyDataRetriever.fetchProjectedCommodityData(any(), any())).thenReturn(projPercentileData);

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

        final ActionCommodityData percentileData = dataRetriever.getActionCommodityData(actionSpecsById);
        final CommodityPercentileChange percentileChange = percentileData.getPercentileChange(vm.getOid(), commType);
        assertThat(percentileChange.getBefore(), is(srcPercentile * 100));
        assertThat(percentileChange.getAfter(), is(projPercentile));
        assertThat(percentileChange.getAggressiveness(), is(aggresiveness));
        assertThat(percentileChange.getObservationPeriodDays(), is(observationPeriod));

        final Map<String, ImpactedMetric> impactedMetrics = percentileData.getEntityImpact(vm.getOid());
        ImpactedMetric impactedMetric = impactedMetrics.get(ExportUtils.getCommodityTypeJsonKey(UICommodityType.VMEM.typeNumber()));

        final ActionCommodity beforeActionCommodity = impactedMetric.getBeforeActions();
        final ActionCommodity afterActionCommodity = impactedMetric.getAfterActions();
        assertThat(beforeActionCommodity.getUsed(), is(used));
        assertThat(beforeActionCommodity.getCapacity(), is(currentCapacity));
        assertThat(beforeActionCommodity.getUtilization(), is(used / currentCapacity * 100.0f));
        assertThat(afterActionCommodity.getUsed(), is(used));
        assertThat(afterActionCommodity.getCapacity(), is(projectedCapacity));
        assertThat(afterActionCommodity.getUtilization(), is(used / projectedCapacity * 100));

        verify(percentileSettingsRetriever).getPercentileSettingsData(LongSets.singleton(vm.getOid()));
        verify(projectedTopologyDataRetriever).fetchProjectedCommodityData(LongSets.singleton(vm.getOid()),
                new IntOpenHashSet(Collections.singleton(UICommodityType.VMEM.typeNumber())));
    }

    /**
     * Test visiting the atomic resize action entities/commodities.
     */
    @Test
    public void testVisitAtomicResizeCommodities() {
        CommodityType commodityType = CommodityType.newBuilder()
                .setType(1)
                .build();
        ActionEntity target1 = ActionEntity.newBuilder()
                .setId(1)
                .setType(10)
                .build();
        ActionEntity target2 = ActionEntity.newBuilder()
                .setId(2)
                .setType(10)
                .build();

        Map<ActionEntity, CommodityType> percentileMap = new HashMap<>();
        Set<ActionEntity> commoditySet = new HashSet<>();

        dataRetriever.visitCommodityData(makeActionSpec(1, ActionInfo.newBuilder()
            .setAtomicResize(AtomicResize.newBuilder()
                    .setExecutionTarget(target1)
                    .addResizes(ResizeInfo.newBuilder()
                            .setTarget(target1)
                            .setCommodityType(commodityType))
                    .addResizes(ResizeInfo.newBuilder()
                            .setTarget(target2)
                            .setCommodityType(commodityType)))
                .build()), percentileMap::put, commoditySet::add);

        assertThat(percentileMap.get(target1), is(commodityType));
        assertThat(percentileMap.get(target2), is(commodityType));
        assertThat(commoditySet, containsInAnyOrder(target1, target2));
    }

    /**
     * Test visiting the move action entities/commodities.
     */
    @Test
    public void testVisitMove() {
        Map<ActionEntity, CommodityType> percentileMap = new HashMap<>();
        Set<Long> commodityEntities = new HashSet<>();
        dataRetriever.visitCommodityData(makeActionSpec(1, ActionInfo.newBuilder()
            .setMove(Move.newBuilder()
                .addChanges(ChangeProvider.newBuilder()
                    .setSource(actionEntity(1))
                    .setDestination(actionEntity(2)))
            .addChanges(ChangeProvider.newBuilder()
                    .setSource(actionEntity(3))
                    .setDestination(actionEntity(4)))
            .setTarget(actionEntity(5)))
            .build()), percentileMap::put, e -> commodityEntities.add(e.getId()));

        assertTrue(percentileMap.isEmpty());
        // Target entity not matched
        assertThat(commodityEntities, containsInAnyOrder(1L, 2L, 3L, 4L));
    }

    /**
     * Test visiting the scale action entities/commodities.
     */
    @Test
    public void testVisitScale() {
        Map<ActionEntity, CommodityType> percentileMap = new HashMap<>();
        Set<Long> commodityEntities = new HashSet<>();
        dataRetriever.visitCommodityData(makeActionSpec(1, ActionInfo.newBuilder()
                .setScale(Scale.newBuilder()
                    .setTarget(actionEntity(1))
                    .addChanges(ChangeProvider.newBuilder()
                            .setSource(actionEntity(2))
                            .setDestination(actionEntity(2))))
                    .build()), percentileMap::put, e -> commodityEntities.add(e.getId()));

        assertTrue(percentileMap.isEmpty());
        assertThat(commodityEntities, containsInAnyOrder(1L));
    }

    private ActionEntity actionEntity(final long id) {
        return ActionEntity.newBuilder()
                .setType(10)
                .setId(id)
                .build();
    }

    private ActionSpec makeActionSpec(final long id, ActionInfo actionInfo) {
        return ActionSpec.newBuilder()
            .setRecommendation(Action.newBuilder()
                .setId(id)
                .setExplanation(Explanation.getDefaultInstance())
                .setDeprecatedImportance(1)
                .setInfo(actionInfo))
            .build();
    }

}