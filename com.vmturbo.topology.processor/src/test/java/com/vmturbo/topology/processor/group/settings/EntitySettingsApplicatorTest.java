package com.vmturbo.topology.processor.group.settings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.group.api.SettingPolicySetting;
import com.vmturbo.platform.common.dto.CommonDTOREST.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator.MoveApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator.SingleSettingApplicator;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;

/**
 * Unit test for {@link EntitySettingsApplicator}.
 */
public class EntitySettingsApplicatorTest {

    private static final Setting MOVE_DISABLED_SETTING = Setting.newBuilder()
            .setSettingSpecName(SettingPolicySetting.Move.getSettingName())
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue("DISABLED"))
            .build();

    @Test
    public void testMoveApplicator() {
        final MoveApplicator moveApplicator = new MoveApplicator();
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(1L)
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE.getValue())
                        .setMovable(true));
        moveApplicator.apply(entity, MOVE_DISABLED_SETTING);
        assertThat(entity.getCommoditiesBoughtFromProviders(0).getMovable(), is(false));
    }

    @Test
    public void testMoveApplicatorNoProvider() {
        final MoveApplicator moveApplicator = new MoveApplicator();
        final TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .addCommoditiesBoughtFromProviders(
                        CommoditiesBoughtFromProvider.newBuilder().setMovable(true));
        moveApplicator.apply(entity, MOVE_DISABLED_SETTING);
        assertThat(entity.getCommoditiesBoughtFromProviders(0).getMovable(), is(true));
    }

    @Test
    public void testApplySettings() {
        final Function<SettingPolicySetting, SingleSettingApplicator> applicatorProvider =
                mock(Function.class);
        final EntitySettingsApplicator applicator =
                new EntitySettingsApplicator(applicatorProvider);

        final GraphWithSettings graphWithSettings = mock(GraphWithSettings.class);
        final TopologyGraph topologyGraph = mock(TopologyGraph.class);
        final Vertex vertex = mock(Vertex.class);
        TopologyEntityDTO.Builder entity =
                TopologyEntityDTO.newBuilder().setOid(7L).setEntityType(10);
        when(vertex.getTopologyEntityDtoBuilder()).thenReturn(entity);
        when(topologyGraph.vertices()).thenReturn(Stream.of(vertex));
        when(graphWithSettings.getTopologyGraph()).thenReturn(topologyGraph);

        when(graphWithSettings.getSettingsForEntity(eq(7L))).thenReturn(
                Collections.singleton(MOVE_DISABLED_SETTING));

        final SingleSettingApplicator singleApplicator = mock(SingleSettingApplicator.class);
        when(applicatorProvider.apply(eq(SettingPolicySetting.Move))).thenReturn(singleApplicator);

        applicator.applySettings(graphWithSettings);

        verify(singleApplicator).apply(entity, MOVE_DISABLED_SETTING);
    }

    /**
     * Tests application of utilization threshold entity settings.
     */
    @Test
    public void testUtilizationThresholdSettings() {
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.CPU,
                SettingPolicySetting.CpuUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.MEM,
                SettingPolicySetting.MemoryUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.IO_THROUGHPUT,
                SettingPolicySetting.IoThroughput);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.NET_THROUGHPUT,
                SettingPolicySetting.NetThroughput);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.SWAPPING,
                SettingPolicySetting.SwappingUtilization);
        testUtilizationSettings(EntityType.PHYSICAL_MACHINE, CommodityType.QN_VCPU,
                SettingPolicySetting.ReadyQueueUtilization);

        testUtilizationSettings(EntityType.SWITCH, CommodityType.NET_THROUGHPUT,
                SettingPolicySetting.NetThroughput);

        testUtilizationSettings(EntityType.STORAGE, CommodityType.STORAGE_AMOUNT,
                SettingPolicySetting.StorageAmmountUtilization);
        testUtilizationSettings(EntityType.STORAGE, CommodityType.STORAGE_ACCESS,
                SettingPolicySetting.IopsUtilization);
        testUtilizationSettings(EntityType.STORAGE, CommodityType.STORAGE_LATENCY,
                SettingPolicySetting.LatencyUtilization);

        testUtilizationSettings(EntityType.STORAGE_CONTROLLER, CommodityType.STORAGE_AMOUNT,
                SettingPolicySetting.StorageAmmountUtilization);
        testUtilizationSettings(EntityType.STORAGE_CONTROLLER, CommodityType.CPU,
                SettingPolicySetting.CpuUtilization);
    }

    private TopologyEntityDTO.Builder createEntityWithCommodity(@Nonnull EntityType entityType,
            @Nonnull CommodityType commodityType, float initialEffectiveCapacity) {
        final TopologyEntityDTO.Builder builder =
                TopologyEntityDTO.newBuilder().setEntityType(entityType.getValue()).setOid(1);

        builder.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(TopologyDTO.CommodityType.newBuilder()
                        .setType(commodityType.getValue())
                        .build())
                .setCapacity(1000)
                .setEffectiveCapacityPercentage(initialEffectiveCapacity));
        return builder;
    }

    private void testUtilizationSettings(EntityType entityType, CommodityType commodityType,
            SettingPolicySetting setting) {
        final EntitySettingsApplicator applicator = new EntitySettingsApplicator();

        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(entityType, commodityType, 100f);
        final TopologyGraph graph = new TopologyGraph(Collections.singletonMap(1L, builder));
        final EntitySettings settings = EntitySettings.newBuilder()
                .setEntityOid(1L)
                .setDefaultSettingPolicyId(2L)
                .addUserSettings(Setting.newBuilder()
                        .setSettingSpecName(setting.getSettingName())
                        .setNumericSettingValue(
                                NumericSettingValue.newBuilder().setValue(57f).build())
                        .build())
                .build();
        final SettingPolicy policy = SettingPolicy.getDefaultInstance();
        final GraphWithSettings withSettings =
                new GraphWithSettings(graph, Collections.singletonMap(1L, settings),
                        Collections.singletonMap(2L, policy));
        applicator.applySettings(withSettings);
        Assert.assertEquals(57, builder.getCommoditySoldList(0).getEffectiveCapacityPercentage(),
                0.00001f);
    }

    /**
     * Tests not settings applied. Effective capacity is expected not to be changed.
     */
    @Test
    public void testNoSettings() {
        final EntitySettingsApplicator applicator = new EntitySettingsApplicator();
        final TopologyEntityDTO.Builder builder =
                createEntityWithCommodity(EntityType.PHYSICAL_MACHINE, CommodityType.CPU, 123f);
        final TopologyGraph graph = new TopologyGraph(Collections.singletonMap(1L, builder));
        final SettingPolicy policy = SettingPolicy.getDefaultInstance();
        final GraphWithSettings withSettings = new GraphWithSettings(graph, Collections.emptyMap(),
                Collections.singletonMap(2L, policy));
        applicator.applySettings(withSettings);
        Assert.assertEquals(123, builder.getCommoditySoldList(0).getEffectiveCapacityPercentage(),
                0.00001f);
    }
}
