package com.vmturbo.topology.processor.group.settings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator.MoveApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator.SingleSettingApplicator;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;

public class EntitySettingsApplicatorTest {

    private static final Setting MOVE_DISABLED_SETTING = Setting.newBuilder()
            .setSettingSpecName("move")
            .setEnumSettingValue(EnumSettingValue.newBuilder()
                    .setValue("DISABLED"))
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
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setMovable(true));
        moveApplicator.apply(entity, MOVE_DISABLED_SETTING);
        assertThat(entity.getCommoditiesBoughtFromProviders(0).getMovable(), is(true));
    }

    @Test
    public void testApplySettings() {
        final Function<String, SingleSettingApplicator> applicatorProvider = mock(Function.class);
        final EntitySettingsApplicator applicator =
                new EntitySettingsApplicator(applicatorProvider);

        final GraphWithSettings graphWithSettings = mock(GraphWithSettings.class);
        final TopologyGraph topologyGraph = mock(TopologyGraph.class);
        final Vertex vertex = mock(Vertex.class);
        TopologyEntityDTO.Builder entity = TopologyEntityDTO.newBuilder()
                .setOid(7L)
                .setEntityType(10);
        when(vertex.getTopologyEntityDtoBuilder()).thenReturn(entity);
        when(topologyGraph.vertices()).thenReturn(Stream.of(vertex));
        when(graphWithSettings.getTopologyGraph()).thenReturn(topologyGraph);

        when(graphWithSettings.getSettingsForEntity(eq(7L)))
                .thenReturn(ImmutableMap.of("TEST", MOVE_DISABLED_SETTING));

        final SingleSettingApplicator singleApplicator = mock(SingleSettingApplicator.class);
        when(applicatorProvider.apply(eq("TEST"))).thenReturn(singleApplicator);

        applicator.applySettings(graphWithSettings);

        verify(singleApplicator).apply(entity, MOVE_DISABLED_SETTING);
    }
}
