package com.vmturbo.api.component.external.api.mapper.aspect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.communication.RepositoryApi.SingleEntityRequest;
import com.vmturbo.api.component.external.api.util.StatsUtils;
import com.vmturbo.api.component.external.api.util.StatsUtils.PrecisionEnum;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.PortsAspectApiDTO;
import com.vmturbo.api.dto.statistic.PortChannelApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTOREST.CommodityDTO;

public class PortsAspectMapperTest extends BaseAspectMapperTest {

    private static final int PORT_CHANNEL_VALUE = CommodityDTO.CommodityType.PORT_CHANEL.getValue();
    private static final int NET_THROUGHPUT_VALUE = CommodityDTO.CommodityType.NET_THROUGHPUT.getValue();
    private static final double USED = 123.456;
    private static final double PEAK = 233.3;
    private static final double CAPACITY = 666.666;
    private static final String CHANNEL_1 = "c1";
    private static final String CHANNEL_2 = "c2";
    private static final String CHANNEL_3 = "c3";
    private static final String CHANNEL_KEY = "ck";
    private static final String CHANNEL_NON_EXIST_KEY = "cnek";
    private static final String NON_AGGEGATE_KEY_1 = "nak1";
    private static final String NON_AGGEGATE_KEY_2 = "nak2";
    private static final String AGGEGATE_KEY_1 = "ak1";
    private static final String AGGEGATE_KEY_2 = "ak2";
    private static final String AGGEGATE_KEY_3 = "ak3";

    private static final double DELTA = 1e-2;
    private static final int MULTIPLIER = StatsUtils.getConvertedUnits(PORT_CHANNEL_VALUE,
        CommodityTypeUnits.NET_THROUGHPUT).second;

    private final List<CommodityBoughtDTO> commodityBoughts = Lists.newArrayList();
    private final List<CommoditySoldDTO> commoditySolds = Lists.newArrayList();

    private final RepositoryApi repositoryApi = mock(RepositoryApi.class);

    @Before
    public void setup() {
        // init
        commoditySolds.addAll(ImmutableList.of(
            CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(PORT_CHANNEL_VALUE)
                    .setKey(CHANNEL_KEY))
                .setDisplayName(CHANNEL_1)
                .setUsed(USED)
                .setPeak(PEAK)
                .setCapacity(CAPACITY)
                .addAllAggregates(ImmutableList.of(AGGEGATE_KEY_1))
                .build(),
            CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(NET_THROUGHPUT_VALUE)
                    .setKey(AGGEGATE_KEY_1))
                .setDisplayName(AGGEGATE_KEY_1)
                .setUsed(USED)
                .setPeak(PEAK)
                .setCapacity(CAPACITY)
                .build(),
            CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(NET_THROUGHPUT_VALUE)
                    .setKey(NON_AGGEGATE_KEY_2))
                .setDisplayName(NON_AGGEGATE_KEY_2)
                .setUsed(USED)
                .setPeak(PEAK)
                .setCapacity(CAPACITY)
                .build()
        ));
        commodityBoughts.addAll(ImmutableList.of(
            CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(PORT_CHANNEL_VALUE)
                    .setKey(CHANNEL_KEY))
                .setDisplayName(CHANNEL_2)
                .setUsed(USED)
                .setPeak(PEAK)
                .addAllAggregates(ImmutableList.of(AGGEGATE_KEY_2, AGGEGATE_KEY_3))
                .build(),
            CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(PORT_CHANNEL_VALUE)
                    .setKey(CHANNEL_NON_EXIST_KEY))
                .setDisplayName(CHANNEL_3)
                .setUsed(USED)
                .setPeak(PEAK)
                .build(),
            CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(NET_THROUGHPUT_VALUE)
                    .setKey(AGGEGATE_KEY_2))
                .setDisplayName(AGGEGATE_KEY_2)
                .setUsed(USED)
                .setPeak(PEAK)
                .build(),
            CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(NET_THROUGHPUT_VALUE)
                    .setKey(AGGEGATE_KEY_3))
                .setDisplayName(AGGEGATE_KEY_3)
                .setUsed(USED)
                .setPeak(PEAK)
                .build(),
            CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(NET_THROUGHPUT_VALUE)
                    .setKey(NON_AGGEGATE_KEY_1))
                .setDisplayName(NON_AGGEGATE_KEY_1)
                .setUsed(USED)
                .setPeak(PEAK)
                .build()
        ));
    }

    @Test
    public void testMapEntityToAspect() {
        // arrange
        final TopologyEntityDTO.Builder switchDTO = topologyEntityDTOBuilder(
            EntityType.SWITCH, TypeSpecificInfo.getDefaultInstance());
        switchDTO.addAllCommoditiesBoughtFromProviders(ImmutableList.of(
            CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(123L)
                .addAllCommodityBought(commodityBoughts)
                .build()));
        switchDTO.addAllCommoditySoldList(commoditySolds);

        final MultiEntityRequest providerReq = ApiTestUtils.mockMultiFullEntityReq(
            Lists.newArrayList(TopologyEntityDTO.newBuilder()
                .setOid(123L)
                .setEntityType(EntityType.SWITCH_VALUE)
                .addAllCommoditySoldList(commoditySolds)
                .build()));
        when(repositoryApi.entitiesRequest(Collections.singleton(123L))).thenReturn(providerReq);

        final PortsAspectMapper testMapper = new PortsAspectMapper(repositoryApi);

        // act
        final EntityAspect aspectResult = testMapper.mapEntityToAspect(switchDTO.build());

        // assert
        assertTrue(aspectResult instanceof PortsAspectApiDTO);
        final PortsAspectApiDTO portsAspect = (PortsAspectApiDTO) aspectResult;

        final Collection<PortChannelApiDTO> portChannels = portsAspect.getPortChannels();
        final Collection<StatApiDTO> ports = portsAspect.getPorts();

        assertEquals(3, portChannels.size());
        assertEquals(2, ports.size());

        // test commodity sold values
        final PortChannelApiDTO channel1 = portChannels.stream()
            .filter(portChannelApiDTO -> portChannelApiDTO.getDisplayName().equals(CHANNEL_1))
            .findFirst()
            .get();
        assertEquals(1, channel1.getPorts().size());
        assertEquals(ImmutableList.of(AGGEGATE_KEY_1),
            channel1.getPorts().stream().map(StatApiDTO::getName).collect(Collectors.toList()));
        assertEquals(StatsUtils.round(CAPACITY, PrecisionEnum.STATS.getPrecision()) * MULTIPLIER,
            channel1.getCapacity().getTotal(), DELTA);

        // test commodity bought values
        final PortChannelApiDTO channel2 = portChannels.stream()
            .filter(portChannelApiDTO -> portChannelApiDTO.getDisplayName().equals(CHANNEL_2))
            .findFirst()
            .get();
        assertEquals(2, channel2.getPorts().size());
        assertEquals(ImmutableList.of(AGGEGATE_KEY_2, AGGEGATE_KEY_3),
            channel2.getPorts().stream().map(StatApiDTO::getName).collect(Collectors.toList()));
        assertEquals(StatsUtils.round(USED, PrecisionEnum.STATS.getPrecision()) * MULTIPLIER,
            channel2.getValues().getAvg(), DELTA);
        assertEquals(StatsUtils.round(PEAK, PrecisionEnum.STATS.getPrecision()) * MULTIPLIER,
            channel2.getValues().getMax(), DELTA);
        // bought capacity value come from channel 1 since they share the same key and type
        assertEquals(StatsUtils.round(CAPACITY, PrecisionEnum.STATS.getPrecision()) * MULTIPLIER,
            channel2.getCapacity().getTotal(), DELTA);

        // test commodity bought values with no capacity provided
        final PortChannelApiDTO channel3 = portChannels.stream()
            .filter(portChannelApiDTO -> portChannelApiDTO.getDisplayName().equals(CHANNEL_3))
            .findFirst()
            .get();
        // bought capacity value should be 0
        assertEquals(0D, channel3.getCapacity().getTotal(), DELTA);

        // test non-aggregate ports are created correctly
        assertEquals(ImmutableList.of(NON_AGGEGATE_KEY_1, NON_AGGEGATE_KEY_2),
            ports.stream().map(StatApiDTO::getName).collect(Collectors.toList()));
    }
}