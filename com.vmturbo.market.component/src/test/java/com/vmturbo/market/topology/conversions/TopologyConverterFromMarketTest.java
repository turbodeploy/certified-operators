package com.vmturbo.market.topology.conversions;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_ENTITY_VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.market.runner.ReservedCapacityAnalysis;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.topology.conversions.CommodityIndex.CommodityIndexFactory;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.utilities.NumericIDAllocator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link TopologyConverter}.
 */
public class TopologyConverterFromMarketTest {

    private static final long PM_OID = 10000L;
    private static final long VM_OID = 10001L;
    private static final long DS_OID = 20000L;
    private static final long VOLUME_ID = 10L;
    private static final Float SCALING_FACTOR = 1.5F;
    private static final Float RAW_PM_USED = 0.5F;
    private static final Float RAW_VM_USED = 0.2177F;
    private static final Float RAW_PM_CAPACITY = 2.0F;
    private static final Float MARKET_PM_USED = RAW_PM_USED * SCALING_FACTOR;
    private static final Float MARKET_VM_USED = RAW_VM_USED * SCALING_FACTOR;
    private static final Float MARKET_PM_CAPACITY = RAW_PM_CAPACITY * SCALING_FACTOR;
    private MarketPriceTable marketPriceTable = mock(MarketPriceTable.class);
    private CommodityConverter mockCommodityConverter = mock(CommodityConverter.class);
    private CloudCostData mockCCD = mock(CloudCostData.class);
    private final ReservedCapacityAnalysis reservedCapacityAnalysis =
        new ReservedCapacityAnalysis(Collections.emptyMap());

    private static final TopologyInfo REALTIME_TOPOLOGY_INFO =  TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.REALTIME)
            .build();

    private CommodityType topologyCommodity1;
    private CommodityType topologyCommodity2;

    private final static NumericIDAllocator ID_ALLOCATOR = new NumericIDAllocator();

    private final static int BICLIQUE_TYPE_ID = ID_ALLOCATOR.allocate("BICLIQUE");
    private final static int CPU_TYPE_ID = ID_ALLOCATOR.allocate("CPU");
    private final static int DSPM_TYPE_ID = ID_ALLOCATOR.allocate("DSPM");


    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        // The commodity types in topologyCommodity
        // map to the base type in economy commodity.
        topologyCommodity1 = CommodityType.newBuilder()
                        .setType(CPU_TYPE_ID)
                        .setKey("blah")
                        .build();
        topologyCommodity2 = CommodityType.newBuilder()
                        .setType(DSPM_TYPE_ID)
                        .setKey("blahblah")
                        .build();
    }

    /**
     * Load a json file into a DTO.
     * @param fileName the name of the file to load
     * @return The entity DTO represented by the file
     * @throws IOException when the file is not found
     */
    public static TopologyDTO.TopologyEntityDTO messageFromJsonFile(String fileName) throws IOException {
        URL fileUrl = TopologyConverterFromMarketTest.class.getClassLoader().getResources(fileName).nextElement();
        TopologyDTO.TopologyEntityDTO.Builder builder = TopologyDTO.TopologyEntityDTO.newBuilder();
        JsonFormat.parser().merge(new InputStreamReader(fileUrl.openStream()), builder);
        return builder.build();
    }

    /**
     * Test conversion from {@link TraderTO} to {@link TopologyDTO} with biclique
     * commodity.
     *
     * @throws Exception not supposed to happen in this test
     */
    @Test
    public void testTraderWithBicliqueCommodityConversion() throws Exception {
        TopologyConverter converter = Mockito.spy(
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, false,
                AnalysisUtil.QUOTE_FACTOR, AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR, marketPriceTable,
                    mockCommodityConverter, CommodityIndex.newFactory()));
        final TopologyDTO.CommoditySoldDTO topologyDSPMSold =
                    TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)
                                .build())
                        .build();
        final TopologyDTO.CommoditySoldDTO topologyCPUSold =
                    TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.CPU_VALUE)
                                        .build())
                        .build();
        final List<CommodityBoughtDTO> topologyDSPMBought =
                    Lists.newArrayList(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE))
                    .build());

        final List<CommodityBoughtDTO> topologyCPUBought =
                    Lists.newArrayList(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.CPU_VALUE))
                        .build());

        Map<Long, ShoppingListInfo> shoppingListMap = new HashMap<>();
        shoppingListMap.put(VM_OID, new ShoppingListInfo(DSPM_TYPE_ID, DS_OID, PM_OID, null,
                EntityType.PHYSICAL_MACHINE_VALUE, topologyDSPMBought));
        Field commTypeAllocator =
                        TopologyConverter.class.getDeclaredField("commodityTypeAllocator");
        commTypeAllocator.setAccessible(true);
            commTypeAllocator.set(converter, ID_ALLOCATOR);
        Field shoppingListInfos =
                        TopologyConverter.class.getDeclaredField("shoppingListOidToInfos");
        shoppingListInfos.setAccessible(true);
        shoppingListInfos.set(converter, shoppingListMap);
        shoppingListInfos.setAccessible(true);
        shoppingListInfos.set(converter, shoppingListMap);

        final CommodityDTOs.CommoditySoldTO economyCPUSold = CommodityDTOs.CommoditySoldTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                            .setBaseType(DSPM_TYPE_ID)
                            .setType(CPU_TYPE_ID)
                            .build())
                        .build();
        final CommodityDTOs.CommoditySoldTO economyDSPMSold = CommodityDTOs.CommoditySoldTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                            .setBaseType(BICLIQUE_TYPE_ID)
                            .setType(DSPM_TYPE_ID)
                            .build())
                        .build();
        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType())).when(mockCommodityConverter)
                        .economyToTopologyCommodity(Mockito.eq(economyCPUSold.getSpecification()));
        Mockito.doReturn(Optional.empty()).when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(economyDSPMSold.getSpecification()));
        // create a topology entity DTO with DSPM sold
        TopologyDTO.TopologyEntityDTO expectedEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setOid(PM_OID)
                        .addCommoditySoldList(topologyDSPMSold)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(PM_OID)
                            .addAllCommodityBought(topologyCPUBought)
                            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE))
                        .putEntityPropertyMap("dummy", "dummy")
                        .build();
        // create a topology entity DTO with DSPM bought
        TopologyDTO.TopologyEntityDTO expectedEntity2 = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setOid(DS_OID)
                        .setEntityType(BUSINESS_ENTITY_VALUE)
                        .addCommoditySoldList(topologyCPUSold)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .addAllCommodityBought(topologyDSPMBought))
                        .putEntityPropertyMap("dummy", "dummy")
                        .build();
        // create trader DTO corresponds to originalEntity
        EconomyDTOs.TraderTO trader = EconomyDTOs.TraderTO.newBuilder()
                        .setOid(PM_OID)
                        .addCommoditiesSold(economyDSPMSold)
                        .addShoppingLists(ShoppingListTO.newBuilder()
                            .setOid(VM_OID)
                            .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO.newBuilder()
                                    .setBaseType(DSPM_TYPE_ID)
                                    .setType(CPU_TYPE_ID)
                                    .build())
                                .build())
                            .build())
                        .build();
        when(mockCCD.getRiCoverageForEntity(anyLong())).thenReturn(Optional.empty());

        Map<Long, TopologyDTO.ProjectedTopologyEntity> entity =
            converter.convertFromMarket(Collections.singletonList(trader),
                ImmutableMap.of(expectedEntity.getOid(), expectedEntity, expectedEntity2.getOid(), expectedEntity2),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis);
        assertEquals(1L, entity.size());
        assertThat(entity.get(PM_OID).getEntity().getCommoditySoldListList(),
            contains(expectedEntity.getCommoditySoldListList().toArray()));

        assertThat(entity.get(PM_OID).getEntity().getCommoditiesBoughtFromProvidersCount(), is(1));
        assertThat(entity.get(PM_OID).getEntity().getCommoditiesBoughtFromProviders(0).getProviderEntityType(),
            is(EntityType.PHYSICAL_MACHINE_VALUE));
    }

    /**
     * Test conversion from {@link TraderTO} to {@link TopologyDTO} when entity is VM and state is
     * SUSPEND. It's to ensure after conversion, the VM entity state is still SUSPEND.
     * Entity (SUSPEND) -> TRADER (IDLE) -> TRADER (ACTIVE) -> Entity (SUSPEND)
     *
     * @throws Exception not supposed to happen in this test
     */
    @Test
    public void testVMTraderToEntityConversion() throws Exception {
        when(mockCCD.getExistingRiBought()).thenReturn(Collections.emptyList());
        TopologyConverter converter = Mockito.spy(
                new TopologyConverter(TopologyInfo.newBuilder().setTopologyType(TopologyType.PLAN).build(),
                    false, AnalysisUtil.QUOTE_FACTOR, AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR, marketPriceTable,
                    mockCommodityConverter, mockCCD, CommodityIndex.newFactory()));
        final TopologyDTO.CommoditySoldDTO topologyDSPMSold =
                TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)
                                .build())
                        .build();
        final TopologyDTO.CommoditySoldDTO topologyCPUSold =
                TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.CPU_VALUE)
                                .build())
                        .build();
        final List<CommodityBoughtDTO> topologyDSPMBought =
                Lists.newArrayList(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE))
                        .build());

        CommodityType cpuCommType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.CPU_VALUE).build();

        final List<CommodityBoughtDTO> topologyCPUBought =
                Lists.newArrayList(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(cpuCommType)
                        .build());

        Map<Long, ShoppingListInfo> shoppingListMap = new HashMap<>();
        shoppingListMap.put(VM_OID, new ShoppingListInfo(2, DS_OID, PM_OID, null,
                EntityType.PHYSICAL_MACHINE_VALUE, topologyDSPMBought));
        Field commTypeAllocator =
                TopologyConverter.class.getDeclaredField("commodityTypeAllocator");
        commTypeAllocator.setAccessible(true);
        commTypeAllocator.set(converter, ID_ALLOCATOR);
        Field shoppingListInfos =
                TopologyConverter.class.getDeclaredField("shoppingListOidToInfos");
        shoppingListInfos.setAccessible(true);
        shoppingListInfos.set(converter, shoppingListMap);

        CommoditySpecificationTO cpuCommSpecTO = CommoditySpecificationTO.newBuilder()
                .setBaseType(BICLIQUE_TYPE_ID)
                .setType(CPU_TYPE_ID)
                .build();

        final CommodityDTOs.CommoditySoldTO economyDSPMSold = CommodityDTOs.CommoditySoldTO.newBuilder()
                .setSpecification(CommoditySpecificationTO.newBuilder()
                        .setBaseType(BICLIQUE_TYPE_ID)
                        .setType(DSPM_TYPE_ID)
                        .build())
                .build();
        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType())).when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(CommoditySpecificationTO.newBuilder()
                        .setBaseType(DSPM_TYPE_ID)
                        .setType(CPU_TYPE_ID)
                        .build()));
        Mockito.doReturn(Optional.empty()).when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(CommoditySpecificationTO.newBuilder()
                        .setBaseType(BICLIQUE_TYPE_ID)
                        .setType(DSPM_TYPE_ID)
                        .build()));
        Mockito.doReturn(cpuCommSpecTO).when(mockCommodityConverter)
                .commoditySpecification(cpuCommType);
        // create a topology entity DTO with DSPM sold
        TopologyDTO.TopologyEntityDTO expectedEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(PM_OID)
                .setEntityState(EntityState.SUSPENDED)
                .addCommoditySoldList(topologyDSPMSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PM_OID)
                        .addAllCommodityBought(topologyCPUBought)
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE))
                .putEntityPropertyMap("dummy", "dummy")
                .build();

        final long entityId = PM_OID;
        final TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(1)
                .setOid(entityId)
                .setAnalysisSettings(AnalysisSettings.newBuilder().setControllable(true).build())
                .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(topologyCommodity1))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(topologyCommodity2))
                .build();
        when(mockCCD.getRiCoverageForEntity(10000L)).thenReturn(Optional.empty());

        Field entityOidToDto = TopologyConverter.class.getDeclaredField("entityOidToDto");
        Map<Long, TopologyEntityDTO> map = new HashMap<>();
        map.put(entityId, entityDTO);
        entityOidToDto.setAccessible(true);
        entityOidToDto.set(converter, map);
        converter.convertToMarket(ImmutableMap.of(entityDTO.getOid(), entityDTO));

        // create trader DTO corresponds to originalEntity
        EconomyDTOs.TraderTO trader = EconomyDTOs.TraderTO.newBuilder()
                .setOid(10000L)
                .addCommoditiesSold(economyDSPMSold)
                .setState(TraderStateTO.ACTIVE)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addShoppingLists(ShoppingListTO.newBuilder()
                        .setOid(10001L)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO.newBuilder()
                                        .setBaseType(DSPM_TYPE_ID)
                                        .setType(CPU_TYPE_ID)
                                        .build())
                                .build())
                        .build())
                .build();

        Map<Long, TopologyDTO.ProjectedTopologyEntity> entity =
                converter.convertFromMarket(Collections.singletonList(trader),
                        ImmutableMap.of(expectedEntity.getOid(), expectedEntity),
                        PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis);
        assertEquals(1L, entity.size());
        // Ensure the entity is having SUSPENDED state.
        assertThat(entity.get(trader.getOid()).getEntity().getEntityState(), is(EntityState.SUSPENDED));
    }

    /**
     * Test that when createResources is called for a cloud VM, its projected volumes are created.
     *
     * @throws Exception not supposed to happen in this test
     */
    @Test
    public void testCreateResources() throws Exception {
        // Arrange
        TopologyConverter converter = Mockito.spy(
                new TopologyConverter(TopologyInfo.newBuilder().setTopologyType(TopologyType.PLAN).build(),
                        false, AnalysisUtil.QUOTE_FACTOR, AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR, marketPriceTable,
                        mockCommodityConverter, mockCCD, CommodityIndex.newFactory()));
        long azOid = 1l;
        long volumeOid = 2l;
        long storageTierOid = 3l;
        long vmOid = 4l;
        TopologyDTO.TopologyEntityDTO az = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
                .setOid(azOid)
                .build();
        TopologyDTO.TopologyEntityDTO storageTier = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.STORAGE_TIER_VALUE)
                .setOid(storageTierOid)
                .build();
        TopologyDTO.TopologyEntityDTO volume = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setOid(volumeOid)
                .setDisplayName("volume1")
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();
        TopologyDTO.TopologyEntityDTO vm = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(vmOid)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(storageTierOid)
                        .setVolumeId(volumeOid)
                        .setProviderEntityType(EntityType.STORAGE_TIER_VALUE))
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityId(azOid)
                        .setConnectedEntityType(az.getEntityType()))
                .build();
        // Use reflection and these entities to the entityOidToDto map
        Field entityOidToDto = TopologyConverter.class.getDeclaredField("entityOidToDto");
        Map<Long, TopologyEntityDTO> map = ImmutableMap.of(
                azOid, az,
                storageTierOid, storageTier,
                volumeOid, volume);
        entityOidToDto.setAccessible(true);
        entityOidToDto.set(converter, map);

        // Act
        Set<TopologyEntityDTO> resources = converter.createResources(vm);

        // Assert that the projected volume is connected to the storage and AZ which the VM is
        // connected to
        assertEquals(1, resources.size());
        TopologyEntityDTO projectedVolume = resources.iterator().next();
        assertEquals(2, projectedVolume.getConnectedEntityListList().size());
        ConnectedEntity connectedStorageTier = projectedVolume.getConnectedEntityListList()
                .stream().filter(c -> c.getConnectedEntityType() == EntityType.STORAGE_TIER_VALUE).findFirst().get();
        ConnectedEntity connectedAz = projectedVolume.getConnectedEntityListList().stream()
                .filter(c -> c.getConnectedEntityType() == EntityType.AVAILABILITY_ZONE_VALUE).findFirst().get();
        assertEquals(storageTierOid, connectedStorageTier.getConnectedEntityId());
        assertEquals(azOid, connectedAz.getConnectedEntityId());
        assertEquals(volume.getDisplayName(), projectedVolume.getDisplayName());
        assertEquals(volume.getEnvironmentType(), projectedVolume.getEnvironmentType());
    }


    /**
     * Test conversion from {@link TraderTO} to {@link TopologyDTO} with 'scaleFactor'
     * so that the CPU commodity will need to be scaled down to produce the TopologyDTO.
     *
     * @throws Exception not supposed to happen in this test
     */
    @Test
    public void testCommodityReverseScaling() throws Exception {

        // used in assertEquals(double, double, epsilon)
        final double epsilon = 1e-5;

        final CommodityIndex commodityIndex = CommodityIndex.newFactory().newIndex();

        // Arrange
        final TopologyDTO.CommoditySoldDTO topologyCPUSold =
                TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.CPU_VALUE)
                                .build())
                        .setUsed(RAW_PM_USED)
                        .setCapacity(RAW_PM_CAPACITY)
                        .setScalingFactor(SCALING_FACTOR)
                        .build();
        final List<CommodityBoughtDTO> topologyDSPMBought =
                Lists.newArrayList(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE))
                        .build());

        // create a PM topology entity DTO that seels CPU (with scalingFactor set)
        TopologyDTO.TopologyEntityDTO expectedEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setOid(PM_OID)
            .addCommoditySoldList(topologyCPUSold)
            .putEntityPropertyMap("dummy", "dummy")
            .build();
        // add a VM to buy CPU from the PM
        // create a topology entity DTO with CPU bought
        final List<CommodityBoughtDTO> topologyCpuBought =
            Lists.newArrayList(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.CPU_VALUE))
                .setUsed(RAW_VM_USED)
                .setScalingFactor(SCALING_FACTOR)
                .build());
        TopologyDTO.TopologyEntityDTO expectedEntity2 = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(VM_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(topologyCPUSold)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PM_OID)
                .setVolumeId(VOLUME_ID)
                .addAllCommodityBought(topologyCpuBought))
            .putEntityPropertyMap("dummy", "dummy")
            .build();

        // Add the entities to the commodity index. Normally this would get done prior to the
        // conversion from market.
        commodityIndex.addEntity(expectedEntity);
        commodityIndex.addEntity(expectedEntity2);

        final CommodityIndexFactory indexFactory = mock(CommodityIndexFactory.class);
        when(indexFactory.newIndex()).thenReturn(commodityIndex);

        // converter under test
        TopologyConverter converter = Mockito.spy(
                new TopologyConverter(REALTIME_TOPOLOGY_INFO, false,
                    AnalysisUtil.QUOTE_FACTOR, AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR, marketPriceTable,
                        mockCommodityConverter, indexFactory));

        // warning: introspection follows...
        Map<Long, ShoppingListInfo> shoppingListMap = new HashMap<>();
        shoppingListMap.put(VM_OID, new ShoppingListInfo(DSPM_TYPE_ID, DS_OID, PM_OID, VOLUME_ID,
                EntityType.PHYSICAL_MACHINE_VALUE, topologyDSPMBought));
        Field commTypeAllocator =
                TopologyConverter.class.getDeclaredField("commodityTypeAllocator");
        commTypeAllocator.setAccessible(true);
        commTypeAllocator.set(converter, ID_ALLOCATOR);
        Field shoppingListInfos =
                TopologyConverter.class.getDeclaredField("shoppingListOidToInfos");
        shoppingListInfos.setAccessible(true);
        shoppingListInfos.set(converter, shoppingListMap);
        // ... end introspection

        final CommodityDTOs.CommoditySoldTO economyCPUSold = CommodityDTOs.CommoditySoldTO.newBuilder()
                .setSpecification(CommoditySpecificationTO.newBuilder()
                        .setBaseType(DSPM_TYPE_ID)
                        .setType(CPU_TYPE_ID)
                        .build())
                .setQuantity(MARKET_PM_USED)
                .setCapacity(MARKET_PM_CAPACITY)
                .build();

        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType())).when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(economyCPUSold.getSpecification()));

        // create trader TO (i.e. as returned from Market) that corresponds to expected PM Entity
        EconomyDTOs.TraderTO pmTrader = EconomyDTOs.TraderTO.newBuilder()
                .setOid(PM_OID)
                .addCommoditiesSold(economyCPUSold)
                .addShoppingLists(ShoppingListTO.newBuilder()
                        .setOid(VM_OID)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO.newBuilder()
                                        .setBaseType(DSPM_TYPE_ID)
                                        .setType(CPU_TYPE_ID)
                                        .build())
                                .setQuantity(MARKET_VM_USED)
                                .build())
                        .build())
                .build();

        // create trader TO (i.e. as returned from Market) that corresponds to expectedVM Entity
        EconomyDTOs.TraderTO vmTrader = EconomyDTOs.TraderTO.newBuilder()
                .setOid(VM_OID)
                .addShoppingLists(ShoppingListTO.newBuilder()
                    .setOid(VM_OID)
                    .setSupplier(PM_OID)
                    .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                            .setSpecification(CommoditySpecificationTO.newBuilder()
                                    .setBaseType(DSPM_TYPE_ID)
                                    .setType(CPU_TYPE_ID)
                                    .build())
                            .setQuantity(MARKET_VM_USED)
                            .build())
                    .build())
            .build();

        // Act
        Map<Long, TopologyDTO.ProjectedTopologyEntity> entity =
                converter.convertFromMarket(
                        // the traders in the market
                        Lists.newArrayList(pmTrader, vmTrader),
                        // map back to original TopologyEntityDTOs
                        ImmutableMap.of(expectedEntity.getOid(), expectedEntity,
                                expectedEntity2.getOid(), expectedEntity2),
                        PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis);

        // Assert two entities returned - they will be in the original order - VM and PM
        assertEquals(2L, entity.size());
        // check that the PM expected capacity and used matches the actual converted capacity and used
        final CommoditySoldDTO actualCpuCommodity = entity.get(pmTrader.getOid()).getEntity().getCommoditySoldList(0);
        assertEquals(topologyCPUSold.getCapacity(), actualCpuCommodity.getCapacity(), epsilon);
        assertEquals(topologyCPUSold.getUsed(), actualCpuCommodity.getUsed(), epsilon);
        // check that the VM expected used matches the actual converted used
        assertEquals(1, entity.get(vmTrader.getOid()).getEntity().getCommoditiesBoughtFromProvidersList().size());
        final List<CommodityBoughtDTO> actualBoughtCommodityList = entity.get(vmTrader.getOid()).getEntity()
                .getCommoditiesBoughtFromProvidersList().iterator().next().getCommodityBoughtList();
        assertEquals(1, actualBoughtCommodityList.size());
        assertEquals(RAW_VM_USED, actualBoughtCommodityList.iterator().next().getUsed(), epsilon);

    }


    /**
     * Test conversion from {@link TraderTO} to {@link TopologyDTO} with edge 'scaleFactor'
     * Double.MIN_VALUE. It's to prevent "Infinity" capacity.
     *
     * @throws Exception not supposed to happen in this test
     */
    @Test
    public void testCommodityReverseScalingEdge() throws Exception {

        final CommodityIndex commodityIndex = CommodityIndex.newFactory().newIndex();

        // Arrange
        final TopologyDTO.CommoditySoldDTO topologyCPUSold =
                TopologyDTO.CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.CPU_VALUE)
                                .build())
                        .setUsed(RAW_PM_USED)
                        .setCapacity(RAW_PM_CAPACITY)
                        .setScalingFactor(Double.MIN_VALUE) // set edge scaling factor
                        .build();
        // create a PM topology entity DTO that seels CPU (with scalingFactor set)
        TopologyDTO.TopologyEntityDTO expectedEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setOid(PM_OID)
            .addCommoditySoldList(topologyCPUSold)
            .putEntityPropertyMap("dummy", "dummy")
            .build();

        // add a VM to buy CPU from the PM
        // create a topology entity DTO with CPU bought
        final List<CommodityBoughtDTO> topologyCpuBought =
            Lists.newArrayList(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.CPU_VALUE))
                .setUsed(RAW_VM_USED)
                .setScalingFactor(SCALING_FACTOR)
                .build());
        TopologyDTO.TopologyEntityDTO expectedEntity2 = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(VM_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(topologyCPUSold)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PM_OID)
                .setVolumeId(VOLUME_ID)
                .addAllCommodityBought(topologyCpuBought))
            .putEntityPropertyMap("dummy", "dummy")
            .build();

        // Add the entities to the commodity index. Normally this would get done prior to the
        // conversion from market.
        commodityIndex.addEntity(expectedEntity);
        commodityIndex.addEntity(expectedEntity2);

        final CommodityIndexFactory indexFactory = mock(CommodityIndexFactory.class);
        when(indexFactory.newIndex()).thenReturn(commodityIndex);

        final List<CommodityBoughtDTO> topologyDSPMBought =
                Lists.newArrayList(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE))
                        .build());

        TopologyConverter converter = Mockito.spy(
                new TopologyConverter(TopologyInfo.newBuilder().setTopologyType(TopologyType.PLAN).build(),
                    false, AnalysisUtil.QUOTE_FACTOR, AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR, marketPriceTable,
                        mockCommodityConverter, mockCCD, indexFactory));

        // warning: introspection follows...
        Map<Long, ShoppingListInfo> shoppingListMap = new HashMap<>();
        shoppingListMap.put(VM_OID, new ShoppingListInfo(DSPM_TYPE_ID, DS_OID, PM_OID, VOLUME_ID,
                EntityType.PHYSICAL_MACHINE_VALUE, topologyDSPMBought));
        Field commTypeAllocator =
                TopologyConverter.class.getDeclaredField("commodityTypeAllocator");
        commTypeAllocator.setAccessible(true);
        commTypeAllocator.set(converter, ID_ALLOCATOR);
        Field shoppingListInfos =
                TopologyConverter.class.getDeclaredField("shoppingListOidToInfos");
        shoppingListInfos.setAccessible(true);
        shoppingListInfos.set(converter, shoppingListMap);
        // ... end introspection

        final CommodityDTOs.CommoditySoldTO economyCPUSold = CommodityDTOs.CommoditySoldTO.newBuilder()
                .setSpecification(CommoditySpecificationTO.newBuilder()
                        .setBaseType(DSPM_TYPE_ID)
                        .setType(CPU_TYPE_ID)
                        .build())
                .setQuantity(MARKET_PM_USED)
                .setCapacity(MARKET_PM_CAPACITY)
                .build();

        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType())).when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(economyCPUSold.getSpecification()));

        // create trader TO (i.e. as returned from Market) that corresponds to expected PM Entity
        EconomyDTOs.TraderTO pmTrader = EconomyDTOs.TraderTO.newBuilder()
                .setOid(PM_OID)
                .addCommoditiesSold(economyCPUSold)
                .addShoppingLists(ShoppingListTO.newBuilder()
                        .setOid(VM_OID)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO.newBuilder()
                                        .setBaseType(DSPM_TYPE_ID)
                                        .setType(CPU_TYPE_ID)
                                        .build())
                                .setQuantity(MARKET_VM_USED)
                                .build())
                        .build())
                .build();

        // create trader TO (i.e. as returned from Market) that corresponds to expectedVM Entity
        EconomyDTOs.TraderTO vmTrader = EconomyDTOs.TraderTO.newBuilder()
                .setOid(VM_OID)
                .addShoppingLists(ShoppingListTO.newBuilder()
                    .setOid(VM_OID)
                    .setSupplier(PM_OID)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO.newBuilder()
                                        .setBaseType(DSPM_TYPE_ID)
                                        .setType(CPU_TYPE_ID)
                                        .build())
                                .setQuantity(MARKET_VM_USED)
                                .build())
                        .build())
                .build();

        // Act
        Map<Long, TopologyDTO.ProjectedTopologyEntity> entity =
                converter.convertFromMarket(
                        // the traders in the market
                        Lists.newArrayList(pmTrader, vmTrader),
                        // map back to original TopologyEntityDTOs
                        ImmutableMap.of(expectedEntity.getOid(), expectedEntity,
                                expectedEntity2.getOid(), expectedEntity2),
                        PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis);

        // Assert two entities returned - they will be in the original order - VM and PM
        assertEquals(2L, entity.size());
        // check that the PM expected capacity and used matches the actual converted capacity and used
        final CommoditySoldDTO actualCpuCommodity = entity.get(pmTrader.getOid()).getEntity().getCommoditySoldList(0);
        // Since the scaling factor is infinitely small we don't use it.
        assertEquals(MARKET_PM_CAPACITY, actualCpuCommodity.getCapacity(), TopologyConverter.EPSILON);
        assertEquals(MARKET_PM_USED, actualCpuCommodity.getUsed(), TopologyConverter.EPSILON);
        // check that the VM expected used matches the actual converted used
        assertEquals(1, entity.get(vmTrader.getOid()).getEntity().getCommoditiesBoughtFromProvidersList().size());
        final List<CommodityBoughtDTO> actualBoughtCommodityList = entity.get(vmTrader.getOid()).getEntity()
                .getCommoditiesBoughtFromProvidersList().iterator().next().getCommodityBoughtList();
        assertEquals(1, actualBoughtCommodityList.size());
        assertEquals(RAW_VM_USED, actualBoughtCommodityList.iterator().next().getUsed(), TopologyConverter.EPSILON);

    }

    @Test
    public void testConvertFromMarketPreservesOriginalEnvType() {
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(
                TopologyInfo.newBuilder()
                    .setTopologyType(TopologyType.REALTIME)
                    .build(),
                false,
                AnalysisUtil.QUOTE_FACTOR,
                AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable,
                mockCommodityConverter,
                mockCCD, CommodityIndex.newFactory()));

        final TopologyDTO.TopologyEntityDTO originalVm = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(VM_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();

        final EconomyDTOs.TraderTO vmTrader = EconomyDTOs.TraderTO.newBuilder()
            .setOid(VM_OID)
            .build();

        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity =
            converter.convertFromMarket(Collections.singletonList(vmTrader),
                ImmutableMap.of(VM_OID, originalVm),
                    PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis);

        assertThat(entity.get(VM_OID).getEntity().getEnvironmentType(),
            is(originalVm.getEnvironmentType()));
    }

    @Test
    public void testConvertFromMarketPreservesOriginalTypeSpecificInfo() {
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(
                TopologyInfo.newBuilder()
                    .setTopologyType(TopologyType.REALTIME)
                    .build(),
                false,
                AnalysisUtil.QUOTE_FACTOR,
                AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable,
                mockCommodityConverter,
                mockCCD, CommodityIndex.newFactory()));

        final TopologyDTO.TopologyEntityDTO originalVm = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(VM_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder()
                    .setNumCpus(100)))
            .build();

        final EconomyDTOs.TraderTO vmTrader = EconomyDTOs.TraderTO.newBuilder()
            .setOid(VM_OID)
            .build();

        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity =
            converter.convertFromMarket(Collections.singletonList(vmTrader),
                ImmutableMap.of(VM_OID, originalVm),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis);

        assertThat(entity.get(VM_OID).getEntity().getTypeSpecificInfo(),
            is(originalVm.getTypeSpecificInfo()));
    }

    @Test
    public void testConvertFromMarketPreservesOriginalOrigin() {
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(
                TopologyInfo.newBuilder()
                    .setTopologyType(TopologyType.REALTIME)
                    .build(),
                false,
                AnalysisUtil.QUOTE_FACTOR,
                AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable,
                mockCommodityConverter, mockCCD, CommodityIndex.newFactory()));

        final TopologyDTO.TopologyEntityDTO originalVm = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(VM_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .addDiscoveringTargetIds(123)))
            .build();

        final EconomyDTOs.TraderTO vmTrader = EconomyDTOs.TraderTO.newBuilder()
            .setOid(VM_OID)
            .build();

        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity =
            converter.convertFromMarket(Collections.singletonList(vmTrader),
                ImmutableMap.of(VM_OID, originalVm),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis);

        assertThat(entity.get(VM_OID).getEntity().getOrigin(),
            is(originalVm.getOrigin()));
    }

    @Test
    public void testConvertFromMarketCloneRetainsOriginalEnvType() {
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME)
                .build(),
                false,
                AnalysisUtil.QUOTE_FACTOR,
                AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable,
                mockCommodityConverter, mockCCD, CommodityIndex.newFactory()));

        final TopologyDTO.TopologyEntityDTO originalVm = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(VM_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .build();

        final long cloneId = 127329;
        final EconomyDTOs.TraderTO cloneTrader = EconomyDTOs.TraderTO.newBuilder()
            .setOid(cloneId)
            .setCloneOf(VM_OID)
            .build();

        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity =
            converter.convertFromMarket(Collections.singletonList(cloneTrader),
                ImmutableMap.of(VM_OID, originalVm),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis);

        assertThat(entity.get(cloneId).getEntity().getEnvironmentType(),
            is(originalVm.getEnvironmentType()));
    }

    @Test
    public void testConvertFromMarketCloneRetainsOriginalTypeSpecificInfo() {
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(
                TopologyInfo.newBuilder()
                    .setTopologyType(TopologyType.REALTIME)
                    .build(),
                false,
                AnalysisUtil.QUOTE_FACTOR,
                AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable,
                mockCommodityConverter,
                mockCCD,
                CommodityIndex.newFactory()));

        final TopologyDTO.TopologyEntityDTO originalVm = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(VM_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder()
                    .setNumCpus(100)))
            .build();

        final long cloneId = 127329;
        final EconomyDTOs.TraderTO cloneTrader = EconomyDTOs.TraderTO.newBuilder()
            .setOid(cloneId)
            .setCloneOf(VM_OID)
            .build();

        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity =
            converter.convertFromMarket(Collections.singletonList(cloneTrader),
                ImmutableMap.of(VM_OID, originalVm),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis);

        assertThat(entity.get(cloneId).getEntity().getTypeSpecificInfo(),
            is(originalVm.getTypeSpecificInfo()));
    }

    @Test
    public void testConvertFromMarketCloneHasAnalysisOrigin() {
        final TopologyConverter converter = Mockito.spy(
            new TopologyConverter(
                TopologyInfo.newBuilder()
                    .setTopologyType(TopologyType.REALTIME)
                    .build(),
                false,
                AnalysisUtil.QUOTE_FACTOR,
                AnalysisUtil.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable,
                mockCommodityConverter,
                mockCCD,
                CommodityIndex.newFactory()));

        final TopologyDTO.TopologyEntityDTO originalVm = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(VM_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .addDiscoveringTargetIds(123)))
            .build();

        final long cloneId = 127329;
        final EconomyDTOs.TraderTO cloneTrader = EconomyDTOs.TraderTO.newBuilder()
            .setOid(cloneId)
            .setCloneOf(VM_OID)
            .build();

        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity =
            converter.convertFromMarket(Collections.singletonList(cloneTrader),
                ImmutableMap.of(VM_OID, originalVm),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis);

        // The origin is NOT the same - it's a market origin since this is a clone.
        assertThat(entity.get(cloneId).getEntity().getOrigin().getAnalysisOrigin().getOriginalEntityId(),
            is(originalVm.getOid()));
    }


}

