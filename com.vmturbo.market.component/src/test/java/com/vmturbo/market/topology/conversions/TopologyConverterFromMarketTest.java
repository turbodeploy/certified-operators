package com.vmturbo.market.topology.conversions;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_ENTITY_VALUE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.runner.cost.MarketPriceTable;
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
    private static final Float SCALING_FACTOR = 1.5F;
    private static final Float RAW_PM_USED = 0.5F;
    private static final Float RAW_VM_USED = 0.2177F;
    private static final Float RAW_PM_CAPACITY = 2.0F;
    private static final Float MARKET_PM_USED = RAW_PM_USED * SCALING_FACTOR;
    private static final Float MARKET_VM_USED = RAW_VM_USED * SCALING_FACTOR;
    private static final Float MARKET_PM_CAPACITY = RAW_PM_CAPACITY * SCALING_FACTOR;
    private MarketPriceTable marketPriceTable = mock(MarketPriceTable.class);


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
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, marketPriceTable));
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
        shoppingListMap.put(VM_OID, new ShoppingListInfo(DSPM_TYPE_ID, DS_OID, PM_OID,
                EntityType.PHYSICAL_MACHINE_VALUE, topologyDSPMBought));
        Field commTypeAllocator =
                        TopologyConverter.class.getDeclaredField("commodityTypeAllocator");
        commTypeAllocator.setAccessible(true);
            commTypeAllocator.set(converter, ID_ALLOCATOR);
        Field shoppingListInfos =
                        TopologyConverter.class.getDeclaredField("shoppingListOidToInfos");
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
        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType())).when(converter)
                        .economyToTopologyCommodity(Mockito.eq(economyCPUSold.getSpecification()));
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

        List<TopologyDTO.ProjectedTopologyEntity> entity =
            converter.convertFromMarket(Collections.singletonList(trader),
                ImmutableMap.of(expectedEntity.getOid(), expectedEntity, expectedEntity2.getOid(), expectedEntity2),
                PriceIndexMessage.getDefaultInstance());
        assertEquals(1L, entity.size());
        assertEquals(expectedEntity.getCommoditySoldList(0), entity.get(0).getEntity().getCommoditySoldList(0));
        assertEquals(1, entity.get(0).getEntity().getCommoditiesBoughtFromProvidersCount());
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE,
                entity.get(0).getEntity().getCommoditiesBoughtFromProviders(0).getProviderEntityType());
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
        TopologyConverter converter = Mockito.spy(
                new TopologyConverter(TopologyInfo.newBuilder()
                        .setTopologyType(TopologyType.PLAN)
                        .build(),
                        marketPriceTable));
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
        shoppingListMap.put(VM_OID, new ShoppingListInfo(2, DS_OID, PM_OID,
                EntityType.PHYSICAL_MACHINE_VALUE, topologyDSPMBought));
        Field commTypeAllocator =
                TopologyConverter.class.getDeclaredField("commodityTypeAllocator");
        commTypeAllocator.setAccessible(true);
        commTypeAllocator.set(converter, ID_ALLOCATOR);
        Field shoppingListInfos =
                TopologyConverter.class.getDeclaredField("shoppingListOidToInfos");
        shoppingListInfos.setAccessible(true);
        shoppingListInfos.set(converter, shoppingListMap);

        final CommodityDTOs.CommoditySoldTO economyCPUSold = CommodityDTOs.CommoditySoldTO.newBuilder()
                .setSpecification(CommoditySpecificationTO.newBuilder()
                        .setBaseType(BICLIQUE_TYPE_ID)
                        .setType(CPU_TYPE_ID)
                        .build())
                .build();
        final CommodityDTOs.CommoditySoldTO economyDSPMSold = CommodityDTOs.CommoditySoldTO.newBuilder()
                .setSpecification(CommoditySpecificationTO.newBuilder()
                        .setBaseType(BICLIQUE_TYPE_ID)
                        .setType(DSPM_TYPE_ID)
                        .build())
                .build();
        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType())).when(converter)
                .economyToTopologyCommodity(Mockito.eq(economyCPUSold.getSpecification()));
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

        List<TopologyDTO.ProjectedTopologyEntity> entity =
                converter.convertFromMarket(Collections.singletonList(trader),
                        ImmutableMap.of(expectedEntity.getOid(), expectedEntity),
                        PriceIndexMessage.getDefaultInstance());
        assertEquals(1L, entity.size());
        // Ensure the entity is having SUSPENDED state.
        assertEquals(EntityState.SUSPENDED, entity.get(0).getEntity().getEntityState());
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

        // converter under test
        TopologyConverter converter = Mockito.spy(
                new TopologyConverter(REALTIME_TOPOLOGY_INFO, marketPriceTable));

        // warning: introspection follows...
        Map<Long, ShoppingListInfo> shoppingListMap = new HashMap<>();
        shoppingListMap.put(VM_OID, new ShoppingListInfo(DSPM_TYPE_ID, DS_OID, PM_OID,
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

        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType())).when(converter)
                .economyToTopologyCommodity(Mockito.eq(economyCPUSold.getSpecification()));

        // create a PM topology entity DTO that seels CPU (with scalingFactor set)
        TopologyDTO.TopologyEntityDTO expectedEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setOid(PM_OID)
                .addCommoditySoldList(topologyCPUSold)
                .putEntityPropertyMap("dummy", "dummy")
                .build();

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
                        .addAllCommodityBought(topologyCpuBought))
                .putEntityPropertyMap("dummy", "dummy")
                .build();

        // create trader TO (i.e. as returned from Market) that corresponds to expectedVM Entity
        EconomyDTOs.TraderTO vmTrader = EconomyDTOs.TraderTO.newBuilder()
                .setOid(VM_OID)
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

        // Act
        List<TopologyDTO.ProjectedTopologyEntity> entity =
                converter.convertFromMarket(
                        // the traders in the market
                        Lists.newArrayList(pmTrader, vmTrader),
                        // map back to original TopologyEntityDTOs
                        ImmutableMap.of(expectedEntity.getOid(), expectedEntity,
                                expectedEntity2.getOid(), expectedEntity2),
                        PriceIndexMessage.getDefaultInstance());

        // Assert two entities returned - they will be in the original order - VM and PM
        assertEquals(2L, entity.size());
        // check that the PM expected capacity and used matches the actual converted capacity and used
        final CommoditySoldDTO actualCpuCommodity = entity.get(0).getEntity().getCommoditySoldList(0);
        assertEquals(topologyCPUSold.getCapacity(), actualCpuCommodity.getCapacity(), epsilon);
        assertEquals(topologyCPUSold.getUsed(), actualCpuCommodity.getUsed(), epsilon);
        // check that the VM expected used matches the actual converted used
        assertEquals(1, entity.get(1).getEntity().getCommoditiesBoughtFromProvidersList().size());
        final List<CommodityBoughtDTO> actualBoughtCommodityList = entity.get(1).getEntity()
                .getCommoditiesBoughtFromProvidersList().iterator().next().getCommodityBoughtList();
        assertEquals(1, actualBoughtCommodityList.size());
        assertEquals(RAW_VM_USED, actualBoughtCommodityList.iterator().next().getUsed(), epsilon);

    }
}

