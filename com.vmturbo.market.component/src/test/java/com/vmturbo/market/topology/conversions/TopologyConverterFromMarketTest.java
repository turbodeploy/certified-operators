package com.vmturbo.market.topology.conversions;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_ENTITY_VALUE;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.longThat;
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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.protobuf.util.JsonFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
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
import com.vmturbo.commons.Units;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.market.runner.ReservedCapacityAnalysis;
import com.vmturbo.market.runner.WastedFilesAnalysis;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.market.topology.conversions.CommodityIndex.CommodityIndexFactory;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
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
    private static final long DA_OID = 30000L;
    private static final long SC_OID = 40000L;
    private static final long VOLUME_ID = 10L;
    private static final Float SCALING_FACTOR = 1.5F;
    private static final Float RAW_PM_USED = 0.5F;
    private static final Float RAW_VM_USED = 0.2177F;
    private static final Float RAW_PM_CAPACITY = 2.0F;
    private static final Float MARKET_PM_USED = RAW_PM_USED * SCALING_FACTOR;
    private static final Float MARKET_VM_USED = RAW_VM_USED * SCALING_FACTOR;
    private static final Float MARKET_PM_CAPACITY = RAW_PM_CAPACITY * SCALING_FACTOR;
    private static final long CLOUD_VM_OID = 1;
    private static final long CLOUD_NEW_COMPUTE_TIER_OID = 111;
    private static final long CLOUD_COMPUTE_TIER_OID = 222;
    private static final double OLD_TIER_CAPACITY = 50;
    private static final double DELTA = 0.001d;
    private static final float THRUGHPUT_USED = 30;
    private static final double VMEM_USAGE = 10;

    private final MarketPriceTable marketPriceTable = mock(MarketPriceTable.class);
    private final CommodityConverter mockCommodityConverter = mock(CommodityConverter.class);
    private final CloudCostData mockCCD = mock(CloudCostData.class);
    private TopologyConverter converter;

    private final ReservedCapacityAnalysis reservedCapacityAnalysis =
                    new ReservedCapacityAnalysis(Collections.emptyMap());

    CommodityType ioType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE).build();

    CommodityType vMemType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VMEM_VALUE).build();

    TopologyEntityDTO region1 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.REGION_VALUE).setOid(1).build();

    private static final TopologyInfo REALTIME_TOPOLOGY_INFO =
            TopologyInfo.newBuilder().setTopologyType(TopologyType.REALTIME).build();

    private CommodityType topologyCommodity1;
    private CommodityType topologyCommodity2;

    private static final NumericIDAllocator ID_ALLOCATOR = new NumericIDAllocator();
    private TierExcluderFactory tierExcluderFactory = mock(TierExcluderFactory.class);

    private static final int BICLIQUE_TYPE_ID = ID_ALLOCATOR.allocate("BICLIQUE");
    private static final int CPU_TYPE_ID = ID_ALLOCATOR.allocate("CPU");
    private static final int ST_AMT_TYPE_ID = ID_ALLOCATOR.allocate("StorageAmount");
    private static final int DSPM_TYPE_ID = ID_ALLOCATOR.allocate("DSPM");

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        // The commodity types in topologyCommodity
        // map to the base type in economy commodity.
        topologyCommodity1 = CommodityType.newBuilder().setType(CPU_TYPE_ID).setKey("blah").build();
        topologyCommodity2 =
                CommodityType.newBuilder().setType(DSPM_TYPE_ID).setKey("blahblah").build();
        when(tierExcluderFactory.newExcluder(any(), any(), any())).thenReturn(mock(TierExcluder.class));
        constructTopologyConverter();
    }

    /**
     * Construct default TopologyConverter.
     */
    private void constructTopologyConverter() {
        converter = Mockito.spy(new TopologyConverter(
            REALTIME_TOPOLOGY_INFO,
            false,
            MarketAnalysisUtils.QUOTE_FACTOR,
            MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
            marketPriceTable,
            mockCommodityConverter,
            mockCCD,
            CommodityIndex.newFactory(),
            tierExcluderFactory));
    }

    /**
     * Load a json file into a DTO.
     * @param fileName the name of the file to load
     * @return The entity DTO represented by the file
     * @throws IOException when the file is not found
     */
    static TopologyDTO.TopologyEntityDTO messageFromJsonFile(String fileName)
            throws IOException {
        URL fileUrl = TopologyConverterFromMarketTest.class.getClassLoader().getResources(fileName)
                .nextElement();
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
        final TopologyDTO.CommoditySoldDTO topologyDSPMSold = TopologyDTO.CommoditySoldDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)
                        .build())
                .build();
        final TopologyDTO.CommoditySoldDTO topologyCPUSold = TopologyDTO.CommoditySoldDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CPU_VALUE).build())
                .build();
        final List<CommodityBoughtDTO> topologyDSPMBought = Lists.newArrayList(CommodityBoughtDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE))
                .build());

        final List<CommodityBoughtDTO> topologyCPUBought =
                Lists.newArrayList(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder().setType(
                                CommodityDTO.CommodityType.CPU_VALUE))
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

        final CommodityDTOs.CommoditySoldTO economyCPUSold = CommodityDTOs.CommoditySoldTO
                .newBuilder()
                .setSpecification(CommoditySpecificationTO.newBuilder()
                        .setBaseType(DSPM_TYPE_ID).setType(CPU_TYPE_ID).build())
                .build();
        final CommodityDTOs.CommoditySoldTO economyDSPMSold =
                CommodityDTOs.CommoditySoldTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                                .setBaseType(BICLIQUE_TYPE_ID)
                                .setType(DSPM_TYPE_ID).build())
                        .build();
        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType()))
                .when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(economyCPUSold.getSpecification()));
        Mockito.doReturn(Optional.empty()).when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(economyDSPMSold.getSpecification()));
        // create a topology entity DTO with DSPM sold
        TopologyDTO.TopologyEntityDTO expectedEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).setOid(PM_OID)
                .addCommoditySoldList(topologyDSPMSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(PM_OID)
                        .addAllCommodityBought(topologyCPUBought)
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE))
                .putEntityPropertyMap("dummy", "dummy").build();
        // create a topology entity DTO with DSPM bought
        TopologyDTO.TopologyEntityDTO expectedEntity2 = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(DS_OID).setEntityType(BUSINESS_ENTITY_VALUE)
                .addCommoditySoldList(topologyCPUSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().addAllCommodityBought(topologyDSPMBought))
                .putEntityPropertyMap("dummy", "dummy").build();
        // create trader DTO corresponds to originalEntity
        EconomyDTOs.TraderTO trader = EconomyDTOs.TraderTO.newBuilder().setOid(PM_OID)
                .addCommoditiesSold(economyDSPMSold)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(VM_OID)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO
                                        .newBuilder()
                                        .setBaseType(DSPM_TYPE_ID)
                                        .setType(CPU_TYPE_ID)
                                        .build())
                                .build())
                        .build())
                .build();
        when(mockCCD.getRiCoverageForEntity(anyLong())).thenReturn(Optional.empty());

        Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                Collections.singletonList(trader),
                ImmutableMap.of(expectedEntity.getOid(), expectedEntity,
                        expectedEntity2.getOid(), expectedEntity2),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis,
                setUpWastedFileAnalysis());
        assertEquals(1L, entity.size());
        assertThat(entity.get(PM_OID).getEntity().getCommoditySoldListList(),
                contains(expectedEntity.getCommoditySoldListList().toArray()));

        assertThat(entity.get(PM_OID).getEntity().getCommoditiesBoughtFromProvidersCount(), is(1));
        assertThat(entity.get(PM_OID).getEntity().getCommoditiesBoughtFromProviders(0)
                .getProviderEntityType(), is(EntityType.PHYSICAL_MACHINE_VALUE));
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
        final TopologyDTO.CommoditySoldDTO topologyDSPMSold = TopologyDTO.CommoditySoldDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)
                        .build())
                .build();
        final TopologyDTO.CommoditySoldDTO topologyCPUSold = TopologyDTO.CommoditySoldDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CPU_VALUE).build())
                .build();
        final List<CommodityBoughtDTO> topologyDSPMBought = Lists.newArrayList(CommodityBoughtDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE))
                .build());

        CommodityType cpuCommType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.CPU_VALUE).build();

        final List<CommodityBoughtDTO> topologyCPUBought = Lists.newArrayList(
                CommodityBoughtDTO.newBuilder().setCommodityType(cpuCommType).build());

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
                .setBaseType(BICLIQUE_TYPE_ID).setType(CPU_TYPE_ID).build();

        final CommodityDTOs.CommoditySoldTO economyDSPMSold =
                CommodityDTOs.CommoditySoldTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                                .setBaseType(BICLIQUE_TYPE_ID)
                                .setType(DSPM_TYPE_ID).build())
                        .build();
        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType()))
                .when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(CommoditySpecificationTO.newBuilder()
                        .setBaseType(DSPM_TYPE_ID).setType(CPU_TYPE_ID).build()));
        Mockito.doReturn(Optional.empty()).when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(CommoditySpecificationTO.newBuilder()
                        .setBaseType(BICLIQUE_TYPE_ID).setType(DSPM_TYPE_ID)
                        .build()));
        Mockito.doReturn(cpuCommSpecTO).when(mockCommodityConverter)
                .commoditySpecification(cpuCommType);
        // create a topology entity DTO with DSPM sold
        TopologyDTO.TopologyEntityDTO expectedEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).setOid(PM_OID)
                .setEntityState(EntityState.SUSPENDED)
                .addCommoditySoldList(topologyDSPMSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(PM_OID)
                        .addAllCommodityBought(topologyCPUBought)
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE))
                .putEntityPropertyMap("dummy", "dummy").build();

        final long entityId = PM_OID;
        final TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder().setEntityType(1)
                .setOid(entityId)
                .setAnalysisSettings(
                        AnalysisSettings.newBuilder().setControllable(true).build())
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(topologyCommodity1))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(topologyCommodity2))
                .build();
        when(mockCCD.getRiCoverageForEntity(10000L)).thenReturn(Optional.empty());

        Field entityOidToDto = TopologyConverter.class.getDeclaredField("entityOidToDto");
        Map<Long, TopologyEntityDTO> map = new HashMap<>();
        map.put(entityId, entityDTO);
        entityOidToDto.setAccessible(true);
        entityOidToDto.set(converter, map);
        converter.convertToMarket(ImmutableMap.of(entityDTO.getOid(), entityDTO));

        // create trader DTO corresponds to originalEntity
        EconomyDTOs.TraderTO trader = EconomyDTOs.TraderTO.newBuilder().setOid(10000L)
                .addCommoditiesSold(economyDSPMSold).setState(TraderStateTO.ACTIVE)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(10001L)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO
                                        .newBuilder()
                                        .setBaseType(DSPM_TYPE_ID)
                                        .setType(CPU_TYPE_ID)
                                        .build())
                                .build())
                        .build())
                .build();

        Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                Collections.singletonList(trader),
                ImmutableMap.of(expectedEntity.getOid(), expectedEntity),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis,
                setUpWastedFileAnalysis());
        assertEquals(1L, entity.size());
        // Ensure the entity is having SUSPENDED state.
        assertThat(entity.get(trader.getOid()).getEntity().getEntityState(),
                is(EntityState.SUSPENDED));
    }

    /**
     * Test conversion from {@link TraderTO} to {@link TopologyDTO} for multiple entities
     * where some of them are original entities and some are clones.
     * A clone of datastore is created as copy of original and a clone of DiskArray
     * is created as a larger sized copy of original DiskArray.
     * <p>
     * All the traders are sent to convert to topology entities and we assert to check
     * if conversion was successful. Assert conversion from ActionTO to ActionDTO as well.
     * </p>
     * @throws Exception not supposed to happen in this test
     */
    @Test
    public void testProvByDemandTraderToEntityConversion() throws Exception {
        when(mockCCD.getExistingRiBought()).thenReturn(Collections.emptyList());
        // Commodities to use for the topology of test
        double stAmtCapacity = 1024000d;
        double stAmtBought = 102400d;
        float stAmoutScaledCapacity = 3072000f;
        double originalStorageAmtUsed = 4096d;
        final TopologyDTO.CommoditySoldDTO topologyStAmtSold = TopologyDTO.CommoditySoldDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                        .build())
                .setCapacity(stAmtCapacity)
                .setUsed(originalStorageAmtUsed)
                .build();
        final List<CommodityBoughtDTO> topologyStAmtBought = Lists.newArrayList(CommodityBoughtDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE))
                .setUsed(stAmtBought).build());

        // 1. Create a topoDTO in origTopoDTOs for a DiskArray
        TopologyDTO.TopologyEntityDTO daTopo = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.DISK_ARRAY_VALUE).setOid(DA_OID)
                .setEntityState(EntityState.POWERED_ON)
                .addCommoditySoldList(topologyStAmtSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(SC_OID)
                        .addAllCommodityBought(topologyStAmtBought)
                        .setProviderEntityType(EntityType.STORAGE_CONTROLLER_VALUE))
                .putEntityPropertyMap("dummy", "dummy").build();
        // 2. Create a topoDTO in origTopoDTOs for a Datastore (storage)
        TopologyDTO.TopologyEntityDTO dsTopo = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.STORAGE_VALUE).setOid(DS_OID)
                .setEntityState(EntityState.POWERED_ON)
                .addCommoditySoldList(topologyStAmtSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(DA_OID)
                        .addAllCommodityBought(topologyStAmtBought)
                        .setProviderEntityType(EntityType.DISK_ARRAY_VALUE))
                .putEntityPropertyMap("dummy", "dummy").build();

        // Fill in the original topo dto map
        Map<Long, TopologyEntityDTO> origTopoMap =
                ImmutableMap.of(dsTopo.getOid(), dsTopo, daTopo.getOid(), daTopo);

        // 3. Create Trader that is copy of Storage TopoDTO from step 2
        CommoditySpecificationTO stAmtCommSpecTO = CommoditySpecificationTO.newBuilder()
                .setBaseType(ST_AMT_TYPE_ID).setType(ST_AMT_TYPE_ID).build();
        final CommodityDTOs.CommoditySoldTO economyStAmtSold = CommodityDTOs.CommoditySoldTO
                .newBuilder()
                .setSpecification(stAmtCommSpecTO)
                .setQuantity((float)originalStorageAmtUsed)
                .build();

        final CommodityDTOs.CommoditySoldTO economyStAmtBiggerSold =
                CommodityDTOs.CommoditySoldTO.newBuilder().setSpecification(stAmtCommSpecTO)
                        .setCapacity(stAmoutScaledCapacity)
                        .build();

        final CommodityDTOs.CommodityBoughtTO economyStAmtBought =
                CommodityBoughtTO.newBuilder().setSpecification(stAmtCommSpecTO).build();

        EconomyDTOs.TraderTO traderDS = EconomyDTOs.TraderTO.newBuilder().setOid(DS_OID)
                .addCommoditiesSold(economyStAmtSold).setState(TraderStateTO.ACTIVE)
                .setType(EntityType.STORAGE_VALUE)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(DS_OID + 1L)
                        .setSupplier(DA_OID)
                        .addCommoditiesBought(economyStAmtBought).build())
                .build();
        EconomyDTOs.TraderTO traderDA = EconomyDTOs.TraderTO.newBuilder().setOid(DA_OID)
                .addCommoditiesSold(economyStAmtSold).setState(TraderStateTO.ACTIVE)
                .setType(EntityType.DISK_ARRAY_VALUE)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(DA_OID + 1L)
                        .setSupplier(SC_OID)
                        .addCommoditiesBought(economyStAmtBought).build())
                .build();
        // 4. Create a trader DS that is a clone of original trader (provisionBySupply)
        EconomyDTOs.TraderTO traderDSClone = EconomyDTOs.TraderTO.newBuilder().setOid(DS_OID + 100L)
                .addCommoditiesSold(economyStAmtSold).setState(TraderStateTO.ACTIVE)
                .setType(EntityType.STORAGE_VALUE).setCloneOf(DS_OID)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(-DS_OID)
                        .setSupplier(DA_OID + 100L)
                        .addCommoditiesBought(economyStAmtBought).build())
                .build();
        // 5. Create Trader that is bigger copy of original DiskArray from step 1
        EconomyDTOs.TraderTO traderDAClone = EconomyDTOs.TraderTO.newBuilder().setOid(DA_OID + 100L)
                .addCommoditiesSold(economyStAmtBiggerSold).setState(TraderStateTO.ACTIVE)
                .setType(EntityType.DISK_ARRAY_VALUE).setCloneOf(DA_OID)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(-DA_OID)
                        .setSupplier(SC_OID)
                        .addCommoditiesBought(economyStAmtBought).build())
                .build();
        // mock the commodity converter value
        Mockito.doReturn(Optional.of(topologyStAmtSold.getCommodityType()))
                .when(mockCommodityConverter).economyToTopologyCommodity(
                Mockito.eq(economyStAmtSold.getSpecification()));

        // Set wasted file for DS
        WastedFilesAnalysis wastedFilesAnalysisMock = mock(WastedFilesAnalysis.class);
        // Set up mock for any call other than DS
        when(wastedFilesAnalysisMock.getStorageAmountReleasedForOid(anyLong()))
                .thenReturn(Optional.empty());
        // Set up mock for DS
        Optional<Long> wastedFileSizeInKB = Optional.of(2048L);
        when(wastedFilesAnalysisMock.getStorageAmountReleasedForOid(DS_OID)).thenReturn(wastedFileSizeInKB);

        // 5. Call TC.convertFromMarket
        List<TraderTO> traderTOs =
                Lists.newArrayList(traderDS, traderDA, traderDSClone, traderDAClone);
        Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedTopo = converter.convertFromMarket(
                traderTOs, origTopoMap, PriceIndexMessage.getDefaultInstance(), mockCCD,
                reservedCapacityAnalysis, wastedFilesAnalysisMock);
        // 6. Assert shoppingListInfo map
        assertNotNull(converter.getShoppingListOidToInfos().get(-DA_OID));
        assertNotNull(projectedTopo.get(DA_OID + 100L));

        // Call interpret
        // Create ActionTO for provision by demand
        ActionTO provByDemandTO = ActionTO.newBuilder().setImportance(0).setIsNotExecutable(false)
                .setProvisionByDemand(ProvisionByDemandTO.newBuilder()
                        .setModelBuyer(-DS_OID).setModelSeller(DA_OID)
                        .setProvisionedSeller(DA_OID + 100L).build())
                .build();
        Optional<Action> provByDemandOptional =
                converter.interpretAction(provByDemandTO, projectedTopo, null, null, null);
        assertNotNull(provByDemandOptional.get());

        //assert for wasted file actions
        Optional<CommoditySoldDTO> daStorageAmtCommSold = projectedTopo.get(traderDS.getOid()).getEntity().getCommoditySoldListList().stream()
                .filter(commSold -> commSold.getCommodityType().getType()
                        == CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                .findFirst();

        // Check original entity has reduced st amt value
        assertTrue(daStorageAmtCommSold.isPresent());
        assertEquals(originalStorageAmtUsed - wastedFileSizeInKB.get() / Units.NUM_OF_KB_IN_MB,
                daStorageAmtCommSold.get().getUsed(), 1e12);

        // Check provisioned entity is unchanged
        Optional<CommoditySoldDTO> daStorageAmtCommSoldForClone =
                projectedTopo.get(traderDSClone.getOid()).getEntity().getCommoditySoldListList().stream()
                        .filter(commSold -> commSold.getCommodityType().getType()
                                == CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                        .findFirst();


        // Check clone is unchanged
        assertTrue(daStorageAmtCommSoldForClone.isPresent());
        assertEquals(originalStorageAmtUsed, daStorageAmtCommSoldForClone.get().getUsed(), 1e12);
    }

    /**
     * Test that when createResources is called for a cloud VM, its projected volumes are created.
     *
     * @throws Exception not supposed to happen in this test
     */
    @Test
    public void testCreateResources() throws Exception {
        long azOid = 1l;
        long volumeOid = 2l;
        long storageTierOid = 3l;
        long vmOid = 4l;
        TopologyDTO.TopologyEntityDTO az = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE).setOid(azOid).build();
        TopologyDTO.TopologyEntityDTO storageTier = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.STORAGE_TIER_VALUE).setOid(storageTierOid)
                .build();
        TopologyDTO.TopologyEntityDTO volume = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE).setOid(volumeOid)
                .setDisplayName("volume1").setEnvironmentType(EnvironmentType.CLOUD)
                .build();
        TopologyDTO.TopologyEntityDTO vm = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).setOid(vmOid)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(storageTierOid)
                        .setVolumeId(volumeOid)
                        .setProviderEntityType(EntityType.STORAGE_TIER_VALUE))
                .addConnectedEntityList(
                        ConnectedEntity.newBuilder().setConnectedEntityId(azOid)
                                .setConnectedEntityType(az.getEntityType()))
                .build();
        // Use reflection and these entities to the entityOidToDto map
        Field entityOidToDto = TopologyConverter.class.getDeclaredField("entityOidToDto");
        Map<Long, TopologyEntityDTO> map =
                ImmutableMap.of(azOid, az, storageTierOid, storageTier, volumeOid, volume);
        entityOidToDto.setAccessible(true);
        entityOidToDto.set(converter, map);

        // Act
        Set<TopologyEntityDTO> resources = converter.createResources(vm);

        // Assert that the projected volume is connected to the storage and AZ which the VM is
        // connected to
        assertEquals(1, resources.size());
        TopologyEntityDTO projectedVolume = resources.iterator().next();
        assertEquals(2, projectedVolume.getConnectedEntityListList().size());
        ConnectedEntity connectedStorageTier = projectedVolume.getConnectedEntityListList().stream()
                .filter(c -> c.getConnectedEntityType() == EntityType.STORAGE_TIER_VALUE)
                .findFirst().get();
        ConnectedEntity connectedAz = projectedVolume.getConnectedEntityListList().stream().filter(
                c -> c.getConnectedEntityType() == EntityType.AVAILABILITY_ZONE_VALUE)
                .findFirst().get();
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
        final TopologyDTO.CommoditySoldDTO topologyCPUSold = TopologyDTO.CommoditySoldDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CPU_VALUE).build())
                .setUsed(RAW_PM_USED).setCapacity(RAW_PM_CAPACITY)
                .setScalingFactor(SCALING_FACTOR).build();
        final List<CommodityBoughtDTO> topologyDSPMBought = Lists.newArrayList(CommodityBoughtDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE))
                .build());

        // create a PM topology entity DTO that seels CPU (with scalingFactor set)
        TopologyDTO.TopologyEntityDTO expectedEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE).setOid(PM_OID)
                .addCommoditySoldList(topologyCPUSold)
                .putEntityPropertyMap("dummy", "dummy").build();
        // add a VM to buy CPU from the PM
        // create a topology entity DTO with CPU bought
        final List<CommodityBoughtDTO> topologyCpuBought = Lists.newArrayList(CommodityBoughtDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CPU_VALUE))
                .setUsed(RAW_VM_USED).setScalingFactor(SCALING_FACTOR).build());
        TopologyDTO.TopologyEntityDTO expectedEntity2 = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(VM_OID).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditySoldList(topologyCPUSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(PM_OID).setVolumeId(VOLUME_ID)
                        .addAllCommodityBought(topologyCpuBought))
                .putEntityPropertyMap("dummy", "dummy").build();

        // Add the entities to the commodity index. Normally this would get done prior to the
        // conversion from market.
        commodityIndex.addEntity(expectedEntity);
        commodityIndex.addEntity(expectedEntity2);

        final CommodityIndexFactory indexFactory = mock(CommodityIndexFactory.class);
        when(indexFactory.newIndex()).thenReturn(commodityIndex);

        // converter under test
        TopologyConverter converter = Mockito.spy(new TopologyConverter(REALTIME_TOPOLOGY_INFO,
                false, MarketAnalysisUtils.QUOTE_FACTOR, MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable, mockCommodityConverter, indexFactory, tierExcluderFactory));

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

        final CommodityDTOs.CommoditySoldTO economyCPUSold = CommodityDTOs.CommoditySoldTO
                .newBuilder()
                .setSpecification(CommoditySpecificationTO.newBuilder()
                        .setBaseType(DSPM_TYPE_ID).setType(CPU_TYPE_ID).build())
                .setQuantity(MARKET_PM_USED).setCapacity(MARKET_PM_CAPACITY).build();

        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType()))
                .when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(economyCPUSold.getSpecification()));

        // create trader TO (i.e. as returned from Market) that corresponds to expected PM Entity
        EconomyDTOs.TraderTO pmTrader = EconomyDTOs.TraderTO.newBuilder().setOid(PM_OID)
                .addCommoditiesSold(economyCPUSold)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(VM_OID)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO
                                        .newBuilder()
                                        .setBaseType(DSPM_TYPE_ID)
                                        .setType(CPU_TYPE_ID)
                                        .build())
                                .setQuantity(MARKET_VM_USED).build())
                        .build())
                .build();

        // create trader TO (i.e. as returned from Market) that corresponds to expectedVM Entity
        EconomyDTOs.TraderTO vmTrader = EconomyDTOs.TraderTO.newBuilder().setOid(VM_OID)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(VM_OID)
                        .setSupplier(PM_OID)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO
                                        .newBuilder()
                                        .setBaseType(DSPM_TYPE_ID)
                                        .setType(CPU_TYPE_ID)
                                        .build())
                                .setQuantity(MARKET_VM_USED).build())
                        .build())
                .build();

        // Act
        Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                // the traders in the market
                Lists.newArrayList(pmTrader, vmTrader),
                // map back to original TopologyEntityDTOs
                ImmutableMap.of(expectedEntity.getOid(), expectedEntity,
                        expectedEntity2.getOid(), expectedEntity2),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis,
                setUpWastedFileAnalysis());

        // Assert two entities returned - they will be in the original order - VM and PM
        assertEquals(2L, entity.size());
        // check that the PM expected capacity and used matches the actual converted capacity and used
        final CommoditySoldDTO actualCpuCommodity =
                entity.get(pmTrader.getOid()).getEntity().getCommoditySoldList(0);
        assertEquals(topologyCPUSold.getCapacity(), actualCpuCommodity.getCapacity(), epsilon);
        assertEquals(topologyCPUSold.getUsed(), actualCpuCommodity.getUsed(), epsilon);
        // check that the VM expected used matches the actual converted used
        assertEquals(1, entity.get(vmTrader.getOid()).getEntity()
                .getCommoditiesBoughtFromProvidersList().size());
        final List<CommodityBoughtDTO> actualBoughtCommodityList = entity.get(vmTrader.getOid())
                .getEntity().getCommoditiesBoughtFromProvidersList().iterator().next()
                .getCommodityBoughtList();
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
        final TopologyDTO.CommoditySoldDTO topologyCPUSold = TopologyDTO.CommoditySoldDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CPU_VALUE).build())
                .setUsed(RAW_PM_USED).setCapacity(RAW_PM_CAPACITY)
                .setScalingFactor(Double.MIN_VALUE) // set edge scaling factor
                .build();
        // create a PM topology entity DTO that seels CPU (with scalingFactor set)
        TopologyDTO.TopologyEntityDTO expectedEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE).setOid(PM_OID)
                .addCommoditySoldList(topologyCPUSold)
                .putEntityPropertyMap("dummy", "dummy").build();

        // add a VM to buy CPU from the PM
        // create a topology entity DTO with CPU bought
        final List<CommodityBoughtDTO> topologyCpuBought = Lists.newArrayList(CommodityBoughtDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CPU_VALUE))
                .setUsed(RAW_VM_USED).setScalingFactor(SCALING_FACTOR).build());
        TopologyDTO.TopologyEntityDTO expectedEntity2 = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(VM_OID).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditySoldList(topologyCPUSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(PM_OID).setVolumeId(VOLUME_ID)
                        .addAllCommodityBought(topologyCpuBought))
                .putEntityPropertyMap("dummy", "dummy").build();

        // Add the entities to the commodity index. Normally this would get done prior to the
        // conversion from market.
        commodityIndex.addEntity(expectedEntity);
        commodityIndex.addEntity(expectedEntity2);

        final CommodityIndexFactory indexFactory = mock(CommodityIndexFactory.class);
        when(indexFactory.newIndex()).thenReturn(commodityIndex);

        final List<CommodityBoughtDTO> topologyDSPMBought = Lists.newArrayList(CommodityBoughtDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE))
                .build());

        TopologyConverter converter = Mockito.spy(new TopologyConverter(
                        TopologyInfo.newBuilder().setTopologyType(TopologyType.PLAN).build(), false,
                        MarketAnalysisUtils.QUOTE_FACTOR, MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                        marketPriceTable, mockCommodityConverter, mockCCD, indexFactory,
                        tierExcluderFactory));

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

        final CommodityDTOs.CommoditySoldTO economyCPUSold = CommodityDTOs.CommoditySoldTO
                .newBuilder()
                .setSpecification(CommoditySpecificationTO.newBuilder()
                        .setBaseType(DSPM_TYPE_ID).setType(CPU_TYPE_ID).build())
                .setQuantity(MARKET_PM_USED).setCapacity(MARKET_PM_CAPACITY).build();

        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType()))
                .when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(economyCPUSold.getSpecification()));

        // create trader TO (i.e. as returned from Market) that corresponds to expected PM Entity
        EconomyDTOs.TraderTO pmTrader = EconomyDTOs.TraderTO.newBuilder().setOid(PM_OID)
                .addCommoditiesSold(economyCPUSold)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(VM_OID)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO
                                        .newBuilder()
                                        .setBaseType(DSPM_TYPE_ID)
                                        .setType(CPU_TYPE_ID)
                                        .build())
                                .setQuantity(MARKET_VM_USED).build())
                        .build())
                .build();

        // create trader TO (i.e. as returned from Market) that corresponds to expectedVM Entity
        EconomyDTOs.TraderTO vmTrader = EconomyDTOs.TraderTO.newBuilder().setOid(VM_OID)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(VM_OID)
                        .setSupplier(PM_OID)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO
                                        .newBuilder()
                                        .setBaseType(DSPM_TYPE_ID)
                                        .setType(CPU_TYPE_ID)
                                        .build())
                                .setQuantity(MARKET_VM_USED).build())
                        .build())
                .build();

        // Act
        Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                // the traders in the market
                Lists.newArrayList(pmTrader, vmTrader),
                // map back to original TopologyEntityDTOs
                ImmutableMap.of(expectedEntity.getOid(), expectedEntity,
                        expectedEntity2.getOid(), expectedEntity2),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis,
                setUpWastedFileAnalysis());

        // Assert two entities returned - they will be in the original order - VM and PM
        assertEquals(2L, entity.size());
        // check that the PM expected capacity and used matches the actual converted capacity and used
        final CommoditySoldDTO actualCpuCommodity =
                entity.get(pmTrader.getOid()).getEntity().getCommoditySoldList(0);
        // Since the scaling factor is infinitely small we don't use it.
        assertEquals(MARKET_PM_CAPACITY, actualCpuCommodity.getCapacity(),
                TopologyConverter.EPSILON);
        assertEquals(MARKET_PM_USED, actualCpuCommodity.getUsed(), TopologyConverter.EPSILON);
        // check that the VM expected used matches the actual converted used
        assertEquals(1, entity.get(vmTrader.getOid()).getEntity()
                .getCommoditiesBoughtFromProvidersList().size());
        final List<CommodityBoughtDTO> actualBoughtCommodityList = entity.get(vmTrader.getOid())
                .getEntity().getCommoditiesBoughtFromProvidersList().iterator().next()
                .getCommodityBoughtList();
        assertEquals(1, actualBoughtCommodityList.size());
        assertEquals(RAW_VM_USED, actualBoughtCommodityList.iterator().next().getUsed(),
                TopologyConverter.EPSILON);

    }

    @Test
    public void testConvertFromMarketPreservesOriginalEnvType() {
        final TopologyDTO.TopologyEntityDTO originalVm = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(VM_OID).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD).build();

        final EconomyDTOs.TraderTO vmTrader =
                EconomyDTOs.TraderTO.newBuilder().setOid(VM_OID).build();

        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                Collections.singletonList(vmTrader), ImmutableMap.of(VM_OID, originalVm),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis,
                setUpWastedFileAnalysis());

        assertThat(entity.get(VM_OID).getEntity().getEnvironmentType(),
                is(originalVm.getEnvironmentType()));
    }

    @Test
    public void testConvertFromMarketPreservesOriginalTypeSpecificInfo() {
        final TopologyDTO.TopologyEntityDTO originalVm = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(VM_OID).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setVirtualMachine(
                        VirtualMachineInfo.newBuilder().setNumCpus(100)))
                .build();

        final EconomyDTOs.TraderTO vmTrader =
                EconomyDTOs.TraderTO.newBuilder().setOid(VM_OID).build();

        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                Collections.singletonList(vmTrader), ImmutableMap.of(VM_OID, originalVm),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis,
                setUpWastedFileAnalysis());

        assertThat(entity.get(VM_OID).getEntity().getTypeSpecificInfo(),
                is(originalVm.getTypeSpecificInfo()));
    }

    @Test
    public void testConvertFromMarketPreservesOriginalOrigin() {
        final TopologyDTO.TopologyEntityDTO originalVm = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(VM_OID).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(
                    DiscoveryOrigin.newBuilder().putDiscoveredTargetData(123,
                        PerTargetEntityInformation.getDefaultInstance())))
                .build();

        final EconomyDTOs.TraderTO vmTrader =
                EconomyDTOs.TraderTO.newBuilder().setOid(VM_OID).build();

        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                Collections.singletonList(vmTrader), ImmutableMap.of(VM_OID, originalVm),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis,
                setUpWastedFileAnalysis());

        assertThat(entity.get(VM_OID).getEntity().getOrigin(), is(originalVm.getOrigin()));
    }

    @Test
    public void testConvertFromMarketCloneRetainsOriginalEnvType() {
        final TopologyDTO.TopologyEntityDTO originalVm = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(VM_OID).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD).build();

        final long cloneId = 127329;
        final EconomyDTOs.TraderTO cloneTrader = EconomyDTOs.TraderTO.newBuilder().setOid(cloneId)
                .setCloneOf(VM_OID).build();

        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                Collections.singletonList(cloneTrader), ImmutableMap.of(VM_OID, originalVm),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis,
                setUpWastedFileAnalysis());

        assertThat(entity.get(cloneId).getEntity().getEnvironmentType(),
                is(originalVm.getEnvironmentType()));
    }

    @Test
    public void testConvertFromMarketCloneRetainsOriginalTypeSpecificInfo() {
        final TopologyDTO.TopologyEntityDTO originalVm = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(VM_OID).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setVirtualMachine(
                        VirtualMachineInfo.newBuilder().setNumCpus(100)))
                .build();

        final long cloneId = 127329;
        final EconomyDTOs.TraderTO cloneTrader = EconomyDTOs.TraderTO.newBuilder().setOid(cloneId)
                .setCloneOf(VM_OID).build();

        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                Collections.singletonList(cloneTrader), ImmutableMap.of(VM_OID, originalVm),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis,
                setUpWastedFileAnalysis());

        assertThat(entity.get(cloneId).getEntity().getTypeSpecificInfo(),
                is(originalVm.getTypeSpecificInfo()));
    }

    @Test
    public void testConvertFromMarketCloneHasAnalysisOrigin() {
        final TopologyDTO.TopologyEntityDTO originalVm = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(VM_OID).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOrigin(Origin.newBuilder().setDiscoveryOrigin(
                    DiscoveryOrigin.newBuilder().putDiscoveredTargetData(123,
                        PerTargetEntityInformation.getDefaultInstance())))
                .build();

        final long cloneId = 127329;
        final EconomyDTOs.TraderTO cloneTrader = EconomyDTOs.TraderTO.newBuilder().setOid(cloneId)
                .setCloneOf(VM_OID).build();

        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                Collections.singletonList(cloneTrader), ImmutableMap.of(VM_OID, originalVm),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis,
                setUpWastedFileAnalysis());

        // The origin is NOT the same - it's a market origin since this is a clone.
        assertThat(entity.get(cloneId).getEntity().getOrigin().getAnalysisOrigin()
                .getOriginalEntityId(), is(originalVm.getOid()));
    }

    /**
     * Test that the shopping list with negative oid in a trader with positive oid
     * is added in shoppingListOidToInfos map, when it is not there,
     * when calling convertFromMarket().
     */
    @Test
    public void testInsertShoppingListInMapPositiveTrader() {
        final TopologyDTO.CommoditySoldDTO topologyDSPMSold = TopologyDTO.CommoditySoldDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)
                        .build())
                .build();
        final TopologyDTO.CommoditySoldDTO topologyCPUSold = TopologyDTO.CommoditySoldDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CPU_VALUE).build())
                .build();

        CommodityType cpuCommType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.CPU_VALUE).build();

        final List<CommodityBoughtDTO> topologyCPUBought = Lists.newArrayList(
                CommodityBoughtDTO.newBuilder().setCommodityType(cpuCommType).build());

        final CommodityDTOs.CommoditySoldTO economyDSPMSold =
                CommodityDTOs.CommoditySoldTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                                .setBaseType(BICLIQUE_TYPE_ID)
                                .setType(DSPM_TYPE_ID).build())
                        .build();
        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType()))
                .when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(CommoditySpecificationTO.newBuilder()
                        .setBaseType(DSPM_TYPE_ID).setType(CPU_TYPE_ID).build()));
        Mockito.doReturn(Optional.empty()).when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(CommoditySpecificationTO.newBuilder()
                        .setBaseType(BICLIQUE_TYPE_ID).setType(DSPM_TYPE_ID)
                        .build()));
        // create a topology entity DTO with DSPM sold
        TopologyDTO.TopologyEntityDTO expectedEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).setOid(10000L)
                .setEntityState(EntityState.SUSPENDED)
                .addCommoditySoldList(topologyDSPMSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(PM_OID)
                        .addAllCommodityBought(topologyCPUBought)
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE))
                .putEntityPropertyMap("dummy", "dummy").build();

        // create trader DTO corresponds to originalEntity
        EconomyDTOs.TraderTO trader = EconomyDTOs.TraderTO.newBuilder().setOid(10000L)
                .addCommoditiesSold(economyDSPMSold).setState(TraderStateTO.ACTIVE)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(-1L)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO
                                        .newBuilder()
                                        .setBaseType(DSPM_TYPE_ID)
                                        .setType(CPU_TYPE_ID)
                                        .build())
                                .build())
                        .build())
                .build();

        Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                Collections.singletonList(trader),
                ImmutableMap.of(expectedEntity.getOid(), expectedEntity),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis,
                setUpWastedFileAnalysis());

        assertEquals(1l, converter.getShoppingListOidToInfos().size());
        assertEquals(10000L, converter.getShoppingListOidToInfos().get(-1L).getBuyerId());
        assertEquals(1L, entity.size());
        assertEquals(10000L, entity.get(10000L).getEntity().getOid());
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE,
                entity.get(10000L).getEntity().getEntityType());
    }

    /**
     * Test that the shopping list with negative oid in a trader with negative oid
     * is added in shoppingListOidToInfos map, when it is not there,
     * when calling convertFromMarket().
     */
    @Test
    public void testInsertShoppingListInMapNegativeTrader() {
        final TopologyDTO.CommoditySoldDTO topologyDSPMSold = TopologyDTO.CommoditySoldDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)
                        .build())
                .build();
        final TopologyDTO.CommoditySoldDTO topologyCPUSold = TopologyDTO.CommoditySoldDTO
                .newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.CPU_VALUE).build())
                .build();

        CommodityType cpuCommType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.CPU_VALUE).build();

        final List<CommodityBoughtDTO> topologyCPUBought = Lists.newArrayList(
                CommodityBoughtDTO.newBuilder().setCommodityType(cpuCommType).build());

        final CommodityDTOs.CommoditySoldTO economyDSPMSold =
                CommodityDTOs.CommoditySoldTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                                .setBaseType(BICLIQUE_TYPE_ID)
                                .setType(DSPM_TYPE_ID).build())
                        .build();
        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType()))
                .when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(CommoditySpecificationTO.newBuilder()
                        .setBaseType(DSPM_TYPE_ID).setType(CPU_TYPE_ID).build()));
        Mockito.doReturn(Optional.empty()).when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(CommoditySpecificationTO.newBuilder()
                        .setBaseType(BICLIQUE_TYPE_ID).setType(DSPM_TYPE_ID)
                        .build()));
        // create a topology entity DTO with DSPM sold
        TopologyDTO.TopologyEntityDTO expectedEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).setOid(-1000L)
                .setEntityState(EntityState.SUSPENDED)
                .addCommoditySoldList(topologyDSPMSold)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(PM_OID)
                        .addAllCommodityBought(topologyCPUBought)
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE))
                .putEntityPropertyMap("dummy", "dummy").build();

        // create trader DTO corresponds to originalEntity
        EconomyDTOs.TraderTO trader = EconomyDTOs.TraderTO.newBuilder().setOid(-1000L)
                .addCommoditiesSold(economyDSPMSold).setState(TraderStateTO.ACTIVE)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(-1L)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO
                                        .newBuilder()
                                        .setBaseType(DSPM_TYPE_ID)
                                        .setType(CPU_TYPE_ID)
                                        .build())
                                .build())
                        .build())
                .build();

        Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                Collections.singletonList(trader),
                ImmutableMap.of(expectedEntity.getOid(), expectedEntity),
                PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis,
                setUpWastedFileAnalysis());

        assertEquals(1l, converter.getShoppingListOidToInfos().size());
        assertEquals(-1000L, converter.getShoppingListOidToInfos().get(-1L).getBuyerId());
        assertEquals(1L, entity.size());
        assertEquals(-1000L, entity.get(-1000L).getEntity().getOid());
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE,
                entity.get(-1000L).getEntity().getEntityType());
    }

    /**
     * Test TopologyConverter#convertFromMarket.
     * Inactive sold commodities should be added to the projected entity.
     */
    @Test
    public void testConvertFromMarketInactiveCommoditySold() {
        final TopologyDTO.CommoditySoldDTO commSold = TopologyDTO.CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.BALLOONING_VALUE).build())
            .setActive(false).build();

        final TopologyDTO.TopologyEntityDTO originalVm = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(VM_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(commSold)
            .build();

        // create trader DTO corresponds to originalEntity
        EconomyDTOs.TraderTO trader = EconomyDTOs.TraderTO.newBuilder()
            .setOid(VM_OID)
            .setState(TraderStateTO.ACTIVE)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();

        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity =
            converter.convertFromMarket(Collections.singletonList(trader),
                ImmutableMap.of(VM_OID, originalVm),
                PriceIndexMessage.getDefaultInstance(), mockCCD,
                reservedCapacityAnalysis, setUpWastedFileAnalysis());

        assertEquals(1, entity.get(VM_OID).getEntity().getCommoditySoldListCount());
        assertEquals(commSold, entity.get(VM_OID).getEntity().getCommoditySoldList(0));
    }

    private WastedFilesAnalysis setUpWastedFileAnalysis() {
        WastedFilesAnalysis wastedFilesAnalysisMock = mock(WastedFilesAnalysis.class);
        when(wastedFilesAnalysisMock.getStorageAmountReleasedForOid(anyLong())).thenReturn(Optional.empty());
        return wastedFilesAnalysisMock;
    }

    private TraderTO createVMTOs(CommodityBoughtTO boughtTO,
                                     CommoditySoldTO soldTO,
                                     long oid, long supplierId,
                                 long shoppinglistOid ) {
        EconomyDTOs.TraderTO vmTO = EconomyDTOs.TraderTO.newBuilder().setOid(CLOUD_VM_OID)
                .setState(TraderStateTO.ACTIVE)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(shoppinglistOid)
                        .setSupplier(supplierId)
                        .addCommoditiesBought(boughtTO)
                        .build())
                .addCommoditiesSold(soldTO).build();
        return vmTO;
    }

    /**
     * Unit test the TO to projected DTO conversions with respect to
     * the bought commodity usage and the sold commodity capacity.
     */
    @Test
    public void testProjectedCommodityUsageCapacity() {

        CloudTopologyConverter mockCloudTc = Mockito.mock(CloudTopologyConverter.class);
        CommodityIndex mockINdex = Mockito.mock(CommodityIndex.class);
        final CommodityIndexFactory indexFactory = mock(CommodityIndexFactory.class);
        when(indexFactory.newIndex()).thenReturn(mockINdex);

        TopologyInfo topoInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.PLAN).build();

        TopologyConverter converter = Mockito.spy(new TopologyConverter(topoInfo, false,
                MarketAnalysisUtils.QUOTE_FACTOR, MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable, mockCommodityConverter, mockCCD, indexFactory, tierExcluderFactory));
        converter.cloudTc = mockCloudTc;

        TopologyDTO.TopologyEntityDTO oldTierDTO = createEntityDTO(CLOUD_COMPUTE_TIER_OID,
                EntityType.COMPUTE_TIER_VALUE, CommodityDTO.CommodityType.MEM_VALUE);
        TopologyDTO.TopologyEntityDTO newTierDTO = createEntityDTO(CLOUD_NEW_COMPUTE_TIER_OID,
                EntityType.COMPUTE_TIER_VALUE, CommodityDTO.CommodityType.MEM_VALUE);

        CommoditySoldTO newTierSold = createSoldTO(CommodityDTO.CommodityType.MEM_VALUE,
                OLD_TIER_CAPACITY * 2);
        EconomyDTOs.TraderTO newTier =
                EconomyDTOs.TraderTO.newBuilder().setOid(CLOUD_NEW_COMPUTE_TIER_OID)
                        .setState(TraderStateTO.ACTIVE)
                        .setOid(CLOUD_NEW_COMPUTE_TIER_OID)
                        .setType(EntityType.COMPUTE_TIER_VALUE)
                        .addCommoditiesSold(newTierSold).build();

        TopologyDTO.TopologyEntityDTO originalEntityDTO = createOriginalDTOAndMockIndex(mockINdex);
        CommodityBoughtTO boughtTO =
                createBoughtTO(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE, THRUGHPUT_USED);
        Mockito.doReturn(Optional.of(ioType)).when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(boughtTO.getSpecification()));

        CommodityBoughtTO oldBoughtTO =
                createBoughtTO(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE,
                        THRUGHPUT_USED / 2);
        CommoditySoldTO soldTO = createSoldTO(CommodityDTO.CommodityType.MEM_ALLOCATION_VALUE,
                OLD_TIER_CAPACITY);
        Mockito.doReturn(Optional.of(vMemType))
                .when(mockCommodityConverter)
                .economyToTopologyCommodity(Mockito.eq(soldTO.getSpecification()));


        EconomyDTOs.TraderTO vmTO = createVMTOs(boughtTO, soldTO, CLOUD_VM_OID,
                CLOUD_NEW_COMPUTE_TIER_OID, CLOUD_VM_OID + 1L);
        EconomyDTOs.TraderTO oldVMTO = createVMTOs(oldBoughtTO, soldTO, CLOUD_VM_OID,
                CLOUD_COMPUTE_TIER_OID, CLOUD_VM_OID + 3L);

        MarketTier marketTier = new OnDemandMarketTier(newTierDTO);
        Mockito.doReturn(marketTier).when(mockCloudTc).getPrimaryMarketTier(Mockito.eq(vmTO));
        Mockito.doReturn(marketTier).when(mockCloudTc).getMarketTier(Mockito.anyLong());
        ImmutableList marketTierIDs = ImmutableList.of(CLOUD_COMPUTE_TIER_OID,
                CLOUD_NEW_COMPUTE_TIER_OID);
        Mockito.doReturn(true).when(mockCloudTc)
                .isMarketTier(longThat(isIn(marketTierIDs)));

        Mockito.doReturn(true).when(mockCloudTc)
                .isMarketTier(Mockito.eq(CLOUD_NEW_COMPUTE_TIER_OID));
        Mockito.doReturn(Collections.singleton(oldTierDTO)).when(mockCloudTc)
                .getTopologyEntityDTOProvidersOfType(any(), Mockito.anyInt());
        Mockito.doReturn(oldVMTO).when(converter).topologyDTOtoTraderTO(Mockito.eq(originalEntityDTO));

        Map<Long, ProjectedTopologyEntity> projectedTOs =
                converter.convertFromMarket(Collections.singletonList(vmTO),
                        ImmutableMap.of(originalEntityDTO.getOid(), originalEntityDTO,
                                CLOUD_NEW_COMPUTE_TIER_OID, newTierDTO),
                        PriceIndexMessage.getDefaultInstance(), mockCCD, reservedCapacityAnalysis,
                        setUpWastedFileAnalysis());
        verifyTOtoDTOConversions(projectedTOs);

    }

    private CommoditySoldTO createSoldTO(final int typeValue,
                                         final double capacity) {
        CommoditySoldTO soldTO = CommoditySoldTO.newBuilder()
                .setSpecification(CommoditySpecificationTO.newBuilder()
                        .setBaseType(typeValue)
                        .setType(typeValue).build())
                .setCapacity((float)capacity)
                .build();
        return soldTO;

    }

    private CommodityBoughtTO createBoughtTO(final int boughtTypeValue,
                                             final float quantity) {
        CommodityBoughtTO boughtTO = CommodityBoughtTO.newBuilder()
                .setSpecification(CommoditySpecificationTO.newBuilder()
                        .setBaseType(boughtTypeValue)
                        .setType(boughtTypeValue).build())
                .setQuantity(quantity)
                .build();
        return boughtTO;
    }

    private TopologyDTO.TopologyEntityDTO createOriginalDTOAndMockIndex(CommodityIndex mockINdex) {
        CommodityBoughtDTO boughtDTO = CommodityBoughtDTO
                .newBuilder()
                .setCommodityType(ioType)
                .setUsed(THRUGHPUT_USED / 2)
                .build();
        final List<CommodityBoughtDTO> boughtDTOList = Lists.newArrayList(boughtDTO);

        CommoditySoldDTO soldDTO = CommoditySoldDTO.newBuilder()
                .setCommodityType(vMemType)
                .setUsed(VMEM_USAGE)
                .setCapacity(OLD_TIER_CAPACITY).build();
        Mockito.doReturn(Optional.of(boughtDTO)).when(mockINdex)
                .getCommBought(Mockito.anyLong(), Mockito.anyLong(),
                        Mockito.anyObject(), Mockito.anyLong());

        TopologyDTO.TopologyEntityDTO originalEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(CLOUD_VM_OID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditySoldList(soldDTO)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(PM_OID)
                        .addAllCommodityBought(boughtDTOList)
                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .build())
                .build();
        Mockito.doReturn(Optional.of(soldDTO)).when(mockINdex)
                .getCommSold(Mockito.anyLong(), Mockito.anyObject());

        return originalEntityDTO;
    }

    private TopologyEntityDTO createEntityDTO(final long entityOid,
                                              final int entityTypeValue,
                                              final int soldCommodityTypeValue) {

        TopologyDTO.TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder()
                .setOid(entityOid)
                .setEntityType(entityTypeValue)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(
                                CommodityType.newBuilder()
                                        .setType(soldCommodityTypeValue).build())
                        .build())
                .build();
        return entityDTO;
    }


    private void verifyTOtoDTOConversions(Map<Long, ProjectedTopologyEntity> projectedTOs) {
        Assert.assertEquals(1, projectedTOs.size());
        ProjectedTopologyEntity projectedTopologyEntity =
                projectedTOs.entrySet().iterator().next().getValue();
        Assert.assertNotNull(projectedTopologyEntity.getEntity());
        final List<CommoditySoldDTO> projectedSoldList =
                projectedTopologyEntity.getEntity().getCommoditySoldListList();
        Assert.assertNotNull(projectedSoldList);
        Assert.assertEquals(1,
                projectedTopologyEntity.getEntity().getCommoditySoldListCount());
        final CommoditySoldDTO projectedSoldDTO =
                projectedTopologyEntity.getEntity().getCommoditySoldList(0);
        Assert.assertNotNull(projectedSoldDTO);
        // Test if returned sold capacity is the new provider (tier capacity)
        Assert.assertEquals(OLD_TIER_CAPACITY * 2, projectedSoldDTO.getCapacity(), DELTA);
        final List<CommoditiesBoughtFromProvider> boughtProvList =
                projectedTopologyEntity.getEntity().getCommoditiesBoughtFromProvidersList();
        Assert.assertNotNull(boughtProvList);
        Assert.assertEquals(1, boughtProvList.size());
        final CommoditiesBoughtFromProvider boughtList = boughtProvList.get(0);
        Assert.assertNotNull(boughtList);
        Assert.assertNotNull(boughtList.getCommodityBoughtList());
        Assert.assertEquals(1, boughtList.getCommodityBoughtList().size());
        CommodityBoughtDTO actualBought = boughtList.getCommodityBoughtList().get(0);
        // Test if returned bought throughput usage is the original bought commodity usage
        Assert.assertEquals(THRUGHPUT_USED / 2f, actualBought.getUsed(), DELTA);
    }

}
