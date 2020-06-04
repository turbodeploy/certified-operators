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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.commons.Units;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.market.runner.MarketMode;
import com.vmturbo.market.runner.ReservedCapacityAnalysis;
import com.vmturbo.market.runner.WastedFilesAnalysis;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.market.topology.conversions.CommodityIndex.CommodityIndexFactory;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
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

    CommodityType vStorageType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VSTORAGE_VALUE).build();

    private static final TopologyInfo REALTIME_TOPOLOGY_INFO =
            TopologyInfo.newBuilder().setTopologyType(TopologyType.REALTIME).build();

    private CommodityType topologyCommodity1;
    private CommodityType topologyCommodity2;

    private static final NumericIDAllocator ID_ALLOCATOR = new NumericIDAllocator();
    private TierExcluderFactory tierExcluderFactory = mock(TierExcluderFactory.class);
    private ConsistentScalingHelperFactory consistentScalingHelperFactory =
            mock(ConsistentScalingHelperFactory.class);

    private static final int BICLIQUE_TYPE_ID = ID_ALLOCATOR.allocate("BICLIQUE");
    private static final int CPU_TYPE_ID = ID_ALLOCATOR.allocate("CPU");
    private static final int ST_AMT_TYPE_ID = ID_ALLOCATOR.allocate("StorageAmount");
    private static final int DSPM_TYPE_ID = ID_ALLOCATOR.allocate("DSPM");
    private CloudTopology<TopologyEntityDTO> cloudTopology =
            mock(TopologyEntityCloudTopology.class);

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        // The commodity types in topologyCommodity
        // map to the base type in economy commodity.
        topologyCommodity1 = CommodityType.newBuilder().setType(CPU_TYPE_ID).setKey("blah").build();
        topologyCommodity2 =
                CommodityType.newBuilder().setType(DSPM_TYPE_ID).setKey("blahblah").build();
        when(tierExcluderFactory.newExcluder(any(), any(), any())).thenReturn(mock(TierExcluder.class));
        ConsistentScalingHelper consistentScalingHelper = mock(ConsistentScalingHelper.class);
        when(consistentScalingHelper.getScalingGroupId(any())).thenReturn(Optional.empty());
        when(consistentScalingHelper.getScalingGroupUsage(any())).thenReturn(Optional.empty());
        when(consistentScalingHelperFactory.newConsistentScalingHelper(any(), any()))
            .thenReturn(consistentScalingHelper);
        constructTopologyConverter();
    }

    /**
     * Construct default TopologyConverter.
     */
    private void constructTopologyConverter() {
        constructTopologyConverter(CommodityIndex.newFactory());
    }

    /**
     * Construct TopologyConverter wit provided {@link CommodityIndexFactory}.
     *
     * @param commodityIndexFactory Commodity index factory
     */
    private void constructTopologyConverter(final CommodityIndexFactory commodityIndexFactory) {
        converter = Mockito.spy(new TopologyConverter(
            REALTIME_TOPOLOGY_INFO,
            false,
            MarketAnalysisUtils.QUOTE_FACTOR,
            MarketMode.M2Only,
            MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
            marketPriceTable,
            mockCommodityConverter,
            mockCCD,
            commodityIndexFactory,
            tierExcluderFactory,
            consistentScalingHelperFactory, cloudTopology));
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
                .marketToTopologyCommodity(Mockito.eq(economyCPUSold.getSpecification()));
        Mockito.doReturn(Optional.empty()).when(mockCommodityConverter)
                .marketToTopologyCommodity(Mockito.eq(economyDSPMSold.getSpecification()));
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
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
                .marketToTopologyCommodity(Mockito.eq(CommoditySpecificationTO.newBuilder()
                        .setBaseType(DSPM_TYPE_ID).setType(CPU_TYPE_ID).build()));
        Mockito.doReturn(Optional.empty()).when(mockCommodityConverter)
                .marketToTopologyCommodity(Mockito.eq(CommoditySpecificationTO.newBuilder()
                        .setBaseType(BICLIQUE_TYPE_ID).setType(DSPM_TYPE_ID)
                        .build()));
        Mockito.doReturn(ImmutableList.of(cpuCommSpecTO)).when(mockCommodityConverter)
                .commoditySpecification(cpuCommType, 1);
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
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
    public void testProvByDemandTraderToEntityConversion() {
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
                .when(mockCommodityConverter).marketToTopologyCommodity(
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
                traderTOs, origTopoMap, PriceIndexMessage.getDefaultInstance(),
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
                                .setConnectedEntityType(az.getEntityType())
                                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION))
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
            false, MarketAnalysisUtils.QUOTE_FACTOR, MarketMode.M2Only, MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
            marketPriceTable, mockCommodityConverter, indexFactory, tierExcluderFactory,
            consistentScalingHelperFactory));

        // warning: introspection follows...
        Map<Long, ShoppingListInfo> shoppingListMap = new HashMap<>();
        shoppingListMap.put(VM_OID, new ShoppingListInfo(DSPM_TYPE_ID, DS_OID, PM_OID, VOLUME_ID,
            EntityType.PHYSICAL_MACHINE_VALUE, topologyDSPMBought));
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
            .marketToTopologyCommodity(Mockito.eq(economyCPUSold.getSpecification()));

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
            PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
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
        assertEquals((double)SCALING_FACTOR, actualBoughtCommodityList.iterator().next().getScalingFactor(), TopologyConverter.EPSILON);
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
                        MarketAnalysisUtils.QUOTE_FACTOR, MarketMode.M2Only, MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                        marketPriceTable, mockCommodityConverter, mockCCD, indexFactory,
                        tierExcluderFactory,
            consistentScalingHelperFactory, cloudTopology));

        // warning: introspection follows...
        Map<Long, ShoppingListInfo> shoppingListMap = new HashMap<>();
        shoppingListMap.put(VM_OID, new ShoppingListInfo(DSPM_TYPE_ID, DS_OID, PM_OID, VOLUME_ID,
                EntityType.PHYSICAL_MACHINE_VALUE, topologyDSPMBought));
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
                .marketToTopologyCommodity(Mockito.eq(economyCPUSold.getSpecification()));

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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
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
        assertEquals(Double.MIN_VALUE, actualCpuCommodity.getScalingFactor(), TopologyConverter.EPSILON);
        // check that the VM expected used matches the actual converted used
        assertEquals(1, entity.get(vmTrader.getOid()).getEntity()
                .getCommoditiesBoughtFromProvidersList().size());
        final List<CommodityBoughtDTO> actualBoughtCommodityList = entity.get(vmTrader.getOid())
                .getEntity().getCommoditiesBoughtFromProvidersList().iterator().next()
                .getCommodityBoughtList();
        assertEquals(1, actualBoughtCommodityList.size());
        assertEquals(RAW_VM_USED, actualBoughtCommodityList.iterator().next().getUsed(),
                TopologyConverter.EPSILON);
        assertEquals((double)SCALING_FACTOR, actualBoughtCommodityList.iterator().next().getScalingFactor(), TopologyConverter.EPSILON);
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
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
                .marketToTopologyCommodity(Mockito.eq(CommoditySpecificationTO.newBuilder()
                        .setBaseType(DSPM_TYPE_ID).setType(CPU_TYPE_ID).build()));
        Mockito.doReturn(Optional.empty()).when(mockCommodityConverter)
                .marketToTopologyCommodity(Mockito.eq(CommoditySpecificationTO.newBuilder()
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

        try {
            Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                    Collections.singletonList(trader),
                    ImmutableMap.of(expectedEntity.getOid(), expectedEntity),
                    PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
                    setUpWastedFileAnalysis());

            assertEquals(1l, converter.getShoppingListOidToInfos().size());
            assertEquals(10000L, converter.getShoppingListOidToInfos().get(-1L).getBuyerId());
            assertEquals(1L, entity.size());
            assertEquals(10000L, entity.get(10000L).getEntity().getOid());
            assertEquals(EntityType.VIRTUAL_MACHINE_VALUE,
                    entity.get(10000L).getEntity().getEntityType());
        } catch (RuntimeException e) {

        }
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
                .marketToTopologyCommodity(Mockito.eq(CommoditySpecificationTO.newBuilder()
                        .setBaseType(DSPM_TYPE_ID).setType(CPU_TYPE_ID).build()));
        Mockito.doReturn(Optional.empty()).when(mockCommodityConverter)
                .marketToTopologyCommodity(Mockito.eq(CommoditySpecificationTO.newBuilder()
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

        try {
            Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                    Collections.singletonList(trader),
                    ImmutableMap.of(expectedEntity.getOid(), expectedEntity),
                    PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
                    setUpWastedFileAnalysis());

            assertEquals(1l, converter.getShoppingListOidToInfos().size());
            assertEquals(-1000L, converter.getShoppingListOidToInfos().get(-1L).getBuyerId());
            assertEquals(1L, entity.size());
            assertEquals(-1000L, entity.get(-1000L).getEntity().getOid());
            assertEquals(EntityType.VIRTUAL_MACHINE_VALUE,
                    entity.get(-1000L).getEntity().getEntityType());
        } catch (RuntimeException e) {

        }
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

        try {
            final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity =
            converter.convertFromMarket(Collections.singletonList(trader),
                ImmutableMap.of(VM_OID, originalVm),
                PriceIndexMessage.getDefaultInstance(),
                reservedCapacityAnalysis, setUpWastedFileAnalysis());

            assertEquals(1, entity.get(VM_OID).getEntity().getCommoditySoldListCount());
            assertEquals(commSold, entity.get(VM_OID).getEntity().getCommoditySoldList(0));
        } catch (RuntimeException e) {

        }
    }

    private WastedFilesAnalysis setUpWastedFileAnalysis() {
        WastedFilesAnalysis wastedFilesAnalysisMock = mock(WastedFilesAnalysis.class);
        when(wastedFilesAnalysisMock.getStorageAmountReleasedForOid(anyLong())).thenReturn(Optional.empty());
        return wastedFilesAnalysisMock;
    }

    private TraderTO createVMTOs(CommodityBoughtTO boughtTO,
                                     List<CommoditySoldTO> soldTOList,
                                     long oid, long supplierId,
                                 long shoppinglistOid ) {
        EconomyDTOs.TraderTO vmTO = EconomyDTOs.TraderTO.newBuilder().setOid(CLOUD_VM_OID)
                .setState(TraderStateTO.ACTIVE)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(shoppinglistOid)
                        .setSupplier(supplierId)
                        .addCommoditiesBought(boughtTO)
                        .build())
                .addAllCommoditiesSold(soldTOList).build();
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
                MarketAnalysisUtils.QUOTE_FACTOR, MarketMode.M2Only, MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable, mockCommodityConverter, mockCCD, indexFactory, tierExcluderFactory,
            consistentScalingHelperFactory, cloudTopology));
        converter.setCloudTc(mockCloudTc);

        TopologyDTO.TopologyEntityDTO oldTierDTO = createEntityDTO(CLOUD_COMPUTE_TIER_OID,
                EntityType.COMPUTE_TIER_VALUE,
                ImmutableList.of(CommodityDTO.CommodityType.MEM_VALUE,
                                CommodityDTO.CommodityType.VSTORAGE_VALUE),
                OLD_TIER_CAPACITY);
        TopologyDTO.TopologyEntityDTO newTierDTO = createEntityDTO(CLOUD_NEW_COMPUTE_TIER_OID,
                EntityType.COMPUTE_TIER_VALUE,
                ImmutableList.of(CommodityDTO.CommodityType.MEM_VALUE,
                        CommodityDTO.CommodityType.VSTORAGE_VALUE),
                OLD_TIER_CAPACITY * 2);

        CommoditySoldTO newTierSold = createSoldTO(CommodityDTO.CommodityType.MEM_VALUE,
                OLD_TIER_CAPACITY * 2, true);
        Mockito.doReturn(Optional.of(vMemType))
                .when(mockCommodityConverter)
                .marketToTopologyCommodity(Mockito.eq(newTierSold.getSpecification()));
        CommoditySoldTO newTierVstorageSold = createSoldTO(CommodityDTO.CommodityType.VSTORAGE_VALUE,
                OLD_TIER_CAPACITY * 2, true);
        Mockito.doReturn(Optional.of(vStorageType))
                .when(mockCommodityConverter)
                .marketToTopologyCommodity(Mockito.eq(newTierVstorageSold.getSpecification()));

        TopologyDTO.TopologyEntityDTO originalEntityDTO = createOriginalDTOAndMockIndex(mockINdex);
        CommodityBoughtTO boughtTO =
                createBoughtTO(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE, THRUGHPUT_USED);
        Mockito.doReturn(Optional.of(ioType)).when(mockCommodityConverter)
                .marketToTopologyCommodity(Mockito.eq(boughtTO.getSpecification()));

        CommodityBoughtTO oldBoughtTO =
                createBoughtTO(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE,
                        THRUGHPUT_USED / 2);
        CommoditySoldTO soldTO = createSoldTO(CommodityDTO.CommodityType.MEM_ALLOCATION_VALUE,
                OLD_TIER_CAPACITY, true);
        Mockito.doReturn(Optional.of(vMemType))
                .when(mockCommodityConverter)
                .marketToTopologyCommodity(Mockito.eq(soldTO.getSpecification()));
        CommoditySoldTO soldvStorage = createSoldTO(CommodityDTO.CommodityType.VSTORAGE_VALUE,
                OLD_TIER_CAPACITY, true);
        Mockito.doReturn(Optional.of(vStorageType))
                .when(mockCommodityConverter)
                .marketToTopologyCommodity(Mockito.eq(soldvStorage.getSpecification()));


        EconomyDTOs.TraderTO vmTO = createVMTOs(boughtTO,
                ImmutableList.of(newTierSold, newTierVstorageSold),
                CLOUD_VM_OID,
                CLOUD_NEW_COMPUTE_TIER_OID, CLOUD_VM_OID + 1L);
        EconomyDTOs.TraderTO oldVMTO = createVMTOs(oldBoughtTO,
                ImmutableList.of(soldTO, soldvStorage), CLOUD_VM_OID,
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
        Mockito.doReturn(oldVMTO).when(converter)
                .topologyDTOtoTraderTO(Mockito.eq(originalEntityDTO));
        Mockito.doReturn(Optional.empty()).when(mockCloudTc).getComputeTier(any());
        Mockito.doReturn(Optional.empty()).when(mockCloudTc)
                .getRiDiscountedMarketTier(Mockito.anyLong());

        Map<Long, ProjectedTopologyEntity> projectedTOs =
                converter.convertFromMarket(Collections.singletonList(vmTO),
                        ImmutableMap.of(originalEntityDTO.getOid(), originalEntityDTO,
                                CLOUD_NEW_COMPUTE_TIER_OID, newTierDTO),
                        PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
                        setUpWastedFileAnalysis());
        verifyTOtoDTOConversions(projectedTOs,
                ImmutableList.of(CommodityDTO.CommodityType.VMEM_VALUE,
                        CommodityDTO.CommodityType.VMEM_VALUE));

    }

    /**
     * Unit test the TO to projected DTO conversions with respect to
     * the bought commodity resizable attribute.
     * The projected capacity will reflect the new tier capacity even if it's not resizable.
     */
    @Test
    public void testProjectedNotResizableCommodityCapacity() {

        CloudTopologyConverter mockCloudTc = Mockito.mock(CloudTopologyConverter.class);
        CommodityIndex mockINdex = Mockito.mock(CommodityIndex.class);
        final CommodityIndexFactory indexFactory = mock(CommodityIndexFactory.class);
        when(indexFactory.newIndex()).thenReturn(mockINdex);

        TopologyInfo topoInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.PLAN).build();
        TopologyConverter converter = Mockito.spy(new TopologyConverter(topoInfo, false,
                MarketAnalysisUtils.QUOTE_FACTOR, MarketMode.M2Only, MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
                marketPriceTable, mockCommodityConverter, mockCCD, indexFactory, tierExcluderFactory,
                consistentScalingHelperFactory, cloudTopology));
        converter.setCloudTc(mockCloudTc);

        TopologyDTO.TopologyEntityDTO oldTierDTO = createEntityDTO(CLOUD_COMPUTE_TIER_OID,
                EntityType.COMPUTE_TIER_VALUE,
                ImmutableList.of(CommodityDTO.CommodityType.MEM_VALUE),
                OLD_TIER_CAPACITY);
        TopologyDTO.TopologyEntityDTO newTierDTO = createEntityDTO(CLOUD_NEW_COMPUTE_TIER_OID,
                EntityType.COMPUTE_TIER_VALUE,
                ImmutableList.of(CommodityDTO.CommodityType.MEM_VALUE),
                OLD_TIER_CAPACITY * 2);

        CommoditySoldTO newTierMemSold = createSoldTO(CommodityDTO.CommodityType.MEM_VALUE,
                OLD_TIER_CAPACITY * 2, false);

        EconomyDTOs.TraderTO newTier =
                EconomyDTOs.TraderTO.newBuilder().setOid(CLOUD_NEW_COMPUTE_TIER_OID)
                        .setState(TraderStateTO.ACTIVE)
                        .setOid(CLOUD_NEW_COMPUTE_TIER_OID)
                        .setType(EntityType.COMPUTE_TIER_VALUE)
                        .addCommoditiesSold(newTierMemSold).build();

        TopologyDTO.TopologyEntityDTO originalEntityDTO = createOriginalDTOAndMockIndex(mockINdex);
        CommodityBoughtTO boughtTO =
                createBoughtTO(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE, THRUGHPUT_USED);
        Mockito.doReturn(Optional.of(ioType)).when(mockCommodityConverter)
                .marketToTopologyCommodity(Mockito.eq(boughtTO.getSpecification()));

        CommodityBoughtTO oldBoughtTO =
                createBoughtTO(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE,
                        THRUGHPUT_USED / 2);
        CommoditySoldTO soldVMem = createSoldTO(CommodityDTO.CommodityType.VMEM_VALUE,
                OLD_TIER_CAPACITY, false);
        Mockito.doReturn(Optional.of(vMemType))
                .when(mockCommodityConverter)
                .marketToTopologyCommodity(Mockito.eq(soldVMem.getSpecification()));


        EconomyDTOs.TraderTO vmTO = createVMTOs(boughtTO,
                ImmutableList.of(soldVMem),
                CLOUD_VM_OID,
                CLOUD_NEW_COMPUTE_TIER_OID, CLOUD_VM_OID + 1L);
        EconomyDTOs.TraderTO oldVMTO = createVMTOs(oldBoughtTO,
                ImmutableList.of(soldVMem), CLOUD_VM_OID,
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
        Mockito.doReturn(oldVMTO).when(converter)
                .topologyDTOtoTraderTO(Mockito.eq(originalEntityDTO));
        Mockito.doReturn(Optional.empty()).when(mockCloudTc).getComputeTier(any());
        Mockito.doReturn(Optional.empty()).when(mockCloudTc)
                .getRiDiscountedMarketTier(Mockito.anyLong());

        Map<Long, ProjectedTopologyEntity> projectedTOs =
                converter.convertFromMarket(Collections.singletonList(vmTO),
                        ImmutableMap.of(originalEntityDTO.getOid(), originalEntityDTO,
                                CLOUD_NEW_COMPUTE_TIER_OID, newTierDTO),
                        PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
                        setUpWastedFileAnalysis());
        verifyTOtoDTOConversions(projectedTOs,
                ImmutableList.of(CommodityDTO.CommodityType.VMEM_VALUE));

    }

    /**
     * Test timeslot bought commodities merge.
     */
    @Test
    public void testMergeTimeSlotCommoditiesBought() {
        final float[] used = {0.5f, 0.6f, 0.65f};
        final float[] peak = {0.55f, 0.65f, 0.75f};

        final float averageUsed = (used[0] + used[1] + used[2]) / 3;
        final float averagePeak = (peak[0] + peak[1] + peak[2]) / 3;

        final CommodityType vCpuCommType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VCPU_VALUE).build();

        final CommodityType vMemCommType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VMEM_VALUE).build();

        final CommodityType poolCpuCommType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.POOL_CPU_VALUE).build();

        final CommodityBoughtTO vcpu = createBoughtTO(CommodityDTO.CommodityType.VCPU_VALUE,
            0.5f);
        final CommodityBoughtTO vMem = createBoughtTO(CommodityDTO.CommodityType.VMEM_VALUE,
            0.6f);
        final CommodityBoughtTO poolCpu1 = createBoughtTO(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            used[0], peak[0]);
        final CommodityBoughtTO poolCpu2 = createBoughtTO(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            used[1], peak[1]);
        final CommodityBoughtTO poolCpu3 = createBoughtTO(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            used[2], peak[2]);
        final Collection<CommodityBoughtTO> commoditiesBought = ImmutableList.of(vcpu, vMem, poolCpu1,
            poolCpu2, poolCpu3);

        doReturn(Optional.of(vCpuCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(vcpu.getSpecification()));
        doReturn(Optional.of(vMemCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(vMem.getSpecification()));
        doReturn(Optional.of(poolCpuCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(poolCpu1.getSpecification()));
        doReturn(Optional.of(poolCpuCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(poolCpu2.getSpecification()));
        doReturn(Optional.of(poolCpuCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(poolCpu3.getSpecification()));

        doReturn(false).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(vcpu.getSpecification()));
        doReturn(false).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(vMem.getSpecification()));
        doReturn(true).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(poolCpu1.getSpecification()));
        doReturn(true).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(poolCpu2.getSpecification()));
        doReturn(true).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(poolCpu3.getSpecification()));

        final Map<CommodityType, List<Double>> timeSlotsByCommType = Maps.newHashMap();
        final Collection<CommodityBoughtTO> mergedComms = converter.mergeTimeSlotCommodities(
            commoditiesBought.stream().map(CommodityBoughtTO::toBuilder)
                .collect(Collectors.toList()),
            timeSlotsByCommType,
            CommodityBoughtTO.Builder::getSpecification,
            CommodityBoughtTO.Builder::getQuantity,
            CommodityBoughtTO.Builder::getPeakQuantity,
            CommodityBoughtTO.Builder::setQuantity,
            CommodityBoughtTO.Builder::setPeakQuantity)
            .stream().map(CommodityBoughtTO.Builder::build).collect(Collectors.toList());
        assertEquals(3, mergedComms.size());
        List<CommodityBoughtTO> mergedComm = mergedComms.stream().filter(comm ->
            CommodityDTO.CommodityType.POOL_CPU_VALUE == comm.getSpecification().getBaseType())
            .collect(Collectors.toList());
        assertEquals(1, mergedComm.size());
        assertEquals(averageUsed, mergedComm.get(0).getQuantity(), DELTA);
        assertEquals(averagePeak, mergedComm.get(0).getPeakQuantity(), DELTA);
        assertEquals(1, timeSlotsByCommType.size());
        final List<Double> timeSlotValues = timeSlotsByCommType.get(poolCpuCommType);
        assertNotNull(timeSlotValues);
        assertEquals(3, timeSlotValues.size());
        assertEquals(used[0], timeSlotValues.get(0), DELTA);
        assertEquals(used[1], timeSlotValues.get(1), DELTA);
        assertEquals(used[2], timeSlotValues.get(2), DELTA);
    }

    /**
     * Test timeslot bought commodities merge.
     */
    @Test
    public void testMergeTimeSlotCommoditiesSold() {
        final float used = 0.01f;
        final float peak = 0.05f;
        final float capacity = 1.0f;

        final CommodityType vCpuCommType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VCPU_VALUE).build();

        final CommodityType vMemCommType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VMEM_VALUE).build();

        final CommodityType poolCpuCommType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.POOL_CPU_VALUE).build();

        final CommoditySoldTO vcpu = createSoldTO(CommodityDTO.CommodityType.VCPU_VALUE,
            capacity, true);
        final CommoditySoldTO vMem = createSoldTO(CommodityDTO.CommodityType.VMEM_VALUE,
            capacity, true);
        final CommoditySoldTO poolCpu1 = createSoldTO(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            capacity, used, peak, true);
        final CommoditySoldTO poolCpu2 = createSoldTO(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            capacity, used, peak, true);
        final CommoditySoldTO poolCpu3 = createSoldTO(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            capacity, used, peak, true);
        Collection<CommoditySoldTO> commditiesSold = ImmutableList.of(vcpu, vMem, poolCpu1, poolCpu2,
            poolCpu3);

        doReturn(Optional.of(vCpuCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(vcpu.getSpecification()));
        doReturn(Optional.of(vMemCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(vMem.getSpecification()));
        doReturn(Optional.of(poolCpuCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(poolCpu1.getSpecification()));
        doReturn(Optional.of(poolCpuCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(poolCpu2.getSpecification()));
        doReturn(Optional.of(poolCpuCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(poolCpu3.getSpecification()));

        doReturn(false).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(vcpu.getSpecification()));
        doReturn(false).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(vMem.getSpecification()));
        doReturn(true).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(poolCpu1.getSpecification()));
        doReturn(true).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(poolCpu2.getSpecification()));
        doReturn(true).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(poolCpu3.getSpecification()));

        final Map<CommodityType, List<Double>> timeSlotsByCommType = Maps.newHashMap();
        final Collection<CommoditySoldTO> mergedComms = converter.mergeTimeSlotCommodities(
            commditiesSold.stream().map(CommoditySoldTO::toBuilder)
                .collect(Collectors.toList()),
            timeSlotsByCommType,
            CommoditySoldTO.Builder::getSpecification,
            CommoditySoldTO.Builder::getQuantity,
            CommoditySoldTO.Builder::getPeakQuantity,
            CommoditySoldTO.Builder::setQuantity,
            CommoditySoldTO.Builder::setPeakQuantity)
            .stream().map(CommoditySoldTO.Builder::build).collect(Collectors.toList());
        assertEquals(3, mergedComms.size());
        List<CommoditySoldTO> mergedComm = mergedComms.stream().filter(comm ->
            CommodityDTO.CommodityType.POOL_CPU_VALUE == comm.getSpecification().getBaseType())
            .collect(Collectors.toList());
        assertEquals(1, mergedComm.size());
        assertEquals(used, mergedComm.get(0).getQuantity(), DELTA);
        assertEquals(peak, mergedComm.get(0).getPeakQuantity(), DELTA);
        assertEquals(1, timeSlotsByCommType.size());
        final List<Double> timeSlotValues = timeSlotsByCommType.get(poolCpuCommType);
        assertNotNull(timeSlotValues);
        assertEquals(3, timeSlotValues.size());
        assertEquals(used, timeSlotValues.get(0), DELTA);
        assertEquals(used, timeSlotValues.get(1), DELTA);
        assertEquals(used, timeSlotValues.get(2), DELTA);
    }

    /**
     * Test timeslot bought commodities.
     */
    @Test
    public void testConvertFromMarketTimeSlotCommoditiesBought() {
        final float[] used = {0.5f, 0.6f, 0.65f};
        final float[] peak = {0.55f, 0.65f, 0.75f};

        final float averageUsed = (used[0] + used[1] + used[2]) / 3;
        final float averagePeak = (peak[0] + peak[1] + peak[2]) / 3;

        final float percentile = 0.7f;

        final CommodityIndex mockIndex = mock(CommodityIndex.class);
        final CommodityIndexFactory indexFactory = mock(CommodityIndexFactory.class);
        doReturn(mockIndex).when(indexFactory).newIndex();
        constructTopologyConverter(indexFactory);

        final CommodityType poolCpuCommType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.POOL_CPU_VALUE).build();

        // Bought commodities
        final CommodityBoughtTO poolCpu1 = createBoughtTO(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            used[0], peak[0]);
        final CommodityBoughtTO poolCpu2 = createBoughtTO(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            used[1], peak[1]);
        final CommodityBoughtTO poolCpu3 = createBoughtTO(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            used[2], peak[2]);
        final Collection<CommodityBoughtTO> boughtCommodities = ImmutableList.of(poolCpu1, poolCpu2, poolCpu3);

        doReturn(Optional.of(poolCpuCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(poolCpu1.getSpecification()));
        doReturn(Optional.of(poolCpuCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(poolCpu2.getSpecification()));
        doReturn(Optional.of(poolCpuCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(poolCpu3.getSpecification()));

        doReturn(true).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(poolCpu1.getSpecification()));
        doReturn(true).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(poolCpu2.getSpecification()));
        doReturn(true).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(poolCpu3.getSpecification()));

        final long businessUserId = 10L;
        final long shoppinglistId = 20L;
        final long supplierId = 30L;
        final long desktopPoolId = 40L;
        final EconomyDTOs.TraderTO businessUserTO = EconomyDTOs.TraderTO.newBuilder()
            .setOid(businessUserId)
            .setState(TraderStateTO.ACTIVE)
            .setType(EntityType.BUSINESS_USER_VALUE)
            .addShoppingLists(ShoppingListTO.newBuilder().setOid(shoppinglistId)
                .setSupplier(supplierId)
                .addAllCommoditiesBought(boughtCommodities)
                .build())
            .build();

        // For testing percentile
        final HistoricalValues historicalValues = HistoricalValues.newBuilder()
                .setPercentile(percentile)
            .build();
        final TopologyDTO.CommodityBoughtDTO originalCommodityBought = createBoughtDTO(
            CommodityDTO.CommodityType.POOL_CPU_VALUE, 0.01, 0.05, historicalValues);

        doReturn(Optional.of(originalCommodityBought)).when(mockIndex).getCommBought(
            Mockito.eq(businessUserId), Mockito.eq(supplierId),
            Mockito.eq(poolCpuCommType), Mockito.eq(0L));

        final TopologyDTO.TopologyEntityDTO originalEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(businessUserId)
            .setEntityType(EntityType.BUSINESS_USER_VALUE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                .newBuilder().setProviderId(desktopPoolId)
                .addCommodityBought(originalCommodityBought)
                .setProviderEntityType(EntityType.DESKTOP_POOL_VALUE)
                .build())
            .build();

        final Map<Long, ProjectedTopologyEntity> projectedTOs =
            converter.convertFromMarket(Collections.singletonList(businessUserTO),
                ImmutableMap.of(originalEntityDTO.getOid(), originalEntityDTO),
                PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
                setUpWastedFileAnalysis());

        assertEquals(1, projectedTOs.size());
        assertTrue(projectedTOs.containsKey(businessUserId));
        final ProjectedTopologyEntity projectedTopologyEntity = projectedTOs.get(businessUserId);
        assertNotNull(projectedTopologyEntity);
        final TopologyEntityDTO topologyEntityDTO = projectedTopologyEntity.getEntity();
        assertEquals(1, topologyEntityDTO.getCommoditiesBoughtFromProvidersCount());
        final CommoditiesBoughtFromProvider commoditiesBoughtFromProvider =
            topologyEntityDTO.getCommoditiesBoughtFromProvidersList().get(0);
        assertEquals(1, commoditiesBoughtFromProvider.getCommodityBoughtCount());
        final CommodityBoughtDTO commodityBoughtDTO =
            commoditiesBoughtFromProvider.getCommodityBoughtList().get(0);
        assertTrue(commodityBoughtDTO.hasHistoricalUsed());
        assertEquals(percentile, commodityBoughtDTO.getHistoricalUsed().getPercentile(), DELTA);
        assertEquals(averageUsed, commodityBoughtDTO.getUsed(), DELTA);
        assertEquals(averagePeak, commodityBoughtDTO.getPeak(), DELTA);
        assertEquals(used.length, commodityBoughtDTO.getHistoricalUsed().getTimeSlotCount());
        assertEquals(used[0], commodityBoughtDTO.getHistoricalUsed().getTimeSlot(0), DELTA);
        assertEquals(used[1], commodityBoughtDTO.getHistoricalUsed().getTimeSlot(1), DELTA);
        assertEquals(used[2], commodityBoughtDTO.getHistoricalUsed().getTimeSlot(2), DELTA);
    }

    /**
     * Test timeslot sold commodities.
     */
    @Test
    public void testConvertFromMarketTimeSlotCommoditiesSold() {
        final float used = 0.01f;
        final float peak = 0.05f;
        final float capacity = 1.0f;
        final float percentile = 0.7f;

        final CommodityIndex mockIndex = mock(CommodityIndex.class);
        final CommodityIndexFactory indexFactory = mock(CommodityIndexFactory.class);
        doReturn(mockIndex).when(indexFactory).newIndex();
        constructTopologyConverter(indexFactory);

        final CommodityType poolCpuCommType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.POOL_CPU_VALUE).build();

        // Sold commodities
        final CommoditySoldTO poolCpu1 = createSoldTO(CommodityDTO.CommodityType.POOL_CPU_VALUE,
           capacity, used, peak, true);
        final CommoditySoldTO poolCpu2 = createSoldTO(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            capacity, used, peak, true);
        final CommoditySoldTO poolCpu3 = createSoldTO(CommodityDTO.CommodityType.POOL_CPU_VALUE,
            capacity, used, peak, true);

        doReturn(Optional.of(poolCpuCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(poolCpu1.getSpecification()));
        doReturn(Optional.of(poolCpuCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(poolCpu2.getSpecification()));
        doReturn(Optional.of(poolCpuCommType)).when(mockCommodityConverter).marketToTopologyCommodity(
            Mockito.eq(poolCpu3.getSpecification()));

        doReturn(true).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(poolCpu1.getSpecification()));
        doReturn(true).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(poolCpu2.getSpecification()));
        doReturn(true).when(mockCommodityConverter).isTimeSlotCommodity(
            Mockito.eq(poolCpu3.getSpecification()));

        final long shoppinglistId = 20L;
        final long supplierId = 30L;
        final long desktopPoolId = 40L;
        final EconomyDTOs.TraderTO desktopPoolTO = EconomyDTOs.TraderTO.newBuilder()
            .setOid(desktopPoolId)
            .setState(TraderStateTO.ACTIVE)
            .setType(EntityType.DESKTOP_POOL_VALUE)
            .addShoppingLists(ShoppingListTO.newBuilder().setOid(shoppinglistId)
                .setSupplier(supplierId)
                .build())
            .addAllCommoditiesSold(ImmutableList.of(poolCpu1, poolCpu2, poolCpu3))
            .build();

        // For testing percentile
        final HistoricalValues historicalValues = HistoricalValues.newBuilder()
            .setPercentile(percentile)
        .build();

        final CommoditySoldDTO originalCommoditySold = createSoldDTO(
            CommodityDTO.CommodityType.POOL_CPU_VALUE, used, peak, historicalValues);

        doReturn(Optional.of(originalCommoditySold)).when(mockIndex).getCommSold(
            Mockito.eq(desktopPoolId), Mockito.eq(poolCpuCommType));

        final TopologyDTO.TopologyEntityDTO originalEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(desktopPoolId)
            .setDisplayName("test")
            .setEntityType(EntityType.DESKTOP_POOL_VALUE)
            .addAllCommoditySoldList(ImmutableList.of(originalCommoditySold))
            .build();

        final Map<Long, ProjectedTopologyEntity> projectedTOs =
            converter.convertFromMarket(Collections.singletonList(desktopPoolTO),
                ImmutableMap.of(originalEntityDTO.getOid(), originalEntityDTO),
                PriceIndexMessage.getDefaultInstance(), reservedCapacityAnalysis,
                setUpWastedFileAnalysis());

        assertEquals(1, projectedTOs.size());
        assertTrue(projectedTOs.containsKey(desktopPoolId));
        final ProjectedTopologyEntity projectedTopologyEntity = projectedTOs.get(desktopPoolId);
        assertNotNull(projectedTopologyEntity);
        final TopologyEntityDTO topologyEntityDTO = projectedTopologyEntity.getEntity();
        assertEquals(1, topologyEntityDTO.getCommoditySoldListCount());
        final CommoditySoldDTO commoditySoldDTO =
            topologyEntityDTO.getCommoditySoldListList().get(0);
        assertTrue(commoditySoldDTO.hasHistoricalUsed());
        assertEquals(used, commoditySoldDTO.getUsed(), DELTA);
        assertEquals(peak, commoditySoldDTO.getPeak(), DELTA);
        assertEquals(3, commoditySoldDTO.getHistoricalUsed().getTimeSlotCount());
        assertEquals(used, commoditySoldDTO.getHistoricalUsed().getTimeSlot(0), DELTA);
        assertEquals(used, commoditySoldDTO.getHistoricalUsed().getTimeSlot(1), DELTA);
        assertEquals(used, commoditySoldDTO.getHistoricalUsed().getTimeSlot(2), DELTA);
    }

    private CommoditySoldTO createSoldTO(final int typeValue,
                                         final double capacity,
                                         final boolean resizable) {
        return createSoldTO(typeValue, capacity, 0, 0, resizable);
    }

    private CommoditySoldTO createSoldTO(final int typeValue,
                                         final double capacity,
                                         final double quantity,
                                         final double peakQuantity,
                                         final boolean resizable) {
        CommoditySoldTO soldTO = CommoditySoldTO.newBuilder()
            .setSpecification(CommoditySpecificationTO.newBuilder()
                .setBaseType(typeValue)
                .setType(typeValue).build())
            .setCapacity((float)capacity)
            .setQuantity((float)quantity)
            .setPeakQuantity((float)peakQuantity)
            .setSettings(CommoditySoldSettingsTO.newBuilder()
                .setResizable(resizable).build())
            .build();
        return soldTO;

    }

    private CommodityBoughtTO createBoughtTO(final int boughtTypeValue,
                                             final float quantity) {
        return createBoughtTO(boughtTypeValue, quantity, 0);
    }

    private CommodityBoughtTO createBoughtTO(final int boughtTypeValue,
             final float quantity, final float peakQuantity) {
        CommodityBoughtTO boughtTO = CommodityBoughtTO.newBuilder()
            .setSpecification(CommoditySpecificationTO.newBuilder()
                .setBaseType(boughtTypeValue)
                .setType(boughtTypeValue).build())
            .setQuantity(quantity)
            .setPeakQuantity(peakQuantity)
            .build();
        return boughtTO;
    }

    private CommodityBoughtDTO createBoughtDTO(final int commodityTypeValue,
            final double quantity, final double peakQuantity, final HistoricalValues historicalValues) {
        final TopologyDTO.CommodityBoughtDTO.Builder commodityBoughtDTO =
            CommodityBoughtDTO.newBuilder()
            .setCommodityType(
                CommodityType.newBuilder().setType(commodityTypeValue))
            .setUsed(quantity)
            .setPeak(peakQuantity);
        if (historicalValues != null) {
            commodityBoughtDTO.setHistoricalUsed(historicalValues);
        }
        return commodityBoughtDTO.build();
    }

    private CommoditySoldDTO createSoldDTO(final int commodityTypeValue,
           final double quantity, final double peakQuantity, final HistoricalValues historicalValues) {
        final TopologyDTO.CommoditySoldDTO.Builder commoditySoldDTO =
            CommoditySoldDTO.newBuilder()
                .setCommodityType(
                    CommodityType.newBuilder().setType(commodityTypeValue))
                .setUsed(quantity)
                .setPeak(peakQuantity);
        if (historicalValues != null) {
            commoditySoldDTO.setHistoricalUsed(historicalValues);
        }
        return commoditySoldDTO.build();
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
        CommoditySoldDTO vStorageSoldDTO = CommoditySoldDTO.newBuilder()
                .setCommodityType(vStorageType)
                .setUsed(VMEM_USAGE)
                .setCapacity(OLD_TIER_CAPACITY).build();

        Mockito.doReturn(Optional.of(boughtDTO)).when(mockINdex)
                .getCommBought(Mockito.anyLong(), Mockito.anyLong(),
                        Mockito.anyObject(), Mockito.anyLong());

        TopologyDTO.TopologyEntityDTO originalEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(CLOUD_VM_OID)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditySoldList(soldDTO)
                .addCommoditySoldList(vStorageSoldDTO)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(PM_OID)
                        .addAllCommodityBought(boughtDTOList)
                        .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .build())
                .build();
        Mockito.doReturn(Optional.of(soldDTO)).when(mockINdex)
                .getCommSold(anyLong(), Mockito.anyObject());
        Mockito.doReturn(Optional.of(vStorageSoldDTO)).when(mockINdex)
                .getCommSold(CLOUD_VM_OID, vStorageType);

        return originalEntityDTO;
    }

    private static TopologyEntityDTO createEntityDTO(final long entityOid,
                                              final int entityTypeValue,
                                              final List<Integer> soldCommodityTypeValues,
                                              double cap) {

        Builder entityDTO = TopologyEntityDTO.newBuilder()
                .setOid(entityOid)
                .setEntityType(entityTypeValue);
        for (int type: soldCommodityTypeValues) {
            boolean resizable = true;
            if (type == CommodityDTO.CommodityType.VSTORAGE_VALUE) {
                resizable = false;
            }
            entityDTO
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setIsResizeable(resizable)
                        .setCapacity(cap)
                        .setCommodityType(
                                CommodityType.newBuilder()
                                        .setType(type).build()));
        }
        return entityDTO.build();
    }


    private void verifyTOtoDTOConversions(Map<Long, ProjectedTopologyEntity> projectedTOs,
                                          List<Integer> types) {
        Assert.assertEquals(1, projectedTOs.size());
        ProjectedTopologyEntity projectedTopologyEntity =
                projectedTOs.entrySet().iterator().next().getValue();
        Assert.assertNotNull(projectedTopologyEntity.getEntity());
        final List<CommoditySoldDTO> projectedSoldList =
                projectedTopologyEntity.getEntity().getCommoditySoldListList();
        Assert.assertNotNull(projectedSoldList);

        for (int type: types) {
            final Optional<CommoditySoldDTO> projectedSoldDTO =
                    projectedTopologyEntity.getEntity().getCommoditySoldListList().stream()
                            .filter(e -> e.getCommodityType().getType() == type)
                            .findFirst();
            projectedTopologyEntity.getEntity().getCommoditySoldList(0);
            Assert.assertTrue(projectedSoldDTO.isPresent());
            double newCapacity = OLD_TIER_CAPACITY * 2;
            Assert.assertEquals(newCapacity, projectedSoldDTO.get().getCapacity(), DELTA);
        }

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
