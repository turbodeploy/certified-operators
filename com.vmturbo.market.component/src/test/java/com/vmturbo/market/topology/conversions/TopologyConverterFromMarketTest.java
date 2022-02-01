package com.vmturbo.market.topology.conversions;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_ENTITY_VALUE;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyByte;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.longThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.util.JsonFormat;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.ChangeProviderExplanationTypeCase;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.utils.CommodityTypeAllocatorConstants;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.runner.MarketMode;
import com.vmturbo.market.runner.reservedcapacity.ReservedCapacityResults;
import com.vmturbo.market.runner.wastedfiles.WastedFilesResults;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.market.topology.conversions.CommodityIndex.CommodityIndexFactory;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator;
import com.vmturbo.market.topology.conversions.cloud.CloudActionSavingsCalculator.CalculatedSavings;
import com.vmturbo.mediation.hybrid.cloud.common.OsType;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.Compliance;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveExplanation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionByDemandTO;
import com.vmturbo.platform.analysis.protobuf.BalanceAccountDTOs.BalanceAccountDTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.AnalysisResults.NewShoppingListToBuyerEntry;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Unit tests for {@link TopologyConverter}.
 */
public class TopologyConverterFromMarketTest {

    private static final long PM_OID = 10000L;
    private static final long VM_OID = 10001L;
    private static final long CLONE_VM_OID = 10002L;
    private static final long VM2_OID = 10003L;
    private static final long DS_OID = 20000L;
    private static final long DA_OID = 30000L;
    private static final long SC_OID = 40000L;
    private static final long VOLUME_ID = 10L;
    private static final long POD_ID = 11L;
    private static final Float SCALING_FACTOR = 1.5F;
    private static final Float RAW_PM_USED = 0.5F;
    private static final Float RAW_VM_USED = 0.2177F;
    private static final Float RAW_VM_RESERVED = 0.25F;
    private static final Float RAW_PM_CAPACITY = 2.0F;
    private static final Float MARKET_PM_USED = RAW_PM_USED * SCALING_FACTOR;
    private static final Float MARKET_VM_USED = RAW_VM_USED * SCALING_FACTOR;
    private static final Float MARKET_PM_CAPACITY = RAW_PM_CAPACITY * SCALING_FACTOR;
    private static final long CLOUD_VM_OID = 1;
    private static final long CLOUD_NEW_COMPUTE_TIER_OID = 111;
    private static final long CLOUD_COMPUTE_TIER_OID = 222;
    private static final double OLD_TIER_CAPACITY = 50;
    private static final float VM_VCPU_CAPACITY = 20;
    private static final double DELTA = 0.001d;
    private static final float THRUGHPUT_USED = 30;
    private static final double VMEM_USAGE = 10;
    private static final int COMM_TYPE1 = 44;
    private static final int COMM_TYPE2 = 45;
    private static final String COMM_KEY1 = "key1";
    private static final String COMM_KEY2 = "key2";

    private final CloudRateExtractor marketCloudRateExtractor = mock(CloudRateExtractor.class);
    private final CommodityConverter mockCommodityConverter = mock(CommodityConverter.class);
    private final CloudCostData mockCCD = mock(CloudCostData.class);
    private TopologyConverter converter;

    private final ReservedCapacityResults reservedCapacityResults = ReservedCapacityResults.EMPTY;

    CommodityType ioType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE).build();

    CommodityType vMemType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VMEM_VALUE).build();

    CommodityType vStorageType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.VSTORAGE_VALUE).build();

    private static final TopologyInfo REALTIME_TOPOLOGY_INFO =
            TopologyInfo.newBuilder().setTopologyType(TopologyType.REALTIME).build();
    private static final TopologyInfo PLAN_TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.PLAN).setPlanInfo(PlanTopologyInfo.newBuilder().build()).build();

    private CommodityType topologyCommodity1;
    private CommodityType topologyCommodity2;

    private static final NumericIDAllocator ID_ALLOCATOR = new NumericIDAllocator();
    private TierExcluderFactory tierExcluderFactory = mock(TierExcluderFactory.class);
    private ConsistentScalingHelperFactory consistentScalingHelperFactory =
            mock(ConsistentScalingHelperFactory.class);
    private ReversibilitySettingFetcher reversibilitySettingFetcher =
            mock(ReversibilitySettingFetcher.class);
    private AnalysisConfig analysisConfig = mock(AnalysisConfig.class);

    private static final int BICLIQUE_TYPE_ID = ID_ALLOCATOR.allocate("BICLIQUE",
            CommodityTypeAllocatorConstants.ACCESS_COMM_TYPE_START_COUNT);
    private static final int CPU_TYPE_ID = ID_ALLOCATOR.allocate("CPU", 0);
    private static final int ST_AMT_TYPE_ID = ID_ALLOCATOR.allocate("StorageAmount", 0);
    private static final int DSPM_TYPE_ID = ID_ALLOCATOR.allocate("DSPM",
            CommodityTypeAllocatorConstants.ACCESS_COMM_TYPE_START_COUNT);
    private CloudTopology<TopologyEntityDTO> cloudTopology =
            mock(TopologyEntityCloudTopology.class);

    /**
     * The expected exception.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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

        when(analysisConfig.getIncludeVdc()).thenReturn(false);
        when(analysisConfig.getQuoteFactor()).thenReturn(MarketAnalysisUtils.QUOTE_FACTOR);
        when(analysisConfig.getMarketMode()).thenReturn(MarketMode.M2Only);
        when(analysisConfig.getLiveMarketMoveCostFactor()).thenReturn(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR);
        when(analysisConfig.getGlobalSetting(any()))
                .thenReturn(Optional.empty());

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
        constructTopologyConverter(commodityIndexFactory, REALTIME_TOPOLOGY_INFO);
    }

    /**
     * Creates converter with specified topologyInfo.
     *
     * @param commodityIndexFactory The factory.
     * @param topologyInfo Real-time or plan topologyInfo.
     */
    private void constructTopologyConverter(final CommodityIndexFactory commodityIndexFactory,
            final TopologyInfo topologyInfo) {
        final TopologyConverter topologyConverter = new TopologyConverter(
            topologyInfo,
            marketCloudRateExtractor,
            mockCommodityConverter,
            mockCCD,
            commodityIndexFactory,
            tierExcluderFactory,
            consistentScalingHelperFactory, cloudTopology, reversibilitySettingFetcher,
            analysisConfig);
        topologyConverter.setConvertToMarketComplete();
        converter = Mockito.spy(topologyConverter);
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
        shoppingListMap.put(VM_OID,
                        new ShoppingListInfo(DSPM_TYPE_ID, DS_OID, PM_OID, Collections.emptySet(),
                                        null, EntityType.PHYSICAL_MACHINE_VALUE,
                                        topologyDSPMBought));
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
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
        shoppingListMap.put(VM_OID, new ShoppingListInfo(2, DS_OID, PM_OID, Collections.emptySet(),
                        null, EntityType.PHYSICAL_MACHINE_VALUE, topologyDSPMBought));
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
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
        Map<Long, TopologyEntityDTO> origTopoMap = ImmutableMap.of(
                dsTopo.getOid(), dsTopo,
                daTopo.getOid(), daTopo,
                // Also add mappings for cloned traders
                dsTopo.getOid() + 100L, dsTopo,
                daTopo.getOid() + 100L, daTopo);

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
        WastedFilesResults wastedFilesAnalysisMock = mock(WastedFilesResults.class);
        // Set up mock for any call other than DS
        when(wastedFilesAnalysisMock.getMbReleasedOnProvider(anyLong()))
                .thenReturn(OptionalLong.empty());
        // Set up mock for DS
        OptionalLong wastedFileSizeInMB = OptionalLong.of(2L);
        when(wastedFilesAnalysisMock.getMbReleasedOnProvider(DS_OID)).thenReturn(wastedFileSizeInMB);

        // 5. Call TC.convertFromMarket
        List<TraderTO> traderTOs =
                Lists.newArrayList(traderDS, traderDA, traderDSClone, traderDAClone);
        Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedTopo = converter.convertFromMarket(
                traderTOs, origTopoMap, PriceIndexMessage.getDefaultInstance(),
                reservedCapacityResults, wastedFilesAnalysisMock);
        // 6. Assert shoppingListInfo map
        assertNotNull(converter.getShoppingListOidToInfos().get(-DA_OID));
        assertNotNull(projectedTopo.get(DA_OID + 100L));

        // mock action savings
        final CloudActionSavingsCalculator savingsCalculator =
                mock(CloudActionSavingsCalculator.class);
        when(savingsCalculator.calculateSavings(any())).thenReturn(CalculatedSavings.NO_SAVINGS_USD);

        // Call interpret
        // Create ActionTO for provision by demand
        ActionTO provByDemandTO = ActionTO.newBuilder().setImportance(0).setIsNotExecutable(false)
                .setProvisionByDemand(ProvisionByDemandTO.newBuilder()
                        .setModelBuyer(-DS_OID).setModelSeller(DA_OID)
                        .setProvisionedSeller(DA_OID + 100L).build())
                .build();
        List<Action> provByDemandList =
                converter.interpretAction(provByDemandTO, projectedTopo, null, savingsCalculator);
        assertTrue(!provByDemandList.isEmpty());
        assertNotNull(provByDemandList.get(0));

        //assert for wasted file actions
        Optional<CommoditySoldDTO> daStorageAmtCommSold = projectedTopo.get(traderDS.getOid()).getEntity().getCommoditySoldListList().stream()
                .filter(commSold -> commSold.getCommodityType().getType()
                        == CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                .findFirst();

        // Check original entity has reduced st amt value
        assertTrue(daStorageAmtCommSold.isPresent());
        assertEquals(originalStorageAmtUsed - 2L,
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
        long volume1oid = 21l;
        long volume2oid = 22l;
        long storageTierOid = 3l;
        long vmOid = 4l;
        TopologyDTO.TopologyEntityDTO az = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE).setOid(azOid).build();
        TopologyDTO.TopologyEntityDTO storageTier = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.STORAGE_TIER_VALUE).setOid(storageTierOid)
                .build();
        TopologyDTO.TopologyEntityDTO volume1 = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE).setOid(volume1oid)
                .setDisplayName("volume1").setEnvironmentType(EnvironmentType.CLOUD)
                .build();
        TopologyDTO.TopologyEntityDTO volume2 = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE).setOid(volume2oid)
                .setDisplayName("volume2").setEnvironmentType(EnvironmentType.CLOUD)
                .build();
        CommoditiesBoughtFromProvider sameCommBoughtGroupingFor2DifferentSLs = CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(storageTierOid)
                        .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE))
                                .setUsed(30))
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE))
                                .setUsed(40)).build();

        // Use reflection and these entities to the entityOidToDto map
        Field entityOidToDto = TopologyConverter.class.getDeclaredField("entityOidToDto");
        Map<Long, TopologyEntityDTO> map =
                ImmutableMap.of(azOid, az, storageTierOid, storageTier, volume1oid, volume1, volume2oid, volume2);
        Map<Long, TopologyEntityDTO> volumeMap = new HashMap<>();
        volumeMap.put(volume1oid, volume1);
        volumeMap.put(volume2oid, volume2);
        entityOidToDto.setAccessible(true);
        entityOidToDto.set(converter, map);

        Map<CommoditiesBoughtFromProvider, Map<ShoppingListTO, ShoppingListInfo>> commBought2shoppingList =
                new HashMap<>();
        Map<ShoppingListTO, ShoppingListInfo> slMapping = new HashMap<>();

        // Act
        long sl1Oid = 222;
        slMapping.put(ShoppingListTO.newBuilder()
                .setOid(sl1Oid)
                .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                                .setType(1)
                                .setBaseType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE))
                        .setAssignedCapacityForBuyer(1000))
                .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                                .setType(2)
                                .setBaseType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE))
                        .setAssignedCapacityForBuyer(20))
                .build(),
            new ShoppingListInfo(sl1Oid, vmOid, null,
                            Collections.singleton(volume1oid), null,
                            EntityType.STORAGE_TIER_VALUE, Collections.emptyList()));
        commBought2shoppingList.put(sameCommBoughtGroupingFor2DifferentSLs, slMapping);

        long sl2Oid = 333;
        slMapping.put(ShoppingListTO.newBuilder()
                        .setOid(sl2Oid)
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO.newBuilder()
                                        .setType(1)
                                        .setBaseType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE))
                                .setAssignedCapacityForBuyer(1000))
                        .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO.newBuilder()
                                        .setType(2)
                                        .setBaseType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE))
                                .setAssignedCapacityForBuyer(20))
                        .build(),
                new ShoppingListInfo(sl2Oid, vmOid, null,
                        Collections.singleton(volume2oid), null,
                        EntityType.STORAGE_TIER_VALUE, Collections.emptyList()));

        commBought2shoppingList.put(sameCommBoughtGroupingFor2DifferentSLs, slMapping);
        TopologyDTO.TopologyEntityDTO.Builder vm = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).setOid(vmOid)
                        // leg1
                        .addCommoditiesBoughtFromProviders(sameCommBoughtGroupingFor2DifferentSLs)
                        // leg2
                        .addCommoditiesBoughtFromProviders(sameCommBoughtGroupingFor2DifferentSLs)
                        .addConnectedEntityList(
                                        ConnectedEntity.newBuilder().setConnectedEntityId(azOid)
                                                        .setConnectedEntityType(az.getEntityType())
                                                        .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION));
        Set<TopologyEntityDTO> resources = converter.createResources(vm, commBought2shoppingList);
        // Assert that the projected volume is connected to the storage and AZ which the VM is
        // connected to
        assertEquals(2, resources.size());
        for (TopologyEntityDTO projectedVolume : resources) {
            TopologyEntityDTO volume = volumeMap.get(projectedVolume.getOid());
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

            // Market can assign new capacities for various commodities. The new value is set in
            // the assignedCapacityForBuyer field of the corresponding commodityBought of the shopping list.
            // This value needs to be set in the capacity field of the volume commodity sold.
            List<CommoditySoldDTO> commSold = projectedVolume.getCommoditySoldListList();
            Optional<CommoditySoldDTO> storageAccessCommSold =
                    commSold.stream().filter(c -> c.getCommodityType().getType()
                            == CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE).findFirst();
            assertTrue(storageAccessCommSold.isPresent());
            assertEquals(1000, storageAccessCommSold.get().getCapacity(), 0);
            Optional<CommoditySoldDTO> storageAmountCommSold =
                    commSold.stream().filter(c -> c.getCommodityType().getType()
                            == CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE).findFirst();
            assertTrue(storageAmountCommSold.isPresent());
            // 20GB = 20480MB
            assertEquals(20480, storageAmountCommSold.get().getCapacity(), 0);
        }
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
                .setUsed(RAW_VM_USED).setReservedCapacity(RAW_VM_RESERVED).setScalingFactor(SCALING_FACTOR).build());
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
        final TopologyConverter topologyConverter = new TopologyConverter(REALTIME_TOPOLOGY_INFO,
            false, MarketAnalysisUtils.QUOTE_FACTOR, MarketMode.M2Only, MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
            marketCloudRateExtractor, mockCommodityConverter, indexFactory, tierExcluderFactory,
            consistentScalingHelperFactory, reversibilitySettingFetcher, MarketAnalysisUtils.PRICE_WEIGHT_SCALE,
            false, false, false, 0.5f);
        topologyConverter.setConvertToMarketComplete();
        TopologyConverter converter = Mockito.spy(topologyConverter);

        // warning: introspection follows...
        Map<Long, ShoppingListInfo> shoppingListMap = new HashMap<>();
        shoppingListMap.put(VM_OID, new ShoppingListInfo(DSPM_TYPE_ID, DS_OID, PM_OID, VOLUME_ID,
                null, EntityType.PHYSICAL_MACHINE_VALUE, topologyDSPMBought));
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

        // Clone PM test
        final long cloneId = 6767;
        final EconomyDTOs.TraderTO clonePM = EconomyDTOs.TraderTO.newBuilder().setOid(cloneId)
            .addCommoditiesSold(economyCPUSold).setCloneOf(PM_OID).build();

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
                Lists.newArrayList(pmTrader, vmTrader, clonePM),
                // map back to original TopologyEntityDTOs
                ImmutableMap.of(expectedEntity.getOid(), expectedEntity,
                        expectedEntity2.getOid(), expectedEntity2),
                PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
                setUpWastedFileAnalysis());

        // Assert three entities returned - they will be in the original order - VM and PM and PMClone
        assertEquals(3L, entity.size());
        // check that the PM expected capacity and used matches the actual converted capacity and used
        CommoditySoldDTO actualCpuCommodity =
                entity.get(pmTrader.getOid()).getEntity().getCommoditySoldList(0);
        assertEquals(topologyCPUSold.getCapacity(), actualCpuCommodity.getCapacity(), epsilon);
        assertEquals(topologyCPUSold.getUsed(), actualCpuCommodity.getUsed(), epsilon);
        // check clone pm
        final CommoditySoldDTO actualCpuCommodityOfClone =
                entity.get(clonePM.getOid()).getEntity().getCommoditySoldList(0);
        assertEquals(topologyCPUSold.getCapacity(), actualCpuCommodityOfClone.getCapacity(), epsilon);
        assertEquals(topologyCPUSold.getUsed(), actualCpuCommodityOfClone.getUsed(), epsilon);
        // check that the VM expected used matches the actual converted used
        assertEquals(1, entity.get(vmTrader.getOid()).getEntity()
                .getCommoditiesBoughtFromProvidersList().size());
        final List<CommodityBoughtDTO> actualBoughtCommodityList = entity.get(vmTrader.getOid())
                .getEntity().getCommoditiesBoughtFromProvidersList().iterator().next()
                .getCommodityBoughtList();
        assertEquals(1, actualBoughtCommodityList.size());
        assertEquals(RAW_VM_USED, actualBoughtCommodityList.iterator().next().getUsed(), epsilon);
        assertEquals((double)SCALING_FACTOR, actualBoughtCommodityList.iterator().next().getScalingFactor(), TopologyConverter.EPSILON);

        // Assert when reserved capacity is larger than used.
        converter.setUseVMReservationAsUsed(true);

        Field commoditiesWithReservationGreaterThanUsed = TopologyConverter.class.getDeclaredField("commoditiesWithReservationGreaterThanUsed");
        commoditiesWithReservationGreaterThanUsed.setAccessible(true);
        commoditiesWithReservationGreaterThanUsed.set(converter, ImmutableMap.of(VM_OID, ImmutableMap.of(topologyCpuBought.get(0), PM_OID)));

        Field entityOidToDto = TopologyConverter.class.getDeclaredField("entityOidToDto");
        Map<Long, TopologyEntityDTO> map = new HashMap<>();
        map.put(expectedEntity.getOid(), expectedEntity);
        map.put(expectedEntity2.getOid(), expectedEntity2);
        entityOidToDto.setAccessible(true);
        entityOidToDto.set(converter, map);

        // Act
        entity = converter.convertFromMarket(
            // the traders in the market
            Lists.newArrayList(pmTrader, vmTrader, clonePM),
            // map back to original TopologyEntityDTOs
            ImmutableMap.of(expectedEntity.getOid(), expectedEntity,
                expectedEntity2.getOid(), expectedEntity2),
            PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
            setUpWastedFileAnalysis());

        // check that the PM expected capacity and used matches the actual converted capacity and used
        actualCpuCommodity = entity.get(pmTrader.getOid()).getEntity().getCommoditySoldList(0);
        assertEquals(topologyCPUSold.getCapacity(), actualCpuCommodity.getCapacity(), epsilon);
        assertEquals(topologyCPUSold.getUsed() - (RAW_VM_RESERVED - RAW_VM_USED), actualCpuCommodity.getUsed(), epsilon);
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

        final TopologyConverter topologyConverter = new TopologyConverter(
            TopologyInfo.newBuilder().setTopologyType(TopologyType.PLAN).build(),
            marketCloudRateExtractor, mockCommodityConverter, mockCCD, indexFactory,
            tierExcluderFactory, consistentScalingHelperFactory, cloudTopology,
            reversibilitySettingFetcher, analysisConfig);
        topologyConverter.setConvertToMarketComplete();
        TopologyConverter converter = Mockito.spy(topologyConverter);

        // warning: introspection follows...
        Map<Long, ShoppingListInfo> shoppingListMap = new HashMap<>();
        shoppingListMap.put(VM_OID, new ShoppingListInfo(DSPM_TYPE_ID, DS_OID, PM_OID, VOLUME_ID, null,
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
                setUpWastedFileAnalysis());

        assertThat(entity.get(VM_OID).getEntity().getTypeSpecificInfo(),
                is(originalVm.getTypeSpecificInfo()));
    }

    /**
     * Migrating VMs may change OS. The destination OS is indicated by properties added
     * to the migrating entity DTO. Verify that these are applied correctly.
     */
    @Test
    public void testMigratedOSUpdate() {
        final TopologyDTO.TopologyEntityDTO.Builder vmBuilder = TopologyDTO.TopologyEntityDTO
            .newBuilder()
            .setOid(VM_OID).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setVirtualMachine(
                VirtualMachineInfo.newBuilder()
                    .setNumCpus(100)
                    .setGuestOsInfo(OS.newBuilder()
                        .setGuestOsType(OSType.LINUX)
                        .setGuestOsName("Linux")
                    )
                )
            ).putEntityPropertyMap(
                StringConstants.PLAN_NEW_OS_TYPE_PROPERTY, OsType.RHEL.getDtoOS().name()
            ).putEntityPropertyMap(
                StringConstants.PLAN_NEW_OS_NAME_PROPERTY, OsType.RHEL.getDisplayName()
            );

        converter.updateProjectedEntityOsType(vmBuilder);

        OS osInfo = vmBuilder.build().getTypeSpecificInfo().getVirtualMachine()
            .getGuestOsInfo();

        assertEquals(OSType.RHEL, osInfo.getGuestOsType());
        assertEquals(OsType.RHEL.getDisplayName(), osInfo.getGuestOsName());
    }

    /**
     * Projected costs for VMs may include licensing that depends on the number of cores.
     * If the VM is buying from a new compute tier, the VMs number of cores should be updated
     * to match that of the new tier.
     */
    @Test
    public void testMigratedVMCoreUpdate() {
        final int vmOldNumberOfCpus = 100;
        final int newComputeTierCpus = 42;

        TopologyDTO.TopologyEntityDTO newTierDTO = createEntityDTO(CLOUD_NEW_COMPUTE_TIER_OID,
            EntityType.COMPUTE_TIER_VALUE, Collections.EMPTY_LIST, 0)
            .toBuilder()
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setComputeTier(ComputeTierInfo.newBuilder()
                    .setNumCores(newComputeTierCpus)
                    .build())
            ).build();

        CommoditySoldTO x = CommoditySoldTO.getDefaultInstance();

        when(mockCommodityConverter.createCommoditySoldTO(Mockito.anyObject(), Mockito.anyFloat(),
            Mockito.anyFloat(), Mockito.anyObject())).thenReturn(x);

        // Prime the topology converter with the compute tier by converting it,
        // so that it can be looked up in the new topology.
        converter.convertToMarket(ImmutableMap.of(CLOUD_NEW_COMPUTE_TIER_OID, newTierDTO),
            Collections.EMPTY_SET);

        // This VM had 100 CPUs, but is migrating to the new compute tier, which has 42
        final TopologyDTO.TopologyEntityDTO.Builder vmBuilder = TopologyDTO.TopologyEntityDTO
            .newBuilder()
            .setOid(VM_OID).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualMachine(VirtualMachineInfo.newBuilder().setNumCpus(vmOldNumberOfCpus)))
            .addCommoditiesBoughtFromProviders(
                CommoditiesBoughtFromProvider.newBuilder()
                    .setProviderId(CLOUD_NEW_COMPUTE_TIER_OID)
                    .setProviderEntityType(EntityType.COMPUTE_TIER_VALUE)
                    .build()
            );

        converter.updateProjectedCores(vmBuilder);

        assertEquals(newComputeTierCpus,
            vmBuilder.build().getTypeSpecificInfo().getVirtualMachine().getNumCpus());
    }

    /**
     * Validate that the creation of BApp and BTx is skipped.
     */
    @Test
    public void testSkipBAppAndBTxEntities() {

        TopologyDTO.TopologyEntityDTO bAppDTO = createEntityDTO(1000,
                EntityType.BUSINESS_APPLICATION_VALUE, Collections.EMPTY_LIST, 0);

        TopologyDTO.TopologyEntityDTO bTxDTO = createEntityDTO(1001,
                EntityType.BUSINESS_APPLICATION_VALUE, Collections.EMPTY_LIST, 0);

        CommoditySoldTO x = CommoditySoldTO.getDefaultInstance();

        when(mockCommodityConverter.createCommoditySoldTO(Mockito.anyObject(), Mockito.anyFloat(),
                Mockito.anyFloat(), Mockito.anyObject())).thenReturn(x);

        Collection<TraderTO> traderTOs = converter.convertToMarket(ImmutableMap.of(1000L, bAppDTO,
                1001L, bTxDTO),
                Collections.EMPTY_SET);
        assertTrue(traderTOs.size() == 0);
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
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
                    PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
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
                    PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityResults, setUpWastedFileAnalysis());

            assertEquals(1, entity.get(VM_OID).getEntity().getCommoditySoldListCount());
            assertEquals(commSold, entity.get(VM_OID).getEntity().getCommoditySoldList(0));
        } catch (RuntimeException e) {

        }
    }

    private WastedFilesResults setUpWastedFileAnalysis() {
        WastedFilesResults wastedFilesAnalysisMock = mock(WastedFilesResults.class);
        when(wastedFilesAnalysisMock.getMbReleasedOnProvider(anyLong())).thenReturn(OptionalLong.empty());
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
        final TopologyConverter topologyConverter = new TopologyConverter(topoInfo,
            marketCloudRateExtractor, mockCommodityConverter, mockCCD, indexFactory,
            tierExcluderFactory, consistentScalingHelperFactory, cloudTopology,
            reversibilitySettingFetcher, analysisConfig);
        topologyConverter.setConvertToMarketComplete();
        TopologyConverter converter = Mockito.spy(topologyConverter);
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
                        PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
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
        final TopologyConverter topologyConverter = new TopologyConverter(topoInfo,
            marketCloudRateExtractor, mockCommodityConverter, mockCCD, indexFactory,
            tierExcluderFactory, consistentScalingHelperFactory, cloudTopology,
            reversibilitySettingFetcher, analysisConfig);
        topologyConverter.setConvertToMarketComplete();
        TopologyConverter converter = Mockito.spy(topologyConverter);
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
                        PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
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
     * Ensure we throw an IllegalArgumentException if we create a CommodityIndex before
     * the CommodityConverter has the required data.
     */
    @Test
    public void testIllegalCommodityConverterConstruction() {
        final CommodityIndex mockIndex = mock(CommodityIndex.class);
        final CommodityIndexFactory indexFactory = mock(CommodityIndexFactory.class);
        doReturn(mockIndex).when(indexFactory).newIndex();

        expectedException.expect(IllegalStateException.class);
        final TopologyConverter topologyConverter = new TopologyConverter(
            REALTIME_TOPOLOGY_INFO,
            marketCloudRateExtractor,
            mockCommodityConverter,
            mockCCD,
            indexFactory,
            tierExcluderFactory,
            consistentScalingHelperFactory, cloudTopology, reversibilitySettingFetcher,
            analysisConfig);
        topologyConverter.getCommodityIndex();
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
            Mockito.eq(poolCpuCommType));

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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
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
                PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
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

    /**
     * Test that {@link TopologyConverter#convertFromMarket(List, Map, PriceIndexMessage, ReservedCapacityResults, WastedFilesResults)}
     * method preserved original commodities bought by VMs from Volumes.
     */
    @Test
    public void testConvertFromMarketPreservesCommoditiesBoughtByVmsFromVolumes() {
        TopologyEntityDTO region = TopologyEntityDTO.newBuilder().setOid(73442089143124L).setEntityType(EntityType.REGION_VALUE).build();
        TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder().setOid(15678904L).setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).build();
        AccountPricingData accountPricingData = mock(AccountPricingData.class);
        TopologyEntityDTO serviceProvider = TopologyEntityDTO.newBuilder().setOid(5678974832L).setEntityType(EntityType.SERVICE_PROVIDER_VALUE).build();
        when(mockCCD.getAccountPricingData(businessAccount.getOid())).thenReturn(Optional.of(accountPricingData));
        when(cloudTopology.getServiceProvider(businessAccount.getOid())).thenReturn(Optional.of(serviceProvider));
        when(cloudTopology.getRegionsFromServiceProvider(serviceProvider.getOid())).thenReturn(new HashSet(Collections.singleton(region)));
        final long vmOid = 1L;
        final long volume1Oid = 2L;
        final long volume2Oid = 3L;
        final long storageTierOid = 4L;
        final long storageAmountCapacity = 2000;
        final Origin volumeOrigin = Origin.newBuilder().build();
        final TopologyDTO.TopologyEntityDTO originalVm = TopologyEntityDTO.newBuilder()
                .setOid(vmOid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setDisplayName("")
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                                .build())
                            .build())
                        .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .setProviderId(volume1Oid))
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                            .build())
                        .build())
                        .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .setProviderId(volume2Oid))
                .setAnalysisSettings(AnalysisSettings.newBuilder())
                .build();
        final CommodityType commodityType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
            .build();
        final TopologyDTO.TopologyEntityDTO originalVolume1 =
                createVirtualVolume(false, commodityType, volume1Oid,
                        storageTierOid, volumeOrigin, storageAmountCapacity);
        final TopologyDTO.TopologyEntityDTO originalVolume2 =
                createVirtualVolume(false, commodityType, volume2Oid,
                        storageTierOid, volumeOrigin, storageAmountCapacity);
        final TopologyDTO.TopologyEntityDTO storageTier = TopologyEntityDTO.newBuilder()
            .setOid(storageTierOid)
            .setEntityType(EntityType.STORAGE_TIER_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(commodityType)
                .build())
            .build();
        final Map<Long, TopologyDTO.TopologyEntityDTO> originalTopology = ImmutableMap.of(
                vmOid, originalVm,
                storageTierOid, storageTier,
                volume1Oid, originalVolume1,
                volume2Oid, originalVolume2,
                businessAccount.getOid(), businessAccount);
        when(cloudTopology.getAggregated(region.getOid(), TopologyConversionConstants.cloudTierTypes)).thenReturn(Collections.singleton(storageTier));
        final CommoditySpecificationTO commoditySpecificationTO =
            CommoditySpecificationTO.newBuilder()
            .setBaseType(1111)
            .setType(1112)
            .build();
        Mockito.doReturn(Collections.singletonList(commoditySpecificationTO)).when(mockCommodityConverter)
            .commoditySpecification(any(), anyByte());

        Mockito.doReturn(Optional.of(commodityType)).when(mockCommodityConverter).marketToTopologyCommodity(any());

        Mockito.doReturn(commodityType).when(mockCommodityConverter)
            .commodityIdToCommodityType(anyInt());

        final Collection<TraderTO> traders = converter.convertToMarket(originalTopology);

        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                new ArrayList<>(traders), originalTopology, PriceIndexMessage.getDefaultInstance(),
                reservedCapacityResults, setUpWastedFileAnalysis());

        final ProjectedTopologyEntity projectedVm = entity.get(vmOid);
        assertNotNull(projectedVm);
        final Set<Long> projectedProviders = projectedVm.getEntity()
                .getCommoditiesBoughtFromProvidersList().stream()
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .collect(Collectors.toSet());
        assertEquals(ImmutableSet.of(volume1Oid, volume2Oid), projectedProviders);

        final ProjectedTopologyEntity projectedVolume1 = entity.get(volume1Oid);
        final List<CommoditySoldDTO> commoditySoldDTOS = projectedVolume1.getEntity()
            .getCommoditySoldListList();
        Assert.assertFalse(commoditySoldDTOS.isEmpty());
        Assert.assertEquals(volumeOrigin, projectedVolume1.getEntity().getOrigin());

        final CommoditySoldDTO commoditySoldDTO = commoditySoldDTOS.iterator().next();
        Assert.assertEquals(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
            commoditySoldDTO.getCommodityType().getType());
        Assert.assertEquals(storageAmountCapacity, commoditySoldDTO.getCapacity(), 0.1);
        // Storage Amount projected commodity should not have historicalUsed and percentile set
        // since the original Storage Amount commodity did not have historicalUsed and percentile
        // set
        Assert.assertFalse(commoditySoldDTO.hasHistoricalUsed());
        Assert.assertFalse(commoditySoldDTO.getHistoricalUsed().hasPercentile());

        // Check projected volume bought commodities
        final List<CommoditiesBoughtFromProvider> commBoughtList = projectedVolume1.getEntity()
                .getCommoditiesBoughtFromProvidersList();
        Assert.assertEquals(1, commBoughtList.size());
        Assert.assertEquals(storageAmountCapacity,
                commBoughtList.get(0).getCommodityBought(0).getUsed(), DELTA);
    }

    /**
     * Test that {@link TopologyConverter#convertFromMarket(List, Map, PriceIndexMessage, ReservedCapacityResults, WastedFilesResults)}
     * method preserved original commodities bought by VMs from Volumes. This verifies that
     * ephemeral volumes are ignored.
     */
    @Test
    public void testConvertFromMarketPreservesCommoditiesBoughtByVmsIgnoringEphemeralVolume() {
        TopologyEntityDTO region = TopologyEntityDTO.newBuilder().setOid(73442089143124L).setEntityType(EntityType.REGION_VALUE).build();
        TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder().setOid(15678904L).setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).build();
        AccountPricingData accountPricingData = mock(AccountPricingData.class);
        TopologyEntityDTO serviceProvider = TopologyEntityDTO.newBuilder().setOid(5678974832L).setEntityType(EntityType.SERVICE_PROVIDER_VALUE).build();
        when(mockCCD.getAccountPricingData(businessAccount.getOid())).thenReturn(Optional.of(accountPricingData));
        when(cloudTopology.getServiceProvider(businessAccount.getOid())).thenReturn(Optional.of(serviceProvider));
        when(cloudTopology.getRegionsFromServiceProvider(serviceProvider.getOid())).thenReturn(new HashSet(Collections.singleton(region)));
        constructTopologyConverter(CommodityIndex.newFactory(), PLAN_TOPOLOGY_INFO);
        final long vmOid = 1L;
        final long volume1Oid = 2L;
        final long ephemeralVolumeOid = 3L;
        final long storageTierOid = 4L;
        final long storageAmountCapacity = 2000L;
        final Origin volumeOrigin = Origin.newBuilder().build();
        final TopologyDTO.TopologyEntityDTO originalVm = TopologyEntityDTO.newBuilder()
                .setOid(vmOid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setDisplayName("")
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                            .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)))
                        .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .setProviderId(volume1Oid))
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                    .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)))
                        .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .setProviderId(ephemeralVolumeOid))
                .setAnalysisSettings(AnalysisSettings.newBuilder())
                .build();
        final CommodityType commodityType = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
            .build();
        final TopologyDTO.TopologyEntityDTO originalVolume1 =
                createVirtualVolume(false, commodityType, volume1Oid,
                        storageTierOid, volumeOrigin, storageAmountCapacity);
        final TopologyDTO.TopologyEntityDTO originalEphemeralVolume =
                createVirtualVolume(true, commodityType, ephemeralVolumeOid,
                        storageTierOid, volumeOrigin, storageAmountCapacity);
        final TopologyDTO.TopologyEntityDTO storageTier = TopologyEntityDTO.newBuilder()
            .setOid(storageTierOid)
            .setEntityType(EntityType.STORAGE_TIER_VALUE)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(commodityType)
                .build())
            .build();
        final Map<Long, TopologyDTO.TopologyEntityDTO> originalTopology = ImmutableMap.of(
                vmOid, originalVm,
                storageTierOid, storageTier,
                volume1Oid, originalVolume1,
                ephemeralVolumeOid, originalEphemeralVolume,
                businessAccount.getOid(), businessAccount);
        when(cloudTopology.getAggregated(region.getOid(), TopologyConversionConstants.cloudTierTypes)).thenReturn(Collections.singleton(storageTier));
        final CommoditySpecificationTO commoditySpecificationTO =
            CommoditySpecificationTO.newBuilder()
            .setBaseType(1111)
            .setType(1112)
            .build();
        Mockito.doReturn(Collections.singletonList(commoditySpecificationTO)).when(mockCommodityConverter)
            .commoditySpecification(any(), anyByte());
        Mockito.doReturn(Optional.of(commodityType)).when(mockCommodityConverter).marketToTopologyCommodity(any());
        Mockito.doReturn(commodityType).when(mockCommodityConverter)
            .commodityIdToCommodityType(anyInt());

        // Test code
        final Collection<TraderTO> traders = converter.convertToMarket(originalTopology);
        final Map<Long, TopologyDTO.ProjectedTopologyEntity> entity = converter.convertFromMarket(
                new ArrayList<>(traders), originalTopology, PriceIndexMessage.getDefaultInstance(),
                reservedCapacityResults, setUpWastedFileAnalysis());

        // Verify results
        final ProjectedTopologyEntity projectedVm = entity.get(vmOid);
        assertNotNull(projectedVm);
        final Set<Long> projectedProviders = projectedVm.getEntity()
                .getCommoditiesBoughtFromProvidersList().stream()
                .map(CommoditiesBoughtFromProvider::getProviderId)
                .collect(Collectors.toSet());
        // Ensure that the ephemeral volume is not included.
        assertEquals(ImmutableSet.of(volume1Oid), projectedProviders);
    }

    private static TopologyEntityDTO createVirtualVolume(
            final boolean isEphemeral,
            @Nonnull final CommodityType commodityType,
            final long volumeOid,
            final long storageTierOid,
            @Nonnull final Origin volumeOrigin,
            final long storageAmountCapacity) {
        final TopologyDTO.TopologyEntityDTO.Builder volumeBuilder = TopologyEntityDTO.newBuilder()
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(commodityType)
                                .setUsed(storageAmountCapacity))
                        .setProviderEntityType(EntityType.STORAGE_TIER_VALUE)
                        .setProviderId(storageTierOid))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCapacity(storageAmountCapacity)
                        .setCommodityType(commodityType))
                .setOid(volumeOid)
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setOrigin(volumeOrigin);
        if (isEphemeral) {
            volumeBuilder.setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                    .setIsEphemeral(true)));
        }
        return volumeBuilder.build();
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
                        Mockito.anyObject());

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

    /**
     * Tests getNewlyConnectedProjectedEntitiesFromOriginalTopo to ensure new connections are
     * made appropriately.
     */
    @Test
    public void testGetNewlyConnectedProjectedEntitiesFromOriginalTopo() {
        final long baOid = 1L;
        final long vmOid = 2L;
        final long regionOid = 3L;

        TopologyDTO.TopologyEntityDTO ba = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(baOid)
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .build();

        TopologyDTO.TopologyEntityDTO vm = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(vmOid)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        // create trader DTO corresponds to originalEntity
        EconomyDTOs.TraderTO trader = TraderTO.newBuilder()
                .setOid(vmOid)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addShoppingLists(ShoppingListTO.newBuilder().setOid(vmOid)
                        .setContext(Context.newBuilder()
                                .setRegionId(regionOid)
                                .setBalanceAccount(BalanceAccountDTO.newBuilder()
                                        .setId(baOid)
                                        .build()).build()).build()).build();


        DiscountApplicator discountApplicator = mock(DiscountApplicator.class);
        converter.getCloudTc().insertIntoAccountPricingDataByBusinessAccountOidMap(baOid,
                new AccountPricingData(discountApplicator,
                        com.vmturbo.common.protobuf.cost.Pricing.PriceTable.newBuilder().getDefaultInstanceForType(),
                        baOid, 15L, baOid));
        Map<Long, Set<ConnectedEntity>> businessAccountsToNewlyOwnedEntities =
                converter.getCloudTc().getBusinessAccountsToNewlyOwnedEntities(
                        Lists.newArrayList(trader),
                        new HashMap<Long, TopologyDTO.TopologyEntityDTO>() {{
                            put(baOid, ba);
                            put(vmOid, vm);
                        }}, new HashMap<Long, TopologyDTO.TopologyEntityDTO>() {{
                            put(baOid, ba);
                        }});
        assertFalse(businessAccountsToNewlyOwnedEntities.isEmpty());
        Set<ConnectedEntity> connectedEntities = businessAccountsToNewlyOwnedEntities.get(baOid);
        assertEquals(1, connectedEntities.size());
        ConnectedEntity connectedEntity = connectedEntities.iterator().next();
        assertEquals(ConnectionType.OWNS_CONNECTION, connectedEntity.getConnectionType());
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, connectedEntity.getConnectedEntityType());
        assertEquals(vmOid, connectedEntity.getConnectedEntityId());
    }

    /**
     * Checks if logic to skip shopping list creation for ephemeral storage works or not.
     */
    @Test
    public void skipShoppingListForEphemeralStorage() {
        final TopologyDTO.TopologyEntityDTO ephemeralVolume =
                TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(VOLUME_ID).setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setVirtualVolume(
                        VirtualVolumeInfo.newBuilder().setIsEphemeral(true)))
                .build();
        assertTrue(converter.skipShoppingListCreation(ephemeralVolume, CommoditiesBoughtFromProvider.getDefaultInstance()));

        final TopologyDTO.TopologyEntityDTO nonEphemeralVolume =
                TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setOid(VOLUME_ID).setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .build();
        assertFalse(converter.skipShoppingListCreation(nonEphemeralVolume, CommoditiesBoughtFromProvider.getDefaultInstance()));
    }

    /**
     * Checks if logic to skip shopping list creation for pods works or not.
     */
    @Test
    public void skipShoppingListTestForPods() {
        final TopologyDTO.TopologyEntityDTO pod = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(POD_ID).setEntityType(EntityType.CONTAINER_POD_VALUE).build();
        final CommoditiesBoughtFromProvider computeBought = CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(
                        CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE)))
                .build();
        final CommoditiesBoughtFromProvider storageBought = CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(
                        CommodityType.newBuilder().setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)))
                .build();

        // realtime - no skip
        assertFalse(converter.skipShoppingListCreation(pod, computeBought));
        assertFalse(converter.skipShoppingListCreation(pod, storageBought));
        // plan - skip the storage, but not the compute
        constructTopologyConverter(CommodityIndex.newFactory(), PLAN_TOPOLOGY_INFO);
        assertFalse(converter.skipShoppingListCreation(pod, computeBought));
        assertTrue(converter.skipShoppingListCreation(pod, storageBought));
    }

    /**
     * Checks if logic to skip shopping list creation for vm -> storage commBoughtGrouping works or not.
     */
    @Test
    public void skipShoppingListForVMStorageCommBoughtGrouping() {
        final TopologyDTO.TopologyEntityDTO vm =
            TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(VOLUME_ID).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build();
        assertTrue(converter.skipShoppingListCreation(vm,
            CommoditiesBoughtFromProvider.newBuilder().setProviderEntityType(EntityType.STORAGE_VALUE)
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(
                    CommodityType.newBuilder().setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)))
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(
                    CommodityType.newBuilder().setType(CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE)))
                .build()));

        assertFalse(converter.skipShoppingListCreation(vm,
            CommoditiesBoughtFromProvider.newBuilder().setProviderEntityType(EntityType.STORAGE_VALUE)
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(
                    CommodityType.newBuilder().setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)))
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(
                    CommodityType.newBuilder().setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)))
                .build()));

        assertFalse(converter.skipShoppingListCreation(vm,
            CommoditiesBoughtFromProvider.newBuilder().setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE).build()));
    }

    /**
     * Verify compliance explanation override function works as expected.
     */
    @Test
    public void getExplanationOverride() throws Exception {
        long slOid = 1009;
        long sourceId = 73629179255014L;
        long destinationId = 144013120451328L;
        long vmBuyerId = 2001L;
        long computeTierId = 1001L;
        long storageTierId = 1002L;
        long volumeId = 73629179255255L;
        double diskUsedBeforeMb = 10240d;
        double diskUsedAfterMb = 20480d;
        final String io1Tier = "IO1";
        final String gp2Tier = "GP2";

        final MoveTO moveTO = MoveTO.newBuilder()
                .setShoppingListToMove(slOid)
                .setSource(sourceId)
                .setDestination(destinationId)
                .setMoveExplanation(MoveExplanation.newBuilder()
                .setCompliance(Compliance.newBuilder().addMissingCommodities(355).build()))
                .build();
        final Map<Long, ProjectedTopologyEntity> projectedTopology = new HashMap<>();
        final ProjectedTopologyEntity projectedVolume = ProjectedTopologyEntity.newBuilder()
                .setEntity(TopologyEntityDTO.newBuilder()
                        .setOid(volumeId)
                        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                                        .build())
                                .setCapacity(diskUsedAfterMb)
                                .build())
                        .build())
                .build();
        projectedTopology.put(volumeId, projectedVolume);

        constructTopologyConverter(CommodityIndex.newFactory(), TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.PLAN)
                .setPlanInfo(PlanTopologyInfo.newBuilder()
                        .setPlanType(PlanProjectType.CLOUD_MIGRATION.name())
                        .setPlanSubType(StringConstants.CLOUD_MIGRATION_PLAN__CONSUMPTION)
                        .build())
                .build());
        converter.setCloudTc(Mockito.mock(CloudTopologyConverter.class));

        // 1. Check compute tier, we should return true, without any explanation value.
        final TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder()
                .setOid(computeTierId)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .build();

        MarketTier marketTier = mock(MarketTier.class);
        when(marketTier.getTier())
                .thenReturn(computeTier);
        when(converter.getCloudTc().getMarketTier(destinationId))
                .thenReturn(marketTier);
        verifyOverrideExplanation(moveTO, projectedTopology, null);

        // 2. Verify that we detect a higher tier (e.g IO1), so a Performance risk.
        final TopologyEntityDTO.Builder storageTierBuilder = TopologyEntityDTO.newBuilder()
                .setOid(storageTierId)
                .setDisplayName(io1Tier)
                .setEntityType(EntityType.STORAGE_TIER_VALUE);
        when(marketTier.getTier())
                .thenReturn(storageTierBuilder.build());
        final List<CommodityBoughtDTO> commBoughtList = new ArrayList<>();
        commBoughtList.add(CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE)
                        .build())
                .setUsed(diskUsedBeforeMb)
                .build());
        ShoppingListInfo slInfo = new ShoppingListInfo(slOid, vmBuyerId, storageTierId,
                volumeId, null, EntityType.STORAGE_TIER_VALUE, commBoughtList);
        final Map<Long, ShoppingListInfo> shoppingListMap = new HashMap<>();
        shoppingListMap.put(slOid, slInfo);
        final Field slInfoField = TopologyConverter.class
                .getDeclaredField("shoppingListOidToInfos");
        slInfoField.setAccessible(true);
        slInfoField.set(converter, shoppingListMap);
        verifyOverrideExplanation(moveTO, projectedTopology,
                ChangeProviderExplanationTypeCase.CONGESTION);

        // 3. Same tier, but a disk size increase, so a Performance risk. Here ShoppingListInfo has
        // the resourceId set to volumeId, with collapsedBuyerId as null, for onPrem -> cloud case.
        storageTierBuilder.setDisplayName(gp2Tier);
        when(marketTier.getTier())
                .thenReturn(storageTierBuilder.build());
        verifyOverrideExplanation(moveTO, projectedTopology,
                ChangeProviderExplanationTypeCase.CONGESTION);

        // 4. For cloud -> cloud case, we don't have resourceId, instead the collapsedBuyerId is
        // the volumeId, so make sure we are picking that up.
        slInfo = new ShoppingListInfo(slOid, vmBuyerId, storageTierId,
                        Collections.emptySet(), volumeId, EntityType.STORAGE_TIER_VALUE, commBoughtList);
        shoppingListMap.put(slOid, slInfo);
        verifyOverrideExplanation(moveTO, projectedTopology,
                ChangeProviderExplanationTypeCase.CONGESTION);
    }

    /**
     * Runs explanation override method and verifies the return values.
     *
     * @param moveTO MoveTO containing buyer info.
     * @param projectedTopology Projected topology having volume info.
     * @param expectedExplanationType Expected return value, can be null.
     */
    private void verifyOverrideExplanation(@Nonnull final MoveTO moveTO,
            @Nonnull final Map<Long, ProjectedTopologyEntity> projectedTopology,
            @Nullable final ChangeProviderExplanationTypeCase expectedExplanationType) {
        final Pair<Boolean, ChangeProviderExplanation> overrideAndExplanation
                = converter.getExplanationOverride().apply(moveTO, projectedTopology);
        assertNotNull(overrideAndExplanation);
        assertTrue(overrideAndExplanation.getFirst());
        if (expectedExplanationType == null) {
            assertNull(overrideAndExplanation.getSecond());
        } else {
            assertEquals(overrideAndExplanation.getSecond().getChangeProviderExplanationTypeCase(),
                    expectedExplanationType);
        }
    }

    /**
     * Convenience method that makes a shoppingListTO with storage amount quantity set.
     *
     * @param slOid Id of shoppingListTO.
     * @param diskSize Storage amount quantity to use.
     * @return Instance of ShoppingListTO.
     */
    @Nonnull
    private ShoppingListTO makeShoppingListTO(long slOid, float diskSize) {
        return ShoppingListTO.newBuilder()
                .setOid(slOid)
                .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                                .setType(1)
                                .setBaseType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
                                .build())
                        .setQuantity(diskSize)
                        .build())
                .build();
    }

    /**
     * Test converting VM TraderTO from market to ProjectedTopologyEntity with VCPU commodity sold
     * and without actions. The capacity of projected entity should be "VCPU capacity / scalingFactor"
     * and will NOT be updated based on host cpuCoreMHz.
     *
     * @throws Exception On exception occurred.
     */
    @Test
    public void testVMTraderToEntityWithNoVCPUCapacityChanges() throws Exception {
        final float vCPUCapacity = 1500f;

        // Create VCPU CommoditySoldDTO with scalingFactor and corresponding TopologyEntityDTO which
        // sells this CommoditySoldDTO.
        final TopologyDTO.CommoditySoldDTO topologyVCPUSoldDTO = TopologyDTO.CommoditySoldDTO
            .newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.VCPU_VALUE))
            .setUsed(1).setCapacity(vCPUCapacity)
            .setScalingFactor(SCALING_FACTOR)
            .build();
        final TopologyDTO.TopologyEntityDTO vmEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(VM_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(topologyVCPUSoldDTO)
            .build();

        final TopologyDTO.TopologyEntityDTO pmEntityDTO = TopologyEntityDTO.newBuilder()
            .setOid(PM_OID)
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setPhysicalMachine(PhysicalMachineInfo.newBuilder()
                    .setCpuCoreMhz(3)))
            .build();

        // Mock commodityIndex.
        final CommodityIndex commodityIndex = CommodityIndex.newFactory().newIndex();
        final CommodityIndexFactory indexFactory = mock(CommodityIndexFactory.class);
        when(indexFactory.newIndex()).thenReturn(commodityIndex);
        commodityIndex.addEntity(vmEntityDTO);
        commodityIndex.addEntity(pmEntityDTO);

        // Create VCPU CommoditySoldTO and corresponding VM TraderTO which sells this CommoditySoldTO.
        final CommodityDTOs.CommoditySoldTO commoditySoldTO = CommoditySoldTO.newBuilder()
            .setCapacity(vCPUCapacity)
            .setSpecification(CommoditySpecificationTO.newBuilder()
                .setBaseType(CommodityDTO.CommodityType.VCPU_VALUE)
                .setType(0))
            .build();
        final EconomyDTOs.TraderTO vmTrader = TraderTO.newBuilder()
            .setOid(VM_OID)
            .addCommoditiesSold(commoditySoldTO)
            .addShoppingLists(ShoppingListTO.newBuilder()
                .setOid(VM_OID)
                .setSupplier(PM_OID))
            .setState(TraderStateTO.ACTIVE)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();

        final EconomyDTOs.TraderTO pmTrader = TraderTO.newBuilder()
            .setOid(PM_OID)
            .setState(TraderStateTO.ACTIVE)
            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
            .build();

        Mockito.doReturn(Optional.of(topologyVCPUSoldDTO.getCommodityType()))
            .when(mockCommodityConverter)
            .marketToTopologyCommodity(Mockito.eq(commoditySoldTO.getSpecification()));

        // Mock a TopologyConverter.
        final TopologyConverter topologyConverter = new TopologyConverter(REALTIME_TOPOLOGY_INFO,
            false, MarketAnalysisUtils.QUOTE_FACTOR, MarketMode.M2Only,
            MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
            marketCloudRateExtractor, mockCommodityConverter, indexFactory, tierExcluderFactory,
            consistentScalingHelperFactory, reversibilitySettingFetcher, MarketAnalysisUtils.PRICE_WEIGHT_SCALE,
            false, false, false, 0.5f);
        topologyConverter.setConvertToMarketComplete();
        final TopologyConverter converter = Mockito.spy(topologyConverter);

        // Mock entityOidToDto field of TopologyConverter.
        final Map<Long, TopologyEntityDTO> entityOidToDtoMap =
            ImmutableMap.of(vmEntityDTO.getOid(), vmEntityDTO, pmEntityDTO.getOid(), pmEntityDTO);
        final Field entityOidToDtoField = TopologyConverter.class.getDeclaredField("entityOidToDto");
        entityOidToDtoField.setAccessible(true);
        entityOidToDtoField.set(converter, entityOidToDtoMap);

        // Mock oidToOriginalTraderTOMap field of TopologyConverter.
        final Long2ObjectOpenHashMap<MinimalOriginalTrader> oidToOriginalTraderTOMap =
            new Long2ObjectOpenHashMap<>(Stream.of(new MinimalOriginalTrader(vmTrader),
                new MinimalOriginalTrader(pmTrader)).collect(Collectors.toMap(MinimalOriginalTrader::getOid, Function.identity())));
        Field oidToOriginalTraderTOMapField =
            TopologyConverter.class.getDeclaredField("oidToOriginalTraderTOMap");
        oidToOriginalTraderTOMapField.setAccessible(true);
        oidToOriginalTraderTOMapField.set(converter, oidToOriginalTraderTOMap);

        ReservedCapacityResults reservedCapacityResults = new ReservedCapacityResults();

        Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedEntity = converter.convertFromMarket(
            Lists.newArrayList(vmTrader, pmTrader),
            ImmutableMap.of(vmEntityDTO.getOid(), vmEntityDTO, pmEntityDTO.getOid(), pmEntityDTO),
            PriceIndexMessage.getDefaultInstance(), reservedCapacityResults,
            setUpWastedFileAnalysis());

        assertEquals(2L, projectedEntity.size());
        // No actions are generated on the given trader, so the expected VCPU capacity is
        // vCPUCapacity / SCALING_FACTOR.
        double expectedCapacity = vCPUCapacity / SCALING_FACTOR;
        CommoditySoldDTO projectedVCPUComm = projectedEntity.get(VM_OID).getEntity().getCommoditySoldList(0);
        assertEquals(expectedCapacity, projectedVCPUComm.getCapacity(), TopologyConverter.EPSILON);
    }


    /**
     * Test that entities provisioned by the market that are placed on other
     * provisioned entities get their provider entity type set.
     */
    @Test
    public void testCommoditiesBoughtProviderTypeOnProvisionedEntity() throws Exception {
        // Create VCPU CommoditySoldDTO with scalingFactor and corresponding TopologyEntityDTO which
        // sells this CommoditySoldDTO.
        final TopologyDTO.CommodityBoughtDTO commBought = TopologyDTO.CommodityBoughtDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.VCPU_VALUE))
            .setUsed(1)
            .build();
        final TopologyDTO.TopologyEntityDTO vmEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(VM_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderId(PM_OID)
                .addCommodityBought(commBought))
            .build();

        final TopologyDTO.TopologyEntityDTO pmEntityDTO = TopologyEntityDTO.newBuilder()
            .setOid(PM_OID)
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                .setPhysicalMachine(PhysicalMachineInfo.newBuilder()
                    .setCpuCoreMhz(3)))
            .build();

        Mockito.doReturn(Optional.of(commBought.getCommodityType()))
            .when(mockCommodityConverter)
            .marketToTopologyCommodity(any());

        // Create VCPU CommoditySoldTO and corresponding VM TraderTO which sells this CommoditySoldTO.
        final CommodityDTOs.CommodityBoughtTO commodityBoughtTO = CommodityBoughtTO.newBuilder()
            .setQuantity(1)
            .setSpecification(CommoditySpecificationTO.newBuilder()
                .setBaseType(CommodityDTO.CommodityType.VCPU_VALUE)
                .setType(0))
            .build();
        final EconomyDTOs.TraderTO vmTrader = TraderTO.newBuilder()
            .setOid(VM_OID)
            .addShoppingLists(ShoppingListTO.newBuilder()
                .addCommoditiesBought(commodityBoughtTO)
                .setOid(VM_OID)
                .setSupplier(PM_OID))
            .setState(TraderStateTO.ACTIVE)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();

        final EconomyDTOs.TraderTO pmTrader = TraderTO.newBuilder()
            .setOid(PM_OID)
            .setState(TraderStateTO.ACTIVE)
            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
            .build();

        final long vmCloneOid = VM_OID + 1234L;
        final long pmCloneOid = PM_OID + 1234L;
        final EconomyDTOs.TraderTO vmTraderClone = TraderTO.newBuilder()
            .setOid(vmCloneOid)
            .setCloneOf(VM_OID)
            .addShoppingLists(ShoppingListTO.newBuilder()
                .addCommoditiesBought(commodityBoughtTO)
                .setOid(vmCloneOid)
                .setSupplier(pmCloneOid))
            .setState(TraderStateTO.ACTIVE)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();

        final EconomyDTOs.TraderTO pmTraderClone = TraderTO.newBuilder()
            .setOid(pmCloneOid)
            .setCloneOf(PM_OID)
            .setState(TraderStateTO.ACTIVE)
            .setType(EntityType.PHYSICAL_MACHINE_VALUE)
            .build();

        final CommodityIndex commodityIndex = CommodityIndex.newFactory().newIndex();
        final CommodityIndexFactory indexFactory = mock(CommodityIndexFactory.class);
        when(indexFactory.newIndex()).thenReturn(commodityIndex);
        commodityIndex.addEntity(vmEntityDTO);
        commodityIndex.addEntity(pmEntityDTO);

        // Mock a TopologyConverter.
        final TopologyConverter topologyConverter = new TopologyConverter(REALTIME_TOPOLOGY_INFO,
            false, MarketAnalysisUtils.QUOTE_FACTOR, MarketMode.M2Only,
            MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR,
            marketCloudRateExtractor, mockCommodityConverter, indexFactory, tierExcluderFactory,
            consistentScalingHelperFactory, reversibilitySettingFetcher, MarketAnalysisUtils.PRICE_WEIGHT_SCALE,
            false, false, false, 0.5f);
        topologyConverter.setConvertToMarketComplete();
        final TopologyConverter converter = Mockito.spy(topologyConverter);

        // Mock entityOidToDto field of TopologyConverter.
        final Map<Long, TopologyEntityDTO> entityOidToDtoMap =
            ImmutableMap.of(vmEntityDTO.getOid(), vmEntityDTO, pmEntityDTO.getOid(), pmEntityDTO);
        final Field entityOidToDtoField = TopologyConverter.class.getDeclaredField("entityOidToDto");
        entityOidToDtoField.setAccessible(true);
        entityOidToDtoField.set(converter, entityOidToDtoMap);

        // Mock oidToOriginalTraderTOMap field of TopologyConverter.
        final Long2ObjectOpenHashMap<MinimalOriginalTrader> oidToOriginalTraderTOMap =
            new Long2ObjectOpenHashMap<>(Stream.of(new MinimalOriginalTrader(vmTrader),
                new MinimalOriginalTrader(pmTrader))
                .collect(Collectors.toMap(MinimalOriginalTrader::getOid, Function.identity())));
        Field oidToOriginalTraderTOMapField =
            TopologyConverter.class.getDeclaredField("oidToOriginalTraderTOMap");
        oidToOriginalTraderTOMapField.setAccessible(true);
        oidToOriginalTraderTOMapField.set(converter, oidToOriginalTraderTOMap);
        converter.updateShoppingListMap(Arrays.asList(
            NewShoppingListToBuyerEntry.newBuilder()
                .setNewShoppingList(vmTrader.getShoppingLists(0).getOid())
                .setBuyer(vmTrader.getOid())
                .build(),
            NewShoppingListToBuyerEntry.newBuilder()
                .setNewShoppingList(vmTraderClone.getShoppingLists(0).getOid())
                .setBuyer(vmTraderClone.getOid())
                .build()));

        Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedEntity = converter.convertFromMarket(
            Lists.newArrayList(vmTrader, pmTrader, vmTraderClone, pmTraderClone),
            ImmutableMap.of(vmEntityDTO.getOid(), vmEntityDTO, pmEntityDTO.getOid(), pmEntityDTO),
            PriceIndexMessage.getDefaultInstance(), new ReservedCapacityResults(),
            setUpWastedFileAnalysis());

        assertEquals(4L, projectedEntity.size());
        final ProjectedTopologyEntity projectedVM = projectedEntity.values().stream()
            .filter(e -> e.getEntity().getOid() == vmCloneOid)
            .findFirst()
            .get();

        final List<CommoditiesBoughtFromProvider> boughtFromProviders =
            projectedVM.getEntity().getCommoditiesBoughtFromProvidersList();
        assertEquals(1, boughtFromProviders.size());
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE, boughtFromProviders.get(0).getProviderEntityType());
    }

    /**
     * Test VM commodity sold capacity changes based on ComputeTier provider and providersOfContainers.
     */
    @Test
    public void testCommSoldCapacityChangesFromComputeTierWithContainerProviders() throws Exception {
        final TopologyDTO.CommoditySoldDTO commSoldDTO = TopologyDTO.CommoditySoldDTO
            .newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.VCPU_VALUE))
            .setCapacity(VM_VCPU_CAPACITY)
            .build();
        Mockito.doReturn(Optional.of(commSoldDTO.getCommodityType()))
            .when(mockCommodityConverter)
            .marketToTopologyCommodity(any());
        final TopologyDTO.TopologyEntityDTO vmEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(VM_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(commSoldDTO)
            .build();
        final TopologyDTO.TopologyEntityDTO vmEntityDTO2 = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(VM2_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(commSoldDTO)
            .build();

        // Mock commodityIndex.
        final CommodityIndex commodityIndex = CommodityIndex.newFactory().newIndex();
        final CommodityIndexFactory indexFactory = mock(CommodityIndexFactory.class);
        when(indexFactory.newIndex()).thenReturn(commodityIndex);
        commodityIndex.addEntity(vmEntityDTO);
        commodityIndex.addEntity(vmEntityDTO2);

        // Create VCPU CommoditySoldTO and corresponding VM TraderTO which sells this CommoditySoldTO.
        final CommodityDTOs.CommoditySoldTO commoditySoldTO = CommoditySoldTO.newBuilder()
            .setCapacity(VM_VCPU_CAPACITY)
            .setSpecification(CommoditySpecificationTO.newBuilder()
                .setBaseType(CommodityDTO.CommodityType.VCPU_VALUE)
                .setType(0))
            .build();
        final EconomyDTOs.TraderTO vmTraderTO = TraderTO.newBuilder()
            .setOid(VM_OID)
            .addCommoditiesSold(commoditySoldTO)
            .addShoppingLists(ShoppingListTO.newBuilder()
                .setOid(VM_OID)
                .setSupplier(CLOUD_COMPUTE_TIER_OID))
            .setState(TraderStateTO.ACTIVE)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();
        final EconomyDTOs.TraderTO vmTraderTOClone = TraderTO.newBuilder()
            .setOid(CLONE_VM_OID)
            .setCloneOf(VM_OID)
            .addCommoditiesSold(commoditySoldTO)
            .addShoppingLists(ShoppingListTO.newBuilder()
                .setOid(CLONE_VM_OID)
                .setSupplier(CLOUD_COMPUTE_TIER_OID))
            .setState(TraderStateTO.ACTIVE)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();
        final EconomyDTOs.TraderTO vmTraderTO2 = TraderTO.newBuilder()
            .setOid(VM2_OID)
            .addCommoditiesSold(commoditySoldTO)
            .addShoppingLists(ShoppingListTO.newBuilder()
                .setOid(VM2_OID)
                .setSupplier(CLOUD_COMPUTE_TIER_OID))
            .setState(TraderStateTO.ACTIVE)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();

        // Mock CloudTopologyConverter.
        CloudTopologyConverter mockCloudTc = Mockito.mock(CloudTopologyConverter.class);
        TopologyDTO.TopologyEntityDTO computeTierDTO = createEntityDTO(CLOUD_COMPUTE_TIER_OID,
            EntityType.COMPUTE_TIER_VALUE,
            ImmutableList.of(CommodityDTO.CommodityType.CPU_VALUE),
            OLD_TIER_CAPACITY);
        MarketTier marketTier = new OnDemandMarketTier(computeTierDTO);
        when(mockCloudTc.getPrimaryMarketTier(vmTraderTO)).thenReturn(marketTier);
        when(mockCloudTc.getPrimaryMarketTier(vmTraderTOClone)).thenReturn(marketTier);
        when(mockCloudTc.getPrimaryMarketTier(vmTraderTO2)).thenReturn(marketTier);
        when(mockCloudTc.isMarketTier(CLOUD_COMPUTE_TIER_OID)).thenReturn(true);
        when(mockCloudTc.getTraderTOOid(marketTier)).thenReturn(CLOUD_COMPUTE_TIER_OID);
        when(mockCloudTc.getMarketTier(Mockito.anyLong())).thenReturn(marketTier);

        final TopologyConverter topologyConverter = new TopologyConverter(REALTIME_TOPOLOGY_INFO,
            marketCloudRateExtractor, mockCommodityConverter, mockCCD, indexFactory,
            tierExcluderFactory, consistentScalingHelperFactory, cloudTopology,
            reversibilitySettingFetcher, analysisConfig);
        topologyConverter.setConvertToMarketComplete();
        TopologyConverter converter = Mockito.spy(topologyConverter);
        converter.setCloudTc(mockCloudTc);

        // Only add vmEntityDTO to providersOfContainers.
        Set<Long> providersOfContainers = new HashSet<>();
        providersOfContainers.add(vmEntityDTO.getOid());
        Field providersOfContainersField = TopologyConverter.class.getDeclaredField(
            "providersOfContainers");
        providersOfContainersField.setAccessible(true);
        providersOfContainersField.set(converter, providersOfContainers);
        Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedEntities = converter.convertFromMarket(Lists.newArrayList(vmTraderTO, vmTraderTOClone, vmTraderTO2),
            ImmutableMap.of(vmEntityDTO.getOid(), vmEntityDTO, vmEntityDTO2.getOid(), vmEntityDTO2),
            PriceIndexMessage.getDefaultInstance(), reservedCapacityResults, setUpWastedFileAnalysis());

        // Expect 3 projected entities.
        assertEquals(3, projectedEntities.size());

        // Case 1: VM_OID VM commodity capacity is not changed because it's included in providersOfContainers.
        List<CommoditySoldDTO> commSold = projectedEntities.get(VM_OID).getEntity().getCommoditySoldListList();
        assertEquals(1, commSold.size());
        assertEquals(VM_VCPU_CAPACITY, commSold.get(0).getCapacity(), DELTA);

        // Case 2: CLONE_VM_OID VM commodity capacity is not changed because its original entity (VM_OID) is
        // included in providersOfContainers.
        List<CommoditySoldDTO> commSold2 = projectedEntities.get(CLONE_VM_OID).getEntity().getCommoditySoldListList();
        assertEquals(1, commSold2.size());
        assertEquals(VM_VCPU_CAPACITY, commSold2.get(0).getCapacity(), DELTA);

        // Case 3: VM2_OID VM commodity capacity is changed because it's not included in providersOfContainers.
        List<CommoditySoldDTO> commSold3 = projectedEntities.get(VM2_OID).getEntity().getCommoditySoldListList();
        assertEquals(1, commSold3.size());
        assertEquals(OLD_TIER_CAPACITY, commSold3.get(0).getCapacity(), DELTA);
    }

    /**
     * Test updating providerID to commoditiesBoughtFromProviders of provisioned VM.
     */
    @Test
    public void testProviderIDUpdateForProvisionedVM() {
        final TopologyDTO.CommoditySoldDTO commSoldDTO = TopologyDTO.CommoditySoldDTO
            .newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.VCPU_VALUE))
            .setCapacity(1.0)
            .build();
        Mockito.doReturn(Optional.of(commSoldDTO.getCommodityType()))
            .when(mockCommodityConverter)
            .marketToTopologyCommodity(any());
        final TopologyDTO.TopologyEntityDTO vmEntityDTO = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setOid(VM_OID)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addCommoditySoldList(commSoldDTO)
            .build();

        // Mock commodityIndex.
        final CommodityIndex commodityIndex = CommodityIndex.newFactory().newIndex();
        final CommodityIndexFactory indexFactory = mock(CommodityIndexFactory.class);
        when(indexFactory.newIndex()).thenReturn(commodityIndex);
        commodityIndex.addEntity(vmEntityDTO);

        final EconomyDTOs.TraderTO vmTraderTO = TraderTO.newBuilder()
            .setOid(VM_OID)
            .addShoppingLists(ShoppingListTO.newBuilder()
                .setOid(VM_OID + 1)
                .setSupplier(CLOUD_COMPUTE_TIER_OID))
            .setState(TraderStateTO.ACTIVE)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();
        // No supplier ID in the shoppingList of clone VM traderTO.
        final EconomyDTOs.TraderTO vmTraderTOClone = TraderTO.newBuilder()
            .setOid(CLONE_VM_OID)
            .setCloneOf(VM_OID)
            .addShoppingLists(ShoppingListTO.newBuilder()
                .setOid(CLONE_VM_OID + 1)
                .setSupplier(0))
            .setState(TraderStateTO.ACTIVE)
            .setType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();

        Map<Long, TopologyDTO.ProjectedTopologyEntity> projectedEntities =
            converter.convertFromMarket(Lists.newArrayList(vmTraderTO, vmTraderTOClone),
            ImmutableMap.of(vmEntityDTO.getOid(), vmEntityDTO),
            PriceIndexMessage.getDefaultInstance(), reservedCapacityResults, setUpWastedFileAnalysis());
        assertEquals(2, projectedEntities.size());
        assertEquals(1, projectedEntities.get(CLONE_VM_OID).getEntity().getCommoditiesBoughtFromProvidersList().size());
        // ProviderId is updated to cloned VM.
        assertEquals(CLOUD_COMPUTE_TIER_OID, projectedEntities.get(CLONE_VM_OID).getEntity().getCommoditiesBoughtFromProviders(0).getProviderId());
    }

    @Test
    public void checkCommodityKeysComparison() {
        CommodityType ctNoKey1 = CommodityType.newBuilder().setType(COMM_TYPE1).build();
        CommodityType ctNoKey2 = CommodityType.newBuilder().setType(COMM_TYPE2).build();
        //Both has no keys - we consider that as match. If we have 2 commodities without keys - we
        // ignore keys and we will compare only commodity type
        Assert.assertTrue(TopologyConverter.compareKeys(ctNoKey1, ctNoKey2));

        CommodityType ctKey1 = CommodityType.newBuilder().setType(COMM_TYPE1).setKey(COMM_KEY1).build();
        CommodityType ctKey2 = CommodityType.newBuilder().setType(COMM_TYPE1).setKey(COMM_KEY2).build();
        //If one has key, and second doesn't have - that is mismatch
        Assert.assertFalse(TopologyConverter.compareKeys(ctNoKey1, ctKey1));
        Assert.assertFalse(TopologyConverter.compareKeys(ctKey1, ctNoKey2));
        //Two different keys - mismatch
        Assert.assertFalse(TopologyConverter.compareKeys(ctKey1, ctKey2));

        CommodityType ctKey3 = CommodityType.newBuilder().setType(COMM_TYPE2).setKey(COMM_KEY1).build();
        //Same keys  - match
        Assert.assertTrue(TopologyConverter.compareKeys(ctKey1, ctKey3));
    }
}
