package com.vmturbo.market.topology.conversions;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.*;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ArrayUtils.isSorted;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.util.JsonFormat;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopology;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Thresholds;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Edit;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Removed;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.utils.CommodityTypeAllocatorConstants;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;
import com.vmturbo.market.runner.FakeEntityCreator;
import com.vmturbo.market.runner.MarketMode;
import com.vmturbo.market.runner.wasted.WastedEntityResults;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.market.topology.TopologyConverterUtil;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper.ConsistentScalingHelperFactory;
import com.vmturbo.market.topology.conversions.TierExcluder.TierExcluderFactory;
import com.vmturbo.market.topology.conversions.TopologyConverter.UsedAndPeak;
import com.vmturbo.platform.analysis.protobuf.BalanceAccountDTOs.BalanceAccountDTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO.PriceFunctionTypeCase;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO.UpdatingFunctionTypeCase;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityCapacityLimit;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Unit tests for {@link TopologyConverter}.
 */
public class TopologyConverterToMarketTest {

    private static final TopologyInfo REALTIME_TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.REALTIME)
            .build();

    private static final TopologyInfo PLAN_TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.PLAN)
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                    .setPlanProjectType(PlanProjectType.USER))
            .build();

    private static final TopologyInfo MCP_COSUMPTION_PLAN_TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.PLAN)
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                    .setPlanProjectType(PlanProjectType.CLOUD_MIGRATION)
                    .setPlanType(PlanProjectType.CLOUD_MIGRATION.name())
                    .setPlanSubType(StringConstants.CLOUD_MIGRATION_PLAN__CONSUMPTION))
            .build();

    private static final TopologyInfo MCP_ALLOCATION_PLAN_TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.PLAN)
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                    .setPlanProjectType(PlanProjectType.CLOUD_MIGRATION)
                    .setPlanType(PlanProjectType.CLOUD_MIGRATION.name())
                    .setPlanSubType(StringConstants.CLOUD_MIGRATION_PLAN__ALLOCATION))
            .build();

    private static final long PROVIDER_ID = 1;
    private static final double DELTA = 0.001d;

    private static final double epsilon = 1e-5; // used in assertEquals(double, double, epsilon)
    private static final long VM1_OID = 101L; // matches the oid in vm-1.dto.json

    private CommoditySpecificationTO economyCommodity1;
    private CommodityType topologyCommodity1;
    private CommoditySpecificationTO economyCommodity2;
    private CommodityType topologyCommodity2;
    private CloudRateExtractor marketCloudRateExtractor = mock(CloudRateExtractor.class);
    private CloudCostData ccd = spy(new CloudCostData<>(new HashMap<>(), new HashMap<>(), new HashMap<>(),
            new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), Optional.empty(), ImmutableSetMultimap.of(), new HashMap<>()));
    private TierExcluderFactory tierExcluderFactory = mock(TierExcluderFactory.class);
    private AccountPricingData accountPricingData = mock(AccountPricingData.class);
    private ConsistentScalingHelperFactory consistentScalingHelperFactory;
    private ReversibilitySettingFetcher reversibilitySettingFetcher
            = mock(ReversibilitySettingFetcher.class);
    private SettingPolicyServiceMole settingPolicyServiceMole = spy(new SettingPolicyServiceMole());

    /**
     * Stub server for group queries.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(settingPolicyServiceMole);

    /**
     * Rule to manage feature flag enablement.
     */
    @Rule
    public FeatureFlagTestRule mergedPeakFeatureFlag =
            new FeatureFlagTestRule(FeatureFlags.ENABLE_MERGED_PEAK_UPDATE_FUNCTION);

    @Before
    public void setup() throws IOException {
        IdentityGenerator.initPrefix(0);
        // The commodity types in topologyCommodity
        // map to the base type in economy commodity.
        topologyCommodity1 = CommodityType.newBuilder()
                .setType(1)
                .setKey("blah")
                .build();
        topologyCommodity2 = CommodityType.newBuilder()
                .setType(2)
                .setKey("blahblah")
                .build();
        economyCommodity1 = CommoditySpecificationTO.newBuilder()
                .setType(0 + CommodityTypeAllocatorConstants.ACCESS_COMM_TYPE_START_COUNT)
                .setBaseType(1)
                .build();
        economyCommodity2 = CommoditySpecificationTO.newBuilder()
                .setType(1 + CommodityTypeAllocatorConstants.ACCESS_COMM_TYPE_START_COUNT)
                .setBaseType(2)
                .build();
        when(tierExcluderFactory.newExcluder(any(), any(), any())).thenReturn(mock(TierExcluder.class));
        SettingPolicyServiceBlockingStub settingsPolicyService =
                SettingPolicyServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        consistentScalingHelperFactory = new ConsistentScalingHelperFactory(settingsPolicyService);
    }

    @Test
    public void testConvertCommodityCloneWithNewType() {
        TopologyDTO.TopologyEntityDTO entityDto = DTOWithProvisionedAndCloneWithNewTypeComm();
        TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();
        TraderTO traderTO = converter.convertToMarket(ImmutableMap.of(entityDto.getOid(), entityDto)).iterator().next();
        assertTrue(traderTO.getCommoditiesSold(1).getSpecification().getCloneWithNewType());
    }

    private TopologyEntityDTO DTOWithProvisionedAndCloneWithNewTypeComm() {
        return TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setOid(100)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE)
                                .build())
                        .build())
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.APPLICATION_VALUE)
                                .build())
                        .build())
                .build();
    }

    @Test
    public void testProvisionedCommodityResizable() {
        TopologyDTO.TopologyEntityDTO entityDto = DTOWithProvisionedAndCloneWithNewTypeComm();
        final TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();
        TraderTO traderTO = converter.convertToMarket(ImmutableMap.of(entityDto.getOid(), entityDto)).iterator().next();
        assertFalse(traderTO.getCommoditiesSold(0).getSettings().getResizable());
    }

    /**
     * Test loading one DTO. Verify the properties after the conversion match the file.
     * @throws IOException when the loaded file is missing
     */
    @Test
    public void testConvertOneDTO() throws IOException {
        TopologyEntityDTO vdcTopologyDTO =
                messageFromJsonFile("protobuf/messages/vdc-1.dto.json");
        Set<TopologyEntityDTO> topologyDTOs = Sets.newHashSet(vdcTopologyDTO);
        Collection<TraderTO> traderTOs = convertToMarketTO(topologyDTOs, REALTIME_TOPOLOGY_INFO);
        TraderTO vdcTraderTO = traderTOs.iterator().next();
        assertEquals(100, vdcTraderTO.getOid());
        assertEquals(EntityType.VIRTUAL_DATACENTER_VALUE, vdcTraderTO.getType());
        assertEquals(TraderStateTO.ACTIVE, vdcTraderTO.getState());
        assertEquals("VIRTUAL_DATACENTER|100|CDV-1", vdcTraderTO.getDebugInfoNeverUseInCode());
        List<CommoditySoldTO> commSoldList = vdcTraderTO.getCommoditiesSoldList();
        assertEquals(1, commSoldList.size());
        CommoditySoldTO commSold = commSoldList.get(0);
        assertEquals(5000.0, commSold.getQuantity(), epsilon );
        assertEquals(8000.0, commSold.getPeakQuantity(), epsilon);
        assertEquals(100000.0, commSold.getCapacity(), epsilon);
        assertTrue(commSold.getThin());
        CommoditySpecificationTO commSoldSpec = commSold.getSpecification();
        // zero because it is the first assigned type, plus the start count of access commodity
        assertEquals(CommodityTypeAllocatorConstants.ACCESS_COMM_TYPE_START_COUNT + 0, commSoldSpec.getType());
        assertEquals(CommodityDTO.CommodityType.MEM_ALLOCATION_VALUE, commSoldSpec.getBaseType());
        assertEquals("MEM_ALLOCATION|P1", commSoldSpec.getDebugInfoNeverUseInCode());
        CommoditySoldSettingsTO commSoldSettings = commSold.getSettings();
        assertTrue(commSoldSettings.getResizable());
        assertEquals(3.4028230607370965E38, commSoldSettings.getCapacityUpperBound(), epsilon);
        assertThat(commSoldSettings.getCapacityIncrement(), is(20.0f));
        assertEquals(0.8, commSoldSettings.getUtilizationUpperBound(), epsilon);
        assertEquals(1.0, commSoldSettings.getPriceFunction().getStandardWeighted().getWeight(), epsilon);
        TraderSettingsTO vdcSettings = vdcTraderTO.getSettings();
        assertFalse(vdcSettings.getClonable());
        assertFalse(vdcSettings.getSuspendable());
        assertEquals(0.65, vdcSettings.getMinDesiredUtilization(), epsilon); // hard-coded value
        assertEquals(0.75, vdcSettings.getMaxDesiredUtilization(), epsilon); // hard-coded value
        assertTrue(vdcSettings.getCanAcceptNewCustomers());
    }

    static TraderTO getVmTrader(Collection<TraderTO> traders) {
        return traders.stream()
                .filter(tto -> tto.getType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .findFirst()
                .orElse(null);
    }

    static TraderTO getPmTrader(Collection<TraderTO> traders) {
        return traders.stream()
                .filter(tto -> tto.getType() == EntityType.PHYSICAL_MACHINE_VALUE)
                .findFirst()
                .orElse(null);
    }

    static CommodityBoughtTO getMem(List<CommodityBoughtTO> commoditiesBoughtList) {
        return commoditiesBoughtList.stream()
                .filter(comm -> comm.getSpecification().getBaseType() == CommodityDTO.CommodityType.MEM_VALUE)
                .findFirst()
                .orElse(null);
    }

    static CommodityBoughtTO getCPU(List<CommodityBoughtTO> commoditiesBoughtList) {
        return commoditiesBoughtList.stream()
                .filter(comm -> comm.getSpecification().getBaseType() == CommodityDTO.CommodityType.CPU_VALUE)
                .findFirst()
                .orElse(null);
    }

    static CommoditySoldTO getCPUSold(List<CommoditySoldTO> commoditiesSoldList) {
        return commoditiesSoldList.stream()
                .filter(comm -> comm.getSpecification().getBaseType() == CommodityDTO.CommodityType.CPU_VALUE)
                .findFirst()
                .orElse(null);
    }

    private static final Set<TopologyEntityDTO> TOPOLOGY_DTOS = new HashSet<>();

    @BeforeClass
    public static void setupClass() throws IOException {
        TOPOLOGY_DTOS.add(messageFromJsonFile("protobuf/messages/vm-1.dto.json"));
        TOPOLOGY_DTOS.add(messageFromJsonFile("protobuf/messages/pm-1.dto.json"));
        TOPOLOGY_DTOS.add(messageFromJsonFile("protobuf/messages/ds-1.dto.json"));
        TOPOLOGY_DTOS.add(messageFromJsonFile("protobuf/messages/ds-2.dto.json"));
        TOPOLOGY_DTOS.add(messageFromJsonFile("protobuf/messages/vdc-1.dto.json"));
    }

    /**
     * Test loading a group of DTOs and that the properties of bought commodities
     * after the conversion match the file.
     */
    @Test
    public void testConvertDTOs() {
        Collection<TraderTO> traderTOs = convertToMarketTO(TOPOLOGY_DTOS, REALTIME_TOPOLOGY_INFO);
        assertEquals(TOPOLOGY_DTOS.size(), traderTOs.size());

        final TraderTO vmTraderTO = getVmTrader(traderTOs);
        assertNotNull(vmTraderTO);
        List<ShoppingListTO> shoppingLists = vmTraderTO.getShoppingListsList();
        // Buys from storage and PM
        assertEquals(3, shoppingLists.size());
        // Get the shopping list that buys from PM
        ShoppingListTO pmShoppingList = shoppingLists.stream()
                .filter(sl -> sl.getSupplier() == 102)
                .findAny().orElseThrow(() -> new RuntimeException("cannot find supplier 102"));
        // PM shopping list is created first because we sort the commBoughtGroupings by provider
        // type and then by provider.
        assertEquals(1000, pmShoppingList.getOid());
        assertTrue(pmShoppingList.getMovable());
        List<CommodityBoughtTO> commoditiesBoughtList = pmShoppingList.getCommoditiesBoughtList();
        // Buys MEM and CPU
        assertEquals(2, commoditiesBoughtList.size());
        CommodityBoughtTO aCommodityBought = getMem(commoditiesBoughtList);
        assertEquals(50.0, aCommodityBought.getQuantity(), epsilon);
        assertEquals(80.0, aCommodityBought.getPeakQuantity(), epsilon);
        CommoditySpecificationTO commBoughtSpec = aCommodityBought.getSpecification();
        assertEquals("MEM", commBoughtSpec.getDebugInfoNeverUseInCode());
        CommodityBoughtTO cpuCommodityBought = getCPU(commoditiesBoughtList);
        //reservation * scalingFactor
        assertEquals(105, cpuCommodityBought.getQuantity(), epsilon);
        assertEquals(0, pmShoppingList.getStorageMoveCost(), epsilon);
        // Get the shopping list that buys from DS
        ShoppingListTO dsShoppingList = shoppingLists.stream()
                .filter(sl -> sl.getSupplier() == 205)
                .findAny().orElseThrow(() -> new RuntimeException("cannot find supplier 205"));
        // 2048 bought from DS based on commodities bought From provider list
        // and divide by 1024, and then scaled by storageMoveCostFactor of 0.01.
        // Which will result in getStorageMoveCost returning 0.02
        assertEquals(0.02, dsShoppingList.getStorageMoveCost(), epsilon);

        final TraderTO pmTraderTO = getPmTrader(traderTOs);
        CommoditySoldTO pmSoldCPU = getCPUSold(pmTraderTO.getCommoditiesSoldList());
        assertEquals(2075, pmSoldCPU.getQuantity(), epsilon);
    }

    /**
     * Test that in a plan, the move cost is zero.
     * We already tested that in a realtime market the move cost is set.
     */
    @Test
    public void testZeroMoveCostInPlan() {
        Collection<TraderTO> traderTOs = convertToMarketTO(TOPOLOGY_DTOS, PLAN_TOPOLOGY_INFO);
        final TraderTO vmTraderTO = getVmTrader(traderTOs);
        List<ShoppingListTO> shoppingLists = vmTraderTO.getShoppingListsList();
        // Get the shopping list that buys from DS
        ShoppingListTO dsShoppingList = shoppingLists.stream()
                .filter(sl -> sl.getSupplier() == 205)
                .findAny().orElseThrow(() -> new RuntimeException("Cannot find supplier 205"));
        assertEquals(0.0, dsShoppingList.getStorageMoveCost(), epsilon);
    }

    /**
     * Test that the topology state is converted correctly to trader state.
     */
    @Test
    public void testTraderState() {
        TopologyEntityDTO vmOn = entity(EntityType.VIRTUAL_MACHINE_VALUE,
                10,  EntityState.POWERED_ON, Collections.emptyList(), Collections.emptyList());
        TraderTO traderVmOn = convertToMarketTO(Sets.newHashSet(vmOn), PLAN_TOPOLOGY_INFO).iterator().next();
        assertEquals(TraderStateTO.ACTIVE, traderVmOn.getState());

        TopologyEntityDTO vmOff = entity(EntityType.VIRTUAL_MACHINE_VALUE,
                10, EntityState.POWERED_OFF, Collections.emptyList(), Collections.emptyList());
        TraderTO traderVmOff = convertToMarketTO(Sets.newHashSet(vmOff), PLAN_TOPOLOGY_INFO).iterator().next();
        assertEquals(TraderStateTO.IDLE, traderVmOff.getState());

        TopologyEntityDTO appOn = entity(EntityType.APPLICATION_COMPONENT_VALUE, 10, EntityState.POWERED_ON,
                Collections.emptyList(), Collections.emptyList());
        TraderTO traderAppOn = convertToMarketTO(Sets.newHashSet(appOn), PLAN_TOPOLOGY_INFO).iterator().next();
        assertEquals(TraderStateTO.ACTIVE, traderAppOn.getState());

        TopologyEntityDTO appOff = entity(EntityType.APPLICATION_COMPONENT_VALUE, 10,
                EntityState.POWERED_OFF, Collections.emptyList(), Collections.emptyList());
        TraderTO traderAppOff = convertToMarketTO(Sets.newHashSet(appOff), PLAN_TOPOLOGY_INFO).iterator().next();
        assertEquals(TraderStateTO.INACTIVE, traderAppOff.getState());
    }

    private TopologyEntityDTO entity(int type, long oid, EntityState state,
                                     List<ConnectedEntity> connectedEntities,
                                     List<CommoditySoldDTO> soldCommodities) {
        final TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder()
                .setEntityType(type)
                .setEntityState(state)
                .setOid(oid);
        if (!CollectionUtils.isEmpty(connectedEntities)) {
            builder.addAllConnectedEntityList(connectedEntities);
        }
        if (!CollectionUtils.isEmpty(soldCommodities)) {
            builder.addAllCommoditySoldList(soldCommodities);
        }
        return builder.build();
    }

    /**
     * Verify that plan and RT VMs that do not host containers are suspendable, and that plan
     * entities are eligible for resize.
     */
    @Test
    public void testPlanVsRealtimeEntities() {
        CommodityType commodityType = CommodityType.newBuilder()
                .setType(1)
                .build();
        final TopologyEntityDTO vm = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(10)
                // commodities sold so it is not top of the supply chain
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(commodityType)
                        .build())
                // commodities bought so it is not bottom of the supply chain
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(10L)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(commodityType)))
                .build();
        TraderTO planVm = convertToMarketTO(Sets.newHashSet(vm), PLAN_TOPOLOGY_INFO).iterator().next();
        TraderTO realtimeVm = convertToMarketTO(Sets.newHashSet(vm), REALTIME_TOPOLOGY_INFO).iterator().next();
        assertFalse(planVm.getSettings().getSuspendable());
        assertFalse(realtimeVm.getSettings().getSuspendable());
        assertTrue(planVm.getSettings().getIsEligibleForResizeDown());
        assertFalse(realtimeVm.getSettings().getIsEligibleForResizeDown());
    }

    @Test
    public void testEconomyCommodityMapping() {
        final long entityId = 10;
        final TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(1)
                .setOid(entityId)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(topologyCommodity1))
                .addCommoditySoldList(CommoditySoldDTO.newBuilder().setCommodityType(topologyCommodity2))
                .build();

        // We rely on the commodityType allocator in TopologyConverter to
        // allocate commodityTypes to the economyCommodities that
        // are equal to what we hard-code in setup().
        // Since we create a new TopologyConverter here that's fine, as long
        // as the implementation of the ID allocator doesn't change.
        final TopologyConverter converter =
                new TopologyConverterUtil.Builder()
                        .topologyInfo(REALTIME_TOPOLOGY_INFO)
                        .includeGuaranteedBuyer(true)
                        .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                        .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                        .marketCloudRateExtractor(marketCloudRateExtractor)
                        .cloudCostData(ccd)
                        .commodityIndexFactory(CommodityIndex.newFactory())
                        .tierExcluderFactory(tierExcluderFactory)
                        .consistentScalingHelperFactory(consistentScalingHelperFactory)
                        .reversibilitySettingFetcher(reversibilitySettingFetcher)
                        .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                        .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                        .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                        .useVMReservationAsUsed(true)
                        .singleVMonHost(false)
                        .customUtilizationThreshold(0.5f)
                        .build();
        converter.convertToMarket(ImmutableMap.of(entityDTO.getOid(), entityDTO));

        assertEquals(topologyCommodity1, converter.getCommodityConverter()
                .marketToTopologyCommodity(economyCommodity1).orElseThrow(() ->
                        new RuntimeException("cannot convert commodity1")));
        assertEquals(topologyCommodity2, converter.getCommodityConverter()
                .marketToTopologyCommodity(economyCommodity2).orElseThrow(() ->
                        new RuntimeException("cannot convert commodity2")));
    }

    /**
     * We sort the shopping lists by provider entity type followed by provider id. This test verifies this.
     * We setup a VM with 3 commBoughtGroupings in this order - PM grouping (provider type 14),
     * volume 2 grouping (volume id 10, provider type 2), volume 1 SL (volume id 9, provider type 2)
     * The trader should be created with shopping lists in the order - PM, volume 1, volume 2
     * @throws IOException when the file is not found.
     */
    @Test
    public void testShoppingListSorting() throws IOException {
        TopologyEntityDTO vmDTO = messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        final TopologyEntityDTO vsanDsDTO = messageFromJsonFile("protobuf/messages/vsan-ds-1.dto.json");
        CommoditiesBoughtFromProvider.Builder pmCommBoughtGrouping = vmDTO
                .getCommoditiesBoughtFromProvidersList().get(0).toBuilder();
        CommoditiesBoughtFromProvider.Builder volume1CommBoughtGrouping = vmDTO
                .getCommoditiesBoughtFromProvidersList().get(1).toBuilder();
        CommoditiesBoughtFromProvider.Builder volume2CommBoughtGrouping = vmDTO
                .getCommoditiesBoughtFromProvidersList().get(2).toBuilder();
        TopologyEntityDTO.Builder entityDTOBuilder = vmDTO.toBuilder();
        entityDTOBuilder.clearCommoditiesBoughtFromProviders();
        entityDTOBuilder.addAllCommoditiesBoughtFromProviders(Arrays.asList(pmCommBoughtGrouping.build(),
                volume2CommBoughtGrouping.build(), volume1CommBoughtGrouping.build()));
        vmDTO = entityDTOBuilder.build();

        final TopologyConverter converter =
                new TopologyConverterUtil.Builder()
                        .topologyInfo(REALTIME_TOPOLOGY_INFO)
                        .includeGuaranteedBuyer(true)
                        .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                        .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                        .marketCloudRateExtractor(marketCloudRateExtractor)
                        .cloudCostData(ccd)
                        .commodityIndexFactory(CommodityIndex.newFactory())
                        .tierExcluderFactory(tierExcluderFactory)
                        .consistentScalingHelperFactory(consistentScalingHelperFactory)
                        .reversibilitySettingFetcher(reversibilitySettingFetcher)
                        .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                        .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                        .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                        .useVMReservationAsUsed(true)
                        .customUtilizationThreshold(0.5f)
                        .build();
        Collection<TraderTO> traders = converter.convertToMarket(ImmutableMap.of(
                vmDTO.getOid(), vmDTO,
                vsanDsDTO.getOid(), vsanDsDTO));

        Map<Integer, TraderTO> traderByType = traders.stream().collect(
                Collectors.toMap(TraderTO::getType, Function.identity()));
        TraderTO vm = traderByType.get(EntityType.VIRTUAL_MACHINE_VALUE);
        ShoppingListInfo pmSlInfo = converter.getShoppingListOidToInfos().get(
                vm.getShoppingListsList().get(0).getOid());
        assertEquals(14, (int)pmSlInfo.getSellerEntityType().get());
        ShoppingListInfo volume1slInfo = converter.getShoppingListOidToInfos().get(
            vm.getShoppingListsList().get(1).getOid());
        Assert.assertEquals(205L, volume1slInfo.getSellerId().longValue());
        ShoppingListInfo volume2slInfo = converter.getShoppingListOidToInfos().get(
                vm.getShoppingListsList().get(2).getOid());
        Assert.assertEquals(206L, volume2slInfo.getSellerId().longValue());

        TraderTO vsanDataStore = traderByType.get(EntityType.STORAGE_VALUE);
        Long[] slProviderOids = new Long[3];
        vsanDataStore.getShoppingListsList().stream()
                .map(ShoppingListTO::getSupplier).collect(Collectors.toList()).toArray(slProviderOids);
        assertTrue(isSorted(slProviderOids));
    }

    /**
     * Load a json file into a DTO.
     * @param fileName the name of the file to load
     * @return The entity DTO represented by the file
     * @throws IOException when the file is not found
     */
    public static TopologyEntityDTO messageFromJsonFile(String fileName) throws IOException {
        URL fileUrl = TopologyConverterToMarketTest.class.getClassLoader().getResources(fileName).nextElement();
        TopologyEntityDTO.Builder builder = TopologyEntityDTO.newBuilder();
        JsonFormat.parser().merge(new InputStreamReader(fileUrl.openStream()), builder);
        return builder.build();
    }

    @Nonnull
    private Collection<TraderTO> convertToMarketTO(@Nonnull final Set<TopologyDTO.TopologyEntityDTO> topology,
                                                   @Nonnull final TopologyInfo topologyInfo) {
        return new TopologyConverterUtil.Builder()
                .topologyInfo(topologyInfo)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build()
                .convertToMarket(topology.stream()
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity())));
    }

    /**
     * Load three protobuf files and test if includeVDC flag is false, conversion of certain
     * entities, commoditiesSold and shoppingList should be skipped.
     * @throws IOException when one of the files cannot be load
     */
    @Test
    public void testSkipVDC() throws IOException {
        final Map<Long, TopologyEntityDTO> topologyDTOs = Stream.of(
                messageFromJsonFile("protobuf/messages/vdc-2.dto.json"),
                messageFromJsonFile("protobuf/messages/pm-2.dto.json"),
                messageFromJsonFile("protobuf/messages/vm-2.dto.json"))
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        Collection<TraderTO> traderTOs = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(false)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build()
                .convertToMarket(topologyDTOs);
        assertEquals(2, traderTOs.size());
        for (TraderTO traderTO : traderTOs) {
            if (traderTO.getType() == EntityType.PHYSICAL_MACHINE_VALUE) {
                // this is the pm, so trader should not contain MemAllocation in commSold
                assertFalse(traderTO.getCommoditiesSoldList().stream()
                        .anyMatch(c -> c.getSpecification().getType() ==
                                CommodityDTO.CommodityType.MEM_ALLOCATION_VALUE));
            }
            if (traderTO.getType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                // this is the vm, so trader should not contain shoppinglist buys from VDC
                assertFalse(traderTO.getShoppingListsList().stream()
                        .anyMatch(s -> s.getSupplier() == 100));
            }
        }
    }
    private static final Set<String> CONSTANT_PRICE_TYPES_S = ImmutableSet.of(
            CommodityDTO.CommodityType.COOLING.name(), CommodityDTO.CommodityType.POWER.name(), CommodityDTO.CommodityType.SPACE.name(),
            CommodityDTO.CommodityType.APPLICATION.name(), CommodityDTO.CommodityType.CLUSTER.name(),
            // DSPM_ACCESS and DATASTORE are excluded because the converter never creates them
            CommodityDTO.CommodityType.DATACENTER.name(), CommodityDTO.CommodityType.NETWORK.name(),
            CommodityDTO.CommodityType.STORAGE_CLUSTER.name(),
            CommodityDTO.CommodityType.VAPP_ACCESS.name(), CommodityDTO.CommodityType.VDC.name(),
            CommodityDTO.CommodityType.VMPM_ACCESS.name()
    );

    private static final Set<String> SEGMENTATION_CONSTANT_PRICE_TYPES_S = ImmutableSet.of(
            CommodityDTO.CommodityType.SEGMENTATION.name(),
            CommodityDTO.CommodityType.DRS_SEGMENTATION.name());

    private static final Set<String> STEP_PRICE_TYPES_S = ImmutableSet.of(
            CommodityDTO.CommodityType.STORAGE_AMOUNT.name(),
            CommodityDTO.CommodityType.STORAGE_PROVISIONED.name(),
            CommodityDTO.CommodityType.VSTORAGE.name());

    private static final PriceFunctionTO CONSTANT =
            PriceFunctionTO.newBuilder().setConstant(
                    PriceFunctionDTOs.PriceFunctionTO.Constant.newBuilder()
                            .setValue(1.0f)
                            .build())
                    .build();

    private static final PriceFunctionTO SEGMENTATION_CONSTANT =
            PriceFunctionTO.newBuilder().setConstant(
                    PriceFunctionDTOs.PriceFunctionTO.Constant.newBuilder()
                            .setValue(0.00001f)
                            .build())
                    .build();

    private static final PriceFunctionTO STEP =
            PriceFunctionTO.newBuilder().setStep(
                    PriceFunctionDTOs.PriceFunctionTO.Step.newBuilder()
                            .setStepAt(1)
                            .setPriceAbove(Float.POSITIVE_INFINITY)
                            .setPriceBelow(0.0001f)
                            .build())
                    .build();

    /**
     * Test that step and constant price functions are created as needed.
     */
    @Test
    public void testPriceFunctions() {
        // Create an entity that sells ALL commodity types
        TopologyDTO.TopologyEntityDTO.Builder entityBuilder = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(1001L).setEntityType(3);
        Arrays.stream(CommodityDTO.CommodityType.values())
                .forEach(type ->
                        entityBuilder.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(type.getNumber())
                                        .build())
                                .build())
                                .build());
        Set<TopologyDTO.TopologyEntityDTO> topologyDTOs = Sets.newHashSet(entityBuilder.build());
        TraderTO traderTO = convertToMarketTO(topologyDTOs, REALTIME_TOPOLOGY_INFO).iterator().next();
        verifyPriceFunctions(traderTO, CONSTANT_PRICE_TYPES_S, PriceFunctionTO::hasConstant, CONSTANT);
        verifyPriceFunctions(traderTO, SEGMENTATION_CONSTANT_PRICE_TYPES_S, PriceFunctionTO::hasConstant, SEGMENTATION_CONSTANT);
        verifyPriceFunctions(traderTO, STEP_PRICE_TYPES_S, PriceFunctionTO::hasStep, STEP);
    }

    private void verifyPriceFunctions(TraderTO traderTO, Set<String> types,
                                      Function<PriceFunctionTO, Boolean> f, PriceFunctionTO pf) {
        List<PriceFunctionTO> list = traderTO.getCommoditiesSoldList().stream()
                .filter(comm -> types.contains(comm.getSpecification().getDebugInfoNeverUseInCode()))
                .map(CommoditySoldTO::getSettings)
                .map(CommoditySoldSettingsTO::getPriceFunction)
                .collect(toList());
        assertEquals(types.size(), list.size());
        assertEquals(1, list.stream().distinct().count());
        assertTrue(f.apply(list.get(0)));
        assertEquals(pf, list.get(0));
    }

    @Test
    public void testSkipEntity() {
        TopologyDTO.TopologyEntityDTO container = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(1001L).setEntityType(40).build();
        TopologyDTO.TopologyEntityDTO virtualApp = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(1002L).setEntityType(26).build();
        TopologyDTO.TopologyEntityDTO actionManager = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(1003L).setEntityType(22).build();
        TopologyDTO.TopologyEntityDTO storage = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(1004L).setEntityType(2).build();
        TopologyDTO.TopologyEntityDTO unknownStorage = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(1005L).setEntityType(2).setEntityState(TopologyDTO.EntityState.UNKNOWN).build();
        TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .includeGuaranteedBuyer(TopologyConverter.INCLUDE_GUARANTEED_BUYER_DEFAULT)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .useVMReservationAsUsed(true)
                .customUtilizationThreshold(0.5f)
                .build();
        assertEquals(4, converter.convertToMarket(
                Stream.of(container, virtualApp, actionManager, storage, unknownStorage)
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity())))
                .size());
        converter.getSkippedEntities().containsKey(unknownStorage.getOid());
    }

    /**
     * Test adding virtual volume to skippedEntities map if it's containerPod provider.
     */
    @Test
    public void testSkipEntitiesOfContainerPodProviders() {
        final TopologyDTO.TopologyEntityDTO vm = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(1001L).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).build();
        final TopologyDTO.TopologyEntityDTO volume = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(1002L).setEntityType(EntityType.VIRTUAL_VOLUME_VALUE).build();
        final TopologyDTO.TopologyEntityDTO containerPod =
                TopologyEntityDTO.newBuilder()
                        .setOid(1003L)
                        .setEntityType(EntityType.CONTAINER_POD_VALUE)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderId(vm.getOid()))
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderId(volume.getOid()))
                        .build();
        final TopologyDTO.TopologyEntityDTO container =
                TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setOid(1004L)
                        .setEntityType(EntityType.CONTAINER_VALUE)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                                .setProviderId(containerPod.getOid()))
                        .build();
        TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                //.marketCloudRateExtractor(null)
                .includeGuaranteedBuyer(TopologyConverter.INCLUDE_GUARANTEED_BUYER_DEFAULT)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .useVMReservationAsUsed(true)
                .customUtilizationThreshold(0.5f)
                .build();
        Collection<TraderTO> traderTOs = converter.convertToMarket(
                Stream.of(vm, volume, containerPod, container)
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity())));
        assertEquals(3, traderTOs.size());
        // VirtualVolume as pod provider is added to the skippedEntities map.
        assertTrue(converter.getSkippedEntities().containsKey(volume.getOid()));
        assertEquals(volume, converter.getSkippedEntities().get(volume.getOid()));
    }

    /**
     * Shopping lists with UNKNOWN providers are movable false.
     */
    @Test
    public void testShoppingListsWithUnknownHostNotMovable() {
        TopologyDTO.TopologyEntityDTO unknownStorage = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(1005L).setEntityType(2).setEntityState(TopologyDTO.EntityState.UNKNOWN).build();
        TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(10)
                .setOid(123)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setMovable(true)
                        .setProviderId(1005L)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.CPU_VALUE))))
                .build();
        TraderTO trader = convertToMarketTO(Sets.newHashSet(unknownStorage, entityDTO),
                REALTIME_TOPOLOGY_INFO).stream()
                .filter(a->a.getShoppingListsList().size() > 0).findFirst().get();
        assertFalse(trader.getShoppingLists(0).getMovable());
    }

    /**
     * Shopping lists with FAILOVER providers are movable false.
     */
    @Test
    public void testShoppingListsWithFailoverProviderNotMovable() {
        TopologyDTO.TopologyEntityDTO failOverProvider = TopologyDTO.TopologyEntityDTO.newBuilder()
                .setOid(1005L).setEntityType(2).setEntityState(EntityState.FAILOVER).build();
        TopologyEntityDTO vmConsumer = TopologyEntityDTO.newBuilder()
                .setEntityType(10)
                .setOid(123)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setMovable(true)
                        .setProviderId(1005L)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.CPU_VALUE))))
                .build();
        TraderTO vmTrader = convertToMarketTO(Sets.newHashSet(failOverProvider, vmConsumer), REALTIME_TOPOLOGY_INFO).stream()
                .filter(t -> t.getOid() == 123L)
                .findFirst()
                .get();
        assertFalse(vmTrader.getShoppingLists(0).getMovable());
    }

    /**
     * If the fields in Analysis setting have been set, they will be directly used in trader settings.
     */
    @Test
    public void testTraderSetting() {
        final TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(1)
                .setOid(123)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setMovable(false)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.CPU_VALUE))))
                .setAnalysisSettings(AnalysisSettings.newBuilder()
                        .setIsAvailableAsProvider(false)
                        .setShopTogether(false)
                        .setCloneable(false)
                        .setSuspendable(false)
                        .setDesiredUtilizationTarget(70.0f)
                        .setDesiredUtilizationRange(20.0f)
                        .setRateOfResize(1))
                .build();
        TraderTO trader = convertToMarketTO(Sets.newHashSet(entityDTO), REALTIME_TOPOLOGY_INFO).iterator().next();
        assertFalse(trader.getShoppingLists(0).getMovable());
        assertFalse(trader.getSettings().getCanAcceptNewCustomers());
        assertFalse(trader.getSettings().getIsShopTogether());
        assertFalse(trader.getSettings().getClonable());
        assertFalse(trader.getSettings().getSuspendable());
        assertThat(trader.getSettings().getMinDesiredUtilization(),
                is(((70.0f - (20.0f / 2.0f)) / 100.0f)));
        assertThat(trader.getSettings().getMaxDesiredUtilization(),
                is(((70.0f + (20.0f / 2.0f)) / 100.0f)));
        assertEquals(trader.getSettings().getQuoteFactor(), MarketAnalysisUtils.QUOTE_FACTOR, 0.0001);
        assertEquals(trader.getSettings().getMoveCostFactor(), MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR, 0.0001);
        assertThat(trader.getSettings().getRateOfResize(), is(10000000000.0f));

        final TopologyEntityDTO oppositeEntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(1)
                .setOid(123)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setMovable(true)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.CPU_VALUE))))
                .setAnalysisSettings(AnalysisSettings.newBuilder()
                        .setIsAvailableAsProvider(true)
                        .setShopTogether(true)
                        .setCloneable(true)
                        .setSuspendable(true)
                        .setRateOfResize(2))
                .build();
        TraderTO traderTwo = convertToMarketTO(Sets.newHashSet(oppositeEntityDTO), REALTIME_TOPOLOGY_INFO).iterator().next();
        assertTrue(traderTwo.getShoppingLists(0).getMovable());
        assertTrue(traderTwo.getSettings().getCanAcceptNewCustomers());
        assertTrue(traderTwo.getSettings().getIsShopTogether());
        assertTrue(traderTwo.getSettings().getClonable());
        // OM-51562 marks GuestLoads as daemons, which causes virtually all VMs suspend.
        // Before that, although all VMs were suspendable, it was the presence of the GuestLoad
        // that prevented VM suspension, which effectively caused the suspendable flag on VMs to be
        // ignored.  Since this test entity is at the top of the supply chain, it is marked not
        // suspendable, but the explicit setting above sets it to suspendable true. In order to
        // preserve existing behavior (not suspend VMs that do not host containers), the suspendable
        // flag will continue to be ignored and VMs that do not host containers will not suspend.
        assertFalse(traderTwo.getSettings().getSuspendable());
        assertEquals(traderTwo.getSettings().getQuoteFactor(), MarketAnalysisUtils.QUOTE_FACTOR, epsilon);
        assertThat(traderTwo.getSettings().getRateOfResize(), is(4.0f));

        final TopologyEntityDTO entityThree = TopologyEntityDTO.newBuilder()
                .setEntityType(1)
                .setOid(123)
                .setAnalysisSettings(AnalysisSettings.newBuilder().setRateOfResize(3))
                .build();
        TraderTO traderThree = convertToMarketTO(Sets.newHashSet(entityThree), REALTIME_TOPOLOGY_INFO).iterator().next();
        assertThat(traderThree.getSettings().getRateOfResize(), is(1.0f));

        final TopologyEntityDTO entityFour = TopologyEntityDTO.newBuilder()
                .setEntityType(1)
                .setOid(123)
                .setAnalysisSettings(AnalysisSettings.newBuilder())
                .build();
        TraderTO traderFour = convertToMarketTO(Sets.newHashSet(entityFour), REALTIME_TOPOLOGY_INFO).iterator().next();
        assertThat(traderFour.getSettings().getRateOfResize(), is(4.0f));

        final TopologyEntityDTO entityFive = TopologyEntityDTO.newBuilder()
                .setEntityType(1)
                .setOid(123)
                .setAnalysisSettings(AnalysisSettings.newBuilder().setRateOfResize(10))
                .build();
        TraderTO traderFive = convertToMarketTO(Sets.newHashSet(entityFive), REALTIME_TOPOLOGY_INFO).iterator().next();
        assertThat(traderFive.getSettings().getRateOfResize(), is(4.0f));
    }

    @Test
    public void testLimitFloatRangeWithinRange() {
        assertThat(TopologyConversionUtils.limitFloatRange(1f, 0f, 1f), is(1f));
        assertThat(TopologyConversionUtils.limitFloatRange(0f, 0f, 1f), is(0f));
        assertThat(TopologyConversionUtils.limitFloatRange(0.8f, 0f, 1f), is(0.8f));
    }

    @Test
    public void testLimitFloatRangeLessThanMin() {
        assertThat(TopologyConversionUtils.limitFloatRange(-1.2f, 0f, 1f), is(0f));
    }

    @Test
    public void testLimitFloatRangeGreaterThanMax() {
        assertThat(TopologyConversionUtils.limitFloatRange(100f, 0f, 1f), is(1f));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLimitFloatRangeMinGreaterThanMax() {
        TopologyConversionUtils.limitFloatRange(100f, 10, 0f);
    }

    @Test
    public void testGetMinDesiredUtilization() {
        TopologyEntityDTO entityDTO =
                TopologyEntityDTO.newBuilder()
                        .setEntityType(1)
                        .setOid(1)
                        .setAnalysisSettings(
                                AnalysisSettings.newBuilder()
                                        .setDesiredUtilizationTarget(40)
                                        .setDesiredUtilizationRange(40)
                                        .build())
                        .build();

        // (40 - (40/2.0))/100
        assertThat(TopologyConversionUtils.getMinDesiredUtilization(entityDTO),
                is(0.2f));
    }

    @Test
    public void testGetMinDesiredUtilizationOutofRange() {
        TopologyEntityDTO entityDTO =
                TopologyEntityDTO.newBuilder()
                        .setEntityType(1)
                        .setOid(1)
                        .setAnalysisSettings(
                                AnalysisSettings.newBuilder()
                                        .setDesiredUtilizationTarget(20)
                                        .setDesiredUtilizationRange(80)
                                        .build())
                        .build();

        // (20 - (80/2.0))/100
        assertThat(TopologyConversionUtils.getMinDesiredUtilization(entityDTO),
                is(0f));
    }

    @Test
    public void testGetMaxDesiredUtilization() {
        TopologyEntityDTO entityDTO =
                TopologyEntityDTO.newBuilder()
                        .setEntityType(1)
                        .setOid(1)
                        .setAnalysisSettings(
                                AnalysisSettings.newBuilder()
                                        .setDesiredUtilizationTarget(20)
                                        .setDesiredUtilizationRange(80)
                                        .build())
                        .build();

        // (20 + (80/2.0))/100
        assertThat(TopologyConversionUtils.getMaxDesiredUtilization(entityDTO),
                is(0.6f));
    }

    @Test
    public void testDBUsedValues() {
        TopologyEntityDTO entityDTO =
                TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.DATABASE_VALUE)
                        .setOid(1)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                                .setMovable(true)
                                .setProviderId(1005L))
                        .build();
        CommodityBoughtDTO boughtCommodityDTO = CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE).build())
                .setUsed(102400) // 100 GB.
                .setPeak(512000).build(); // 500 GB.

        final TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .customUtilizationThreshold(0.5f)
                .build();
        final List<CommodityBoughtTO> boughtTOs = converter.createAndValidateCommBoughtTO(entityDTO,
                boughtCommodityDTO, 1005L, Optional.empty());
        CommodityBoughtTO to = boughtTOs.iterator().next();
        assertEquals(100, to.getQuantity(), DELTA);
        assertEquals(500, to.getPeakQuantity(), DELTA);
    }

    @Test
    public void testDBUsedValuesWithReservedCapacity() {
        double used = 102400;
        double peak = 512000;
        double max = 512000;
        final double commSoldCap = 100;
        final double commSoldRtu = 1;
        final double reservedCapacity = 256000;
        double[] resizedCapacity = getResizedCapacityForCloud(EntityType.DATABASE_SERVER_VALUE,
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, used, peak, max,
                commSoldCap, commSoldRtu, 0.8d, 0.8d, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), reservedCapacity, Collections.emptyList());
        assertThat(resizedCapacity[0], is(reservedCapacity));
        assertEquals(peak, resizedCapacity[1], DELTA);
    }

    /**
     * Test to check if an entityId is part of wastedEntityId, trader is not created.
     */
    @Test
    public void testWastedEntityIdTraderCreation() {
            TopologyEntityDTO entityDTO1 =
                    TopologyEntityDTO.newBuilder()
                            .setEntityType(EntityType.VIRTUAL_MACHINE_SPEC_VALUE)
                            .setOid(1).build();
        TopologyEntityDTO entityDTO2 =
                TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_SPEC_VALUE)
                        .setOid(2).build();
        TopologyEntityDTO entityDTO3 =
                TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_SPEC_VALUE)
                        .setOid(3).build();
        TopologyEntityDTO entityDTO4 =
                TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                        .setOid(4).build();

        WastedEntityResults wastedEntityResults = mock(WastedEntityResults.class);
        TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(TopologyConverter.INCLUDE_GUARANTEED_BUYER_DEFAULT)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .useVMReservationAsUsed(true)
                .customUtilizationThreshold(0.5f)
                .build();
        converter.addAllWastedEntityResults(Collections.singleton(wastedEntityResults));
        assertEquals(3, converter.convertToMarket(
                        Stream.of(entityDTO1, entityDTO2, entityDTO3, entityDTO4)
                                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity())),
                        Collections.EMPTY_SET).size());
    }

    @Test
    public void testGetMaxDesiredUtilizationOutOfRange() {
        TopologyEntityDTO entityDTO =
                TopologyEntityDTO.newBuilder()
                        .setEntityType(1)
                        .setOid(1)
                        .setAnalysisSettings(
                                AnalysisSettings.newBuilder()
                                        .setDesiredUtilizationTarget(80)
                                        .setDesiredUtilizationRange(60)
                                        .build())
                        .build();

        // (80 + (60/2.0))/100
        assertThat(TopologyConversionUtils.getMaxDesiredUtilization(entityDTO),
                is(1f));
    }

    @Test
    public void testEntityInControlState() {
        TopologyEntityDTO pmEntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setOid(1)
                .setAnalysisSettings(AnalysisSettings.newBuilder()
                        .setCloneable(true)
                        .setSuspendable(true)
                        .setIsAvailableAsProvider(true)
                        .setControllable(false).build())
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.CPU_VALUE).build())
                        .build())
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setMovable(true)
                        .setProviderId(10000L)
                        .setProviderEntityType(EntityType.DATACENTER_VALUE)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.COOLING_VALUE))))
                .build();
        TopologyEntityDTO vmEntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setOid(100)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.APPLICATION_VALUE).build())
                        .build())
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setMovable(true)
                        .setProviderId(1)
                        .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.CPU_VALUE))))
                .build();
        final TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .customUtilizationThreshold(0.5f)
                .build();
        Map<Long, TopologyEntityDTO> topology = new HashMap<>();
        topology.put(pmEntityDTO.getOid(), pmEntityDTO);
        topology.put(vmEntityDTO.getOid(), vmEntityDTO);
        Collection<TraderTO> traders = converter.convertToMarket(topology);
        for (TraderTO t : traders) {
            if (t.getType() == vmEntityDTO.getEntityType()) {
                assertFalse(t.getShoppingLists(0).getMovable());
            } else if (t.getType() == pmEntityDTO.getEntityType()) {
                assertFalse(t.getSettings().getCanAcceptNewCustomers());
                assertFalse(t.getSettings().getClonable());
                assertFalse(t.getSettings().getSuspendable());
                assertFalse(t.getShoppingLists(0).getMovable());
            }
        }
    }

    @Test
    public void testGetResizedCapacityForCloudZeroResizeTargetUtilNoException() {
        double used = 70;
        double peak = 80;
        double max = 90;
        final double commSoldCap = 200;
        final double commSoldRtu = 0;
        getResizedCapacityForCloud(EntityType.DATABASE_SERVER_VALUE,
                CommodityDTO.CommodityType.CPU_VALUE,
                CommodityDTO.CommodityType.VCPU_VALUE, used, peak, max,
                commSoldCap, commSoldRtu, 70d, 80d, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), 0.0d, Collections.emptyList());
        // Expects no exception with a zero resize target util
    }

    @Test
    public void testGetResizedCapacityForCloudResizeDown() {
        double used = 40;
        double peak = 80;
        double max = 90;
        double histPercentile = 72;
        double histUtilizaiton = 72;
        double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.MEM_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE,
                used, peak, max, 200D, 0.9,
                histPercentile, histUtilizaiton, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), 0.0d, Collections.emptyList());
        // new used = histPercentile * capacity / target util
        // new peak = max(peak, used) / rtu
        // percentile in the historical values is set as a percent value that needs to
        // be converted to the absolute used for the capacity.
        assertEquals(histPercentile * 200 / 0.9, quantities[0], 0.01f);
        assertEquals(88.888, quantities[1], 0.01f);
    }

    /**
     * Test that if the percentile utilization is within target band, then resize quantity is
     * same as current capacity.
     */
    @Test
    public void testGetResizedCapacityWithinTargetBand() {
        double used = 30;
        double peak = 50;
        double max = 100;
        double histPercentile = 0.72;
        double histUtilizaiton = 0.72;
        double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_VOLUME_VALUE,
                CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE,
                CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE,
                used, peak, max, 100D, 0.7,
                histPercentile, histUtilizaiton, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), 0.0d, Collections.emptyList());
        assertEquals(100, quantities[0], 0.01f);
    }

    /**
     * Test that if the percentile utilization is outside of target band, then resize quantity is
     * calculated based on target utilization, percentile and capacity.
     */
    @Test
    public void testGetResizedCapacityOutsideTargetBand() {
        double used = 10;
        double peak = 20;
        double max = 20;
        double histPercentile = 0.2;
        double histUtilizaiton = 0.2;
        double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_VOLUME_VALUE,
                CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE,
                CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE,
                used, peak, max, 100D, 0.7,
                histPercentile, histUtilizaiton, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), 0.0d, Collections.emptyList());
        assertEquals(0.2 * 100 / 0.7, quantities[0], 0.01f);
    }

    /**
     * Test for getResizedCapacity when no history is set.
     */
    @Test
    public void testGetResizedCapacityForCloudResizeDownNoHist() {
        final double capacity = 200;
        double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.MEM_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE,
                null, null, null, capacity, 0.9,
                null, null, EnvironmentType.CLOUD, null, null, REALTIME_TOPOLOGY_INFO,
                true, Optional.empty(), 0.0d, Collections.emptyList());
        // resize quantity is set to current capacity, as no historical used data is set
        // resize peak quantity is set to current capacity, as no historical peak data is set
        assertEquals(capacity, quantities[0], 0.01f);
        assertEquals(capacity, quantities[1], 0.01f);
    }

    /**
     * Test for getResizedCapacity in OnPrem when percentile is not set.
     */
    @Test
    public void testGetResizedCapacityForOnPremNoPercentile() {
        double used = 40;
        double peak = 80;
        double max = 90;
        double histUtil = 70;
        double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.MEM_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE,
                used, peak, max, 200D, 0.9,
                null, histUtil, EnvironmentType.ON_PREM, used, peak, REALTIME_TOPOLOGY_INFO,
                true, Optional.empty(), 0.0d, Collections.emptyList());
        // new used = [(max * 0.9) + (used * 0.1)] / rtu
        // new peak = max(peak, used) / rtu
        assertEquals(histUtil, quantities[0], 0.01f);
        assertEquals(peak, quantities[1], 0.01f);
    }

    /**
     * Test for getResizedCapacity in OnPrem when percentile is not set.
     */
    @Test
    public void testGetResizedCapacityForOnPremPercentile() {
        double used = 40;
        double peak = 80;
        double max = 90;
        double histUtil = 70;
        double histPercentile = 67;
        double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.MEM_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE,
                used, peak, max, 200D, 0.9,
                histPercentile, histUtil, EnvironmentType.ON_PREM, used, peak,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), 0.0d, Collections.emptyList());
        // new used = [(max * 0.9) + (used * 0.1)] / rtu
        // new peak = max(peak, used) / rtu
        assertEquals(histPercentile, quantities[0], 0.01f);
        assertEquals(peak, quantities[1], 0.01f);
    }


    /**
     * Test for getResizedCapacity percentile is set for OnPrem.
     */
    @Test
    public void testGetResizedCapacityForCloudResizeDownNoPercentile() {
        double used = 40;
        double peak = 80;
        double max = 90;
        double histUtil = 70;
        double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.MEM_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE,
                used, peak, max, 200D, 0.9,
                null, histUtil, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), 0.0d, Collections.emptyList());
        assertEquals(histUtil / 0.9, quantities[0], 0.01f);
        assertEquals(88.888, quantities[1], 0.01f);
    }

    /**
     * Tests the getResizeCapacity for a resize up use case.
     */
    @Test
    public void testGetResizedCapacityForCloudResizeUp() {
        double used = 40;
        double peak = 80;
        double max = 90;
        double histUtil = 80;
        double histPercentile = 70;
        double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.MEM_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE,
                used, peak, max, 100D, 0.5,
                histPercentile, histUtil, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), 0.0d, Collections.emptyList());
        // new used = percentile * capacity / target util
        // new peak = max(peak, used) / rtu
        // percentile in the historical values is set as a percent value that needs to
        // be converted to the absolute used for the capacity.
        assertEquals(140 * 100, quantities[0], 0.01f);
        assertEquals(160, quantities[1], 0.01f);
    }

    @Test
    public void testGetResizedCapacityForCloudNoResize() {
        double used = 70;
        double peak = 75;
        double max = 85;
        double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.MEM_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE, used, peak, max,
                100D, 0.8, 0.8, 75d, EnvironmentType.CLOUD, null, null, REALTIME_TOPOLOGY_INFO,
                true, Optional.empty(), 0.0d, Collections.emptyList());
        // new used = capacity
        // new peak = max(peak, used) / rtu
        assertEquals(100, quantities[0], 0.01f);
        assertEquals(peak / 0.8, quantities[1], 0.01f);
    }

    /**
     * Test that if percentile is not set and used is 0, then sold commodity resize quantity is same
     * as current capacity.
     */
    @Test
    public void testResizeQuantityForZeroUsed() {
        final double capacity = 100;
        final double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.MEM_VALUE, CommodityDTO.CommodityType.VMEM_VALUE, null,
                0D, 0D, capacity, 0.8, null, null, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), 0.0d, Collections.emptyList());
        assertEquals(capacity, quantities[0], 0.01f);
    }

    /**
     * Test that if percentile is set to 0, then the resize quantity is also 0.
     */
    @Test
    public void testResizeQuantityForZeroPercentile() {
        final double capacity = 100;
        final double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.MEM_VALUE, CommodityDTO.CommodityType.VMEM_VALUE, 0D,
                0D, 0D, capacity, 0.8, 0d, null, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), 0.0d, Collections.emptyList());
        assertEquals(0, quantities[0], 0.01f);
    }

    /**
     * Test that if commodity sold has resizable flag set to false, then resize quantity is same
     * as current capacity.
     */
    @Test
    public void testResizeQuantityForResizableFlagValues() {
        final double capacity = 100;
        final double histPercentile = 0.7;
        final double targetUtil = 0.9;
        double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.MEM_VALUE, CommodityDTO.CommodityType.VMEM_VALUE, 0D,
                0D, 0D, capacity, targetUtil, histPercentile, null, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), 0.0d, Collections.emptyList());
        // when resizable flag is true, resize quantity should be percentile x capacity /targetUtil
        assertEquals(histPercentile * capacity / targetUtil, quantities[0], 0.01f);

        quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.MEM_VALUE, CommodityDTO.CommodityType.VMEM_VALUE, 0D,
                0D, 0D, capacity, targetUtil, histPercentile, null, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, false, Optional.empty(), 0.0d, Collections.emptyList());
        // when resizable flag is false, resize quantity should be equal to current capacity
        assertEquals(capacity, quantities[0], 0.01f);
    }


    @Test
    public void testScaleDownWithReservedCapacity() {
        double used = 10;
        double peak = 30;
        double max = 40;
        final double commSoldCap = 100;
        final double commSoldRtu = 1;
        final double reservedCapacity  = commSoldCap;
        double[] resizedCapacity = getResizedCapacityForCloud(EntityType.DATABASE_SERVER_VALUE,
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, used, peak, max,
                commSoldCap, commSoldRtu, 0.8d, 0.8d, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), reservedCapacity, Collections.emptyList());
        assertThat(resizedCapacity[0], is(commSoldCap));
    }

    /**
     * Scaling down should not be impacted by lower_bound_scale_up limits.
     */
    @Test
    public void testScaleDownWithCommodityCapacityLimit() {
        double used = 10;
        double peak = 30;
        double max = 40;
        final double commSoldCap = 100;
        final double commSoldRtu = 1;
        final double lowerBoundForResizeUp = 110f;
        CommodityCapacityLimit commodityCapacityLimit = CommodityCapacityLimit.newBuilder()
                .setCapacity((float)lowerBoundForResizeUp)
                .setCommodityType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE).build();
        double[] resizedCapacity = getResizedCapacityForCloud(EntityType.DATABASE_SERVER_VALUE,
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, used, peak, max,
                commSoldCap, commSoldRtu, 0.8d, 0.8d, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.of(commodityCapacityLimit), 0.0d, Collections.emptyList());
        assertThat(resizedCapacity[0], is(commSoldCap * 0.8d));
    }

    @Test
    public void testScaleUpAboveLowerBoundScaleUp() {
        double used = 10;
        double peak = 30;
        double max = 40;
        final double commSoldCap = 100;
        final double commSoldRtu = 1;
        final double lowerBoundForResizeUp = 110f;
        CommodityCapacityLimit commodityCapacityLimit = CommodityCapacityLimit.newBuilder()
                .setCapacity((float)lowerBoundForResizeUp)
                .setCommodityType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE).build();
        final double reservedCapacity  = 0.0d;
        double[] resizedCapacity = getResizedCapacityForCloud(EntityType.DATABASE_SERVER_VALUE,
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, used, peak, max,
                commSoldCap, commSoldRtu, 2d, 2d, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.of(commodityCapacityLimit), reservedCapacity, Collections.emptyList());
        assertThat(resizedCapacity[0], greaterThan(lowerBoundForResizeUp));
    }

    @Test
    public void testScaleUpWithRawMaterials() {
        double used = 100;
        double peak = 100;
        double max = 100;
        final double commSoldCap = 100;
        final double commSoldRtu = 0.9;
        final double commSoldPercentile = 0.95;
        final double reservedCapacity  = commSoldCap;
        HistoricalValues.Builder highPercentileUtil = HistoricalValues.newBuilder().setPercentile(0.95);
        HistoricalValues.Builder lowPercentileUtil = HistoricalValues.newBuilder().setPercentile(0.2);
        CommoditySoldDTO.Builder dbCacheHitRateRawMaterial = CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(DB_CACHE_HIT_RATE_VALUE))
                .setEffectiveCapacityPercentage(90)
                .setCapacity(100);
        double[] resizedCapacity = getResizedCapacityForCloud(EntityType.DATABASE_SERVER_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE, used, peak, max,
                commSoldCap, commSoldRtu, commSoldPercentile, 1d, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), reservedCapacity,
                Collections.singletonList(dbCacheHitRateRawMaterial.setHistoricalUsed(highPercentileUtil).build()));
        assertThat(resizedCapacity[0], is(commSoldCap));

        resizedCapacity = getResizedCapacityForCloud(EntityType.DATABASE_SERVER_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE, used, peak, max,
                commSoldCap, commSoldRtu, commSoldPercentile, 1d, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), reservedCapacity,
                Collections.singletonList(dbCacheHitRateRawMaterial.setHistoricalUsed(lowPercentileUtil).build()));
        assertEquals(commSoldCap * commSoldPercentile/commSoldRtu, resizedCapacity[0], DELTA);
    }

    @Test
    public void testScaleDownWithRawMaterials() {
        double used = 50;
        double peak = 100;
        double max = 100;
        final double commSoldCap = 100;
        final double commSoldRtu = 0.9;
        final double commSoldPercentile = 0.5;
        HistoricalValues.Builder highPercentileUtil = HistoricalValues.newBuilder().setPercentile(0.95);
        HistoricalValues.Builder lowPercentileUtil = HistoricalValues.newBuilder().setPercentile(0.2);
        CommoditySoldDTO.Builder dbCacheHitRateRawMaterial = CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(DB_CACHE_HIT_RATE_VALUE))
                .setEffectiveCapacityPercentage(90)
                .setCapacity(100);
        double[] resizedCapacity = getResizedCapacityForCloud(EntityType.DATABASE_SERVER_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE, used, peak, max,
                commSoldCap, commSoldRtu, commSoldPercentile, 1d, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), 0d,
                Collections.singletonList(dbCacheHitRateRawMaterial.setHistoricalUsed(lowPercentileUtil).build()));
        assertThat(resizedCapacity[0], is(commSoldCap));

        resizedCapacity = getResizedCapacityForCloud(EntityType.DATABASE_SERVER_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE, used, peak, max,
                commSoldCap, commSoldRtu, commSoldPercentile, 1d, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.empty(), 0d,
                Collections.singletonList(dbCacheHitRateRawMaterial.setHistoricalUsed(highPercentileUtil).build()));
        assertEquals(commSoldCap * commSoldPercentile/commSoldRtu, resizedCapacity[0], DELTA);
    }

    private double[] getResizedCapacityForCloud(int entityType, int commBoughtType, int commSoldType,
                                                Double commSoldUsed, Double commSoldPeak,
                                                Double commSoldMax, Double commSoldCap, Double commSoldRtu,
                                                Double histPercentile, Double histoUtilization,
                                                final EnvironmentType envType, Double boughtUsed,
                                                Double boughtPeak, TopologyInfo topologyInfo,
                                                final boolean soldCommodityResizable,
                                                final Optional<CommodityCapacityLimit> commodityCapacityLimit,
                                                final Double reservedCapacity,
                                                final List<CommoditySoldDTO> rawMaterialsSold) {

        final HistoricalValues.Builder histValueBuilder = HistoricalValues.newBuilder();
        if (commSoldMax != null) {
            histValueBuilder.setMaxQuantity(commSoldMax);
        }
        if (histPercentile != null) {
            histValueBuilder.setPercentile(histPercentile);
        }
        if (histoUtilization != null) {
            histValueBuilder.setHistUtilization(histoUtilization);
        }
        HistoricalValues histValue = histValueBuilder.build();
        Builder commBoughtBuilder = CommodityBoughtDTO.newBuilder();
        commBoughtBuilder.setCommodityType(CommodityType.newBuilder()
                .setType(commBoughtType)
                .build());
        if (boughtPeak != null) {
            commBoughtBuilder.setPeak(boughtPeak);
        }
        if (boughtUsed != null) {
            commBoughtBuilder.setUsed(boughtUsed);
        }
        if (envType == EnvironmentType.ON_PREM) {
            commBoughtBuilder.setHistoricalUsed(histValue);
        }
        if (reservedCapacity != null) {
            commBoughtBuilder.setReservedCapacity(reservedCapacity);
        }
        CommodityBoughtDTO commBought = commBoughtBuilder.build();

        final CommoditySoldDTO.Builder soldBuilder = CommoditySoldDTO.newBuilder();
        soldBuilder.setCommodityType(CommodityType.newBuilder()
                .setType(commSoldType).build());
        if (commSoldPeak != null) {
            soldBuilder.setPeak(commSoldPeak);
        }
        if (commSoldCap != null) {
            soldBuilder.setCapacity(commSoldCap);
        }
        if (commSoldRtu != null) {
            soldBuilder.setResizeTargetUtilization(commSoldRtu);
        }
        if (commSoldUsed != null) {
            soldBuilder.setUsed(commSoldUsed);
        }

        // resizable is true by default, set only if soldCommodityResizable argument is false.
        if (!soldCommodityResizable) {
            soldBuilder.setIsResizeable(false);
        }
        if (envType == EnvironmentType.CLOUD) {
            soldBuilder.setHistoricalUsed(histValue);
        }
        TypeSpecificInfo.Builder typeSpecificInfo = TypeSpecificInfo.newBuilder();
        commodityCapacityLimit.ifPresent(capacityLimit -> typeSpecificInfo.setDatabase(
                DatabaseInfo.newBuilder().addLowerBoundScaleUp(capacityLimit).build()).build());
        final CommoditySoldDTO sold = soldBuilder.build();

        TopologyEntityDTO vmEntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(entityType)
                .setEnvironmentType(envType)
                .setOid(100)
                .addCommoditySoldList(sold)
                .addAllCommoditySoldList(rawMaterialsSold)
                .setTypeSpecificInfo(typeSpecificInfo)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PROVIDER_ID)
                        .addCommodityBought(commBought))
                .build();

        final TopologyConverter converter =
                new TopologyConverterUtil.Builder()
                        .topologyInfo(topologyInfo)
                        .marketMode(MarketMode.M2Only)
                        .includeGuaranteedBuyer(true)
                        .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                        .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                        .marketCloudRateExtractor(marketCloudRateExtractor)
                        .cloudCostData(ccd)
                        .commodityIndexFactory(CommodityIndex.newFactory())
                        .tierExcluderFactory(tierExcluderFactory)
                        .consistentScalingHelperFactory(consistentScalingHelperFactory)
                        .reversibilitySettingFetcher(reversibilitySettingFetcher)
                        .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                        .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                        .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                        .useVMReservationAsUsed(true)
                        .singleVMonHost(false)
                        .customUtilizationThreshold(0.5f)
                        .build();
        final float[][] resizedCapacitites = converter.getResizedCapacity(vmEntityDTO, commBought, PROVIDER_ID);
        return new double[]{resizedCapacitites[0][0], resizedCapacitites[1][0]};
    }

    /**
     * Test resize capacity for GCP IOPS commodities and historical used has not been set.
     */
    @Test
    public void testGetResizedCapacityForCloudResizeIOPS() {
        // pass negative values for boughtHistUsed and boughtUsedPeak to prevent hist utilization
        // from being set.
        final double[] ssdWriteQuantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.STORAGE_ACCESS_SSD_WRITE_VALUE, /*commodityBoughtUsed*/
                70, /*commodityBoughtPeak*/80, /*commodityBoughtMax*/
                90, /*commodityBoughtResizeTargetUtilization*/0.7, /*commoditySoldCapacity*/200,
                -1, -1);
        Assert.assertEquals(70 / 0.7, ssdWriteQuantities[0], 0.01f);
        Assert.assertEquals(80 / 0.7, ssdWriteQuantities[1], 0.01f);
        final double[] ssdReadQuantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.STORAGE_ACCESS_SSD_READ_VALUE, /*commodityBoughtUsed*/
                100, /*commodityBoughtPeak*/100, /*commodityBoughtMax*/
                90, /*commodityBoughtResizeTargetUtilization*/0.7, /*commoditySoldCapacity*/200,
                -1, -1);
        Assert.assertEquals(100 / 0.7, ssdReadQuantities[0], 0.01f);
        Assert.assertEquals(100 / 0.7, ssdReadQuantities[1], 0.01f);
        final double[] stWriteQuantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.STORAGE_ACCESS_STANDARD_WRITE_VALUE, /*commodityBoughtUsed*/
                70, /*commodityBoughtPeak*/80, /*commodityBoughtMax*/
                90, /*commodityBoughtResizeTargetUtilization*/0.75, /*commoditySoldCapacity*/200,
                -1, -1);
        Assert.assertEquals(70 / 0.75, stWriteQuantities[0], 0.01f);
        Assert.assertEquals(80 / 0.75, stWriteQuantities[1], 0.01f);
        final double[] stReadQuantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.STORAGE_ACCESS_STANDARD_READ_VALUE, /*commodityBoughtUsed*/
                80, /*commodityBoughtPeak*/90, /*commodityBoughtMax*/
                90, /*commodityBoughtResizeTargetUtilization*/0.75, /*commoditySoldCapacity*/200,
                -1, -1);
        Assert.assertEquals(80 / 0.75, stReadQuantities[0], 0.01f);
        Assert.assertEquals(90 / 0.75, stReadQuantities[1], 0.01f);
    }

    /**
     * Test resize capacity for GCP IO Throughput commodities and historical used has not been set.
     */
    @Test
    public void testGetResizedCapacityForCloudResizeIOThroughput() {
        // pass negative values for boughtHistUsed and boughtUsedPeak to prevent hist utilization
        // from being set.
        final double[] ssdWriteQuantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                IO_THROUGHPUT_READ_VALUE, /*commodityBoughtUsed*/
                70, /*commodityBoughtPeak*/80, /*commodityBoughtMax*/
                90, /*commodityBoughtResizeTargetUtilization*/0.7, /*commoditySoldCapacity*/200,
                -1, -1);
        Assert.assertEquals(70 / 0.7, ssdWriteQuantities[0], 0.01f);
        Assert.assertEquals(80 / 0.7, ssdWriteQuantities[1], 0.01f);
        final double[] ssdReadQuantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                IO_THROUGHPUT_WRITE_VALUE, /*commodityBoughtUsed*/
                100, /*commodityBoughtPeak*/100, /*commodityBoughtMax*/
                90, /*commodityBoughtResizeTargetUtilization*/0.7, /*commoditySoldCapacity*/200,
                -1, -1);
        Assert.assertEquals(100 / 0.7, ssdReadQuantities[0], 0.01f);
        Assert.assertEquals(100 / 0.7, ssdReadQuantities[1], 0.01f);
    }

    /**
     * Test resize capacity down for cloud throughput commodities and historical used has not been set.
     */
    @Test
    public void testGetResizedCapacityForCloudResizeDownThroughput() {
        // pass negative values for boughtHistUsed and boughtUsedPeak to prevent hist utilization
        // from being set.
        final double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE, /*commodityBoughtUsed*/
                70, /*commodityBoughtPeak*/80, /*commodityBoughtMax*/
                90, /*commodityBoughtResizeTargetUtilization*/0.9, /*commoditySoldCapacity*/200,
                -1, -1);
        // new used = used/targetUtil since histUsed is not available
        Assert.assertEquals(70 / 0.9, quantities[0], 0.01f);
        // new peak = max(peak, used) / rtu
        Assert.assertEquals(80 / 0.9, quantities[1], 0.01f);
    }

    /**
     * Test resize capacity down for cloud throughput commodities and historical used has been set.
     */
    @Test
    public void testGetResizedCapacityForCloudResizeDownThroughputWitHist() {
        final double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE, /*commodityBoughtUsed*/
                70, /*commodityBoughtPeak*/80, /*commodityBoughtMax*/
                90, /*commodityBoughtResizeTargetUtilization*/0.9, /*commoditySoldCapacity*/200,
                75, 85);
        // new used = boughtHistUsed/targetUtil since histUsed is available
        Assert.assertEquals(75 / 0.9, quantities[0], 0.01f);
        // new peak = max(peak, used) / rtu
        Assert.assertEquals(85 / 0.9, quantities[1], 0.01f);
    }

    /**
     * Test resize capacity up for cloud throughput commodities.
     */
    @Test
    public void testGetResizedCapacityForCloudResizeUpThroughputwithHist() {
        final double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.IO_THROUGHPUT_VALUE, /*commodityBoughtUsed*/
                0, /*commodityBoughtPeak*/0, /*commodityBoughtMax*/
                90, /*commodityBoughtResizeTargetUtilization*/0.5,
                /*commoditySoldCapacity*/100,
                70, 80);
        // new used = 70/0.9
        Assert.assertEquals(70 / 0.5, quantities[0], 0.01f);
        // new peak = max(peak, used) / rtu
        Assert.assertEquals(80 / 0.5, quantities[1], 0.01f);
    }

    /**
     * Test no resize capacity for cloud throughput commodities since boughtused/targetUtil == capacity.
     */
    @Test
    public void testGetResizedCapacityForCloudNoResizeThroughput() {
        final float capacity = 100;
        final double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE, /*commodityBoughtUsed*/
                70, /*commodityBoughtPeak*/75, /*commodityBoughtMax*/
                85, /*commodityBoughtResizeTargetUtilization*/0.5,
                /*commoditySoldCapacity*/capacity,
                50, 80);
        // resize quantity is set to current capacity, since hist used / target util equals
        // current capacity (which is 100)
        Assert.assertEquals(capacity, quantities[0], 0.01f);
        // resize peak quantity is set to max(peak, used) / rtu
        Assert.assertEquals(160, quantities[1], 0.01f);
    }

    /**
     * Test no resize capacity for cloud throughput commodities when hist bought used is 0.
     */
    @Test
    public void testGetResizedCapacityForZeroHistUsed() {
        final double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.NET_THROUGHPUT_VALUE, /*commodityBoughtUsed*/
                70, /*commodityBoughtPeak*/75, /*commodityBoughtMax*/
                85, /*commodityBoughtResizeTargetUtilization*/0.5,
                /*commoditySoldCapacity*/100,
                0, 0);
        // resize quantity is set to 0, as the historical used is 0
        Assert.assertEquals(0, quantities[0], 0.01f);
        // resize peak quantity is set to max(peak, used) / rtu
        Assert.assertEquals(140, quantities[1], 0.01f);
    }

    /**
     * Test migrate to cloud "Lift and Shift" (a.k.a. Allocation) scenario.
     * The capacity of the commodity sold is used as the demand of the commodity.
     */
    @Test
    public void testGetResizedCapacityForMCPLiftAndShift() {
        double used = 40;
        double peak = 80;
        double max = 90;
        double histPercentile = 75;
        double commSoldCapacity = 200;
        double commSoldRtu = 0.9;
        double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.MEM_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE,
                used, peak, max, commSoldCapacity, commSoldRtu,
                histPercentile, null, EnvironmentType.CLOUD, used, peak,
                MCP_ALLOCATION_PLAN_TOPOLOGY_INFO,
                true, Optional.empty(), 0.0d, Collections.emptyList());
        assertEquals(commSoldCapacity, quantities[0], 0.01f);
        assertEquals(commSoldCapacity, quantities[1], 0.01f);
    }

    /**
     * Test migrate to cloud "Optimization" plan (a.k.a. Consumption) scenario.
     * The percentile utilization and target utilization will be considered to come up with the
     * demand.
     */
    @Test
    public void testGetResizedCapacityForMCPOptimizationPlan() {
        double used = 40;
        double peak = 80;
        double max = 90;
        double histPercentile = 0.8;
        double commSoldCapacity = 200;
        double commSoldRtu = 0.9;
        double[] quantities = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_VALUE,
                CommodityDTO.CommodityType.MEM_VALUE,
                CommodityDTO.CommodityType.VMEM_VALUE,
                used, peak, max, commSoldCapacity, commSoldRtu,
                histPercentile, null, EnvironmentType.CLOUD, used, peak,
                MCP_COSUMPTION_PLAN_TOPOLOGY_INFO,
                true, Optional.empty(), 0.0d, Collections.emptyList());
        assertEquals(histPercentile * commSoldCapacity / commSoldRtu, quantities[0], 0.01f);
        assertEquals(peak / commSoldRtu, quantities[1], 0.01f);
    }

    /**
     * Test that the region_id field is set for a VM's startContext object if the VM is connected
     * to a region.
     */
    @Test
    public void testRegionalContextSetForCloudVm() {
        final long regionId = 5L;
        final long vmId = 3L;
        final long baId = 2L;
        final EntityState on = EntityState.POWERED_ON;
        when(accountPricingData.getAccountPricingDataOid()).thenReturn(baId);
        final TopologyEntityDTO region = entity(EntityType.REGION_VALUE, regionId, on,
                null, Collections.singletonList(createSoldCommodity(CommodityDTO
                        .CommodityType.DATACENTER_VALUE)));
        final TopologyEntityDTO vm = entity(EntityType.VIRTUAL_MACHINE_VALUE, vmId, on,
                Collections.singletonList(createConnectedEntity(regionId,
                        ConnectionType.AGGREGATED_BY_CONNECTION, EntityType.REGION_VALUE)),
                null);
        final TopologyEntityDTO ba = entity(EntityType.BUSINESS_ACCOUNT_VALUE, baId, on,
                Collections.singletonList(createConnectedEntity(vmId,
                        ConnectionType.OWNS_CONNECTION, EntityType.VIRTUAL_MACHINE_VALUE)),
                null);
        final TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();

        final Collection<TraderTO> traders =
                converter.convertToMarket(ImmutableList.of(region, vm, ba).stream()
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity())));

        Assert.assertEquals(1, traders.size());
        final TraderTO vmTrader = traders.iterator().next();
        final Context startContext = vmTrader.getSettings().getCurrentContext();
        Assert.assertEquals(regionId, startContext.getRegionId());
    }


    /**
     * Test that the zone_id and region_id fields are set for a VM's startContext object if the
     * VM is connected to a zone.
     */
    @Test
    public void testZonalContextSetForCloudVm() {
        final long regionId = 5L;
        final long zoneId = 4L;
        final long vmId = 3L;
        final long baId = 2L;
        when(accountPricingData.getAccountPricingDataOid()).thenReturn(baId);
        final EntityState on = EntityState.POWERED_ON;
        final TopologyEntityDTO zone = entity(EntityType.AVAILABILITY_ZONE_VALUE, zoneId, on,
                null, null);
        final TopologyEntityDTO region = entity(EntityType.REGION_VALUE, regionId, on,
                Collections.singletonList(createConnectedEntity(zoneId,
                        ConnectionType.OWNS_CONNECTION, EntityType.AVAILABILITY_ZONE_VALUE)),
                Collections.singletonList(createSoldCommodity(CommodityDTO
                        .CommodityType.DATACENTER_VALUE)));
        final TopologyEntityDTO vm = entity(EntityType.VIRTUAL_MACHINE_VALUE, vmId, on,
                Collections.singletonList(createConnectedEntity(zoneId,
                        ConnectionType.AGGREGATED_BY_CONNECTION, EntityType.AVAILABILITY_ZONE_VALUE)),
                null);
        final TopologyEntityDTO ba = entity(EntityType.BUSINESS_ACCOUNT_VALUE, baId, on,
                Collections.singletonList(createConnectedEntity(vmId,
                        ConnectionType.OWNS_CONNECTION, EntityType.VIRTUAL_MACHINE_VALUE)),
                null);
        final TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();

        final Collection<TraderTO> traders =
                converter.convertToMarket(ImmutableList.of(zone, region, vm, ba).stream()
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity())));

        Assert.assertEquals(1, traders.size());
        final TraderTO vmTrader = traders.iterator().next();
        final Context startContext = vmTrader.getSettings().getCurrentContext();
        Assert.assertEquals(regionId, startContext.getRegionId());
        Assert.assertEquals(zoneId, startContext.getZoneId());
    }

    /**
     * Test that Balance Account in the startContext of VMs have priceId set.
     */
    @Test
    public void testBalanceAccountPriceIdSet() {
        final long priceId = 11111L;
        final long regionId = 5L;
        final long baId = 2L;
        final long vmId = 3L;
        when(accountPricingData.getAccountPricingDataOid()).thenReturn(priceId);
        when(ccd.getAccountPricingData(baId)).thenReturn(Optional.of(accountPricingData));
        final EntityState on = EntityState.POWERED_ON;
        final TopologyEntityDTO region = entity(EntityType.REGION_VALUE, regionId, on,
                null, Collections.singletonList(createSoldCommodity(CommodityDTO
                        .CommodityType.DATACENTER_VALUE)));
        final TopologyEntityDTO vm = entity(EntityType.VIRTUAL_MACHINE_VALUE, vmId, on,
                Collections.singletonList(createConnectedEntity(regionId,
                        ConnectionType.AGGREGATED_BY_CONNECTION, EntityType.REGION_VALUE)),
                null);
        final TopologyEntityDTO ba = entity(EntityType.BUSINESS_ACCOUNT_VALUE, baId, on,
                Collections.singletonList(createConnectedEntity(vmId,
                        ConnectionType.OWNS_CONNECTION, EntityType.VIRTUAL_MACHINE_VALUE)),
                null);
        final TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();

        final Collection<TraderTO> traders =
                converter.convertToMarket(ImmutableList.of(region, vm, ba).stream()
                        .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity())));

        Assert.assertEquals(1, traders.size());
        final TraderTO vmTrader = traders.iterator().next();
        final Context startContext = vmTrader.getSettings().getCurrentContext();
        final BalanceAccountDTO balanceAccount = startContext.getBalanceAccount();
        Assert.assertEquals(priceId, balanceAccount.getPriceId());
        Assert.assertEquals(baId, balanceAccount.getId());
    }

    private static CommoditySoldDTO createSoldCommodity(int type) {
        return CommoditySoldDTO.newBuilder().setCommodityType(CommodityType.newBuilder()
                .setType(type)).build();
    }

    private static ConnectedEntity createConnectedEntity(long oid, ConnectionType type,
                                                         int entityType) {
        return  ConnectedEntity.newBuilder()
                .setConnectionType(type)
                .setConnectedEntityType(entityType)
                .setConnectedEntityId(oid)
                .build();
    }

    private static double[] getResizedCapacityForCloud(int entityType, int commodityType,
                                                       double commodityBoughtUsed, double commodityBoughtPeak, double commodityBoughtMax,
                                                       double commodityBoughtResizeTargetUtilization, double commoditySoldCapacity,
                                                       double boughtHistUsed, double boughtHistPeak) {

        Builder commBoughtBuilder = CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(commodityType).build())
                .setUsed(commodityBoughtUsed)
                .setPeak(commodityBoughtPeak)
                .setResizeTargetUtilization(commodityBoughtResizeTargetUtilization);
        if (boughtHistUsed >= 0) {
            commBoughtBuilder.setHistoricalUsed(HistoricalValues.newBuilder()
                    .setHistUtilization(boughtHistUsed).build());
        }
        if (boughtHistPeak >= 0) {
            commBoughtBuilder.setHistoricalPeak(HistoricalValues.newBuilder()
                    .setHistUtilization(boughtHistPeak).build());
        }
        CommodityBoughtDTO commodityBoughtDTO = commBoughtBuilder.build();


        TopologyEntityDTO topologyEntityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(entityType)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setOid(100)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(PROVIDER_ID)
                        .addCommodityBought(commodityBoughtDTO).build())
                .build();
        final TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder()
                .setOid(PROVIDER_ID)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder().setType(commodityType))
                        .setCapacity(commoditySoldCapacity))
                .build();

        final CloudTopologyConverter cloudTopologyConverter =
                Mockito.mock(CloudTopologyConverter.class);
        final TopologyConverter converter = Mockito.mock(TopologyConverter.class);
        Mockito.when(
                converter.getResizedCapacity(any(), any(), any()))
                .thenCallRealMethod();
        Whitebox.setInternalState(converter, "cloudTc", cloudTopologyConverter);
        Whitebox.setInternalState(converter, "commoditiesResizeTracker", Mockito.mock(CommoditiesResizeTracker.class));
        final MarketTier marketTier = mock(MarketTier.class);
        Mockito.when(marketTier.getTier()).thenReturn(computeTier);
        Mockito.when(cloudTopologyConverter.getMarketTier(PROVIDER_ID)).thenReturn(marketTier);

        final float[][] resizedCapacitites = converter.getResizedCapacity(topologyEntityDTO, commodityBoughtDTO,
                PROVIDER_ID);
        return new double[]{resizedCapacitites[0][0], resizedCapacitites[1][0]};
    }

    /**
     * Test to check if given a CommodityType, we return the same CommSpec inside the Trader and in the economy.
     */
    @Test
    public void testCommodityToAdjustForOverheadMatchTest() {
        CommodityType mem = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.MEM_VALUE).build();
        TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setOid(100)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(mem)
                        .build())
                .build();
        final TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();
        Map<Long, TopologyEntityDTO> entityMap = new HashMap<>();
        entityMap.put(entityDTO.getOid(), entityDTO);
        TraderTO entityTO = converter.convertToMarket(entityMap).iterator().next();

        CommoditySpecificationTO csTO = converter.getCommSpecForCommodity(mem);
        assertEquals(entityTO.getCommoditiesSold(0).getSpecification().getBaseType(), csTO.getBaseType());
        assertEquals(entityTO.getCommoditiesSold(0).getSpecification().getType(), csTO.getType());
    }

    /**
     * Test driving commodity sold affects commodity bought movable in the cloud.
     */
    @Test
    public void testCloudMovable() {
        // VMEM is driving commodity sold for MEM bought.
        final CommodityType vmem = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.VMEM_VALUE).build();
        final CommodityType mem = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.MEM_VALUE).build();
        final TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.DATABASE_SERVER_VALUE)
                .setOid(100)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(vmem)
                        .build())
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setScalable(false).setProviderId(10L)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(mem)
                        ).setMovable(true))
                .build();
        final TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();
        Map<Long, TopologyEntityDTO> entityMap = new HashMap<>();
        entityMap.put(entityDTO.getOid(), entityDTO);
        final TraderTO entityTO = converter.convertToMarket(entityMap).iterator().next();

        assertFalse(entityTO.getShoppingLists(0).getMovable());
    }

    /**
     * Test driving commodity sold does not affect commodity bought movable on-prem.
     */
    @Test
    public void testCloudMovableDoesNotAffectOnPrem() {
        // VMEM is driving commodity sold for MEM bought.
        final CommodityType vmem = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.VMEM_VALUE).build();
        final CommodityType mem = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.MEM_VALUE).build();
        final TopologyEntityDTO entityDTO = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.DATABASE_SERVER_VALUE)
                .setOid(100)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(vmem)
                        .build())
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setScalable(false)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(mem)
                        ).setMovable(true))
                .build();
        final TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();
        Map<Long, TopologyEntityDTO> entityMap = new HashMap<>();
        entityMap.put(entityDTO.getOid(), entityDTO);
        final TraderTO entityTO = converter.convertToMarket(entityMap).iterator().next();

        assertTrue(entityTO.getShoppingLists(0).getMovable());
    }

    /**
     * Setup a host as a {@link TopologyEntityDTO} with flow commodities sold
     * and check the price function and update function applied to commodities.
     */
    @Test
    public void testExternalPfForNCM() {
        // Create a VM TopoEntityDTO with flow commodities
        List<CommoditySoldDTO> flows = Lists.newArrayList();
        CommodityType randomBought = CommodityType.newBuilder().setType(1).build();
        CommodityType flow0 =
                CommodityType.newBuilder().setType(CommodityDTO.CommodityType.FLOW_VALUE)
                        .setKey(MarketAnalysisUtils.FLOW_ZERO_KEY).build();

        CommodityType flow1 =
                CommodityType.newBuilder().setType(CommodityDTO.CommodityType.FLOW_VALUE)
                        .setKey(MarketAnalysisUtils.FLOW_ONE_KEY).build();
        CommodityType flow2 =
                CommodityType.newBuilder().setType(CommodityDTO.CommodityType.FLOW_VALUE)
                        .setKey(MarketAnalysisUtils.FLOW_TWO_KEY).build();
        flows.add(CommoditySoldDTO.newBuilder().setCommodityType(flow0).build());
        flows.add(CommoditySoldDTO.newBuilder().setCommodityType(flow1).build());
        flows.add(CommoditySoldDTO.newBuilder().setCommodityType(flow2).build());

        final TopologyEntityDTO pm = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE).setOid(10)
                // commodities sold so it is not top of the supply chain
                .addAllCommoditySoldList(flows)
                // commodities bought so it is not bottom of the supply chain
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider
                        .newBuilder().setProviderId(12L)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(randomBought)))
                .build();

        // Call convertToMarket
        TraderTO pmTrader = convertToMarketTO(Sets.newHashSet(pm), REALTIME_TOPOLOGY_INFO)
                .iterator().next();

        // Get all commodities with external and constant price functions
        List<CommoditySoldTO> externalPfComms = pmTrader.getCommoditiesSoldList().stream()
                .filter(commSold -> commSold.getSettings().getPriceFunction()
                        .getPriceFunctionTypeCase() == PriceFunctionTypeCase.EXTERNAL_PRICE_FUNCTION)
                .collect(toList());
        List<CommoditySoldTO> constantPfComms = pmTrader.getCommoditiesSoldList().stream()
                .filter(commSold -> commSold.getSettings().getPriceFunction()
                        .getPriceFunctionTypeCase() == PriceFunctionTypeCase.CONSTANT)
                .collect(toList());
        // Get all commodities with external update function
        List<CommoditySoldTO> externalUfComms = pmTrader.getCommoditiesSoldList().stream()
                .filter(commSold -> commSold.getSettings().getUpdateFunction()
                        .getUpdatingFunctionTypeCase() == UpdatingFunctionTypeCase.EXTERNAL_UPDATE)
                .collect(toList());
        // check the price and update function associated with flows
        assertTrue(externalPfComms.size() == 1);
        assertTrue(constantPfComms.size() == 2);
        assertTrue(externalUfComms.size() == 3);
    }

    /**
     * Test the number of consumers of a commodity sold. Ignore inactive commodity bought.
     * TopologyEntityDTO with an inactive commodity bought is not considered as a consumer.
     */
    @Test
    public void testNumConsumers() {
        // Seller
        TopologyEntityDTO storage = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.STORAGE_VALUE)
                .setOid(100)
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                                .setType(CommodityDTO.CommodityType.STORAGE_LATENCY_VALUE)))
                .build();

        CommodityType commodityType = CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.STORAGE_LATENCY_VALUE)
                .build();

        // PMs with inactive commodity bought.
        List<TopologyEntityDTO> pms = new ArrayList<>(5);
        for (int i = 0; i < 5; i++) {
            pms.add(TopologyEntityDTO.newBuilder()
                    .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .setOid(i)
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(10)
                            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                    .setCommodityType(commodityType)
                                    .setActive(false)))
                    .build());
        }

        // VMs with inactive commodity bought.
        List<TopologyEntityDTO> vms = new ArrayList<>(5);
        for (int i = 0; i < 5; i++) {
            vms.add(TopologyEntityDTO.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setOid(i + 5)
                    .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(100)
                            .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                    .setCommodityType(commodityType)
                                    .setActive(true)))
                    .build());
        }

        Set<TopologyEntityDTO> dtos = new HashSet<>(11);
        dtos.add(storage);
        dtos.addAll(pms);
        dtos.addAll(vms);
        Collection<TraderTO> traderTOs = convertToMarketTO(dtos, REALTIME_TOPOLOGY_INFO);

        TraderTO trader = traderTOs.stream().filter(traderTO -> traderTO.getOid() == 100L).findFirst().get();
        assertEquals(1, trader.getCommoditiesSoldCount());
        assertEquals(5, trader.getCommoditiesSold(0).getNumConsumers());
    }

    /**
     * Tests creation of multiple bought commodity TOs as in case of
     * Pool commodities that are time slot based.
     */
    @Test
    public void testCreateAndValidateCommBoughtTO() {

        double[] used = {50d, 60d, 65d};
        double[] peak = {55d, 65d, 75d};
        HistoricalValues historicalValues = HistoricalValues.newBuilder()
                .addTimeSlot(used[0])
                .addTimeSlot(used[1])
                .addTimeSlot(used[2])
                .build();
        HistoricalValues historicalPeak = HistoricalValues.newBuilder()
                .addTimeSlot(peak[0])
                .addTimeSlot(peak[1])
                .addTimeSlot(peak[2])
                .build();
        CommodityBoughtDTO boughtCommodityDTO = createBoughtCommodity(historicalValues, historicalPeak,
                CommodityDTO.CommodityType.POOL_CPU);

        TopologyEntityDTO user = entity(EntityType.BUSINESS_USER_VALUE,
                10, EntityState.POWERED_ON, Collections.emptyList(), Collections.emptyList());

        user.toBuilder().addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setMovable(true)
                .setProviderId(1005L)
                .addCommodityBought(boughtCommodityDTO)
                .build()).build();

        final TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();

        final List<CommodityBoughtTO> boughtTOs = converter.createAndValidateCommBoughtTO(user, boughtCommodityDTO, 1005L, Optional.empty());
        assertEquals(3, boughtTOs.size());
        int index = 0;
        for (CommodityBoughtTO to : boughtTOs) {
            assertEquals(used[index], to.getQuantity(), DELTA);
            assertEquals(peak[index], to.getPeakQuantity(), DELTA);
            index++;
        }
    }

    /**
     * Tests populating "historicalQuantity" field of StorageAccess commodity in volume buyer.
     */
    @Test
    public void testCreateAndValidateCommBoughtTOWithHistoricalQuantity() {
        // The buyer is a volume with "hourlyBilledOps" field populated
        final float historicalUsed = 500F;
        final TopologyEntityDTO volume = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setOid(111L)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualVolume(VirtualVolumeInfo.newBuilder()
                                .setHourlyBilledOps(historicalUsed)
                                .build())
                        .build())
                .build();

        // The bought commodity is StorageAccess. It is expected that hourlyBilledOps is assigned to
        // historicalQuantity of this commodity.
        final CommodityBoughtDTO boughtCommodityDTO = CommodityBoughtDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder()
                        .setType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE)
                        .build())
                .build();

        final TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();

        final List<CommodityBoughtTO> boughtTOs = converter.createAndValidateCommBoughtTO(volume,
                boughtCommodityDTO, 222L, Optional.empty());
        assertEquals(1, boughtTOs.size());
        final CommodityBoughtTO boughtTO = boughtTOs.get(0);
        assertTrue(boughtTO.hasHistoricalQuantity());
        assertEquals(historicalUsed, boughtTO.getHistoricalQuantity(), DELTA);
    }

    private CommodityBoughtDTO createBoughtCommodity(HistoricalValues histUsed, HistoricalValues histPeak,
                                                     CommodityDTO.CommodityType commodityType) {
        final CommodityBoughtDTO.Builder builder =
                CommodityBoughtDTO.newBuilder()
                        .setHistoricalUsed(histUsed)
                        .setHistoricalPeak(histPeak)
                        .setCommodityType(
                                CommodityType.newBuilder()
                                        .setType(commodityType.getNumber())
                                        .build());
        return builder.build();
    }

    /**
     * The intent of this test is to ensure that Containers that are hosted by ContainerPods are
     * marked not movable.  This also tests whether that the VM that hosts the pods is suspendable.
     * @throws IOException when one of the files cannot be load
     */
    @Test
    public void testConvertContainer() throws IOException {
        final Map<Long, TopologyEntityDTO> topologyDTOs = Stream.of(
                messageFromJsonFile("protobuf/messages/vm-1.dto.json"),
                messageFromJsonFile("protobuf/messages/vm-3.dto.json"),
                messageFromJsonFile("protobuf/messages/pod-1.dto.json"),
                messageFromJsonFile("protobuf/messages/container-1.dto.json"),
                messageFromJsonFile("protobuf/messages/container-2.dto.json"),
                messageFromJsonFile("protobuf/messages/vm-4.dto.json"))
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        Collection<TraderTO> traderTOs = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build()
                .convertToMarket(topologyDTOs);
        // Container 1 and 2's SLs are movable.  Container 1 is hosted by a VM, so it should
        // be movable.  Container 2 is hosted by a container pod, so it should not be movable.
        assertEquals(6, traderTOs.size());
        for (TraderTO traderTO : traderTOs) {
            if (traderTO.getOid() == 73305182227091L) {
                // container-1 is movable
                assertTrue(traderTO.getShoppingLists(0).getMovable());
            } else if (traderTO.getOid() == 73305182227092L) {
                // container-2 is not movable
                assertFalse(traderTO.getShoppingLists(0).getMovable());
            } else if (traderTO.getOid() == 73305182227020L) {
                // VM-4 has no suspension setting and hosts pods, so it should have suspendable
                // true.
                assertFalse(traderTO.getSettings().getSuspendable());
            } else if (traderTO.getOid() == VM1_OID) {
                // VM-1 has no suspension setting does not host containers, so it should have
                // suspendable false.
                assertFalse(traderTO.getSettings().getSuspendable());
            } else if (traderTO.getOid() == 73305182227019L) {
                // This is VM-3.  It should be suspendable because it hosts containers, but it was
                // explicitly set to false.
                assertFalse(traderTO.getSettings().getSuspendable());
            }
        }
    }

    /**
     * Ensure that contaienr pods hosted on cloud entities do not get entries inserted into the
     * CommoditiesResizeTracker. The CommoditiesResizeTracker should only be used to track entities
     * whose bought commodities are adjusted prior to entering the market for cloud scaling.
     * @throws IOException when one of the files cannot be load
     */
    @Test
    public void testNoResizeTrackerForCloudPod() throws IOException {
        final List<TopologyEntityDTO.Builder> topologyDTOBuilders = Arrays.asList(
                messageFromJsonFile("protobuf/messages/vm-1.dto.json").toBuilder(),
                messageFromJsonFile("protobuf/messages/vm-3.dto.json").toBuilder(),
                messageFromJsonFile("protobuf/messages/pod-1.dto.json").toBuilder(),
                messageFromJsonFile("protobuf/messages/container-1.dto.json").toBuilder(),
                messageFromJsonFile("protobuf/messages/container-2.dto.json").toBuilder(),
                messageFromJsonFile("protobuf/messages/vm-4.dto.json").toBuilder());
        final Map<Long, TopologyEntityDTO> topologyDTOs = topologyDTOBuilders.stream()
                .map(builder -> {
                    // Set environment type to cloud
                    return builder.setEnvironmentType(EnvironmentType.CLOUD).build();
                }).collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        final TopologyConverter topologyConverter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();
        final CommoditiesResizeTracker resizeTracker = Mockito.mock(CommoditiesResizeTracker.class);
        Whitebox.setInternalState(topologyConverter,
                "commoditiesResizeTracker", resizeTracker);

        topologyConverter.convertToMarket(topologyDTOs);
        // Only the VirtualMachine (OID==101) should be saved to the resize tracker. The other converted
        // entities should not be saved. The other VMs in the test case have no commodities bought.

        Mockito.verify(resizeTracker).save(Mockito.eq(VM1_OID), anyLong(),
                Mockito.eq(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE).setKey("P1").build()), anyBoolean(), any());
        Mockito.verify(resizeTracker).save(Mockito.eq(VM1_OID), anyLong(),
                Mockito.eq(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VMEM_VALUE).setKey("").build()), anyBoolean(), any());
        Mockito.verifyNoMoreInteractions(resizeTracker);
    }

    /**
     * Test IOPs (STORAGE_ACCESS) VM bought from Compute Tier is using the percentile value in the utilization data
     *   when converting from TopologyEntityDTO to TraderTO.
     * STORAGE_ACCESS commodity has percentile utilization bought in VM from CT
     *   which STORAGE_ACCESS in CT sold list is resizable and has capacity.
     *
     * @throws IOException when one of the files cannot be load
     */
    @Test
    public void testCommodityBoughtFromAProviderWithPercentileUtilization() throws IOException {
        // These values should match with data stored in the json files
        final long ctOid = 73403214215586L;
        final long vmOid = 73470833555810L;
        final int ctIopsCapacity = 2000;
        final float targetUtilization = 0.7f;

        final TopologyEntityDTO computeTier = messageFromJsonFile("protobuf/messages/ct-azure-1.topologyDto.json");

        // The topology requires both the VM itself and provider Compute Tier.
        final Map<Long, TopologyEntityDTO> topologyDTOs = Stream.of(
                messageFromJsonFile("protobuf/messages/vm-azure-1.topologyDto.json"),
                computeTier)
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        final MarketTier marketTier = mock(MarketTier.class);
        Mockito.when(marketTier.getTier()).thenReturn(computeTier);
        TopologyEntityDTO region = TopologyEntityDTO.newBuilder().setOid(73442089143124L).setEntityType(EntityType.REGION_VALUE).build();
        TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder().setOid(15678904L).setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).build();
        topologyDTOs.put(businessAccount.getOid(), businessAccount);
        AccountPricingData accountPricingData = mock(AccountPricingData.class);
        TopologyEntityDTO serviceProvider = TopologyEntityDTO.newBuilder().setOid(5678974832L).setEntityType(EntityType.SERVICE_PROVIDER_VALUE).build();
        when(ccd.getAccountPricingData(businessAccount.getOid())).thenReturn(Optional.of(accountPricingData));
        CloudTopology cloudTopology = mock(CloudTopology.class);
        when(cloudTopology.getServiceProvider(businessAccount.getOid())).thenReturn(Optional.of(serviceProvider));
        when(cloudTopology.getRegionsFromServiceProvider(serviceProvider.getOid())).thenReturn(new HashSet(Collections.singleton(region)));
        when(cloudTopology.getAggregated(region.getOid(), TopologyConversionConstants.cloudTierTypes)).thenReturn(Collections.singleton(computeTier));

        Collection<TraderTO> traderTOs = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(false)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .marketMode(MarketMode.M2Only)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .cloudTopology(cloudTopology)
                .enableOP(false)
                .useVMReservationAsUsed(false)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build()
                .convertToMarket(topologyDTOs);

        assertEquals(2, traderTOs.size());

        traderTOs.stream().forEach(traderTO -> {
            if (traderTO.getOid() == vmOid) {
                assertEquals("Trader should be a VM", 10, traderTO.getType());
                assertEquals("Shopping List have one shoppingListTO", 1, traderTO.getShoppingListsCount());
                ShoppingListTO shoppingListTO = traderTO.getShoppingLists(0);
                assertEquals("There should be two commodities in the shopping list", 2, shoppingListTO.getCommoditiesBoughtCount());
                List<CommodityBoughtTO> commoditiesBoughtList = shoppingListTO.getCommoditiesBoughtList();
                boolean hasStorageAccessCommodity = false;
                for (CommodityBoughtTO commodityBoughtTO : commoditiesBoughtList) {
                    if (commodityBoughtTO.getSpecification().getBaseType() == 64) {
                        hasStorageAccessCommodity = true;
                        final float expectedQuantity = (float)(ctIopsCapacity * 0.8 / targetUtilization);

                        assertEquals(expectedQuantity, commodityBoughtTO.getQuantity(), epsilon);
                        assertEquals(expectedQuantity, commodityBoughtTO.getPeakQuantity(), epsilon);
                    }
                }
                assertTrue(hasStorageAccessCommodity);
            }
        });
    }

    /**
     * Test {@link TopologyConverter#createProviderUsedModificationMap(Map, Set)}.
     */
    @Test
    public void testCreateProviderUsedSubtractionMap() {
        final TopologyEntityDTO pm = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .setOid(3L)
                .build();

        final long topologyId = 2222;
        final double used1 = 10;
        final double scalingFactor1 = 20;
        final double used2 = 5;
        final double scalingFactor2 = 3;
        final CommodityType commodityType = CommodityType.newBuilder().setType(10).build();

        final TopologyEntityDTO removedVM1 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .setEdit(Edit.newBuilder()
                        .setRemoved(Removed.newBuilder().setPlanId(topologyId).build())
                        .build())
                .setOid(1L)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(pm.getOid()).addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(commodityType).setUsed(used1).setScalingFactor(scalingFactor1)))
                .build();
        final TopologyEntityDTO removedVM2 = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEntityState(EntityState.POWERED_ON)
                .setEdit(Edit.newBuilder()
                        .setRemoved(Removed.newBuilder().setPlanId(topologyId).build())
                        .build())
                .setOid(2L)
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .setProviderId(pm.getOid()).addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(commodityType).setUsed(used2).setScalingFactor(scalingFactor2)))
                .build();

        final TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();

        final Map<Long, Map<CommodityType, UsedAndPeak>> result =
                converter.createProviderUsedModificationMap(ImmutableMap.of(pm.getOid(), pm,
                        removedVM1.getOid(), removedVM1, removedVM2.getOid(), removedVM2),
                        ImmutableSet.of(removedVM1.getOid(), removedVM2.getOid()));

        assertEquals(1, result.size());
        assertEquals(1, result.get(pm.getOid()).size());
        assertEquals(used1 * scalingFactor1 + used2 * scalingFactor2,
                result.get(pm.getOid()).get(commodityType).used, 10e-7);
    }

    /**
     * The intent of this test is to ensure that for entities which have collapsed relationship, shoppingLists
     * are created based on the collapsed entities.
     * For example, for cloud VMs which consume from cloud volumes, VMs will be trader and volumes are collapsed.
     * @throws IOException when one of the files cannot be load
     */
    @Test
    public void testConvertEntityWithCollapsing() throws IOException {
        final Map<Long, TopologyEntityDTO> topologyDTOs = Stream.of(
                messageFromJsonFile("protobuf/messages/cloud-volume.json"),
                messageFromJsonFile("protobuf/messages/cloud-vm.json"),
                messageFromJsonFile("protobuf/messages/cloud-storageTier.json"),
                messageFromJsonFile("protobuf/messages/cloud-db.json"))
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        CloudTopology cloudTopology = mock(CloudTopology.class);
        final TopologyConverter topologyConverter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(false)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .marketMode(MarketMode.M2Only)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .cloudTopology(cloudTopology)
                .enableOP(false)
                .useVMReservationAsUsed(false)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();
        final long computeTierOid = 111111L;
        final TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder()
                .setOid(computeTierOid)
                .addConnectedEntityList(ConnectedEntity.newBuilder()
                        .setConnectedEntityType(EntityType.STORAGE_TIER_VALUE)
                        .setConnectedEntityId(73363299852962L)
                        .setConnectionType(ConnectionType.NORMAL_CONNECTION)
                        .build())
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .build();
        topologyDTOs.put(computeTierOid, computeTier);
        TopologyEntityDTO region = TopologyEntityDTO.newBuilder().setOid(73442089143124L).setEntityType(EntityType.REGION_VALUE).build();
        topologyDTOs.put(region.getOid(), region);
        TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder().setOid(15678904L).setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).build();
        topologyDTOs.put(businessAccount.getOid(), businessAccount);
        AccountPricingData accountPricingData = mock(AccountPricingData.class);
        TopologyEntityDTO serviceProvider = TopologyEntityDTO.newBuilder().setOid(5678974832L).setEntityType(EntityType.SERVICE_PROVIDER_VALUE).build();
        when(ccd.getAccountPricingData(businessAccount.getOid())).thenReturn(Optional.of(accountPricingData));
        when(cloudTopology.getServiceProvider(businessAccount.getOid())).thenReturn(Optional.of(serviceProvider));
        when(cloudTopology.getRegionsFromServiceProvider(serviceProvider.getOid())).thenReturn(new HashSet(Collections.singleton(region)));
        when(cloudTopology.getAggregated(region.getOid(), TopologyConversionConstants.cloudTierTypes)).thenReturn(topologyDTOs.values().stream()
                .filter(s -> s.getEntityType() == EntityType.STORAGE_TIER_VALUE || s.getEntityType() == EntityType.COMPUTE_TIER_VALUE || s.getEntityType() == EntityType.DATABASE_TIER_VALUE)
                .collect(Collectors.toSet()));
        topologyDTOs.put(computeTierOid, computeTier);
        Collection<TraderTO> traderTOs = topologyConverter.convertToMarket(topologyDTOs);
        assertEquals(4, traderTOs.size());
        // One of the trader is for StorageTier.
        Optional<TraderTO> storageTierTraderTO = traderTOs.stream()
                .filter(t -> t.getType() == EntityType.STORAGE_TIER_VALUE)
                .findAny();
        assertTrue(storageTierTraderTO.isPresent());
        // Check biclique commodity is NOT sold by Storage Tier
        Assert.assertFalse(storageTierTraderTO.get().getCommoditiesSoldList().stream()
                .anyMatch(commoditySold -> commoditySold.getSpecification()
                        .getBaseType() == CommodityDTO.CommodityType.BICLIQUE_VALUE));

        TraderTO vmTraderTO =
                traderTOs.stream()
                        .filter(t -> t.getType() == EntityType.VIRTUAL_MACHINE_VALUE)
                        .findAny()
                        .orElse(null);
        assertNotNull(vmTraderTO);
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, vmTraderTO.getType());

        // Test the shoppingList within the VM trader which represents cloud volume.
        final ShoppingListTO volumeSL = vmTraderTO.getShoppingListsCount() > 0
                ? vmTraderTO.getShoppingLists(0) : null;
        assertNotNull(volumeSL);
        // shoppingList provider is storageTier
        assertEquals(storageTierTraderTO.get().getOid(), volumeSL.getSupplier());
        // Check that no biclique commodity is bought by volume shopping list
        Assert.assertEquals(1, volumeSL.getCommoditiesBoughtCount());
        CommodityBoughtTO commodityBoughtTO = volumeSL.getCommoditiesBoughtCount() > 0 ? volumeSL.getCommoditiesBought(0) : null;
        assertNotNull(commodityBoughtTO);
        assertEquals(1506, commodityBoughtTO.getAssignedCapacityForBuyer(), 0.01f);
        assertEquals(1506 * 0.01f, commodityBoughtTO.getQuantity(), 0.01f);
        // Test ShoppingListInfo
        assertEquals(1, topologyConverter.getShoppingListOidToInfos().size());
        ShoppingListInfo shoppingListInfo = topologyConverter.getShoppingListOidToInfos().values().iterator().next();
        assertTrue(shoppingListInfo.getCollapsedBuyerId().isPresent());
        // Test collapsedBuyerId
        assertEquals(73442089143125L, shoppingListInfo.getCollapsedBuyerId().get().longValue());

        // Check biclique commodity IS sold by Compute Tier
        final TraderTO computeTierTraderTO =
            traderTOs.stream()
                .filter(t -> t.getType() == EntityType.COMPUTE_TIER_VALUE)
                .findAny()
                .orElse(null);
        Assert.assertNotNull(computeTierTraderTO);
        Assert.assertTrue(computeTierTraderTO.getCommoditiesSoldList().stream()
            .anyMatch(commoditySold -> commoditySold.getSpecification()
            .getBaseType() == CommodityDTO.CommodityType.BICLIQUE_VALUE));
    }

    /**
     * Tests that "Savings vs Reversibility" mode is respected by {@code TopologyConverter}.
     * If reversibility is preferred then "demandScalable" flag must be set to {@code false}
     * in the volume shopping list.
     *
     * @throws IOException In case of file loading error.
     */
    @Test
    public void testConvertEntityWithReversibilityPreferred() throws IOException {
        final Map<Long, TopologyEntityDTO> topologyDTOs = Stream.of(
                messageFromJsonFile("protobuf/messages/cloud-volume.json"),
                messageFromJsonFile("protobuf/messages/cloud-vm.json"),
                messageFromJsonFile("protobuf/messages/cloud-storageTier.json"))
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));

        // Mock that Volume is in reversibility mode
        final long volumeOid = 73442089143125L;
        final ReversibilitySettingFetcher settingFetcher = mock(ReversibilitySettingFetcher.class);
        when(settingFetcher.getEntityOidsWithReversibilityPreferred())
                .thenReturn(ImmutableSet.of(volumeOid));
        TopologyEntityDTO region = TopologyEntityDTO.newBuilder().setOid(73442089143124L).setEntityType(EntityType.REGION_VALUE).build();
        topologyDTOs.put(region.getOid(), region);
        TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder().setOid(15678904L).setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).build();
        topologyDTOs.put(businessAccount.getOid(), businessAccount);
        AccountPricingData accountPricingData = mock(AccountPricingData.class);
        TopologyEntityDTO serviceProvider = TopologyEntityDTO.newBuilder().setOid(5678974832L).setEntityType(EntityType.SERVICE_PROVIDER_VALUE).build();
        when(ccd.getAccountPricingData(businessAccount.getOid())).thenReturn(Optional.of(accountPricingData));
        CloudTopology cloudTopology = mock(CloudTopology.class);
        when(cloudTopology.getServiceProvider(businessAccount.getOid())).thenReturn(Optional.of(serviceProvider));
        when(cloudTopology.getRegionsFromServiceProvider(serviceProvider.getOid())).thenReturn(new HashSet(Collections.singleton(region)));
        when(cloudTopology.getAggregated(region.getOid(), TopologyConversionConstants.cloudTierTypes)).thenReturn(topologyDTOs.values().stream()
                .filter(s -> s.getEntityType() == EntityType.STORAGE_TIER_VALUE).collect(Collectors.toSet()));
        final TopologyConverter topologyConverter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(false)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .marketMode(MarketMode.M2Only)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(settingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .cloudTopology(cloudTopology)
                .enableOP(false)
                .useVMReservationAsUsed(false)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();

        final Collection<TraderTO> traderTOs = topologyConverter.convertToMarket(topologyDTOs);

        // Find VirtualMachine trader.
        final TraderTO vmTrader = traderTOs.stream()
                .filter(t -> t.getType() == EntityType.VIRTUAL_MACHINE_VALUE)
                .findAny().orElse(null);
        assertNotNull(vmTrader);

        // Retrieve shopping list representing Volume.
        assertTrue(vmTrader.getShoppingListsCount() > 0);
        final ShoppingListTO volumeSL = vmTrader.getShoppingLists(0);

        // Check that Reversibility mode is enabled.
        assertFalse(volumeSL.getDemandScalable());
    }

    /**
     * Test that the scaling factor is applied in the same way to commodities bought
     * and sold and that floating point precision loss happens in a consistent manner
     * to reduce problems downstream in the market.
     * <p/>
     * We want to ensure that commodities bought and sold have the scaling factor applied
     * in the same way. Consider the following:
     * <p/>
     * (float)((float)540.0 * 2.7617943704573737) == 1491.369f
     * (float)(540.0 * (float)2.7617943704573737) == 1491.3689f
     * <p/>
     * Both the above ways of doing the computation are fine, we just have be sure
     * to do the calculation consistently during the conversion otherwise some quantities
     * that we expected to be equal downstream may not be. For example, if we apply scaling
     * rules to some commodity fields in the first way and to other commodity
     * fields in the second way, that could result in problems like setting an illegal quantity
     * on a field when we subtract off values in the market, or winding up on the wrong side
     * of a threshold when comparing to capacityUpperBound/capacityLowerBound etc.
     * <p/>
     * Market conversion happens using the first option shown above.
     */
    @Test
    public void testScalingFactorAppliedConsistently() {
        final double originalValue = 540.0;
        final double scalingFactor = 2.7617943704573737;
        testScalingFactorAppliedConsistently(originalValue, scalingFactor);

        // Now flip it and all values should still be computed uniformly.
        testScalingFactorAppliedConsistently(scalingFactor, originalValue);
    }

    private void testScalingFactorAppliedConsistently(double originalValue, double scalingFactor) {

        final TopologyEntityDTO dto = TopologyEntityDTO.newBuilder()
                .setOid(1L)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .addCommoditySoldList(
                        CommoditySoldDTO.newBuilder()
                                .setScalingFactor(scalingFactor)
                                .setUsed(originalValue)
                                .setPeak(originalValue)
                                .setCapacity(originalValue)
                                .setThresholds(Thresholds.newBuilder()
                                        .setMax(originalValue)
                                        .setMin(originalValue)
                                        .build())
                                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                .setHistoricalUsed(HistoricalValues.newBuilder().setPercentile(1.0).setMaxQuantity(originalValue))
                                .setHistoricalPeak(HistoricalValues.newBuilder().setPercentile(1.0).setMaxQuantity(originalValue)))
                .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder().setType(CommodityDTO.CommodityType.VCPU_VALUE))
                                .setScalingFactor(scalingFactor)
                                .setUsed(originalValue)
                                .setPeak(originalValue)
                                .setHistoricalUsed(HistoricalValues.newBuilder().setMaxQuantity(originalValue))
                                .setHistoricalPeak(HistoricalValues.newBuilder().setMaxQuantity(originalValue))
                        ))
                .build();

        float expectedValue = (float)((float)originalValue * scalingFactor);
        final TraderTO converted = convertToMarketTO(Collections.singleton(dto), REALTIME_TOPOLOGY_INFO)
                .iterator()
                .next();

        final CommodityBoughtTO cb = converted.getShoppingLists(0).getCommoditiesBought(0);
        assertEquals(expectedValue, cb.getQuantity(), 0);
        assertEquals(expectedValue, cb.getPeakQuantity(), 0);

        final CommoditySoldTO cs = converted.getCommoditiesSold(0);
        assertEquals(expectedValue, cs.getCapacity(), 0);
        assertEquals(expectedValue, cs.getQuantity(), 0);
        assertEquals(expectedValue, cs.getHistoricalQuantity(), 0);
        assertEquals(expectedValue, cs.getSettings().getCapacityLowerBound(), 0);
        assertEquals(expectedValue, cs.getSettings().getCapacityUpperBound(), 0);
        assertEquals(expectedValue, cs.getMaxQuantity(), 0);
    }

    /**
     * Test a vm that consumes on volume with actual commodities(new model) can be converted to traderTO.
     * The volume id is referenced as CollapsedBuyerId in the shoppingListInfo.
     */
    @Test
    public void testCreateMPCVolumeToShoppingList() {
        final long vmId = 1111l;
        final long volumeId = 22222l;
        final long stId = 33333l;
        CommodityType stAccessType = CommodityType.newBuilder().setType(CommodityDTO.CommodityType
                .STORAGE_ACCESS_VALUE).build();
        CommodityType stAmtType = CommodityType.newBuilder().setType(CommodityDTO.CommodityType
                .STORAGE_AMOUNT_VALUE).build();
        CommodityType stProvType = CommodityType.newBuilder().setType(CommodityDTO.CommodityType
                .STORAGE_PROVISIONED_VALUE).build();
        CommoditySoldDTO stAmt = CommoditySoldDTO.newBuilder().setCommodityType(stAmtType)
                .setUsed(50000).setHistoricalUsed(HistoricalValues.newBuilder().setHistUtilization(50000))
                .setCapacity(60000).build();
        CommoditySoldDTO stAccess = CommoditySoldDTO.newBuilder().setCommodityType(stAccessType)
                .setUsed(2000).setHistoricalUsed(HistoricalValues.newBuilder().setHistUtilization(2000))
                .setCapacity(50000).build();
        CommoditySoldDTO stProv = CommoditySoldDTO.newBuilder().setCommodityType(stProvType)
                .setUsed(80000).setHistoricalUsed(HistoricalValues.newBuilder().setHistUtilization(80000))
                .setCapacity(80000).build();
        final TopologyEntityDTO.Builder volume = entity(EntityType.VIRTUAL_VOLUME_VALUE, volumeId,
                EntityState.POWERED_ON, Collections.emptyList(), Arrays.asList(stAmt, stAccess, stProv)).toBuilder();
        CommoditiesBoughtFromProvider volumeCommBoughtPrvd = CommoditiesBoughtFromProvider.newBuilder()
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(stAmtType)
                        .setUsed(50000))
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(stAccessType)
                        .setUsed(2000))
                .addCommodityBought(CommodityBoughtDTO.newBuilder().setCommodityType(stProvType)
                        .setUsed(80000))
                .setProviderEntityType(EntityType.STORAGE_VALUE)
                .setProviderId(stId).build();
        TopologyEntityDTO volumeDTO = volume.addAllCommoditiesBoughtFromProviders(Arrays.asList(volumeCommBoughtPrvd)).build();
        final TopologyEntityDTO.Builder vm = entity(EntityType.VIRTUAL_MACHINE_VALUE, vmId,
                EntityState.POWERED_ON, Collections.emptyList(), Collections.emptyList()).toBuilder();
        CommoditiesBoughtFromProvider vmCommBoughtPrvd = CommoditiesBoughtFromProvider.newBuilder()
                .setMovable(true).setScalable(true).setProviderEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setProviderId(volumeId)
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(stAmtType).setUsed(50000))
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(stAccessType).setUsed(2000))
                .addCommodityBought(CommodityBoughtDTO.newBuilder()
                        .setCommodityType(stProvType).setUsed(80000)).build();
        TopologyEntityDTO vmDTO = vm.addAllCommoditiesBoughtFromProviders(Arrays.asList(vmCommBoughtPrvd)).build();
        TopologyConverter converter = new TopologyConverterUtil.Builder()
                .topologyInfo(REALTIME_TOPOLOGY_INFO)
                .includeGuaranteedBuyer(true)
                .quoteFactor(MarketAnalysisUtils.QUOTE_FACTOR)
                .liveMarketMoveCostFactor(MarketAnalysisUtils.LIVE_MARKET_MOVE_COST_FACTOR)
                .marketCloudRateExtractor(marketCloudRateExtractor)
                .cloudCostData(ccd)
                .commodityIndexFactory(CommodityIndex.newFactory())
                .tierExcluderFactory(tierExcluderFactory)
                .consistentScalingHelperFactory(consistentScalingHelperFactory)
                .reversibilitySettingFetcher(reversibilitySettingFetcher)
                .licensePriceWeightScale(MarketAnalysisUtils.PRICE_WEIGHT_SCALE)
                .storageMoveCostFactor(MarketAnalysisUtils.STORAGE_MOVE_COST_FACTOR)
                .fakeEntityCreator(Mockito.mock(FakeEntityCreator.class))
                .useVMReservationAsUsed(true)
                .singleVMonHost(false)
                .customUtilizationThreshold(0.5f)
                .build();
        Collection<EconomyDTOs.TraderTO> traderTOs = converter.convertToMarket(new HashMap(){{
            put(vmId, vmDTO);
            put(volumeId, volumeDTO);
        }}, new HashSet());
        // Only VM is converted to traderTO.
        assertEquals(1, traderTOs.size());
        TraderTO vmTO = traderTOs.stream().findFirst().get();
        assertTrue(converter.getShoppingListOidToInfos().containsKey(vmTO.getShoppingLists(0).getOid()));
        assertEquals(Long.valueOf(volumeId), converter.getShoppingListOidToInfos().get(vmTO.getShoppingLists(0).getOid())
                .getCollapsedBuyerId().get());
    }

    /**
     * Access commodities bought by VM from storage should be added to the collapsed shopping list
     * created from volume -> storage commBought.
     *
     * @throws IOException In case of file loading error.
     */
    @Test
    public void tesOnPremVolume() throws IOException {
        final Map<Long, TopologyEntityDTO> topologyDTOs = Stream.of(
            messageFromJsonFile("protobuf/messages/vm-5.dto.json"),
            messageFromJsonFile("protobuf/messages/pm-3.dto.json"),
            messageFromJsonFile("protobuf/messages/ds-3.dto.json"),
            messageFromJsonFile("protobuf/messages/volume-1.dto.json"))
            .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity()));
        assertThat(topologyDTOs.size(), is(4));

        AnalysisConfig analysisConfig = mock(AnalysisConfig.class);
        when(analysisConfig.getGlobalSetting(GlobalSettingSpecs.AllowUnlimitedHostOverprovisioning))
            .thenReturn(Optional.empty());
        when(analysisConfig.getGlobalSetting(GlobalSettingSpecs.OnPremVolumeAnalysis))
            .thenReturn(Optional.of(Setting.newBuilder().setBooleanSettingValue(
                BooleanSettingValue.newBuilder().setValue(true)).build()));

        final CommodityConverter commodityConverter = mock(CommodityConverter.class);
        when(commodityConverter.commoditySpecification(CommodityType.newBuilder().setType(STORAGE_AMOUNT_VALUE).setKey("").build(), 1))
            .thenReturn(Collections.singletonList(CommoditySpecificationTO.newBuilder().setBaseType(STORAGE_AMOUNT_VALUE).setType(1).build()));
        when(commodityConverter.commoditySpecificationBiClique("BC-T1-208"))
            .thenReturn(CommoditySpecificationTO.newBuilder().setBaseType(BICLIQUE_VALUE).setType(2).build());
        final Collection<TraderTO> traderTOs = new TopologyConverter(REALTIME_TOPOLOGY_INFO,
            marketCloudRateExtractor,commodityConverter, ccd,
            CommodityIndex.newFactory(), tierExcluderFactory, consistentScalingHelperFactory,
            mock(TopologyEntityCloudTopology.class), reversibilitySettingFetcher, analysisConfig, null)
            .convertToMarket(topologyDTOs);
        // We don't create TraderTO for volume.
        assertEquals(3, traderTOs.size());
        for (TraderTO traderTO : traderTOs) {
            if (traderTO.getType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                // one PM shopping list, one storage shopping list
                assertThat(traderTO.getShoppingListsCount(), is(2));
                for (ShoppingListTO sl : traderTO.getShoppingListsList()) {
                    // storage provider
                    if (sl.getSupplier() == 208L) {
                        assertThat(sl.getCommoditiesBoughtCount(), is(2));
                        assertThat(sl.getCommoditiesBoughtList().stream()
                            .map(commBought -> commBought.getSpecification().getBaseType()).collect(Collectors.toSet()),
                            containsInAnyOrder(BICLIQUE_VALUE, STORAGE_AMOUNT_VALUE));
                    }
                }
            }
        }
    }

    @Test
    public void testScaleDownForVirtualMachineSpec() {
        double used = 10;
        double peak = 30;
        double max = 40;
        final double commSoldCap = 100;
        final double commSoldRtu = 1;
        final double lowerBoundForResizeUp = 110f;
        CommodityCapacityLimit commodityCapacityLimit = CommodityCapacityLimit.newBuilder()
                .setCapacity((float)lowerBoundForResizeUp)
                .setCommodityType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE).build();
        double[] resizedCapacity = getResizedCapacityForCloud(EntityType.VIRTUAL_MACHINE_SPEC_VALUE,
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE,
                CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, used, peak, max,
                commSoldCap, commSoldRtu, 0.8d, 0.8d, EnvironmentType.CLOUD, null, null,
                REALTIME_TOPOLOGY_INFO, true, Optional.of(commodityCapacityLimit), 0.0d, Collections.emptyList());
        assertThat(resizedCapacity[0], is(commSoldCap * 0.8d));
    }
}
