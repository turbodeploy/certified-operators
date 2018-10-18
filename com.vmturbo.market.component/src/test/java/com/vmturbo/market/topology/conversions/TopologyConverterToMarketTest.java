package com.vmturbo.market.topology.conversions;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Arrays;
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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.utilities.NumericIDAllocator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link TopologyConverter}.
 */
public class TopologyConverterToMarketTest {

    private static final TopologyInfo REALTIME_TOPOLOGY_INFO =  TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.REALTIME)
            .build();

    private static final TopologyInfo PLAN_TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.PLAN)
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                    .setPlanProjectType(PlanProjectType.USER))
            .build();

    private double epsilon = 1e-5; // used in assertEquals(double, double, epsilon)

    private CommoditySpecificationTO economyCommodity1;
    private CommodityType topologyCommodity1;
    private CommoditySpecificationTO economyCommodity2;
    private CommodityType topologyCommodity2;
    private MarketPriceTable marketPriceTable = mock(MarketPriceTable.class);

    @Before
    public void setup() {
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
                        .setType(0)
                        .setBaseType(1)
                        .build();
        economyCommodity2 = CommoditySpecificationTO.newBuilder()
                        .setType(1)
                        .setBaseType(2)
                        .build();
    }

    @Test
    public void testConvertCommodityCloneWithNewType() {
        TopologyDTO.TopologyEntityDTO entityDto = DTOWithProvisionedAndCloneWithNewTypeComm();
        final TopologyConverter converter = new TopologyConverter(REALTIME_TOPOLOGY_INFO, true, 0.75f, marketPriceTable);
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
        final TopologyConverter converter = new TopologyConverter(REALTIME_TOPOLOGY_INFO, true, 0.75f, marketPriceTable);
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
        Set<TraderTO> traderTOs = convertToMarketTO(topologyDTOs, REALTIME_TOPOLOGY_INFO);
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
        assertEquals(0, commSoldSpec.getType()); // zero because it is the first assigned type
        assertEquals(CommodityDTO.CommodityType.MEM_ALLOCATION_VALUE, commSoldSpec.getBaseType());
        assertEquals("MEM_ALLOCATION|P1", commSoldSpec.getDebugInfoNeverUseInCode());
        CommoditySoldSettingsTO commSoldSettings = commSold.getSettings();
        assertTrue(commSoldSettings.getResizable());
        assertEquals(100000.0, commSoldSettings.getCapacityUpperBound(), epsilon);
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

    static TraderTO getVmTrader(Set<TraderTO> traders) {
        return traders.stream()
            .filter(tto -> tto.getType() == EntityType.VIRTUAL_MACHINE_VALUE)
            .findFirst()
            .orElse(null);
    }

    static CommodityBoughtTO getMem(List<CommodityBoughtTO> commoditiesBoughtList) {
        return commoditiesBoughtList.stream()
            .filter(comm -> comm.getSpecification().getBaseType() == CommodityDTO.CommodityType.MEM_VALUE)
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
        Set<TraderTO> traderTOs = convertToMarketTO(TOPOLOGY_DTOS, REALTIME_TOPOLOGY_INFO);
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
        assertEquals(0, pmShoppingList.getStorageMoveCost(), epsilon);
        // Get the shopping list that buys from DS
        ShoppingListTO dsShoppingList = shoppingLists.stream()
                        .filter(sl -> sl.getSupplier() == 205)
                        .findAny().orElseThrow(() -> new RuntimeException("cannot find supplier 205"));
        // Buys 1024 from on DS and 2048 from another DS
        assertEquals(3.0, dsShoppingList.getStorageMoveCost(), epsilon);
    }

    /**
     * Test that in a plan, the move cost is zero.
     * We already tested that in a realtime market the move cost is set.
     */
    @Test
    public void testZeroMoveCostInPlan() {
        Set<TraderTO> traderTOs = convertToMarketTO(TOPOLOGY_DTOS, PLAN_TOPOLOGY_INFO);
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
        TopologyEntityDTO vmOn = entity(EntityType.VIRTUAL_MACHINE_VALUE, EntityState.POWERED_ON);
        TraderTO traderVmOn = convertToMarketTO(Sets.newHashSet(vmOn), PLAN_TOPOLOGY_INFO).iterator().next();
        assertEquals(TraderStateTO.ACTIVE, traderVmOn.getState());

        TopologyEntityDTO vmOff = entity(EntityType.VIRTUAL_MACHINE_VALUE, EntityState.POWERED_OFF);
        TraderTO traderVmOff = convertToMarketTO(Sets.newHashSet(vmOff), PLAN_TOPOLOGY_INFO).iterator().next();
        assertEquals(TraderStateTO.IDLE, traderVmOff.getState());

        TopologyEntityDTO appOn = entity(EntityType.APPLICATION_VALUE, EntityState.POWERED_ON);
        TraderTO traderAppOn = convertToMarketTO(Sets.newHashSet(appOn), PLAN_TOPOLOGY_INFO).iterator().next();
        assertEquals(TraderStateTO.ACTIVE, traderAppOn.getState());

        TopologyEntityDTO appOff = entity(EntityType.APPLICATION_VALUE, EntityState.POWERED_OFF);
        TraderTO traderAppOff = convertToMarketTO(Sets.newHashSet(appOff), PLAN_TOPOLOGY_INFO).iterator().next();
        assertEquals(TraderStateTO.INACTIVE, traderAppOff.getState());
    }

    private TopologyEntityDTO entity(int type, EntityState state) {
        return TopologyEntityDTO.newBuilder()
            .setEntityType(type)
            .setEntityState(state)
            .setOid(10)
            .build();
    }

    /**
     * Verify that plan VMs are not suspendable, and that plan entities
     * are eligible for resize.
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
        assertTrue(realtimeVm.getSettings().getSuspendable());
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
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, true, 0.75f, marketPriceTable);
        converter.convertToMarket(ImmutableMap.of(entityDTO.getOid(), entityDTO));

        assertEquals(topologyCommodity1, converter.getCommodityConverter()
                .economyToTopologyCommodity(economyCommodity1).orElseThrow(() ->
                        new RuntimeException("cannot convert commodity1")));
        assertEquals(topologyCommodity2, converter.getCommodityConverter()
                .economyToTopologyCommodity(economyCommodity2).orElseThrow(() ->
                        new RuntimeException("cannot convert commodity2")));
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
    private Set<TraderTO> convertToMarketTO(@Nonnull final Set<TopologyDTO.TopologyEntityDTO> topology,
                                         @Nonnull final TopologyInfo topologyInfo) {

        return new TopologyConverter(topologyInfo, true, 0.75f, marketPriceTable)
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
        Set<TraderTO> traderTOs =
                new TopologyConverter(REALTIME_TOPOLOGY_INFO, false, 0.75f, marketPriceTable)
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
        CommodityDTO.CommodityType.SEGMENTATION.name(), CommodityDTO.CommodityType.DRS_SEGMENTATION.name(),
        CommodityDTO.CommodityType.STORAGE_CLUSTER.name(),
        CommodityDTO.CommodityType.VAPP_ACCESS.name(), CommodityDTO.CommodityType.VDC.name(),
        CommodityDTO.CommodityType.VMPM_ACCESS.name()
        );

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
        verifyPriceFunctions(traderTO, STEP_PRICE_TYPES_S, PriceFunctionTO::hasStep, STEP);
    }

    private void verifyPriceFunctions(TraderTO traderTO, Set<String> types,
                    Function<PriceFunctionTO, Boolean> f, PriceFunctionTO pf) {
        List<PriceFunctionTO> list = traderTO.getCommoditiesSoldList().stream()
            .filter(comm -> types.contains(comm.getSpecification().getDebugInfoNeverUseInCode()))
            .map(CommoditySoldTO::getSettings)
            .map(CommoditySoldSettingsTO::getPriceFunction)
            .collect(Collectors.toList());
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
        TopologyConverter converter = new TopologyConverter(REALTIME_TOPOLOGY_INFO, marketPriceTable);
        assertEquals(2, converter.convertToMarket(Stream.of(container, virtualApp, actionManager)
                .collect(Collectors.toMap(TopologyEntityDTO::getOid, Function.identity())))
                .size());
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
                .setDesiredUtilizationRange(20.0f))
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
        assertEquals(trader.getSettings().getQuoteFactor(), AnalysisUtil.QUOTE_FACTOR, 0.0);

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
                .setSuspendable(true))
            .build();
        TraderTO traderTwo = convertToMarketTO(Sets.newHashSet(oppositeEntityDTO), REALTIME_TOPOLOGY_INFO).iterator().next();
        assertTrue(traderTwo.getShoppingLists(0).getMovable());
        assertTrue(traderTwo.getSettings().getCanAcceptNewCustomers());
        assertTrue(traderTwo.getSettings().getIsShopTogether());
        assertTrue(traderTwo.getSettings().getClonable());
        assertTrue(traderTwo.getSettings().getSuspendable());
        assertEquals(traderTwo.getSettings().getQuoteFactor(), AnalysisUtil.QUOTE_FACTOR, epsilon);
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
        final TopologyConverter converter =
                        new TopologyConverter(REALTIME_TOPOLOGY_INFO, true, 0.75f, marketPriceTable);
        Map<Long, TopologyEntityDTO> topology = new HashMap<>();
        topology.put(pmEntityDTO.getOid(), pmEntityDTO);
        topology.put(vmEntityDTO.getOid(), vmEntityDTO);
        Set<TraderTO> traders = converter.convertToMarket(topology);
        for (TraderTO t : traders) {
            if (t.getType() == vmEntityDTO.getEntityType()) {
                assertFalse(t.getShoppingLists(0).getMovable());
            } else if (t.getType() == pmEntityDTO.getEntityType()) {
                assertFalse(t.getSettings().getCanAcceptNewCustomers());
                assertFalse(t.getSettings().getClonable());
                assertTrue(t.getSettings().getSuspendable());
                assertFalse(t.getShoppingLists(0).getMovable());
            }
        }
    }
}