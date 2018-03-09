package com.vmturbo.market.topology.conversions;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
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
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
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
import com.vmturbo.platform.analysis.utilities.NumericIDAllocator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link TopologyConverter}.
 */
public class TopologyConverterTest {

    private static final TopologyInfo REALTIME_TOPOLOGY_INFO =  TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.REALTIME)
            .build();

    private static final TopologyInfo PLAN_TOPOLOGY_INFO = TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.PLAN)
            .setPlanInfo(PlanTopologyInfo.newBuilder()
                    .setPlanType(PlanProjectType.USER))
            .build();

    private double epsilon = 1e-5; // used in assertEquals(double, double, epsilon)

    private CommodityDTOs.CommoditySpecificationTO economyCommodity1;
    private CommodityType topologyCommodity1;
    private CommodityDTOs.CommoditySpecificationTO economyCommodity2;
    private CommodityType topologyCommodity2;

    private static final Map<Long, Integer> entityIdToEntityTypeMap =
        Collections.emptyMap();

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
        economyCommodity1 = CommodityDTOs.CommoditySpecificationTO.newBuilder()
                        .setType(0)
                        .setBaseType(1)
                        .build();
        economyCommodity2 = CommodityDTOs.CommoditySpecificationTO.newBuilder()
                        .setType(1)
                        .setBaseType(2)
                        .build();
    }

    /**
     * Test loading one DTO. Verify the properties after the conversion match the file.
     * @throws IOException when the loaded file is missing
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void testConvertOneDTO() throws IOException, InvalidTopologyException {
        TopologyDTO.TopologyEntityDTO vdcTopologyDTO =
                messageFromJsonFile("protobuf/messages/vdc-1.dto.json");
        Set<TopologyDTO.TopologyEntityDTO> topologyDTOs = Sets.newHashSet(vdcTopologyDTO);
        Set<TraderTO> traderTOs = convertToMarketTO(topologyDTOs, REALTIME_TOPOLOGY_INFO);
        TraderTO vdcTraderTO = traderTOs.iterator().next();
        assertEquals(100, vdcTraderTO.getOid());
        assertEquals(EntityType.VIRTUAL_DATACENTER_VALUE, vdcTraderTO.getType());
        assertEquals(TraderStateTO.ACTIVE, vdcTraderTO.getState());
        assertEquals("VIRTUAL_DATACENTER::CDV-1", vdcTraderTO.getDebugInfoNeverUseInCode());
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

    private static final Set<TopologyDTO.TopologyEntityDTO> TOPOLOGY_DTOS = topology();

    private static Set<TopologyEntityDTO> topology() {
        try {
            return Sets.newHashSet(
                messageFromJsonFile("protobuf/messages/vm-1.dto.json"),
                messageFromJsonFile("protobuf/messages/pm-1.dto.json"),
                messageFromJsonFile("protobuf/messages/ds-1.dto.json"),
                messageFromJsonFile("protobuf/messages/ds-2.dto.json"),
                messageFromJsonFile("protobuf/messages/vdc-1.dto.json"));
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Test loading a group of DTOs and that the properties of bought commodities
     * after the conversion match the file.
     * @throws IOException when the loaded file is missing
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void testConvertDTOs() throws IOException, InvalidTopologyException {
        Set<TraderTO> traderTOs = convertToMarketTO(TOPOLOGY_DTOS, REALTIME_TOPOLOGY_INFO);
        assertEquals(TOPOLOGY_DTOS.size(), traderTOs.size());

        final TraderTO vmTraderTO = getVmTrader(traderTOs);
        assertTrue(vmTraderTO != null);
        List<ShoppingListTO> shoppingLists = vmTraderTO.getShoppingListsList();
        // Buys from storage and PM
        assertEquals(3, shoppingLists.size());
        // Get the shopping list that buys from PM
        ShoppingListTO pmShoppingList = shoppingLists.stream()
                        .filter(sl -> sl.getSupplier() == 102)
                        .findAny().get();
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
                        .findAny().get();
        // Buys 1024 from on DS and 2048 from another DS
        assertEquals(3.0, dsShoppingList.getStorageMoveCost(), epsilon);
    }

    /**
     * Test that in a plan, the move cost is zero.
     * We already tested that in a realtime market the move cost is set.
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void testZeroMoveCostInPlan() throws InvalidTopologyException {
        Set<TraderTO> traderTOs = convertToMarketTO(TOPOLOGY_DTOS, PLAN_TOPOLOGY_INFO);
        final TraderTO vmTraderTO = getVmTrader(traderTOs);
        List<ShoppingListTO> shoppingLists = vmTraderTO.getShoppingListsList();
        // Get the shopping list that buys from DS
        ShoppingListTO dsShoppingList = shoppingLists.stream()
                        .filter(sl -> sl.getSupplier() == 205)
                        .findAny().get();
        assertEquals(0.0, dsShoppingList.getStorageMoveCost(), epsilon);
    }

    /**
     * Test that the topology state is converted correctly to trader state.
     *
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void testTraderState() throws InvalidTopologyException {
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
     *
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void testPlanVsRealtimeEntities() throws InvalidTopologyException {
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
    public void testEconomyCommodityMapping() throws Exception {
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
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, entityIdToEntityTypeMap, true);
        converter.convertToMarket(Lists.newArrayList(entityDTO));

        assertEquals(topologyCommodity1,
                converter.economyToTopologyCommodity(economyCommodity1).get());
        assertEquals(topologyCommodity2,
                converter.economyToTopologyCommodity(economyCommodity2).get());
    }

    /**
     * Load a json file into a DTO.
     * @param fileName the name of the file to load
     * @return The entity DTO represented by the file
     * @throws IOException when the file is not found
     */
    public static TopologyDTO.TopologyEntityDTO messageFromJsonFile(String fileName) throws IOException {
        URL fileUrl = TopologyConverterTest.class.getClassLoader().getResources(fileName).nextElement();
        TopologyDTO.TopologyEntityDTO.Builder builder = TopologyDTO.TopologyEntityDTO.newBuilder();
        JsonFormat.parser().merge(new InputStreamReader(fileUrl.openStream()), builder);
        TopologyDTO.TopologyEntityDTO message = builder.build();
        return message;
    }

    @Nonnull
    private static Set<TraderTO> convertToMarketTO(@Nonnull final Set<TopologyDTO.TopologyEntityDTO> topology,
                                         @Nonnull final TopologyInfo topologyInfo)
            throws InvalidTopologyException {

        return new TopologyConverter(topologyInfo, entityIdToEntityTypeMap, true)
                        .convertToMarket(topology);
    }

    @Test
    /**
     * Load three protobuf files and test if includeVDC flag is false, conversion of certain
     * entities, commoditiesSold and shoppingList should be skipped.
     * @throws IOException when one of the files cannot be load
     * @throws InvalidTopologyException not supposed to happen here
     */
    public void testSkipVDC() throws IOException, InvalidTopologyException {
        Set<TopologyDTO.TopologyEntityDTO> topologyDTOs = Sets.newHashSet(
                        messageFromJsonFile("protobuf/messages/vdc-2.dto.json"),
                        messageFromJsonFile("protobuf/messages/pm-2.dto.json"),
                        messageFromJsonFile("protobuf/messages/vm-2.dto.json"));
        Set<TraderTO> traderTOs =
                new TopologyConverter(REALTIME_TOPOLOGY_INFO, entityIdToEntityTypeMap, false)
                        .convertToMarket(topologyDTOs.stream().collect(Collectors.toList()));
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

    /**
     * Test conversion from {@link TraderTO} to {@link TopologyDTO} with biclique
     * commodity.
     *
     * @throws Exception not supposed to happen in this test
     */
    @Test
    public void testTraderWithBicliqueCommodityConversion() throws Exception {
        TopologyConverter converter = Mockito.spy(
            new TopologyConverter(REALTIME_TOPOLOGY_INFO, entityIdToEntityTypeMap));
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

        NumericIDAllocator idAllocator = new NumericIDAllocator();
        final int base = idAllocator.allocate("BICLIQUE");
        final int cpuType = idAllocator.allocate("CPU");
        final int dspmType = idAllocator.allocate("DSPM");
        Map<Long, ShoppingListInfo> shoppingListMap = new HashMap<>();
        shoppingListMap.put(10001L, new ShoppingListInfo(2, 20000L, 10000L,
                EntityType.PHYSICAL_MACHINE_VALUE, topologyDSPMBought));
        Field commTypeAllocator =
                        TopologyConverter.class.getDeclaredField("commodityTypeAllocator");
        commTypeAllocator.setAccessible(true);
            commTypeAllocator.set(converter, idAllocator);
        Field shoppingListInfos =
                        TopologyConverter.class.getDeclaredField("shoppingListOidToInfos");
        shoppingListInfos.setAccessible(true);
        shoppingListInfos.set(converter, shoppingListMap);

        final CommodityDTOs.CommoditySoldTO economyCPUSold = CommodityDTOs.CommoditySoldTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                            .setBaseType(2)
                            .setType(cpuType)
                            .build())
                        .build();
        final CommodityDTOs.CommoditySoldTO economyDSPMSold = CommodityDTOs.CommoditySoldTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                            .setBaseType(base)
                            .setType(dspmType)
                            .build())
                        .build();
        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType())).when(converter)
                        .economyToTopologyCommodity(Mockito.eq(economyCPUSold.getSpecification()));
        // create a topology entity DTO with DSPM sold
        TopologyDTO.TopologyEntityDTO expectedEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setOid(10000L)
                        .addCommoditySoldList(topologyDSPMSold)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(10000L)
                            .addAllCommodityBought(topologyCPUBought)
                            .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE))
                        .putEntityPropertyMap("dummy", "dummy")
                        .build();
        // create a topology entity DTO with DSPM bought
        TopologyDTO.TopologyEntityDTO expectedEntity2 = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setOid(20000L)
                        .setEntityType(20)
                        .addCommoditySoldList(topologyCPUSold)
                        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(111)
                            .addAllCommodityBought(topologyDSPMBought))
                        .putEntityPropertyMap("dummy", "dummy")
                        .build();
        // create trader DTO corresponds to originalEntity
        EconomyDTOs.TraderTO trader = EconomyDTOs.TraderTO.newBuilder()
                        .setOid(10000L)
                        .addCommoditiesSold(economyDSPMSold)
                        .addShoppingLists(ShoppingListTO.newBuilder()
                            .setOid(10001L)
                            .addCommoditiesBought(CommodityBoughtTO.newBuilder()
                                .setSpecification(CommoditySpecificationTO.newBuilder()
                                    .setBaseType(2)
                                    .setType(cpuType)
                                    .build())
                                .build())
                            .build())
                        .build();

        List<TopologyDTO.TopologyEntityDTO> entity =
                        converter.convertFromMarket(Arrays.asList(trader),
                            Sets.newHashSet(expectedEntity, expectedEntity2));
        assertEquals(1L, entity.size());
        assertTrue(expectedEntity.getCommoditySoldList(0)
                        .equals(entity.get(0).getCommoditySoldList(0)));
        assertEquals(1, entity.get(0).getCommoditiesBoughtFromProvidersCount());
        assertEquals(EntityType.PHYSICAL_MACHINE_VALUE,
                entity.get(0).getCommoditiesBoughtFromProviders(0).getProviderEntityType());
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
                            .setPriceBelow(0)
                            .build())
                    .build();

    /**
     * Test that step and constant price functions are created as needed.
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void testPriceFunctions() throws InvalidTopologyException {
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
    public void testSkipEntity() throws InvalidTopologyException {
        TopologyDTO.TopologyEntityDTO container = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setOid(1001L).setEntityType(40).build();
        TopologyDTO.TopologyEntityDTO virtualApp = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setOid(1001L).setEntityType(26).build();
        TopologyDTO.TopologyEntityDTO actionManager = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setOid(1001L).setEntityType(22).build();
        TopologyConverter converter = new TopologyConverter(REALTIME_TOPOLOGY_INFO, entityIdToEntityTypeMap);
        assertTrue(converter.convertToMarket(Arrays.asList(container, virtualApp, actionManager))
                        .isEmpty());
    }

    /**
     * If the fields in Analysis setting have been set, they will be directly used in trader settings.
     *
     * @throws InvalidTopologyException
     */
    @Test
    public void testTraderSetting() throws InvalidTopologyException {
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
    }
}
