package com.vmturbo.market.topology.conversions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.analysis.InvalidTopologyException;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.DeactivateTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveExplanation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ProvisionBySupplyTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ReconfigureTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ResizeTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommoditySpecificationTO;
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

    private double epsilon = 1e-5; // used in assertEquals(double, double, epsilon)

    private EconomyDTOs.CommoditySpecificationTO economyCommodity1;
    private CommodityType topologyCommodity1;
    private EconomyDTOs.CommoditySpecificationTO economyCommodity2;
    private CommodityType topologyCommodity2;

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        // The commodity types in topologyCommodity
        // map to the base type in economy commodity.
        topologyCommodity1 =
                CommodityType.newBuilder()
                        .setType(1)
                        .setKey("blah")
                        .build();
        topologyCommodity2 =
                CommodityType.newBuilder()
                        .setType(2)
                        .setKey("blahblah")
                        .build();
        economyCommodity1 =
                EconomyDTOs.CommoditySpecificationTO.newBuilder()
                        .setType(0)
                        .setBaseType(1)
                        .build();
        economyCommodity2 =
                EconomyDTOs.CommoditySpecificationTO.newBuilder()
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
        Set<TraderTO> traderTOs = convert(topologyDTOs);
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
        assertEquals(20000.0, commSoldSettings.getCapacityIncrement(), epsilon); // currently hard-coded as capacity / 5
        assertEquals(0.8, commSoldSettings.getUtilizationUpperBound(), epsilon);
        assertEquals(1.0, commSoldSettings.getPriceFunction().getStandardWeighted().getWeight(), epsilon);
        TraderSettingsTO vdcSettings = vdcTraderTO.getSettings();
        assertFalse(vdcSettings.getClonable());
        assertFalse(vdcSettings.getSuspendable());
        assertEquals(0.7, vdcSettings.getMinDesiredUtilization(), epsilon); // hard-coded value
        assertEquals(0.8, vdcSettings.getMaxDesiredUtilization(), epsilon); // hard-coded value
        assertTrue(vdcSettings.getCanAcceptNewCustomers());
    }

    private TraderTO getVmTrader(Set<TraderTO> traders) {
        return traders.stream()
            .filter(tto -> tto.getType() == EntityType.VIRTUAL_MACHINE_VALUE)
            .findFirst()
            .orElse(null);
    }

    private CommodityBoughtTO getMem(List<CommodityBoughtTO> commoditiesBoughtList) {
        return commoditiesBoughtList.stream()
            .filter(comm -> comm.getSpecification().getBaseType() == CommodityDTO.CommodityType.MEM_VALUE)
            .findFirst()
            .orElse(null);
    }

    /**
     * Test loading a group of DTOs and that the properties of bought commodities
     * after the conversion match the file.
     * @throws IOException when the loaded file is missing
     * @throws InvalidTopologyException not supposed to happen here
     */
    @Test
    public void testConvertDTOs() throws IOException, InvalidTopologyException {
        Set<TopologyDTO.TopologyEntityDTO> topologyDTOs = Sets.newHashSet(
                messageFromJsonFile("protobuf/messages/vm-1.dto.json"),
                messageFromJsonFile("protobuf/messages/pm-1.dto.json"),
                messageFromJsonFile("protobuf/messages/ds-1.dto.json"),
                messageFromJsonFile("protobuf/messages/ds-2.dto.json"),
                messageFromJsonFile("protobuf/messages/vdc-1.dto.json"));
        Set<TraderTO> traderTOs = convert(topologyDTOs);
        assertEquals(topologyDTOs.size(), traderTOs.size());

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
        assertEquals(pmShoppingList.getStorageMoveCost(), 0, epsilon);
        // Get the shopping list that buys from DS
        ShoppingListTO dsShoppingList = shoppingLists.stream()
                        .filter(sl -> sl.getSupplier() == 205)
                        .findAny().get();
        // Buys 1024 from on DS and 2048 from another DS
        assertEquals(3.0, dsShoppingList.getStorageMoveCost(), epsilon);
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
        final TopologyConverter converter = new TopologyConverter(true);
        converter.convertToMarket(Lists.newArrayList(entityDTO));

        assertEquals(topologyCommodity1,
                converter.economyToTopologyCommodity(economyCommodity1).get());
        assertEquals(topologyCommodity2,
                converter.economyToTopologyCommodity(economyCommodity2).get());
    }

    @Test
    public void testExecutableFlag() {
        final TopologyConverter converter = new TopologyConverter(true);

        final ActionTO executableActionTO = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                // Using ProvisionBySupply because it requires the least setup, and all
                // we really care about is the executable flag.
                .setProvisionBySupply(ProvisionBySupplyTO.newBuilder()
                        .setProvisionedSeller(-1)
                        .setModelSeller(1234)
                        .build())
                .build();

        final ActionTO notExecutableActionTO = ActionTO.newBuilder(executableActionTO)
                .setIsNotExecutable(true)
                .build();

        assertTrue(converter.interpretAction(executableActionTO).get().getExecutable());
        assertFalse(converter.interpretAction(notExecutableActionTO).get().getExecutable());
    }

    @Test
    public void testInterpretMoveAction() throws IOException, InvalidTopologyException {
        TopologyConverter converter = new TopologyConverter(true);
        TopologyDTO.TopologyEntityDTO entityDTO =
                messageFromJsonFile("protobuf/messages/vm-1.dto.json");

        Set<TraderTO> traderTOs = converter.convertToMarket(Lists.newArrayList(entityDTO));
        final TraderTO vmTraderTO = getVmTrader(traderTOs);
        ShoppingListTO shoppingList = vmTraderTO.getShoppingListsList().get(0);

        ActionInfo actionInfo = converter.interpretAction(
                ActionTO.newBuilder()
                    .setImportance(0.)
                    .setIsNotExecutable(false)
                    .setMove(MoveTO.newBuilder()
                        .setShoppingListToMove(shoppingList.getOid())
                        .setSource(1234)
                        .setDestination(5678)
                            // set the moveExplanation only for
                            // compilation purpose
                        .setMoveExplanation(MoveExplanation
                            .newBuilder().build())
                        .build())
                    .build()).get().getInfo();

        assertEquals(ActionTypeCase.MOVE, actionInfo.getActionTypeCase());
        assertEquals(1234, actionInfo.getMove().getSourceId());
        assertEquals(5678, actionInfo.getMove().getDestinationId());
    }

    @Test
    public void testInterpretReconfigureAction() throws IOException, InvalidTopologyException {
        TopologyConverter converter = new TopologyConverter(true);
        TopologyDTO.TopologyEntityDTO topologyDTO =
            messageFromJsonFile("protobuf/messages/vm-1.dto.json");

        Set<TraderTO> traderTOs = converter.convertToMarket(Lists.newArrayList(topologyDTO));
        final TraderTO vmTraderTO = getVmTrader(traderTOs);
        ShoppingListTO shoppingList = vmTraderTO.getShoppingListsList().get(0);

        ActionInfo actionInfo = converter.interpretAction(
            ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setReconfigure(ReconfigureTO.newBuilder()
                    .setShoppingListToReconfigure(shoppingList.getOid())
                    .setSource(1234)
                    .build())
                .build()).get().getInfo();

        assertEquals(ActionTypeCase.RECONFIGURE, actionInfo.getActionTypeCase());
        assertEquals(1234, actionInfo.getReconfigure().getSourceId());
    }

    @Test
    public void testInterpretProvisionBySupplyAction() throws Exception {
        TopologyConverter converter = new TopologyConverter(true);

        ActionInfo actionInfo = converter.interpretAction(
                ActionTO.newBuilder()
                    .setImportance(0.)
                    .setIsNotExecutable(false)
                    .setProvisionBySupply(ProvisionBySupplyTO.newBuilder()
                        .setProvisionedSeller(-1)
                        .setModelSeller(1234)
                        .build())
                    .build()).get().getInfo();

        assertEquals(ActionTypeCase.PROVISION, actionInfo.getActionTypeCase());
        assertEquals(-1, actionInfo.getProvision().getProvisionedSeller());
        assertEquals(1234, actionInfo.getProvision().getEntityToCloneId());
    }

    @Test
    public void testInterpretResizeAction() throws Exception {
        final TopologyConverter converter = Mockito.spy(new TopologyConverter(true));
        Mockito.doReturn(Optional.of(topologyCommodity1))
               .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity1));

        final long entityToResize = 1;
        final long oldCapacity = 10;
        final long newCapacity = 9;
        final ActionTO resizeAction = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setResize(ResizeTO.newBuilder()
                    .setSellingTrader(entityToResize)
                    .setNewCapacity(newCapacity)
                    .setOldCapacity(oldCapacity)
                    .setSpecification(economyCommodity1))
                .build();
        final ActionInfo actionInfo = converter.interpretAction(resizeAction).get().getInfo();

        assertEquals(ActionTypeCase.RESIZE, actionInfo.getActionTypeCase());
        assertEquals(entityToResize, actionInfo.getResize().getTargetId());
        assertEquals(topologyCommodity1, actionInfo.getResize().getCommodityType());
        assertEquals(oldCapacity, actionInfo.getResize().getOldCapacity(), 0);
        assertEquals(newCapacity, actionInfo.getResize().getNewCapacity(), 0);
    }

    @Test
    public void testInterpretActivateAction() throws Exception {
        final TopologyConverter converter = Mockito.spy(new TopologyConverter(true));
        Mockito.doReturn(Optional.of(topologyCommodity1))
               .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity1));
        Mockito.doReturn(Optional.of(topologyCommodity2))
                .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity2));

        final long entityToActivate = 1;
        final ActionTO activateAction = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setActivate(ActivateTO.newBuilder()
                    .setTraderToActivate(entityToActivate)
                    .setModelSeller(2)
                    .setMostExpensiveCommodity(economyCommodity1.getBaseType())
                    .addTriggeringBasket(economyCommodity1)
                    .addTriggeringBasket(economyCommodity2))
                .build();
        final ActionInfo actionInfo = converter.interpretAction(activateAction).get().getInfo();

        assertEquals(ActionTypeCase.ACTIVATE, actionInfo.getActionTypeCase());
        assertEquals(entityToActivate, actionInfo.getActivate().getTargetId());
        assertThat(actionInfo.getActivate().getTriggeringCommoditiesList(),
                IsIterableContainingInAnyOrder.containsInAnyOrder(topologyCommodity1,
                        topologyCommodity2));
    }

    @Test
    public void testInterpretDeactivateAction() throws Exception {
        final TopologyConverter converter = Mockito.spy(new TopologyConverter(true));
        Mockito.doReturn(Optional.of(topologyCommodity1))
                .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity1));
        Mockito.doReturn(Optional.of(topologyCommodity2))
                .when(converter).economyToTopologyCommodity(Mockito.eq(economyCommodity2));

        final long entityToDeactivate = 1;
        final ActionTO deactivateAction = ActionTO.newBuilder()
                .setImportance(0.)
                .setIsNotExecutable(false)
                .setDeactivate(DeactivateTO.newBuilder()
                        .setTraderToDeactivate(entityToDeactivate)
                        .addTriggeringBasket(economyCommodity1)
                        .addTriggeringBasket(economyCommodity2)
                        .build())
                .build();
        final ActionInfo actionInfo = converter.interpretAction(deactivateAction).get().getInfo();

        assertEquals(ActionTypeCase.DEACTIVATE, actionInfo.getActionTypeCase());
        assertEquals(entityToDeactivate, actionInfo.getDeactivate().getTargetId());
        assertThat(actionInfo.getDeactivate().getTriggeringCommoditiesList(),
                IsIterableContainingInAnyOrder.containsInAnyOrder(topologyCommodity1,
                        topologyCommodity2));
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
    private static Set<TraderTO> convert(@Nonnull final Set<TopologyDTO.TopologyEntityDTO> topology)
                    throws InvalidTopologyException {
        return new TopologyConverter(true).convertToMarket(topology);
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
        Set<TraderTO> traderTOs = new TopologyConverter(false)
                        .convertToMarket(topologyDTOs.stream().collect(Collectors.toList()));
        assertEquals(2, traderTOs.size());
        for (TraderTO traderTO : traderTOs) {
            if (traderTO.getType() == 14) {
                // this is the pm, so trader should not contain MemAllocation in commSold
                assertFalse(traderTO.getCommoditiesSoldList().stream()
                                .anyMatch(c -> c.getSpecification().getType() == 50));
            }
            if (traderTO.getType() == 10) {
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
        TopologyConverter converter = Mockito.spy(new TopologyConverter());
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
        TopologyEntityDTO.CommodityBoughtList topologyDSPMBought =
                    TopologyEntityDTO.CommodityBoughtList.newBuilder()
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)
                                        .build())
                                .build())
                        .build();
        final TopologyEntityDTO.CommodityBoughtList topologyCPUBought =
                    TopologyEntityDTO.CommodityBoughtList.newBuilder()
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.CPU_VALUE)
                                        .build())
                                .build())
                        .build();

        NumericIDAllocator idAllocator = new NumericIDAllocator();
        final int base = idAllocator.allocate("BICLIQUE");
        final int cpuType = idAllocator.allocate("CPU");
        final int dspmType = idAllocator.allocate("DSPM");
        Map<Long, ShoppingListInfo> shoppingListMap = new HashMap<>();
        shoppingListMap.put(10000L, new ShoppingListInfo(2, 20000L, 10000L, topologyDSPMBought));
        Field commTypeAllocator =
                        TopologyConverter.class.getDeclaredField("commodityTypeAllocator");
        commTypeAllocator.setAccessible(true);
            commTypeAllocator.set(converter, idAllocator);
        Field shoppingListInfos =
                        TopologyConverter.class.getDeclaredField("shoppingListOidToInfos");
        shoppingListInfos.setAccessible(true);
        shoppingListInfos.set(converter, shoppingListMap);

        final EconomyDTOs.CommoditySoldTO economyCPUSold = EconomyDTOs.CommoditySoldTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                            .setBaseType(2)
                            .setType(cpuType)
                            .build())
                        .build();
        final EconomyDTOs.CommoditySoldTO economyDSPMSold = EconomyDTOs.CommoditySoldTO.newBuilder()
                        .setSpecification(CommoditySpecificationTO.newBuilder()
                            .setBaseType(base)
                            .setType(dspmType)
                            .build())
                        .build();
        Mockito.doReturn(Optional.of(topologyCPUSold.getCommodityType())).when(converter)
                        .economyToTopologyCommodity(Mockito.eq(economyCPUSold.getSpecification()));
        // create a topology entity DTO with DSPM sold
        TopologyDTO.TopologyEntityDTO expectedEntity = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setEntityType(10)
                        .setOid(10000L)
                        .addCommoditySoldList(topologyDSPMSold)
                        .putCommodityBoughtMap(10000L, topologyCPUBought)
                        .putEntityPropertyMap("dummy", "dummy")
                        .build();
        // create a topology entity DTO with DSPM bought
        TopologyDTO.TopologyEntityDTO expectedEntity2 = TopologyDTO.TopologyEntityDTO.newBuilder()
                        .setOid(20000L)
                        .setEntityType(20)
                        .addCommoditySoldList(topologyCPUSold)
                        .putCommodityBoughtMap(111, topologyDSPMBought)
                        .putEntityPropertyMap("dummy", "dummy")
                        .build();
        // create trader DTO corresponds to originalEntity
        EconomyDTOs.TraderTO trader = EconomyDTOs.TraderTO.newBuilder()
                        .setOid(10000L)
                        .addCommoditiesSold(economyDSPMSold)
                        .addShoppingLists(ShoppingListTO.newBuilder()
                            .setOid(111)
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
        assertTrue(expectedEntity.getCommoditySoldList(0)
                        .equals(entity.get(0).getCommoditySoldList(0)));
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
        TraderTO traderTO = convert(topologyDTOs).iterator().next();
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
        TopologyConverter converter = new TopologyConverter();
        assertTrue(converter.convertToMarket(Arrays.asList(container, virtualApp, actionManager))
                        .isEmpty());
    }

}
