/**
 *
 */
package com.vmturbo.platform.analysis.testUtilities;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.platform.analysis.topology.Topology;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommodityResizeSpecification;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.RawMaterials;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CbtpCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO.ComputeResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.DatabaseTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceCost;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceLimitation;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceRatioDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;
import com.vmturbo.platform.analysis.utilities.CostFunction;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactory;
import com.vmturbo.platform.analysis.utilities.FunctionalOperatorUtil;
import com.vmturbo.platform.analysis.utilities.M2Utils;

/**
 * This class contains useful utility functions to be used ONLY in unit tests.
 * @author thiru_arun
 *
 */
public class TestUtils {

    public static final int NO_TYPE = -1;

    public static final int VM_TYPE = 0;
    public static final int PM_TYPE = 1;
    public static final int ST_TYPE = 2;
    public static final int VDC_TYPE = 3;
    public static final int APP_TYPE = 4;
    public static final int IOPS_TYPE = 5;
    public static final int CONTAINER_TYPE = 6;
    public static final int VAPP_TYPE = 7;
    public static final int POD_TYPE = 8;
    public static final int DBS_TYPE = 9;
    public static final int APP_SERVER_TYPE = 10;
    public static final int DB_TYPE = 11;
    public static final int DC_TYPE = 12;
    public static final int NAMESPACE_TYPE = 13;

    public static final double FLOATING_POINT_DELTA = 1e-7;
    public static final double FLOATING_POINT_DELTA2 = 1e-15;
    public static final List<Long> NO_CLIQUES = new ArrayList<>();

    private static int commSpecCounter;

    public static final int LICENSE_COMM_BASE_TYPE = 0;
    public static final int DC_COMM_BASE_TYPE = 1;
    public static final int COUPON_COMM_BASE_TYPE = 64;
    public static final int LINUX_COMM_TYPE = 100;
    /**
     * The region comm type.
     */
    public static final int REGION_COMM_TYPE = 10;
    public static final int WINDOWS_COMM_TYPE = 200;
    public static final int DC1_COMM_TYPE = 300;
    public static final int DC2_COMM_TYPE = 301;
    public static final int DC3_COMM_TYPE = 302;
    public static final int DC4_COMM_TYPE = 303;
    /**
     * The DataCenter commodity ID.
     */
    public static final int DC5_COMM_TYPE = 400;

    // CommoditySpecifications to use in tests
    public static final CommoditySpecification CPU = createNewCommSpec();
    public static final CommoditySpecification MEM = createNewCommSpec();
    public static final CommoditySpecification COUPON_COMMODITY = createNewCommSpec();
    public static final CommoditySpecification ST_AMT = createNewCommSpec();
    public static final CommoditySpecification ST_PROV = createNewCommSpec();
    public static final CommoditySpecification ST_LATENCY = createNewCommSpec();
    public static final CommoditySpecification CPU_ALLOC = createNewCommSpec();
    public static final CommoditySpecification MEM_ALLOC = createNewCommSpec();
    public static final CommoditySpecification VCPU = createNewCommSpec();
    public static final CommoditySpecification VMEM = createNewCommSpec();
    public static final CommoditySpecification DBMEM = createNewCommSpec();
    public static final CommoditySpecification HEAP = createNewCommSpec();
    public static final CommoditySpecification COST_COMMODITY = createNewCommSpec();
    public static final CommoditySpecification IOPS = createNewCommSpec();
    public static final CommoditySpecification TRANSACTION = createNewCommSpec();
    public static final CommoditySpecification SEGMENTATION_COMMODITY = createNewCommSpec();
    public static final CommoditySpecification RESPONSE_TIME = createNewCommSpec();
    public static final CommoditySpecification STORAGE = createNewCommSpec();
    public static final CommoditySpecification POWER = createNewCommSpec();
    public static final CommoditySpecification SPACE = createNewCommSpec();
    public static final CommoditySpecification COOLING = createNewCommSpec();
    public static final CommoditySpecification VMEMLIMITQUOTA = createNewCommSpec();

    public static final CommoditySpecificationTO iopsTO =
                    CommoditySpecificationTO.newBuilder().setBaseType(TestUtils.IOPS.getBaseType())
                                    .setType(TestUtils.IOPS.getType()).build();
    public static final CommoditySpecificationTO stAmtTO = CommoditySpecificationTO.newBuilder()
                    .setBaseType(TestUtils.ST_AMT.getBaseType()).setType(TestUtils.ST_AMT.getType())
                    .build();

    /**
     * Returns a new commodity specification each time it is invoked
     *
     * @return new commodity specification
     */
    public static CommoditySpecification createNewCommSpec() {
        CommoditySpecification c = new CommoditySpecification(commSpecCounter);
        commSpecCounter++;
        return c;
    }

    /**
     * @param economy - Economy where you want to create a trader.
     * @param traderType - integer representing the type of trader
     * @param cliques - cliques this trader is member of.
     * @param basketCommodities - commodities that are sold by this trader.
     * @param capacities - capacities of commodities sold in the same order as basketCommodities.
     * @param isCloneable - can the trader be cloned.
     * @param isGuaranteedBuyer - is the trader a guaranteed buyer.
     * @return - Trader which was created.
     */
    public static Trader createTrader(Economy economy, int traderType, List<Long> cliques,
                    List<CommoditySpecification> basketCommodities, double[] capacities, boolean isCloneable, boolean isGuaranteedBuyer) {
        Trader trader = economy.addTrader(traderType, TraderState.ACTIVE, new Basket(basketCommodities),
                        new HashSet<>(cliques));
        for(int i=0;i<basketCommodities.size();i++){
            trader.getCommoditiesSold().get(trader.getBasketSold()
                            .indexOf(basketCommodities.get(i))).setCapacity(capacities[i]);
        }
        trader.getSettings().setCloneable(isCloneable);
        trader.getSettings().setGuaranteedBuyer(isGuaranteedBuyer);
        trader.getSettings().setCanAcceptNewCustomers(true);
        return trader;
    }

    /**
     * @param economy - Economy where you want to create a trader.
     * @param traderType - integer representing the type of trader
     * @param cliques - cliques this trader is member of.
     * @param basketCommodities - commodities that are sold by this trader.
     * @param capacities - capacities of commodities sold in the same order as basketCommodities.
     * @param isCloneable - can the trader be cloned.
     * @param isGuaranteedBuyer - is the trader a guaranteed buyer.
     * @param name the name of the trader
     *
     * @return - Trader which was created.
     */
    public static Trader createTrader(Economy economy, int traderType, List<Long> cliques,
                                      List<CommoditySpecification> basketCommodities,
                                      double[] capacities, boolean isCloneable,
                                      boolean isGuaranteedBuyer, String name) {
        Trader trader = createTrader(economy, traderType, cliques, basketCommodities, capacities,
                isCloneable, isGuaranteedBuyer);
        trader.setDebugInfoNeverUseInCode(name);
        return trader;
    }

    /**
     * @param economy - Economy where you want to create a PM.
     * @param cliques for the PM
     * @param cpuCapacity
     * @param isCloneable - is the PM cloneable
     * @return Trader i.e PM
     */
    public static Trader createPM(Economy economy, List<Long> cliques, double cpuCapacity, double memCapacity, boolean isCloneable) {
        Trader pm = createTrader(economy, PM_TYPE, cliques, Arrays.asList(CPU, MEM), new double[]{cpuCapacity, memCapacity}, isCloneable, false);
        return pm;
    }

    /**
     * This generates a non-clonable DC that sells Power, Space and Cooling
     *
     * @param economy - Economy where you want to create a DC.
     * @param name - Name of the DC
     * @return Trader i.e DC
     */
    public static Trader createDC(Economy economy, String name) {
        Trader dc = createTrader(economy, DC_TYPE, Lists.newArrayList(), Arrays.asList(POWER, SPACE, COOLING),
                new double[]{100, 100, 100}, false, false);
        dc.setDebugInfoNeverUseInCode(name);
        return dc;
    }

    /**
     * @param economy Economy where you want to create a PM.
     * @param cliques for the PM
     * @param cpuCapacity
     * @param isCloneable is the PM cloneable
     * @param name name of the PM
     *
     * @return Trader i.e PM
     */
    public static Trader createPM(Economy economy, List<Long> cliques, double cpuCapacity,
                                  double memCapacity, boolean isCloneable, String name) {
        Trader pm = createPM(economy, cliques, cpuCapacity, memCapacity, isCloneable);
        pm.setDebugInfoNeverUseInCode(name);
        return pm;
    }

    /**
     * @param economy - Economy where you want to create a storage.
     * @param cliques for the storage.
     * @param storageCapacity
     * @param isCloneable - is the storage cloneable
     * @return Trader i.e. storage
     */
    public static Trader createStorage(Economy economy, List<Long> cliques, double storageCapacity, boolean isCloneable) {
        Trader st = createTrader(economy, ST_TYPE, cliques, Arrays.asList(ST_AMT), new double[]{storageCapacity}, isCloneable, false);
        ((CommoditySoldSettings) st.getCommoditiesSold().get(st.getBasketSold().indexOf(ST_AMT))).setPriceFunction(PriceFunction.Cache.createStepPriceFunction(0.8, 1.0, 10000.0));
        return st;
    }

    /**
     * @param economy Economy where you want to create a storage.
     * @param cliques for the storage.
     * @param storageCapacity
     * @param isCloneable is the storage cloneable
     * @param name name of the storage
     *
     * @return Trader i.e. storage
     */
    public static Trader createStorage(Economy economy, List<Long> cliques, double storageCapacity, boolean isCloneable, String name) {
        Trader st = createStorage(economy, cliques, storageCapacity, isCloneable);
        st.setDebugInfoNeverUseInCode(name);
        return st;
    }

    /**
     * @param economy - Economy where you want to add a VM.
     * @return Trader i.e. VM
     */
    public static Trader createVM(Economy economy) {
        Trader vm1 = economy.addTrader(VM_TYPE, TraderState.ACTIVE, new Basket());
        return vm1;
    }

    /**
     * @param economy Economy where you want to add a VM.
     * @param name name of the VM
     *
     * @return Trader i.e. VM
     */
    public static Trader createVM(Economy economy, String name) {
        Trader vm1 = economy.addTrader(VM_TYPE, TraderState.ACTIVE, new Basket());
        vm1.setDebugInfoNeverUseInCode(name);
        return vm1;
    }

    /**
     * @param economy - Economy where you want to add a VDC.
     * @return a VDC trader which is a guaranteed buyer.
     */
    public static Trader createVDC(Economy economy) {
        Trader vdc = economy.addTrader(VDC_TYPE, TraderState.ACTIVE, new Basket());
        vdc.getSettings().setCanAcceptNewCustomers(true);
        return vdc;
    }

    /**
     * @param economy - Economy into which to insert the created virtual application
     * @param capacities - capacities of commodities sold in the same order as basketCommodities.
     * @param name the name of the virtual application
     *
     * @return - virtual application that was created
     */
    public static Trader createVirtualApplication(Economy economy, double[] capacities, String name) {
        return createTrader(economy, VAPP_TYPE, NO_CLIQUES,
                Arrays.asList(RESPONSE_TIME, TRANSACTION), capacities, true, true, name);
    }

    /**
     * @param economy - Economy into which to insert the created application
     * @param capacities - capacities of commodities sold in the same order as basketCommodities.
     * @param name the name of the application
     *
     * @return - application that was created
     */
    public static Trader createApplication(Economy economy, double[] capacities, String name) {
        Trader trader = createTrader(economy, APP_TYPE, NO_CLIQUES, Arrays.asList(RESPONSE_TIME, TRANSACTION),
                capacities, true, false, name);
        trader.getSettings().setProviderMustClone(true);
        return trader;
    }

    /**
     * @param economy - Economy into which to insert the created container
     * @param capacities - capacities of commodities sold in the same order as basketCommodities.
     * @param name the name of the container
     *
     * @return - container that was created
     */
    public static Trader createContainer(Economy economy, double[] capacities, String name) {
        Trader trader =  createTrader(economy, CONTAINER_TYPE, NO_CLIQUES, Arrays.asList(VCPU, VMEM),
                capacities, true, false, name);
        trader.getSettings().setProviderMustClone(true);
        return trader;
    }

    /**
     * @param economy - Economy into which to insert the created pod
     * @param capacities - capacities of commodities sold in the same order as basketCommodities.
     * @param name the name of the pod
     *
     * @return - pod that was created
     */
    public static Trader createContainerPod(Economy economy, double[] capacities, String name) {
        return createTrader(economy, POD_TYPE, NO_CLIQUES,
                Arrays.asList(VCPU, VMEM), capacities, true, false, name);
    }


    /**
     * @param economy - Economy where you want to add a DBS.
     * @return Trader i.e. DBS
     */
    public static Trader createDBS(Economy economy) {
        Trader dbs = economy.addTrader(DBS_TYPE, TraderState.ACTIVE, new Basket());
        return dbs;
    }

    /**
     * @param economy - Economy where you want to create and place shopping list.
     * @param basketCommodities - Basket's commodities bought in this shopping list.
     * @param buyer - The buyer buying this shopping list.
     * @param commQuantities - A list representing the quantities of commodities needed by the buyer in
     *                         the same order as the commodities in the basketCommodities.
     * @param seller - The seller to place this shopping list on.
     * @return The shopping list which was created.
     */
    public static ShoppingList createAndPlaceShoppingList(Economy economy,
                    List<CommoditySpecification> basketCommodities, Trader buyer,
                    double[] commQuantities, Trader seller) {
        Basket basket = new Basket(basketCommodities);
        ShoppingList sl = economy.addBasketBought(buyer, basket);
        for (int i = 0; i < basketCommodities.size(); i++){
            sl.setQuantity(sl.getBasket().indexOf(basketCommodities.get(i)), commQuantities[i]);
        }
        sl.setMovable(true);
        if(seller != null){
            sl.move(seller);
            for(int i = 0; i < basketCommodities.size(); i++) {
                int soldIndex = seller.getBasketSold()
                        .indexOf(basketCommodities.get(i));
                if (soldIndex >= 0) {
                    double sellerQuantity = seller.getCommoditiesSold().get(soldIndex).getQuantity();
                    seller.getCommoditiesSold().get(soldIndex).setQuantity(sellerQuantity + commQuantities[i]);
                }

            }
        }
        return sl;
    }

    /**
     * @param economy - Economy where you want to create and place shopping list.
     * @param basketCommodities - Basket's commodities bought in this shopping list.
     * @param buyer - The buyer buying this shopping list.
     * @param commQuantities - A list representing the quantities of commodities needed by the buyer in
     *                         the same order as the commodities in the basketCommodities.
     * @param peakQuantities - A list of the peak quantities needed by the buyer.
     * @param seller - The seller to place this shopping list on.
     * @return The shopping list which was created.
     */
    public static ShoppingList createAndPlaceShoppingList(Economy economy,
                    List<CommoditySpecification> basketCommodities, Trader buyer,
                    double[] commQuantities, double[] peakQuantities, Trader seller) {
        Basket basket = new Basket(basketCommodities);
        ShoppingList sl = economy.addBasketBought(buyer, basket);
        for (int i = 0; i < basketCommodities.size(); i++){
            sl.setQuantity(sl.getBasket().indexOf(basketCommodities.get(i)), commQuantities[i]);
            sl.setPeakQuantity(sl.getBasket().indexOf(basketCommodities.get(i)), peakQuantities[i]);
        }
        sl.setMovable(true);
        if(seller != null){
            sl.move(seller);
            for(int i=0; i<basketCommodities.size(); i++){
                double sellerQuantity = seller.getCommoditiesSold().get(seller.getBasketSold()
                                .indexOf(basketCommodities.get(i))).getQuantity();
                seller.getCommoditiesSold().get(seller.getBasketSold()
                                .indexOf(basketCommodities.get(i))).setQuantity(sellerQuantity + commQuantities[i]);

                double sellerPeakQuantity = seller.getCommoditiesSold().get(seller.getBasketSold()
                                .indexOf(basketCommodities.get(i))).getPeakQuantity();
                seller.getCommoditiesSold()
                                .get(seller.getBasketSold().indexOf(basketCommodities.get(i)))
                                .setPeakQuantity((peakQuantities[i] > sellerPeakQuantity)
                                                ? peakQuantities[i] : sellerPeakQuantity);
            }
        }
        return sl;
    }

    /**
     * Sets up the commodity resize dependency map for the economy passed in.
     * VCPU's dependency is setup as CPU. VMEM's dependency is setup as MEM.
     * The increment and decrement functions used are such that there is a 1-1 relationship.
     * For ex. If the VCPU decreases by 50 units, then CPU quantity also reduces by 50.
     * @param economy - The economy for which you want to setup the commodityResizeDependencyMap
     */
    public static void setupCommodityResizeDependencyMap(Economy economy) {
        @NonNull Map<@NonNull Integer, @NonNull List<@NonNull CommodityResizeSpecification>>
            commodityResizeDependencyMap = economy.getModifiableCommodityResizeDependencyMap();
        CommodityResizeSpecification vCpuDependency =
                        new CommodityResizeSpecification(TestUtils.CPU.getType(),
                                        M2Utils.ADD_TWO_ARGS, (a,b,c) -> Math.min(a, b));
        CommodityResizeSpecification vMemDependency =
                        new CommodityResizeSpecification(TestUtils.MEM.getType(),
                                        M2Utils.ADD_TWO_ARGS, (a,b,c) -> Math.min(a, b));
        CommodityResizeSpecification vMemQuotaDependency =
                new CommodityResizeSpecification(TestUtils.VMEMLIMITQUOTA.getType(),
                        M2Utils.ADD_TWO_ARGS, (a,b,c) -> Math.min(a, b));
        CommodityResizeSpecification DBMemDependency =
                new CommodityResizeSpecification(TestUtils.VMEM.getType(),
                        M2Utils.ADD_TWO_ARGS, M2Utils.SUBRTRACT_TWO_ARGS);
        CommodityResizeSpecification HeapDependency =
                new CommodityResizeSpecification(TestUtils.VMEM.getType(),
                        M2Utils.ADD_TWO_ARGS, M2Utils.SUBRTRACT_TWO_ARGS);
        commodityResizeDependencyMap.put(TestUtils.VCPU.getType(), Arrays.asList(vCpuDependency));
        commodityResizeDependencyMap.put(TestUtils.VMEM.getType(), Arrays.asList(vMemDependency, vMemQuotaDependency));
        commodityResizeDependencyMap.put(TestUtils.DBMEM.getType(), Arrays.asList(DBMemDependency));
        commodityResizeDependencyMap.put(TestUtils.HEAP.getType(), Arrays.asList(HeapDependency));
    }

    /**
     * Populates the commodity history based resize dependency skip map of a {@link Topology}.
     *
     * @param economy where to place the map
     */
    public static void setupHistoryBasedResizeDependencyMap(final @NonNull Economy economy) {
        economy.getModifiableHistoryBasedResizeDependencySkipMap().putAll(
            ImmutableMap.<Integer, List<Integer>>builder()
                .put(TestUtils.VCPU.getType(), Collections.singletonList(TestUtils.CPU.getType()))
                .put(TestUtils.VMEM.getType(), Collections.singletonList(TestUtils.MEM.getType()))
                .build());
    }

    /**
     * Sets up the raw material map for the economy passed in.
     * VCPU's raw material is set up as CPU. VMEM's raw material is set up as MEM.
     * @param economy - Economy for which you want to setup the raw commodity map.
     */
    public static void setupRawCommodityMap(Economy economy) {
                Map<Integer, RawMaterials> rawMap = economy.getModifiableRawCommodityMap();
        rawMap.put(TestUtils.VCPU.getType(),
                new RawMaterials(Lists.newArrayList(
                        CommunicationDTOs.EndDiscoveredTopology.RawMaterial
                                .newBuilder().setCommodityType(TestUtils.CPU.getType()).build(),
                        CommunicationDTOs.EndDiscoveredTopology.RawMaterial
                                .newBuilder().setCommodityType(TestUtils.VCPU.getType()).build()
                )));
        rawMap.put(TestUtils.VMEM.getType(),
                new RawMaterials(Lists.newArrayList(
                        CommunicationDTOs.EndDiscoveredTopology.RawMaterial
                                .newBuilder().setCommodityType(TestUtils.MEM.getType()).build(),
                        CommunicationDTOs.EndDiscoveredTopology.RawMaterial
                                .newBuilder().setCommodityType(TestUtils.VMEMLIMITQUOTA.getType()).build())));
        rawMap.put(TestUtils.DBMEM.getType(),
                new RawMaterials(Lists.newArrayList(
                        CommunicationDTOs.EndDiscoveredTopology.RawMaterial
                                .newBuilder().setCommodityType(TestUtils.VMEM.getType()).build())));
        rawMap.put(TestUtils.HEAP.getType(),
                new RawMaterials(Lists.newArrayList(
                        CommunicationDTOs.EndDiscoveredTopology.RawMaterial
                                .newBuilder().setCommodityType(TestUtils.VMEM.getType()).build())));
    }

    /**
     * Sets up the raw material map for the economy passed in.
     * VCPU's raw material is set up as CPU. VMEM's raw material is set up as MEM.
     * @param economy - Economy for which you want to setup the raw commodity map.
     */
    public static void setupProducesDependancyMap(Economy economy) {
        List<Integer> vMemCoDepMap = ImmutableList.of(TestUtils.DBMEM.getBaseType(), TestUtils.HEAP.getBaseType());
        economy.addToModifiableCommodityProducesDependencyMap(TestUtils.VMEM.getBaseType(), vMemCoDepMap);
    }

    public static CostFunction setUpGP2CostFunction() {
        // create cost function DTO for gp2
        StorageResourceRatioDependency dependencyDTO =
                StorageResourceRatioDependency.newBuilder().setBaseResourceType(stAmtTO)
                                        .setDependentResourceType(iopsTO).setMaxRatio(3).build();
        StorageTierPriceData stAmtPriceDTO = StorageTierPriceData.newBuilder().setUpperBound(Double.POSITIVE_INFINITY)
                        .setIsUnitPrice(true).setIsAccumulativeCost(false).addCostTupleList(setUpCostTuple(1, -1, 10L, 0.10)).build();
        StorageResourceCost stAmtCostDTO = StorageResourceCost.newBuilder().setResourceType(stAmtTO)
                        .addStorageTierPriceData(stAmtPriceDTO).build();
        CostDTO costDTO = CostDTO.newBuilder()
                        .setStorageTierCost(StorageTierCostDTO.newBuilder()
                                               .addStorageResourceCost(stAmtCostDTO)
                                               .addStorageResourceLimitation(StorageResourceLimitation.newBuilder()
                                               .setResourceType(stAmtTO).setMaxCapacity(16 * 1024)
                                               .setMinCapacity(1).build())
                                               .addStorageResourceLimitation(StorageResourceLimitation.newBuilder()
                                               .setResourceType(iopsTO).setMaxCapacity(10000)
                                               .setMinCapacity(100).build())
                                               .addStorageResourceRatioDependency(dependencyDTO).build())
                        .build();
        return CostFunctionFactory.createCostFunction(costDTO);
    }

    public static CostFunction setUpIO1CostFunction() {
        // create cost function DTO for io1
        StorageResourceRatioDependency dependencyDTO =
                StorageResourceRatioDependency.newBuilder().setBaseResourceType(stAmtTO)
                                        .setDependentResourceType(iopsTO).setMaxRatio(50).build();
        StorageTierPriceData stAmtPriceDTO = StorageTierPriceData.newBuilder().setUpperBound(Double.POSITIVE_INFINITY)
                        .setIsUnitPrice(true).setIsAccumulativeCost(false).addCostTupleList(setUpCostTuple(1, -1, 10L, 0.125)).build();
        StorageTierPriceData iopsPriceDTO = StorageTierPriceData.newBuilder().setUpperBound(Double.POSITIVE_INFINITY)
                        .setIsUnitPrice(true).setIsAccumulativeCost(false).addCostTupleList(setUpCostTuple(1, -1, 10L, 0.065)).build();
        StorageResourceCost stAmtCostDTO = StorageResourceCost.newBuilder().setResourceType(stAmtTO)
                        .addStorageTierPriceData(stAmtPriceDTO).build();
        StorageResourceCost iopsCostDTO = StorageResourceCost.newBuilder().setResourceType(iopsTO)
                        .addStorageTierPriceData(iopsPriceDTO).build();
        CostDTO costDTO = CostDTO.newBuilder()
                        .setStorageTierCost(StorageTierCostDTO.newBuilder()
                                               .addStorageResourceCost(stAmtCostDTO)
                                               .addStorageResourceCost(iopsCostDTO)
                                               .addStorageResourceLimitation(StorageResourceLimitation.newBuilder()
                                               .setResourceType(stAmtTO).setMaxCapacity(16 * 1024)
                                               .setMinCapacity(4).build())
                                               .addStorageResourceLimitation(StorageResourceLimitation.newBuilder()
                                               .setResourceType(iopsTO).setMaxCapacity(20000)
                                               .setMinCapacity(100).build())
                                               .addStorageResourceRatioDependency(dependencyDTO).build())
                        .build();
        return CostFunctionFactory.createCostFunction(costDTO);
    }

    public static CostFunction setUpPremiumManagedCostFunction() {
        // create cost function DTO for azure premium managed storage
        StorageTierPriceData stAmt32GBPriceDTO = StorageTierPriceData.newBuilder().setUpperBound(32).setIsUnitPrice(false)
                        .setIsAccumulativeCost(true).addCostTupleList(setUpCostTuple(1, -1, 10L, 5.28)).build();
        StorageTierPriceData stAmt64GBPriceDTO = StorageTierPriceData.newBuilder().setUpperBound(64).setIsUnitPrice(false)
                        .setIsAccumulativeCost(true).addCostTupleList(setUpCostTuple(1, -1, 10L, 10.21)).build();
        StorageResourceCost stAmtDTO = StorageResourceCost.newBuilder().setResourceType(stAmtTO)
                        .addStorageTierPriceData(stAmt32GBPriceDTO).addStorageTierPriceData(stAmt64GBPriceDTO).build();
        CostDTO costDTO = CostDTO.newBuilder()
                        .setStorageTierCost(StorageTierCostDTO.newBuilder()
                                               .addStorageResourceCost(stAmtDTO)
                                               .addStorageResourceLimitation(StorageResourceLimitation.newBuilder()
                                               .setResourceType(stAmtTO).setMaxCapacity(4 * 1024)
                                               .setMinCapacity(1).build()).build())
                        .build();
        return CostFunctionFactory.createCostFunction(costDTO);
    }

    public static CostFunction setUpT2NanoCostFunction() {
        CostDTO costDTO = CostDTO.newBuilder().setComputeTierCost(
                ComputeTierCostDTO.newBuilder()
                        .setLicenseCommodityBaseType(LICENSE_COMM_BASE_TYPE)
                        .setCouponBaseType(COUPON_COMM_BASE_TYPE)
                        .setRegionCommodityBaseType(REGION_COMM_TYPE)
                        .addCostTupleList(CostTuple.newBuilder()
                                .setLicenseCommodityType(LINUX_COMM_TYPE)
                                .setBusinessAccountId(1)
                                .setRegionId(10L)
                                .setPrice(1.5)
                                .build())
                        .addCostTupleList(CostTuple.newBuilder()
                                .setLicenseCommodityType(WINDOWS_COMM_TYPE)
                                .setBusinessAccountId(1)
                                .setRegionId(10L)
                                .setPrice(2.5)
                                .build())
                        .build())
                .build();
        return CostFunctionFactory.createCostFunction(costDTO);
    }

    public static CostDTO setUpDatabaseTierCostDTO(int licenseBaseType,
                                                   int couponBaseType,
                                                   int regionBaseType) {
        DatabaseTierCostDTO dbTierCostBuilder = DatabaseTierCostDTO.newBuilder()
                .setLicenseCommodityBaseType(licenseBaseType)
                .setCouponBaseType(couponBaseType)
                .setRegionCommodityBaseType(regionBaseType)
                .addAllCostTupleList(setUpMultipleRegionsCostTuples()).build();
        CostDTO costDTO = CostDTO.newBuilder()
                .setDatabaseTierCost(dbTierCostBuilder)
                .build();
        return costDTO;
    }

    public static List<CostTuple> setUpMultipleRegionsCostTuples() {
        return ImmutableList.of(
                setUpCostTuple(1, LINUX_COMM_TYPE, DC1_COMM_TYPE, 1.5),
                setUpCostTuple(1, LINUX_COMM_TYPE, DC2_COMM_TYPE, 2.5),
                setUpCostTuple(1, LINUX_COMM_TYPE, DC3_COMM_TYPE, 3.5),
                setUpCostTuple(1, LINUX_COMM_TYPE, DC4_COMM_TYPE, 4.5),
                setUpCostTuple(1, WINDOWS_COMM_TYPE, DC1_COMM_TYPE, 1.7),
                setUpCostTuple(1, WINDOWS_COMM_TYPE, DC2_COMM_TYPE, 2.7),
                setUpCostTuple(1, WINDOWS_COMM_TYPE, DC3_COMM_TYPE, 3.7),
                setUpCostTuple(1, WINDOWS_COMM_TYPE, DC4_COMM_TYPE, 4.7),
                setUpCostTuple(2, LINUX_COMM_TYPE, DC1_COMM_TYPE, 1.6),
                setUpCostTuple(2, LINUX_COMM_TYPE, DC2_COMM_TYPE, 2.6),
                setUpCostTuple(2, LINUX_COMM_TYPE, DC3_COMM_TYPE, 3.6),
                setUpCostTuple(2, LINUX_COMM_TYPE, DC4_COMM_TYPE, 4.6),
                setUpCostTuple(2, WINDOWS_COMM_TYPE, DC1_COMM_TYPE, 1.9),
                setUpCostTuple(2, WINDOWS_COMM_TYPE, DC2_COMM_TYPE, 2.9),
                setUpCostTuple(2, WINDOWS_COMM_TYPE, DC3_COMM_TYPE, 3.9),
                setUpCostTuple(2, WINDOWS_COMM_TYPE, DC4_COMM_TYPE, 4.9),
                // Values for no License
                setUpCostTuple(1, NO_TYPE, DC1_COMM_TYPE, 1.5),
                setUpCostTuple(1, NO_TYPE, DC2_COMM_TYPE, 2.5),
                setUpCostTuple(1, NO_TYPE, DC3_COMM_TYPE, Double.POSITIVE_INFINITY),
                setUpCostTuple(1, NO_TYPE, DC4_COMM_TYPE, 4.5),
                setUpCostTuple(2, NO_TYPE, DC1_COMM_TYPE, 1.6),
                setUpCostTuple(2, NO_TYPE, DC2_COMM_TYPE, 2.6),
                setUpCostTuple(2, NO_TYPE, DC3_COMM_TYPE, 3.6),
                setUpCostTuple(2, NO_TYPE, DC4_COMM_TYPE, 4.6)
        );
    }

    private static CostTuple setUpCostTuple(long businessAccountId,
                                            int licenseCommodityType,
                                            long regionId,
                                            double cost) {
        return CostTuple.newBuilder()
                .setLicenseCommodityType(licenseCommodityType)
                .setBusinessAccountId(businessAccountId)
                .setRegionId(regionId)
                .setPrice(cost)
                .build();
    }

    public static void moveSlOnSupplier(Economy e, ShoppingList sl, Trader supplier, double[] quantities) {
        for (int i = 0; i < quantities.length ; i++) {
            sl.setQuantity(i, quantities[i]);
            sl.setPeakQuantity(i, quantities[i]);
        }
        sl.move(supplier);
        sl.setMovable(true);
        Move.updateQuantities(e, sl, supplier, FunctionalOperatorUtil.ADD_COMM);
    }

    /**
     * Creates a Trader with the provided parameters.
     *
     * @param cost assocaited with the CBTP.
     * @param name of the CBTP.
     * @param economy to which the Trader should belong.
     * @param regional true if the CBTP is regional, false otherwise.
     * @param locationId region id or zone id of the CBTP.
     * @param accountScopeId account id or price id of the CBTP.
     *
     * @return an instance of Trader.
     */
    public static Trader setAndGetCBTP(double cost, String name, Economy economy,
                                       boolean regional, long locationId, long accountScopeId,
                                       long priceId) {
        double riDeprecationFactor = 0.0000001;
        Trader cbtp = TestUtils.createTrader(economy, TestUtils.PM_TYPE, Arrays.asList(0l),
                        Arrays.asList(TestUtils.CPU, TestUtils.COUPON_COMMODITY),
                new double[] {3000, 2}, true, true, name);
        final BalanceAccount account = new BalanceAccount(0.0, 100000000d, 24, priceId);
        final Context context;
        if (regional) {
            context = new Context(locationId, 0L, account);
        } else {
            context = new Context(0L, locationId, account);
        }
        cbtp.getSettings().setContext(context);
        cbtp.getSettings().setQuoteFunction(QuoteFunctionFactory.budgetDepletionRiskBasedQuoteFunction());
        CbtpCostDTO.Builder cbtpBundleBuilder = createCbtpBundleBuilder(
                TestUtils.COUPON_COMMODITY.getBaseType(), cost * riDeprecationFactor, 50,
               regional, locationId);
        final CostTuple.Builder costTuple = CostTuple.newBuilder()
                .setBusinessAccountId(priceId);
        if (regional) {
            costTuple.setRegionId(locationId);
        } else {
            costTuple.setZoneId(locationId);
        }
        CostDTO costDTOcbtp = CostDTO.newBuilder()
                .setCbtpResourceBundle(cbtpBundleBuilder
                        .addCostTupleList(costTuple)
                        .addScopeIds(accountScopeId))
                .build();
        CbtpCostDTO cdDTo = costDTOcbtp.getCbtpResourceBundle();
        cbtp.getSettings().setCostFunction(CostFunctionFactory.createResourceBundleCostFunctionForCbtp(cdDTo));
        return cbtp;
    }

    public static ComputeTierCostDTO.Builder getComputeTierCostDTOBuilder() {
        ComputeTierCostDTO.Builder costBundleBuilder = ComputeTierCostDTO.newBuilder();
        costBundleBuilder.addComputeResourceDepedency(ComputeResourceDependency.newBuilder()
                .setBaseResourceType(CommoditySpecificationTO.newBuilder()
                        .setBaseType(0)
                        .setType(0))
                .setDependentResourceType(CommoditySpecificationTO.newBuilder()
                        .setBaseType(0)
                        .setType(0)));
        costBundleBuilder.setLicenseCommodityBaseType(3);
        costBundleBuilder.setCouponBaseType(TestUtils.COUPON_COMMODITY.getBaseType());
        return costBundleBuilder;
    }

    /**
     * Create the cbtp bundle builder
     *
     * @param couponBaseType  Coupon value we want to set on the cbtp
     * @param price   The price of the RI we want to set on the cbtp
     * @param averageDiscount  the discount we want to set
     * @param regional true if the CBTP is regional
     * @param locationId region id or zone id of the CBTP
     * @return  the CBTPCostDTO.Builder
     */
    public static CbtpCostDTO.Builder createCbtpBundleBuilder(int couponBaseType, double price,
                                                              double averageDiscount,
                                                              boolean regional,
                                                              long locationId) {
        CbtpCostDTO.Builder cbtpBundleBuilder = CbtpCostDTO.newBuilder();
        cbtpBundleBuilder.setCouponBaseType(couponBaseType);
        final CostTuple.Builder costTuple = CostTuple.newBuilder().setPrice(price);
        if (regional) {
            costTuple.setRegionId(locationId);
        } else {
            costTuple.setZoneId(locationId);
        }
        cbtpBundleBuilder.addCostTupleList(costTuple);
        cbtpBundleBuilder.setDiscountPercentage(averageDiscount);
        cbtpBundleBuilder.addScopeIds(0);
        return cbtpBundleBuilder;
    }
}
