/**
 *
 */
package com.vmturbo.platform.analysis.testUtilities;

import java.util.*;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommodityResizeSpecification;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceLimitation;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceCost;
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

    public static final int VM_TYPE = 0;
    public static final int PM_TYPE = 1;
    public static final int ST_TYPE = 2;
    public static final int VDC_TYPE = 3;
    public static final int APP_TYPE = 4;
    public static final int IOPS_TYPE = 5;
    public static final int CONTAINER_TYPE = 6;
    public static final int VAPP_TYPE = 7;
    public static final int POD_TYPE = 8;

    public static final double FLOATING_POINT_DELTA = 1e-7;
    public static final List<Long> NO_CLIQUES = new ArrayList<>();

    private static int commSpecCounter;

    public static final int LICENSE_COMM_BASE_TYPE = 0;
    public static final int LINUX_COMM_TYPE = 100;
    public static final int WINDOWS_COMM_TYPE = 200;

    // CommoditySpecifications to use in tests
    public static final CommoditySpecification CPU = createNewCommSpec();
    public static final CommoditySpecification MEM = createNewCommSpec();
    public static final CommoditySpecification ST_AMT = createNewCommSpec();
    public static final CommoditySpecification CPU_ALLOC = createNewCommSpec();
    public static final CommoditySpecification MEM_ALLOC = createNewCommSpec();
    public static final CommoditySpecification VCPU = createNewCommSpec();
    public static final CommoditySpecification VMEM = createNewCommSpec();
    public static final CommoditySpecification COST_COMMODITY = createNewCommSpec();
    public static final CommoditySpecification IOPS = createNewCommSpec();
    public static final CommoditySpecification TRANSACTION = createNewCommSpec();
    public static final CommoditySpecification SEGMENTATION_COMMODITY = createNewCommSpec();
    public static final CommoditySpecification RESPONSE_TIME = createNewCommSpec();

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
                                        M2Utils.ADD_TWO_ARGS, M2Utils.SUBRTRACT_TWO_ARGS);
        CommodityResizeSpecification vMemDependency =
                        new CommodityResizeSpecification(TestUtils.MEM.getType(),
                                        M2Utils.ADD_TWO_ARGS, M2Utils.SUBRTRACT_TWO_ARGS);
        commodityResizeDependencyMap.put(TestUtils.VCPU.getType(), Arrays.asList(vCpuDependency));
        commodityResizeDependencyMap.put(TestUtils.VMEM.getType(), Arrays.asList(vMemDependency));
    }

    /**
     * Sets up the raw material map for the economy passed in.
     * VCPU's raw material is set up as CPU. VMEM's raw material is set up as MEM.
     * @param economy - Economy for which you want to setup the raw commodity map.
     */
    public static void setupRawCommodityMap(Economy economy) {
        Map<Integer, List<Integer>> rawMaterialMap = economy.getModifiableRawCommodityMap();
        rawMaterialMap.put(TestUtils.VCPU.getType(), Arrays.asList(TestUtils.CPU.getType()));
        rawMaterialMap.put(TestUtils.VMEM.getType(), Arrays.asList(TestUtils.MEM.getType()));
    }

    public static CostFunction setUpGP2CostFunction() {
        // create cost function DTO for gp2
        StorageResourceDependency dependencyDTO =
                        StorageResourceDependency.newBuilder().setBaseResourceType(stAmtTO)
                                        .setDependentResourceType(iopsTO).setRatio(3).build();
        StorageTierPriceData stAmtPriceDTO = StorageTierPriceData.newBuilder().setUpperBound(Double.POSITIVE_INFINITY)
                        .setIsUnitPrice(true).setIsAccumulativeCost(false).setPrice(0.10)
                        .setBusinessAccountId(1).build();
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
                                               .addStorageResourceDependency(dependencyDTO).build())
                        .build();
        return CostFunctionFactory.createCostFunction(costDTO);
    }

    public static CostFunction setUpIO1CostFunction() {
        // create cost function DTO for io1
        StorageResourceDependency dependencyDTO =
                        StorageResourceDependency.newBuilder().setBaseResourceType(stAmtTO)
                                        .setDependentResourceType(iopsTO).setRatio(50).build();
        StorageTierPriceData stAmtPriceDTO = StorageTierPriceData.newBuilder().setUpperBound(Double.POSITIVE_INFINITY)
                        .setIsUnitPrice(true).setIsAccumulativeCost(false).setBusinessAccountId(1)
                        .setPrice(0.125).build();
        StorageTierPriceData iopsPriceDTO = StorageTierPriceData.newBuilder().setUpperBound(Double.POSITIVE_INFINITY)
                        .setIsUnitPrice(true).setIsAccumulativeCost(false).setBusinessAccountId(1)
                        .setPrice(0.065).build();
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
                                               .addStorageResourceDependency(dependencyDTO).build())
                        .build();
        return CostFunctionFactory.createCostFunction(costDTO);
    }

    public static CostFunction setUpPremiumManagedCostFunction() {
        // create cost function DTO for azure premium managed storage
        StorageTierPriceData stAmt32GBPriceDTO = StorageTierPriceData.newBuilder().setUpperBound(32).setIsUnitPrice(false)
                        .setIsAccumulativeCost(false).setPrice(5.28).setBusinessAccountId(1).build();
        StorageTierPriceData stAmt64GBPriceDTO = StorageTierPriceData.newBuilder().setUpperBound(64).setIsUnitPrice(false)
                        .setIsAccumulativeCost(false).setPrice(10.21).setBusinessAccountId(1).build();
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

    public static CostFunction setUpStandardUnmanagedCostFunction() {
        // create cost function DTO for azure standard unmanaged storage
        StorageTierPriceData stAmtLRS1TBPriceDTO = StorageTierPriceData.newBuilder().setUpperBound(1024)
                        .setIsAccumulativeCost(true).setIsUnitPrice(true).setBusinessAccountId(1).setPrice(0.05).build();
        StorageTierPriceData stAmtLRS50TBPriceDTO = StorageTierPriceData.newBuilder().setUpperBound(1024 * 50)
                        .setIsAccumulativeCost(true).setIsUnitPrice(true).setBusinessAccountId(1).setPrice(0.10).build();
        StorageResourceCost resourceCostDTO = StorageResourceCost.newBuilder().setResourceType(stAmtTO)
                        .addStorageTierPriceData(stAmtLRS1TBPriceDTO).addStorageTierPriceData(stAmtLRS50TBPriceDTO)
                        .build();
        CostDTO costDTO = CostDTO.newBuilder()
                        .setStorageTierCost(StorageTierCostDTO.newBuilder()
                                               .addStorageResourceCost(resourceCostDTO)
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
                        .addCostTupleList(CostTuple.newBuilder()
                                .setLicenseCommodityType(LINUX_COMM_TYPE)
                                .setBusinessAccountId(1)
                                .setPrice(1.5)
                                .build())
                        .addCostTupleList(CostTuple.newBuilder()
                                .setLicenseCommodityType(WINDOWS_COMM_TYPE)
                                .setBusinessAccountId(1)
                                .setPrice(2.5)
                                .build())
                        .build())
                .build();
        return CostFunctionFactory.createCostFunction(costDTO);
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
}
