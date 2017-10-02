/**
 *
 */
package com.vmturbo.platform.analysis.testUtilities;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommodityResizeSpecification;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
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

    // CommoditySpecifications to use in tests
    public static final CommoditySpecification CPU = new CommoditySpecification(0);
    public static final CommoditySpecification MEM = new CommoditySpecification(1);
    public static final CommoditySpecification ST_AMT = new CommoditySpecification(2);
    public static final CommoditySpecification CPU_ALLOC = new CommoditySpecification(3);
    public static final CommoditySpecification MEM_ALLOC = new CommoditySpecification(4);
    public static final CommoditySpecification VCPU = new CommoditySpecification(5);
    public static final CommoditySpecification VMEM = new CommoditySpecification(6);

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
        Trader pm1 = createTrader(economy, PM_TYPE, cliques, Arrays.asList(CPU, MEM), new double[]{cpuCapacity, memCapacity}, isCloneable, false);
        return pm1;
    }

    /**
     * @param economy - Economy where you want to create a storage.
     * @param cliques for the storage.
     * @param storageCapacity
     * @param isCloneable - is the storage cloneable
     * @return Trader i.e. storage
     */
    public static Trader createStorage(Economy economy, List<Long> cliques, double storageCapacity, boolean isCloneable) {
        Trader st1 = economy.addTrader(ST_TYPE, TraderState.ACTIVE, new Basket(ST_AMT),
                        new HashSet<>(cliques));
        st1.getCommoditiesSold().get(st1.getBasketSold().indexOf(ST_AMT)).setCapacity(300);
        st1.getSettings().setCloneable(isCloneable);
        return st1;
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
     * @param economy - Eceonomy where you want to add a VDC.
     * @return a VDC trader which is a guaranteed buyer.
     */
    public static Trader createVDC(Economy economy) {
        Trader vdc1 = economy.addTrader(VDC_TYPE, TraderState.ACTIVE, new Basket());
        vdc1.getSettings().setGuaranteedBuyer(true);
        return vdc1;
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
            for(int i=0; i<basketCommodities.size(); i++){
                double sellerQuantity = seller.getCommoditiesSold().get(seller.getBasketSold()
                                .indexOf(basketCommodities.get(i))).getQuantity();
                seller.getCommoditiesSold().get(seller.getBasketSold()
                                .indexOf(basketCommodities.get(i))).setQuantity(sellerQuantity + commQuantities[i]);
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
}
