/**
 *
 */
package com.vmturbo.platform.analysis.testUtilities;

import java.util.HashSet;
import java.util.List;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

/**
 * This class contains useful utility functions to be used ONLY in unit tests.
 * @author thiru_arun
 *
 */
public class TestUtils {

    private static final int VM_TYPE = 0;
    private static final int PM_TYPE = 1;
    private static final int ST_TYPE = 2;

    // CommoditySpecifications to use in tests
    private static final CommoditySpecification CPU = new CommoditySpecification(0);
    private static final CommoditySpecification MEM = new CommoditySpecification(1);
    private static final CommoditySpecification ST_AMT = new CommoditySpecification(2);

    /**
     * @param economy
     * @param cliques for the PM
     * @param cpuCapacity
     * @param isCloneable - is the PM cloneable
     * @return Trader i.e PM
     */
    public static Trader createPM(Economy economy, List<Long> cliques, double cpuCapacity, boolean isCloneable){
        Trader pm1 = economy.addTrader(PM_TYPE, TraderState.ACTIVE, new Basket(CPU, MEM),
                        new HashSet<>(cliques));
        pm1.getCommoditiesSold().get(pm1.getBasketSold().indexOf(CPU)).setCapacity(cpuCapacity);
        pm1.getSettings().setCloneable(isCloneable);
        return pm1;
    }

    /**
     * @param economy
     * @param cliques for the storage.
     * @param storageCapacity
     * @param isCloneable - is the storage cloneable
     * @return Trader i.e. storage
     */
    public static Trader createStorage(Economy economy, List<Long> cliques, double storageCapacity, boolean isCloneable){
        Trader st1 = economy.addTrader(ST_TYPE, TraderState.ACTIVE, new Basket(ST_AMT),
                        new HashSet<>(cliques));
        st1.getCommoditiesSold().get(st1.getBasketSold().indexOf(ST_AMT)).setCapacity(300);
        st1.getSettings().setCloneable(isCloneable);
        return st1;
    }

    /**
     * @param economy
     * @return Trader i.e. VM
     */
    public static Trader createVM(Economy economy){
        Trader vm1 = economy.addTrader(VM_TYPE, TraderState.ACTIVE, new Basket());
        return vm1;
    }

    /**
     * Creates a shopping list for a trader.
     * @param economy
     * @param slType - enum which can be CPU_MEM or STORAGE
     * @param vm - The trader for which you want to create a shopping list
     * @param commSpec - The commodity spec whose quantity needed by the trader (vm) you want to specify.
     * @param quantity - The quantity of commSpec requested by trader.
     * @param host - The trader to place this shopping list on. Pass in null if you do not want to place it anywhere.
     * @return Shopping list
     */
    public static ShoppingList createAndPlaceShoppingList(Economy economy, List<CommoditySpecification> basketCommodities, Trader vm,
                    CommoditySpecification commSpec, double quantity, Trader host){
        Basket basket = null;
            basket = new Basket(basketCommodities);
        ShoppingList sl = economy.addBasketBought(vm, basket);
        sl.setQuantity(sl.getBasket().indexOf(commSpec), quantity);
        sl.setMovable(true);
        if(host != null){
            sl.move(host);
            double hostQuantity = host.getCommoditiesSold().get(host.getBasketSold()
                            .indexOf(commSpec)).getQuantity();
            host.getCommoditiesSold().get(host.getBasketSold()
                            .indexOf(commSpec)).setQuantity(hostQuantity + quantity);
        }
        return sl;
    }
}
