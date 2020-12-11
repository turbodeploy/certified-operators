package com.vmturbo.platform.analysis.economy;

import com.vmturbo.commons.Pair;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;
import org.javatuples.Triplet;

/**
 * A class representing the RawMaterial.
 */
public class RawMaterials implements Serializable {

    static final Logger logger = LogManager.getLogger(RawMaterials.class);

    private int @NonNull [] materials_;
    private boolean @NonNull [] hasCoMaterial_;

    /**
     * Constructor for the RawMaterials.
     *
     * @param materials list of rawMaterials
     */
    public RawMaterials(@NonNull List<CommunicationDTOs.EndDiscoveredTopology.RawMaterial> materials) {
        int size = materials.size();
        materials_ = new int[size];
        hasCoMaterial_  = new boolean[size];
        int index = 0;
        for (CommunicationDTOs.EndDiscoveredTopology.RawMaterial material : materials) {
            materials_[index] = material.getCommodityType();
            hasCoMaterial_[index] = material.getHasCoMaterial();
            index++;
        }
    }

    /**
     * Constructor for the RawMaterials.
     *
     * @param materials list of rawMaterials
     */
    public RawMaterials(@NonNull Collection<Triplet<Integer, Boolean, Boolean>> materials) {
        int size = materials.size();
        materials_ = new int[size];
        hasCoMaterial_  = new boolean[size];
        int index = 0;
        for (Triplet<Integer, Boolean, Boolean> material : materials) {
            materials_[index] = material.getValue0();
            hasCoMaterial_[index] = material.getValue1();
            index++;
        }
    }

    /**
     * Returns the baseType vector of the rawMaterial commodities.
     *
     * <p>This array contains one baseType entry for each commodity.</p>
     * @return array of commodity base-types.
     */
    @Pure
    public @PolyRead int @NonNull [] getMaterials(@PolyRead RawMaterials this) {
        return materials_;
    }

    /**
     * Returns the vector specifying if rawMaterial commodities have coMaterials.
     *
     * <p>This array contains a boolean that specifies if the commodity hat the index has a co-rawMaterial.</p>
     * @return array of booleans corresponding to commodity base-types.
     */
    @Pure
    public @PolyRead boolean @NonNull [] getCoMaterials(@PolyRead RawMaterials this) {
        return hasCoMaterial_;
    }

    /**
     * Find the commodity sold and supplier by the Seller which is raw material for the given commodity.
     *
     * @param economy The Economy.
     * @param buyer The Buyer of the commodity in the Economy.
     * @param commoditySoldIndex The index of commodity for which we need to find the raw materials.
     * @return List of Pairs containing the commodity sold and its supplier
     */
    public static Map<CommoditySold, Trader>
                            findSellerCommodityAndSupplier(@NonNull Economy economy,
                                                           @NonNull Trader buyer, int commoditySoldIndex) {
        RawMaterials typeOfCommsBought = economy.getAllRawMaterials(buyer.getBasketSold()
                .get(commoditySoldIndex).getBaseType());
        Map<CommoditySold, Trader> rawMaterialToSupplier = new HashMap<>();
        for (ShoppingList shoppingList : economy.getMarketsAsBuyer(buyer).keySet()) {

            Trader supplier = shoppingList.getSupplier();
            Basket basketBought = shoppingList.getBasket();
            for (int index = 0; index < typeOfCommsBought.getMaterials().length; index++) {
                int boughtIndex = basketBought.indexOfBaseType(typeOfCommsBought.getMaterials()[index]);
                if (boughtIndex < 0) {
                    continue;
                }
                CommoditySold commSoldBySeller = supplier.getCommoditySold(basketBought
                        .get(boughtIndex));
                // if the commSold is resold, traverse down the supplychain to identify the actual provider.
                if (commSoldBySeller.getSettings().isResold()) {
                    Pair<CommoditySold, Trader> resource = findResoldRawMaterialOnSeller(economy, supplier,
                            basketBought.get(boughtIndex));
                    if (resource.first != null) {
                        rawMaterialToSupplier.put(resource.first, resource.second);
                    }
                } else {
                    rawMaterialToSupplier.put(commSoldBySeller, supplier);
                }
                if (!typeOfCommsBought.getCoMaterials()[index]) {
                    break;
                }
            }
        }
        return rawMaterialToSupplier;
    }

    /**
     * Find the commodity sold and supplier which sells the rawMaterial for the given commodity.
     *
     * @param economy The Economy.
     * @param seller The seller of the commodity in the Economy.
     * @param cs The specification of the commodity for which we need to find the raw materials.
     * @return Pairs containing the commodity sold and its supplier
     */
    public static Pair<CommoditySold, Trader> findResoldRawMaterialOnSeller(@NonNull Economy economy,
                                                                               @NonNull Trader seller,
                                                                               CommoditySpecification cs) {
        int index = seller.getBasketSold().indexOfBaseType(cs.getBaseType());
        CommoditySold commSold = null;
        if (index != -1) {
            // we look for the baseType of the commodity. In the container model, we will have WorkloadControllers
            // selling quota commodities with a key. However, the pod sells commodity with no key.
            commSold = seller.getCommoditiesSold().get(index);
            if (commSold.getSettings().isResold()) {
                List<Trader> suppliers = economy.getMarketsAsBuyer(seller).keySet().stream()
                        .filter(sl -> sl.getBasket().indexOfBaseType(cs.getBaseType()) != -1)
                        .map(ShoppingList::getSupplier)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                if (suppliers.size() > 1) {
                    logger.error("RawMaterial found on multiple sellers for {}", cs.getDebugInfoNeverUseInCode());
                } else if (suppliers.size() == 1) {
                    return findResoldRawMaterialOnSeller(economy, suppliers.get(0), cs);
                }
            }
        }
        return new Pair<>(commSold, seller);
    }
}
