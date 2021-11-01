package com.vmturbo.platform.analysis.economy;

import com.vmturbo.commons.Pair;
import com.vmturbo.commons.analysis.RawMaterialsMap;
import com.vmturbo.commons.analysis.RawMaterialsMap.RawMaterial;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs;

import java.io.Serializable;
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

/**
 * A class representing the RawMaterial.
 */
public class RawMaterials implements Serializable {

    static final Logger logger = LogManager.getLogger(RawMaterials.class);

    private RawMaterialMetadata @NonNull [] materials_;
    private boolean isFakeRawMaterial;
    private final boolean requiresConsistentScalingFactor;

    /**
     * Constructor for the RawMaterials.
     *
     * @param materials list of rawMaterials
     */
    public RawMaterials(@NonNull List<CommunicationDTOs.EndDiscoveredTopology.RawMaterial> materials) {
        int size = materials.size();
        materials_ = new RawMaterialMetadata[size];
        requiresConsistentScalingFactor = false;
        int index = 0;
        for (CommunicationDTOs.EndDiscoveredTopology.RawMaterial material : materials) {
            materials_[index] = new RawMaterialMetadata(material.getCommodityType(),
                    material.getHasCoMaterial(), material.getIsHardConstraint());
            index++;
        }
    }

    /**
     * Constructor for the RawMaterials.
     *
     * @param materialInfo materialInfo
     */
    public RawMaterials(RawMaterialsMap.RawMaterialInfo materialInfo) {
        isFakeRawMaterial = materialInfo.isFakeRawMaterial();
        requiresConsistentScalingFactor = materialInfo.requiresConsistentScalingFactor();
        int size = materialInfo.getRawMaterials().size();
        materials_ = new RawMaterialMetadata[size];
        int index = 0;
        for (RawMaterial material : materialInfo.getRawMaterials()) {
            materials_[index] = new RawMaterialMetadata(material.getRawMaterial(),
                    material.hasCoMaterial(), material.isHardConstraint());
            index++;
        }
    }

    /**
     * Return true if raw material isn't fake.
     *
     * @return true if raw material isn't fake
     */
    public boolean isRawMaterialRequired() {
        return !isFakeRawMaterial;
    }

    /**
     * Return true if the commodity associated with these raw materials requires the
     * use of consistentScalingFactor during resize.
     *
     * @return true if the commodity associated with these raw materials requires the
     * use of consistentScalingFactor during resize.
     */
    public boolean requiresConsistentScalingFactor() {
        return requiresConsistentScalingFactor;
    }

    /**
     * Returns the baseType vector of the rawMaterial commodities.
     *
     * <p>This array contains one baseType entry for each commodity.</p>
     * @return array of commodity base-types.
     */
    @Pure
    public @PolyRead RawMaterialMetadata @NonNull [] getMaterials(@PolyRead RawMaterials this) {
        return materials_;
    }

    /**
     * Find the commodity sold and supplier by the Seller which is raw material for the given commodity.
     *
     * @param economy The Economy.
     * @param buyer The Buyer of the commodity in the Economy.
     * @param commoditySoldIndex The index of commodity for which we need to find the raw materials.
     * @return A mapping from rawMaterialMetadata to the Commodity and the associated trader.
     */
    public static Map<RawMaterialMetadata, Pair<CommoditySold, Trader>>
                            findSellerCommodityAndSupplier(@NonNull Economy economy,
                                                           @NonNull Trader buyer, int commoditySoldIndex) {
        RawMaterials rawMaterials = economy.getAllRawMaterials(buyer.getBasketSold()
                .get(commoditySoldIndex).getBaseType());
        Map<RawMaterialMetadata, Pair<CommoditySold, Trader>> rawMaterialToSupplier = new HashMap<>();
        // return if there are no rawMaterials
        if (rawMaterials == null) {
            return rawMaterialToSupplier;
        }
        for (ShoppingList shoppingList : economy.getMarketsAsBuyer(buyer).keySet()) {

            Trader supplier = shoppingList.getSupplier();
            if (supplier == null) {
                logger.debug("The supplier is null for buyer " + buyer.getOid());
                continue;
            }
            Basket basketBought = shoppingList.getBasket();
            for (RawMaterialMetadata rawMaterial : rawMaterials.getMaterials()) {
                int rawMaterialType = rawMaterial.getMaterial();
                int boughtIndex = basketBought.indexOfBaseType(rawMaterialType);
                if (boughtIndex < 0) {
                    continue;
                }
                CommoditySold commSoldBySeller = supplier.getCommoditySold(basketBought
                        .get(boughtIndex));
                if (commSoldBySeller == null) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("The commodity {} is not sold by supplier {} to {}",
                                     () -> basketBought.get(boughtIndex)
                                                     .getDebugInfoNeverUseInCode(),
                                     () -> supplier,
                                     () -> buyer);
                    }
                    continue;
                }
                // if the commSold is resold, traverse down the supplychain to identify the actual provider.
                if (commSoldBySeller.getSettings().isResold()) {
                    Pair<CommoditySold, Trader> resource = findResoldRawMaterialOnSeller(economy, supplier,
                            basketBought.get(boughtIndex));
                    if (resource.first != null) {
                        rawMaterialToSupplier.put(rawMaterial, new Pair(resource.first, resource.second));
                    }
                } else {
                    rawMaterialToSupplier.put(rawMaterial, new Pair(commSoldBySeller, supplier));
                }
                if (!rawMaterial.hasCoMaterial()) {
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
