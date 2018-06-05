package com.vmturbo.topology.processor.supplychain.errors;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.SupplyChain.TemplateCommodity;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Missing mandatory commodity sold.
 */
public class MandatoryCommodityNotFoundException extends EntitySpecificSupplyChainException {
    final private TemplateCommodity commodity;
    final private boolean hasKey;

    /**
     * A commodity that the entity must sell does not exist.
     *
     * @param entity the entity.
     * @param commodity missing commodity.
     * @param hasKey true iff the commodity must have a key.
     */
    public MandatoryCommodityNotFoundException(
          @Nonnull TopologyEntity entity,
          @Nonnull TemplateCommodity commodity,
          boolean hasKey) {
        // TODO: commodity must show as a string
        super(
            entity,
            "Mandatory commodity " + (hasKey ? " with key " : "") + commodity.getCommodityType().name() +
                " not found.");
        this.commodity = commodity;
        this.hasKey = hasKey;
    }

    @Nonnull
    public TemplateCommodity getCommodity() {
        return commodity;
    }

    public boolean hasKey() {
        return hasKey;
    }
}
