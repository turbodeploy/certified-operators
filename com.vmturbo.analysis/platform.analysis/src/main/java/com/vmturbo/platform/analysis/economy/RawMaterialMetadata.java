package com.vmturbo.platform.analysis.economy;

import java.io.Serializable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A class representing the RawMaterial.
 */
public class RawMaterialMetadata implements Serializable {

    static final Logger logger = LogManager.getLogger(RawMaterials.class);

    private int material_;
    private boolean hasCoMaterial_;
    private boolean isHardConstraint_;

    /**
     * Constructor for the RawMaterialMetadata.
     *
     * @param material is the rawMaterialType
     * @param hasCoMaterial is true if this rawMaterial is used in conjunction with another raw-material.
     * @param isHardConstraint is true if this is a hardConstraint.
     *
     */
    public RawMaterialMetadata(final int material,
            final boolean hasCoMaterial,
            final boolean isHardConstraint) {
        material_ = material;
        hasCoMaterial_ = hasCoMaterial;
        isHardConstraint_ = isHardConstraint;
    }

    /**
     * Commodity type of the rawMaterial.
     *
     * @return the type of rawMaterial.
     */
    public int getMaterial() {
        return material_;
    }

    /**
     * ContainerPods sell quota commodities from Controller and some co-commodities from Nodes.
     *
     * @return true if this rawMaterial is processed in conjunction with another raw-material.
     */
    public boolean hasCoMaterial() {
        return hasCoMaterial_;
    }

    /**
     * Quota commodities in a Namespace are soft-constraints.
     *
     * @return true if this rawMaterial is a hard-constraint.
     */
    public boolean isHardConstraint() {
        return isHardConstraint_;
    }

    /**
     * Marks this raw-material as a hardConstraint.
     *
     * @param hardConstraint is true if this rawMaterial is a hard-constraint.
     */
    public void setHardConstraint(boolean hardConstraint) {
        isHardConstraint_ = hardConstraint;
    }
}
