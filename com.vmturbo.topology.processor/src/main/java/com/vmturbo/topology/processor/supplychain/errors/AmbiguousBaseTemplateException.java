package com.vmturbo.topology.processor.supplychain.errors;

import javax.annotation.Nonnull;

import com.vmturbo.stitching.TopologyEntity;

/**
 * An entity has no base supply chain templates or more than one supply chain templates with
 * maximum priority.
 */
public class AmbiguousBaseTemplateException extends EntitySpecificSupplyChainException {
    /**
     * An entity does not have exactly one max-priority base template.
     *
     * @param entity the entity.
     * @param numOfBaseTemplates actual number of max-priority templates.
     */
    public AmbiguousBaseTemplateException(@Nonnull TopologyEntity entity, int numOfBaseTemplates) {
        super(
            entity,
            "The entity has " + numOfBaseTemplates +
                " max priority supply chain validation templates.  One is expected.");
    }
}
