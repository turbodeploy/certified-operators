package com.vmturbo.topology.processor.supplychain.errors;

/**
 * A probe returns more than one supply chain templates for one entity category.
 */
public class DuplicateTemplateFailure extends SupplyChainValidationFailure {
    /**
     * A probe returns more than one supply chain templates for one entity category.
     *
     * @param probeId id of the faulty probe.
     * @param entityCategory  entity category.
     */
    public DuplicateTemplateFailure(long probeId, int entityCategory) {
        super(
            Long.toString(probeId), null, null,
            "Probe returns more than one template for entity category " + entityCategory);
    }
}