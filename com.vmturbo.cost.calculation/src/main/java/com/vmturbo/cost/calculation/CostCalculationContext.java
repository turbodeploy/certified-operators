package com.vmturbo.cost.calculation;

import java.util.Optional;

import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.topology.AccountPricingData;

/**
 * This class represents data relevant to the context of cost calculation for an entity like region, business account,
 * priceTable.
 *
 * @param <ENTITY_CLASS> The class used to represent entities in the topology. For example,
 *                      {@link com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO} for
 *                      the realtime topology.
 */
public class CostCalculationContext<ENTITY_CLASS> {

    private final CostJournal.Builder<ENTITY_CLASS> costJournal;

    private final ENTITY_CLASS entity;

    private final long regionId;

    private final AccountPricingData accountPricingData;

    private final Optional<OnDemandPriceTable> onDemandPriceTable;

    private final Optional<SpotInstancePriceTable> spotInstancePriceTable;

    /**
     * The cost calculation context for a given entity and its pricing data.
     *
     * @param costJournal             The cost journal for recording costs.
     * @param entity                  The entity.
     * @param regionId                The region Id.
     * @param accountPricingData      The account specific pricing data.
     * @param onDemandPriceTable      The on Demand price table for a given account specific pricing data.
     * @param spotInstancePriceTable  The spot instance price table specific to the account pricing.
     */
    public CostCalculationContext(CostJournal.Builder<ENTITY_CLASS> costJournal, ENTITY_CLASS entity,
                                  Long regionId, AccountPricingData accountPricingData, Optional<OnDemandPriceTable> onDemandPriceTable,
                                  Optional<SpotInstancePriceTable> spotInstancePriceTable) {
        this.costJournal = costJournal;
        this.entity = entity;
        this.regionId = regionId;
        this.accountPricingData = accountPricingData;
        this.onDemandPriceTable = onDemandPriceTable;
        this.spotInstancePriceTable = spotInstancePriceTable;
    }

    /**
     * Get the onDemandPriceTable for a given business account and region.
     *
     * @return The On Demand Price Table.
     */
    public Optional<OnDemandPriceTable> getOnDemandPriceTable() {
        return onDemandPriceTable;
    }

    public CostJournal.Builder<ENTITY_CLASS> getCostJournal() {
        return this.costJournal;
    }

    public ENTITY_CLASS getEntity() {
        return this.entity;
    }

    public Long getRegionid() {
        return this.regionId;
    }

    public AccountPricingData getAccountPricingData() {
        return this.accountPricingData;
    }

    public Optional<SpotInstancePriceTable> getSpotInstancePriceTable() {
        return spotInstancePriceTable;
    }
}
