package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository;

import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_SPEC;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceSpecRecord;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CommonCost;

/**
 * Spring Repository implementation used to interact with the reserved_instance_spec cost database table.
 */
@Repository
public class PlanReservedInstanceSpecStoreImpl implements PlanReservedInstanceSpecStore {
    /**
     * JOOQ DSL Context.
     */
    private DSLContext context;

    /**
     * Creates a new PlanReservedInstanceSpecStoreImpl, passing it the Jooq DSLContext to use for its database queries.
     *
     * @param context The Jooq DSLContext to use for database queries
     */
    public PlanReservedInstanceSpecStoreImpl(DSLContext context) {
        this.context = context;
    }

    /**
     * Queries the reserved_instance_spec table for the reserved instance spec that meets the specified criteria.
     * TODO: see if we can make one call instead of multiple
     *
     * @param regionId      The OID of the region for which to retrieve the reserved instance spec
     * @param tierId        The OID of the compute tier for which to retrieve the reserved instance spec
     * @param offeringClass The reserved instance offering class for which to retrieve the reserved instance spec
     * @param paymentOption The reserved instance payment option for which to retrieve the reserved instance spec
     * @param term          The reserved instance term (years) for which to retrieve the reserved instance spec
     * @param tenancy       The reserved instance tenancy for which to retrieve the reserved instance spec
     * @param osType        The OS type of the reserved instance for which to retrieve the reserved instance spec
     * @return A list of all reserved instance specs that match this criteria
     */
    @Override
    public List<Cost.ReservedInstanceSpec> getReservedInstanceSpecs(@Nonnull Long regionId, @Nonnull Long tierId, CloudCostDTO.ReservedInstanceType.OfferingClass offeringClass, CommonCost.PaymentOption paymentOption, int term, CloudCostDTO.Tenancy tenancy, CloudCostDTO.OSType osType) {
        return context.selectFrom(RESERVED_INSTANCE_SPEC)
                .where(RESERVED_INSTANCE_SPEC.REGION_ID.eq(regionId)
                    .and(RESERVED_INSTANCE_SPEC.TIER_ID.eq(tierId)))
                    .and(RESERVED_INSTANCE_SPEC.OFFERING_CLASS.eq(offeringClass.getNumber())
                    .and(RESERVED_INSTANCE_SPEC.PAYMENT_OPTION.eq(paymentOption.getNumber())
                    .and(RESERVED_INSTANCE_SPEC.TERM_YEARS.eq(term))
                    .and(RESERVED_INSTANCE_SPEC.TENANCY.eq(tenancy.getNumber()))
                    .and(RESERVED_INSTANCE_SPEC.OS_TYPE.eq(osType.getNumber()))))
                .fetch().stream()
                .map(this::reservedInstancesToProto)
                .collect(Collectors.toList());
    }

    /**
     * Queries the reserved_instance_spec table for the reserved instance specs for the specified region, tier, and term.
     * This method has been added to support Azure, which only defines reserved instances for a template in a region for
     * the specified term.
     *
     * @param regionId The OID of the region for which to retrieve the reserved instance spec
     * @param tierId   The OID of the compute tier for which to retrieve the reserved instance spec
     * @param term     The reserved instance term (years) for which to retrieve the reserved instance spec
     * @return A list of all reserved instance specs that match this criteria
     */
    @Override
    public List<ReservedInstanceSpec> getReservedInstanceSpecs(@Nonnull Long regionId, @Nonnull Long tierId, int term) {
        return context.selectFrom(RESERVED_INSTANCE_SPEC)
                .where(RESERVED_INSTANCE_SPEC.REGION_ID.eq(regionId)
                    .and(RESERVED_INSTANCE_SPEC.TIER_ID.eq(tierId)))
                    .and(RESERVED_INSTANCE_SPEC.TERM_YEARS.eq(term))
                .fetch().stream()
                .map(this::reservedInstancesToProto)
                .collect(Collectors.toList());
    }

    /**
     * Converts a reserved instance Jooq database record into a ReservedInstanceSpec object.
     *
     * @param reservedInstanceSpecRecord The reserved instance Jooq database record
     * @return A ReservedInstanceSpec object
     */
    private Cost.ReservedInstanceSpec reservedInstancesToProto(@Nonnull final ReservedInstanceSpecRecord reservedInstanceSpecRecord) {
        return Cost.ReservedInstanceSpec.newBuilder()
                .setId(reservedInstanceSpecRecord.getId())
                .setReservedInstanceSpecInfo(reservedInstanceSpecRecord.getReservedInstanceSpecInfo())
                .build();
    }
}
