package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.TableField;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter.AccountFilterType;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ReservedInstanceCoverageLatestRecord;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A filter to restrict the {@link ReservedInstanceBought} from the {@link ReservedInstanceBoughtStore}.
 * It provider a easier way to define simple search over reserved instances records in the tables.
 */
public class ReservedInstanceBoughtFilter extends ReservedInstanceBoughtTableFilter {

    public static final ReservedInstanceBoughtFilter SELECT_ALL_FILTER = newBuilder().build();

    protected ReservedInstanceBoughtFilter(@Nonnull Builder builder) {
        super(builder);
     }


    /**
     * Create a builder used to construct a filter.
     *
     * @return The builder object.
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }


    /**
     * Generates filter conditions that would need access to other cost tables
     * for the RI Bought table data retrieval.
     *
     * @param context the DSL context for other tables' access.
     * @return Filter conditions for the RI Bought table data retrieval.
     */
    @Nonnull
    public Condition[] generateConditions(final DSLContext context) {
        final Condition[] conditions = super.generateConditions();
        final List<Condition> allConditions = new ArrayList<>();
        allConditions.addAll(Arrays.asList(conditions));
        if (accountFilter.getAccountIdCount() > 0) {
            AccountFilterType filterType = accountFilter.getAccountFilterType();
            switch (filterType) {
                case USED_AND_PURCHASED_BY:
                    Condition purchasedByCondition = Tables.RESERVED_INSTANCE_BOUGHT.BUSINESS_ACCOUNT_ID.in(
                            accountFilter.getAccountIdList());
                    Condition usedByCondition = getUsedByCondition(context, true,
                            Tables.RESERVED_INSTANCE_COVERAGE_LATEST.BUSINESS_ACCOUNT_ID,
                            accountFilter.getAccountIdList() );
                    allConditions.add(purchasedByCondition.or(usedByCondition));
                    break;
                case USED_BY:
                    allConditions.add(getUsedByCondition(context, true,
                            Tables.RESERVED_INSTANCE_COVERAGE_LATEST.BUSINESS_ACCOUNT_ID,
                            accountFilter.getAccountIdList()));
                    break;
            }
        }

        return allConditions.toArray(new Condition[allConditions.size()]);
    }

    /**
     * Returns a used by clause for RIs given a field.
     *
     * @param context The DSL context to make queries
     * @param includeUndiscovered if true, used clause will also include RI usage
     *                           from undiscovered accounts.
     * @param field the field we want to filter from the coverage table.
     * @param idList the ids for the field values.
     * @return Condition representing the used clause.
     */
    @Nonnull
    protected  Condition getUsedByCondition(final DSLContext context,
                                            final boolean includeUndiscovered,
                                            TableField<ReservedInstanceCoverageLatestRecord, Long> field,
                                            List<Long> idList) {

        Condition usedByCondition = Tables.RESERVED_INSTANCE_BOUGHT.ID.in(
                context.select(Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID)
                        .from(Tables.RESERVED_INSTANCE_COVERAGE_LATEST.join(
                                Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING)
                                .on(Tables.RESERVED_INSTANCE_COVERAGE_LATEST.ENTITY_ID.eq(
                                        Tables.ENTITY_TO_RESERVED_INSTANCE_MAPPING.ENTITY_ID)))
                        .where(field.in(idList)));
        // get the RIs used by undiscovered accounts.
        if (includeUndiscovered && field.getName().equals(
                Tables.RESERVED_INSTANCE_COVERAGE_LATEST.BUSINESS_ACCOUNT_ID.getName())) {
            // An example use case for this condition:
            // Fetch all discovered and undiscovered bought RIs used by a given set of accounts.
            Condition riUsedByUndiscoveredAcctCondition = Tables.RESERVED_INSTANCE_BOUGHT.ID.in(
                    context.select(Tables.ACCOUNT_TO_RESERVED_INSTANCE_MAPPING.RESERVED_INSTANCE_ID)
                            .from(Tables.ACCOUNT_TO_RESERVED_INSTANCE_MAPPING)
                            .where(Tables.ACCOUNT_TO_RESERVED_INSTANCE_MAPPING.BUSINESS_ACCOUNT_OID
                                    .in(accountFilter.getAccountIdList())));
            usedByCondition = usedByCondition.or(riUsedByUndiscoveredAcctCondition);
        }
        return usedByCondition;
    }

    /**
     * This will be used by the BoughtReservedInstanceStore to get a list of RIs used
     * for a given filter. For example, when scoped to a region, the region filter is
     * set and the Used condition returns the condition that would filter RIs used in
     * a given region.
     *
     * @param context the DSL context.
     * @return  Condition on the table.
     */
    @Nonnull
    public Optional<Condition> generateUsedByDiscoveredAccountsCondition(final DSLContext context) {

        /* Assuming the filters are mutually exclusive for simplicity.
         */
        if (accountFilter.getAccountIdCount() > 0) {
            return Optional.of(getUsedByCondition(context, false,
                    Tables.RESERVED_INSTANCE_COVERAGE_LATEST.BUSINESS_ACCOUNT_ID,
                    accountFilter.getAccountIdList()));
        } else if (regionFilter.getRegionIdCount() > 0) {
            return Optional.of(getUsedByCondition(context, false,
                        Tables.RESERVED_INSTANCE_COVERAGE_LATEST.REGION_ID,
                        regionFilter.getRegionIdList()));

        } else if (availabilityZoneFilter.getAvailabilityZoneIdCount() > 0) {
            return Optional.of(getUsedByCondition(context, false,
                        Tables.RESERVED_INSTANCE_COVERAGE_LATEST.AVAILABILITY_ZONE_ID,
                        availabilityZoneFilter.getAvailabilityZoneIdList()));
            }
        return Optional.empty();
    }


    /**
     * A builder class for {@link ReservedInstanceBoughtFilter}.
     */
    public static class Builder extends
            ReservedInstanceBoughtTableFilter.Builder<ReservedInstanceBoughtFilter, Builder> {

        /**
         * A utility method for converting plan scope tuples (in which the entities within scope
         * are queried by scope seed OIDs of the plan). Converts the entity types in
         * {@code cloudScopesTuples} to the corresponding filters supported by this aggregate filter.
         * Only region, account, and AZ are supported entity types. All other types will be ignored.
         *
         * @param cloudScopesTuples The entities in scope, indexed by entity type, in querying for RI
         *                          instances. An empty map represents a global filter (no filtering).
         * @param filterType The account filter type.
         * @return The {@link Builder} instance for method chaining
         */
        @Nonnull
        public Builder cloudScopeTuples(@Nonnull Map<EntityType,
                                        Set<Long>> cloudScopesTuples,
                                        AccountFilterType filterType) {
            cloudScopesTuples.forEach((entityType, entityOids) -> {
                switch (entityType) {
                    case REGION:
                        regionFilter(RegionFilter.newBuilder()
                                .addAllRegionId(entityOids)
                                .build());
                        break;
                    case BUSINESS_ACCOUNT:
                        accountFilter(AccountFilter.newBuilder()
                                .addAllAccountId(entityOids)
                                .setAccountFilterType(filterType)
                                .build());
                        break;
                    case AVAILABILITY_ZONE:
                        availabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                                .addAllAvailabilityZoneId(entityOids)
                                .build());
                        break;
                    default:
                        // not a supported entity type
                        break;
                }
            });

            return this;
        }

        /**
         * A utility method for converting plan scope tuples (in which the entities within scope
         * are queried by scope seed OIDs of the plan). Converts the entity types in
         * {@code cloudScopesTuples} to the corresponding filters supported by this aggregate filter.
         * Only region, account, and AZ are supported entity types. All other types will be ignored.
         *
         * @param cloudScopesTuples The entities in scope, indexed by entity type, in querying for RI
         *                          instances. An empty map represents a global filter (no filtering).
         * @return The {@link Builder} instance for method chaining
         */
        @Nonnull
        public Builder cloudScopeTuples(@Nonnull Map<EntityType, Set<Long>> cloudScopesTuples) {
            return cloudScopeTuples(cloudScopesTuples, AccountFilterType.PURCHASED_BY);
        }


        /**
         * Builds an instance of {@link ReservedInstanceBoughtFilter}.
         * @return The newly created instance of {@link ReservedInstanceBoughtFilter}.
         */
        @Override
        public ReservedInstanceBoughtFilter build() {
            return new ReservedInstanceBoughtFilter(this);
        }


    }
}
