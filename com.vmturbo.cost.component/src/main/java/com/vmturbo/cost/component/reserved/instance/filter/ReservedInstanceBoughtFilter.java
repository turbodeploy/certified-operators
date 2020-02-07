package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
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
     * A builder class for {@link ReservedInstanceBoughtFilter}
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
         * @return The {@link Builder} instance for method chaining
         */
        @Nonnull
        public Builder cloudScopeTuples(@Nonnull Map<EntityType, Set<Long>> cloudScopesTuples) {
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
         * Builds an instance of {@link ReservedInstanceBoughtFilter}.
         * @return The newly created instance of {@link ReservedInstanceBoughtFilter}.
         */
        @Override
        public ReservedInstanceBoughtFilter build() {
            return new ReservedInstanceBoughtFilter(this);
        }
    }
}
