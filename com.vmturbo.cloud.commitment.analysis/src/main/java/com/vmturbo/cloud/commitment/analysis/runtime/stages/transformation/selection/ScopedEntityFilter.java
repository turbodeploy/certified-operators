package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;

/**
 * A filter on scoped entity instances.
 */
public class ScopedEntityFilter {

    private final Set<Long> serviceProviders;

    private final Set<Long> accounts;

    private final Set<Long> regions;

    private final Set<Long> entities;

    private ScopedEntityFilter(@Nonnull Collection<Long> serviceProviders,
                               @Nonnull Collection<Long> accounts,
                               @Nonnull Collection<Long> regions,
                               @Nonnull Collection<Long> entities) {

        this.serviceProviders = ImmutableSet.copyOf(Objects.requireNonNull(serviceProviders));
        this.accounts = ImmutableSet.copyOf(Objects.requireNonNull(accounts));
        this.regions = ImmutableSet.copyOf(Objects.requireNonNull(regions));
        this.entities = ImmutableSet.copyOf(Objects.requireNonNull(entities));
    }

    /**
     * Filters the entity aggregate, based on scope (account, region, service provider, entity).
     * @param entityAggregate The entity demand aggregate to filter.
     * @return True, if the {@code entityAggregate} passes the filter. False, otherwise.
     */
    public boolean filter(@Nonnull ClassifiedEntityDemandAggregate entityAggregate) {

        Preconditions.checkNotNull(entityAggregate);

        return checkAttribute(serviceProviders, entityAggregate.serviceProviderOid())
                && checkAttribute(accounts, entityAggregate.accountOid())
                && checkAttribute(regions, entityAggregate.regionOid())
                && checkAttribute(entities, entityAggregate.entityOid());
    }

    private <AttributeT> boolean checkAttribute(
            @Nonnull Collection<AttributeT> allowedValues,
            @Nullable AttributeT attributeValue) {

        return allowedValues.isEmpty()
                || (attributeValue != null && allowedValues.contains(attributeValue));
    }

    /**
     * A factory class for producing {@link ScopedEntityFilter} instances.
     */
    public static class ScopedEntityFilterFactory {

        /**
         * Creates a new {@link ScopedEntityFilter}, based on the provided demand scope config.
         * @param demandScope The demand scope config. Any empty attributes of the list are ignored
         *                    in filtering entities.
         * @return THe newly created {@link ScopedEntityFilter} instance.
         */
        public ScopedEntityFilter createFilter(@Nonnull DemandScope demandScope) {

            Preconditions.checkNotNull(demandScope);

            return new ScopedEntityFilter(
                    demandScope.getServiceProviderOidList(),
                    demandScope.getAccountOidList(),
                    demandScope.getRegionOidList(),
                    demandScope.getEntityOidList());
        }
    }
}
