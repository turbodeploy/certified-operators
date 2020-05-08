package com.vmturbo.api.component.external.api.util;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.external.api.mapper.UuidMapper;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.common.protobuf.topology.ApiEntityType;

/**
 * Service Provider Expander class is responsible to expand service providers to the connecting
 * regions.
 */
public class ServiceProviderExpander {

    private final UuidMapper uuidMapper;

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    public ServiceProviderExpander(@Nonnull final UuidMapper uuidMapper,
                                   @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory) {
        this.uuidMapper = Objects.requireNonNull(uuidMapper);
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
    }

    /**
     * Expand service providers to regions.
     *
     * @param entityOidsToExpand the input set of ServiceEntity oids
     * @return the input set with oids of regions connected to the service providers
     */
    public Set<Long> expand(@Nonnull final Set<Long> entityOidsToExpand) {
        final Set<Long> expandedScopes = new HashSet<>();

        // split oids between oid of ServiceProvider vs other types
        final Set<Long> serviceProviderOids = new HashSet<>();
        for (Long oid : entityOidsToExpand) {
            ApiId apiId = uuidMapper.fromOid(oid);
            if (apiId.isEntity() && apiId.getClassName().equals(ApiEntityType.SERVICE_PROVIDER.apiStr())) {
                serviceProviderOids.add(apiId.oid());
            } else {
                expandedScopes.add(oid);
            }
        }

        // expand all serviceProviders to connecting regions
        if (!serviceProviderOids.isEmpty()) {
            final Set<Long> regionIds = supplyChainFetcherFactory.expandServiceProviders(serviceProviderOids);
            expandedScopes.addAll(regionIds);
        }

        return expandedScopes;
    }

}
