package com.vmturbo.api.component.external.api.util;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.repository.api.RepositoryListener;

/**
 * Service Provider Expander class is responsible to expand service providers to the connecting
 * regions.
 *
 * </p>The goal here is to determine if an input scope is a service provider and, if so,
 * to get the regions associated with it. We do this by fetching all service provider oids
 * and comparing them to the oids in the input scope. This is <5 entities.
 * We're also caching these oids for the duration of a single topology broadcast.
 * This is slightly overkill, but not too complicated so I thought it's worth it.
 */
public class ServiceProviderExpander implements RepositoryListener {

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;

    private final RepositoryApi repositoryApi;

    private final long realtimeContextId;

    private final AtomicReference<Set<Long>> allServiceProviderIds = new AtomicReference<>(null);

    /**
     * Create a new instance.
     *
     * @param repositoryApi {@link RepositoryApi} to access repository.
     * @param supplyChainFetcherFactory  To fetch supply chain.
     * @param realtimeContextId To distinguish realtime from plan notifications.
     */
    public ServiceProviderExpander(@Nonnull final RepositoryApi repositoryApi,
                                   @Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                                   final long realtimeContextId) {
        this.realtimeContextId = realtimeContextId;
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.supplyChainFetcherFactory = Objects.requireNonNull(supplyChainFetcherFactory);
    }


    @Override
    public void onSourceTopologyAvailable(final long topologyId, final long topologyContextId) {
        // clean cached service provider ids if received realtime topology
        if (topologyContextId == realtimeContextId) {
            allServiceProviderIds.set(null);
        }
    }

    @Nonnull
    private Set<Long> getAllServiceProviderIds() {
        Set<Long> retValue = allServiceProviderIds.get();
        if (retValue == null) {
            retValue = repositoryApi.newSearchRequest(SearchProtoUtil.makeSearchParameters(
                    SearchProtoUtil.entityTypeFilter(ApiEntityType.SERVICE_PROVIDER)).build())
                .getOids();
            // There is the possibility that an "onSourceTopologyAvailable" notification will
            // come in while we are searching, and we will set the ids to the value from the
            // previous topology. Realistically that shouldn't cause problems because the search is quick,
            // and the list of service providers is very stable.
            allServiceProviderIds.set(retValue);
        }
        return retValue;
    }

    /**
     * Expand service providers to regions.
     *
     * @param entityOidsToExpand the input set of ServiceEntity oids
     * @return the input set with oids of regions connected to the service providers
     */
    @Nonnull
    public Set<Long> expand(@Nonnull final Set<Long> entityOidsToExpand) {
        final Set<Long> expandedScopes = new HashSet<>();

        final Set<Long> allServiceProviderIds = getAllServiceProviderIds();

        // split oids between oid of ServiceProvider vs other types
        final Set<Long> serviceProviderOids = new HashSet<>();
        for (Long apiId : entityOidsToExpand) {
            if (allServiceProviderIds.contains(apiId)) {
                serviceProviderOids.add(apiId);
            } else {
                expandedScopes.add(apiId);
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
