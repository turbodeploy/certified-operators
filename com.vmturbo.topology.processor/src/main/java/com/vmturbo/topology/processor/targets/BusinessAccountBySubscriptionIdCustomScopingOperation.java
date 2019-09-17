package com.vmturbo.topology.processor.targets;

import java.util.Collections;
import java.util.Set;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.ObjectFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchEntityOidsRequest;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Custom scoping operation for Azure probe.  It resolves an entity scope that is defined by a
 * subscription ID by returning the OID of the BusinessAccount that has that subscription ID.
 */
public class BusinessAccountBySubscriptionIdCustomScopingOperation
    implements CustomScopingOperation {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public Set<Long> convertScopeValueToOid(final String subscriptionId,
                                            final SearchServiceBlockingStub searchService) {
        logger.debug("Executing convertScopeValueToOid for Subscription ID {}", subscriptionId);
        final SearchEntityOidsRequest.Builder searchTopologyRequest = SearchEntityOidsRequest.newBuilder()
            .addSearchParameters(SearchParameters.newBuilder()
                .setStartingFilter(SearchProtoUtil.entityTypeFilter(EntityType.BUSINESS_ACCOUNT_VALUE))
                .addSearchFilter(SearchFilter.newBuilder()
                    .setPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.BUSINESS_ACCOUNT_INFO_REPO_DTO_PROPERTY_NAME)
                        .setObjectFilter(ObjectFilter.newBuilder()
                            .addFilters(PropertyFilter.newBuilder()
                                .setPropertyName(SearchableProperties.BUSINESS_ACCOUNT_INFO_ACCOUNT_ID)
                                .setStringFilter(StringFilter.newBuilder()
                                    .addOptions(subscriptionId))))))
            .build());
        try {
            return Sets.newHashSet(searchService.searchEntityOids(
                searchTopologyRequest.build()).getEntitiesList());
        } catch (StatusRuntimeException e) {
            logger.error("Unable to fetch BusinessAccount OIDs for subscription {} from repository",
                subscriptionId, e);
            return Collections.emptySet();
        }
    }
}
