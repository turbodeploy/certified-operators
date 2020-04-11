package com.vmturbo.api.component.external.api.util.stats.query.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.topology.EnvironmentTypeUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;

/**
 * Sub-query responsible for getting storage stats with virtual volume specific grouping.
 *
 * <p>For now, it supports:
 * 1. {
 *       "name": "numVolumes",
 *       "relatedEntityType": "VirtualVolume",
 *       "groupBy": ["StorageTier"]
 *     }
 * 2. {
 *       "name": "numVolumes",
 *       "relatedEntityType": "VirtualVolume",
 *       "groupBy": ["attachment"]
 *    }
 *    </p>
 */
public class StorageStatsSubQuery implements StatsSubQuery {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Supported stat.
     */
    public static final String NUM_VOL = "numVolumes";

    /**
     * Allowed filters for environment type for global scope.
     */
    private static final Set<EnvironmentTypeEnum.EnvironmentType> ENVIRONMENT_TYPE_FILTER_ALLOWED =
        Sets.newHashSet(EnvironmentType.CLOUD, EnvironmentType.ON_PREM);

    private final RepositoryApi repositoryApi;

    private final UserSessionContext userSessionContext;

    /**
     * Constructor.
     *
     * @param repositoryApi      the {@link RepositoryApi}
     * @param userSessionContext the {@link UserSessionContext}
     */
    public StorageStatsSubQuery(@Nonnull final RepositoryApi repositoryApi,
                                @Nonnull final UserSessionContext userSessionContext) {
        this.repositoryApi = repositoryApi;
        this.userSessionContext = userSessionContext;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        return !context.getInputScope().isPlan();
    }

    @Nonnull
    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        // Add any supported stats type here.
        Set<StatApiInputDTO> supportedStatsType = context.findStats(Collections.singleton(NUM_VOL));

        // Only support attachment OR storage tier for now
        Predicate<StatApiInputDTO> supportedGroupBy = dto -> dto.getGroupBy() != null && dto.getGroupBy().size() == 1 &&
            (dto.getGroupBy().contains(StringConstants.ATTACHMENT) || dto.getGroupBy().contains(ApiEntityType.STORAGE_TIER.apiStr()));

        Predicate<StatApiInputDTO> onPremOnly = dto -> dto.getFilters() != null && dto.getFilters().stream()
                .anyMatch(statFilterApiDto -> statFilterApiDto.getType().equals(StringConstants.ENVIRONMENT_TYPE) &&
                        statFilterApiDto.getValue().equals(EnvironmentType.ON_PREM.name()));

        Predicate<StatApiInputDTO> noGroupBy = dto ->
            dto.getGroupBy() == null || dto.getGroupBy().isEmpty();

        Set<StatApiInputDTO> supportedStats = supportedStatsType.stream()
            .filter(supportedGroupBy.or(noGroupBy).and(onPremOnly))
            .collect(Collectors.toSet());

        logger.debug("Number of supportedStats from request={}", supportedStats.size());

        return SubQuerySupportedStats.some(supportedStats);
    }

    @Nonnull
    @Override
    public List<StatSnapshotApiDTO> getAggregateStats(@Nonnull final Set<StatApiInputDTO> requestedStats,
                                                      @Nonnull final StatsQueryContext context) throws OperationFailedException {

        if (!context.isGlobalScope() && context.getQueryScope().getExpandedOids().isEmpty()) {
            return Collections.emptyList();
        }

        List<StatApiDTO> results = new ArrayList<>();
        for (StatApiInputDTO requestedStat : requestedStats) {
            // search for attachment string

            // When requested for groupBy Virtual Volume attachment status
            if (requestedStat.getName().equals(NUM_VOL) &&
                requestedStat.getGroupBy() != null &&
                requestedStat.getGroupBy().contains(StringConstants.ATTACHMENT) &&
                requestedStat.getRelatedEntityType().equals(ApiEntityType.VIRTUAL_VOLUME.apiStr())) {

                final SearchParameters searchVvInScope = getSearchScopeBuilder(context, requestedStat, userSessionContext)
                    .addSearchFilter(
                        SearchFilter.newBuilder()
                            .setPropertyFilter(SearchProtoUtil.entityTypeFilter(ApiEntityType.VIRTUAL_VOLUME))
                            .build())
                    .build();

                RepositoryApi.SearchRequest vvsInScope = repositoryApi.newSearchRequest(searchVvInScope);
                final Set<Long> vvOidsAttachedToVM = vvsInScope.getFullEntities()
                    .filter(topologyEntityDTO -> {
                        if (!topologyEntityDTO.hasTypeSpecificInfo()) {
                            return false;
                        }
                        TypeSpecificInfo typeSpecificInfo = topologyEntityDTO.getTypeSpecificInfo();
                        if (!typeSpecificInfo.hasVirtualVolume()) {
                            return false;
                        }
                        TypeSpecificInfo.VirtualVolumeInfo virtualVolumeInfo = typeSpecificInfo.getVirtualVolume();
                        return virtualVolumeInfo.hasAttachmentState() && virtualVolumeInfo.getAttachmentState().equals(
                            VirtualVolumeData.AttachmentState.ATTACHED);
                    })
                    .map(topologyEntityDTO -> topologyEntityDTO.getOid())
                    .collect(Collectors.toSet());

                Long numOfAttachedVvInScope;
                if (context.isGlobalScope()) {
                    numOfAttachedVvInScope =  Long.valueOf(vvOidsAttachedToVM.size());
                } else {
                    // This step is required because vvAttachedToVM can include VVs outside the current scope-
                    // we get VMs connected from VVs in scope, then VVs connected to those VMs- this has the potential to be
                    // a superset of the VVs we started with (VMs can be connected to more than one VV simultaneously)
                    numOfAttachedVvInScope = Long.valueOf(Sets.intersection(context.getQueryScope().getExpandedOids(), vvOidsAttachedToVM).size());
                }

                Map<String, Long> vvAttachmentCountMap = ImmutableMap.<String, Long>builder()
                    .put(StringConstants.ATTACHED, numOfAttachedVvInScope)
                    .put(StringConstants.UNATTACHED, vvsInScope.count() - numOfAttachedVvInScope)
                    .build();

                List<StatApiDTO> stats = toCountStatApiDtos(requestedStat, StringConstants.ATTACHMENT, vvAttachmentCountMap);

                results.addAll(stats);

                // when request for stat group by storage tier of Virtual volume
            } else if (requestedStat.getName().equals(NUM_VOL) &&
                requestedStat.getGroupBy() != null &&
                requestedStat.getGroupBy().contains(ApiEntityType.STORAGE_TIER.apiStr()) &&
                requestedStat.getRelatedEntityType().equals(ApiEntityType.VIRTUAL_VOLUME.apiStr())) {
                // Get all the storage tier in the scope
                // Storage tier is for Cloud only

                final SearchParameters vvInScopeSearchParams = getSearchScopeBuilder(context, requestedStat, userSessionContext)
                    .addSearchFilter(SearchFilter.newBuilder()
                        .setPropertyFilter(
                            SearchProtoUtil.environmentTypeFilter(EnvironmentType.CLOUD))
                        .build())
                    .build();

                List<ApiPartialEntity> vvInScope = repositoryApi.newSearchRequest(vvInScopeSearchParams)
                    .getEntities()
                    .collect(Collectors.toList());

                // get list of storageOid in order to get the display name for each storage tier
                final HashMap<Long, List<PartialEntity.ApiPartialEntity>> vvPartialEntityGroupedByStorageTierOid = new HashMap<>();
                vvInScope.stream().forEach(vv -> {
                    Optional<RelatedEntity> relatedEntityOpt = vv.getConnectedToList().stream()
                        .filter(relatedEntity -> relatedEntity.getEntityType() == EntityType.STORAGE_TIER.getNumber())
                        .findFirst();

                    if (relatedEntityOpt.isPresent()) {
                        vvPartialEntityGroupedByStorageTierOid
                            .computeIfAbsent(relatedEntityOpt.get().getOid(), k -> new ArrayList<>())
                            .add(vv);
                    } else {
                        logger.error("Virtual Volume {} with uuid {} has NO storage tier connected to", vv.getDisplayName(), vv.getOid());
                    }
                });

                final Function<MinimalEntity, String> getStorageTierDisplayName = storageTierEntity -> {
                    if (Strings.isNullOrEmpty(storageTierEntity.getDisplayName())) {
                        logger.warn("No displayName for storage tier entity {}.  Use oid as displayName instead.", storageTierEntity.getOid());
                        return storageTierEntity.getOid() + "";
                    } else {
                        return storageTierEntity.getDisplayName();
                    }
                };

                final Map<String, List<PartialEntity.ApiPartialEntity>> storageTierDisplayNameToDisplayNameMap = repositoryApi
                    .newSearchRequest(
                        SearchProtoUtil
                            .makeSearchParameters(SearchProtoUtil.idFilter(vvPartialEntityGroupedByStorageTierOid.keySet()))
                            .build()
                    )
                    .getMinimalEntities()
                    .collect(Collectors.toMap(getStorageTierDisplayName,
                                              storageEntity -> vvPartialEntityGroupedByStorageTierOid.get(storageEntity.getOid())));

                Map<String, Long> storageTierCountMap = storageTierDisplayNameToDisplayNameMap.entrySet().stream()
                    .collect(Collectors.toMap(
                        e -> e.getKey(),
                        e -> new Long(e.getValue().size())
                    ));

                List<StatApiDTO> stats = toCountStatApiDtos(requestedStat, ApiEntityType.STORAGE_TIER.apiStr(), storageTierCountMap);

                results.addAll(stats);

            } else {
                final SearchParameters searchParameters =
                    getSearchScopeBuilder(context, requestedStat, userSessionContext).build();
                final long total = repositoryApi.newSearchRequest(searchParameters).count();
                results.add(makeCountStatDto(requestedStat, total));
            }
        }

        // Build a snapshot to represent the results
        StatSnapshotApiDTO currentStorageStatsSnapshot = new StatSnapshotApiDTO();
        currentStorageStatsSnapshot.setDate(DateTimeUtil.toString(context.getCurTime()));
        currentStorageStatsSnapshot.setEpoch(Epoch.CURRENT);
        currentStorageStatsSnapshot.setStatistics(results);
        return Collections.singletonList(currentStorageStatsSnapshot);
    }

    /**
     * Create SearchParameters Builder based on search request and context.
     *
     * @param context {@link StatsQueryContext}
     * @param requestStat {@link StatApiInputDTO}
     * @param userSessionContext {@link UserSessionContext}
     * @return {@link SearchParameters.Builder}
     */
    @Nonnull
    private static SearchParameters.Builder getSearchScopeBuilder(@Nonnull final StatsQueryContext context,
                                                                  @Nonnull final StatApiInputDTO requestStat,
                                                                  @Nonnull UserSessionContext userSessionContext) {
        if (userSessionContext.isUserScoped() && userSessionContext.isUserObserver()) {
            return SearchProtoUtil
                    .makeSearchParameters(SearchProtoUtil.idFilter(context.getQueryScope().getScopeOids()))
                    .addSearchFilter(
                            SearchFilter.newBuilder()
                                    .setPropertyFilter(SearchProtoUtil.entityTypeFilter(ApiEntityType.VIRTUAL_VOLUME))
                                    .build());
        } else if (context.isGlobalScope()) {
            SearchParameters.Builder builder = SearchProtoUtil.makeSearchParameters(SearchProtoUtil.entityTypeFilter(
                context.getQueryScope().getGlobalScope().get().entityTypes().stream()
                    .map(et -> et.apiStr())
                    .collect(Collectors.toList())));

            // Apply environment type filter explicitly if exists
            Optional<StatFilterApiDTO> environmentTypeFilterDto = requestStat.getFilters() == null || requestStat.getFilters().isEmpty() ? Optional.empty() :
                requestStat.getFilters().stream().filter(filter -> filter.getType().equals(StringConstants.ENVIRONMENT_TYPE)).findFirst();

            // explicitly add search filter for CLOUD and ONPREM only
            environmentTypeFilterDto.flatMap(envFilter -> EnvironmentTypeUtil.fromApiString(envFilter.getValue()))
                .ifPresent(targetEnvType -> {
                    if (ENVIRONMENT_TYPE_FILTER_ALLOWED.contains(targetEnvType)) {
                        builder.addSearchFilter(
                            SearchFilter.newBuilder()
                                .setPropertyFilter(SearchProtoUtil.environmentTypeFilter(targetEnvType))
                                .build());
                    }
                });

            return builder;
        } else {
            return SearchProtoUtil
                .makeSearchParameters(SearchProtoUtil.idFilter(context.getQueryScope().getExpandedOids()));
        }
    }

    /**
     * Helper method to create SearchFilter with a SearchTraversalFilter.
     *
     * @param direction {@link TraversalDirection}
     * @param apiEntityType {@link ApiEntityType}
     * @return {@link SearchFilter.Builder}
     */
    @Nonnull
    private static SearchFilter.Builder createSearchTraversalFilter(@Nonnull final TraversalDirection direction,
                                                                    @Nonnull final ApiEntityType apiEntityType) {
        return SearchFilter.newBuilder()
            .setTraversalFilter(SearchProtoUtil.traverseToType(direction, apiEntityType.apiStr()));
    }

    /**
     * Convert count stat to StatApiDTOs based on grouping.
     *
     * @param statApiInputDTO {@link StatApiInputDTO}
     * @param groupBy group by type
     * @param countsByGroup {@link Map} Map for Group to Count
     * @return list of StatApiDTO with the grouping and stat information
     */
    @Nonnull
    private static List<StatApiDTO> toCountStatApiDtos(@Nonnull StatApiInputDTO statApiInputDTO,
                                                       @Nonnull String groupBy,
                                                       @Nonnull Map<String, Long> countsByGroup) {

         return countsByGroup.entrySet().stream().map(entry -> {
            final StatApiDTO statApiDTO = makeCountStatDto(statApiInputDTO, entry.getValue());

            final List<StatFilterApiDTO> filters = new ArrayList<>();
            final StatFilterApiDTO resultsTypeFilter = new StatFilterApiDTO();
            resultsTypeFilter.setType(groupBy);
            resultsTypeFilter.setValue(entry.getKey());
            filters.add(resultsTypeFilter);

            statApiDTO.setFilters(filters);

            return statApiDTO;
        }).collect(Collectors.toList());
    }

    private static StatApiDTO makeCountStatDto(@Nonnull final StatApiInputDTO statApiInputDTO, final long count) {
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName(statApiInputDTO.getName());

        final StatValueApiDTO converted = new StatValueApiDTO();
        float countInFloat = (float)count;
        converted.setAvg(countInFloat);
        converted.setMax(countInFloat);
        converted.setMin(countInFloat);
        converted.setTotal(countInFloat);

        statApiDTO.setValues(converted);
        statApiDTO.setValue(countInFloat);
        if (statApiInputDTO.getRelatedEntityType() != null) {
            statApiDTO.setRelatedEntityType(statApiInputDTO.getRelatedEntityType());
        }
        return statApiDTO;
    }
}
