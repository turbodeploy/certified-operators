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
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Sub-query responsible for getting storage stats with virtual volume specific grouping.
 * For now, it supports:
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
    private static final Set<EnvironmentTypeEnum.EnvironmentType> environmentTypeFilterAllowed =
        Sets.newHashSet(EnvironmentType.CLOUD, EnvironmentType.ON_PREM);

    private final RepositoryApi repositoryApi;

    public StorageStatsSubQuery(@Nonnull final RepositoryApi repositoryApi) {
        this.repositoryApi = repositoryApi;
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
            (dto.getGroupBy().contains(StringConstants.ATTACHMENT) || dto.getGroupBy().contains(UIEntityType.STORAGE_TIER.apiStr()));

        Predicate<StatApiInputDTO> noGroupBy = dto ->
            dto.getGroupBy() == null || dto.getGroupBy().isEmpty();

        Set<StatApiInputDTO> supportedStats = supportedStatsType.stream()
            .filter(supportedGroupBy.or(noGroupBy))
            .collect(Collectors.toSet());

        logger.debug("Number of supportedStats from request={}", supportedStats.size());

        return SubQuerySupportedStats.some(supportedStats);
    }

    @Nonnull
    @Override
    public Map<Long, List<StatApiDTO>> getAggregateStats(@Nonnull final Set<StatApiInputDTO> requestedStats,
                                                         @Nonnull final StatsQueryContext context) throws OperationFailedException {

        if (!context.isGlobalScope() && context.getQueryScope().getEntities().isEmpty()) {
            return Collections.emptyMap();
        }

        Map<Long, List<StatApiDTO>> retStats = new HashMap<>();

        List<StatApiDTO> results = new ArrayList<>();
        for (StatApiInputDTO requestedStat : requestedStats) {
            // search for attachment string

            // When requested for groupBy Virtual Volume attachment status
            if (requestedStat.getName().equals(NUM_VOL) &&
                requestedStat.getGroupBy() != null &&
                requestedStat.getGroupBy().contains(StringConstants.ATTACHMENT) &&
                requestedStat.getRelatedEntityType().equals(UIEntityType.VIRTUAL_VOLUME.apiStr())) {

                final SearchFilter.Builder connectedToVVSearchFilter =
                    createSearchTraversalFilter(TraversalDirection.CONNECTED_TO, UIEntityType.VIRTUAL_VOLUME);

                final SearchFilter.Builder connectedFromVMSearchFilter =
                    createSearchTraversalFilter(TraversalDirection.CONNECTED_FROM, UIEntityType.VIRTUAL_MACHINE);

                // VV which are attached to VM within the scope
                final SearchParameters searchVvAttachedToVMInScope = getSearchScopeBuilder(context, requestedStat)
                    .addSearchFilter(connectedFromVMSearchFilter)
                    .addSearchFilter(connectedToVVSearchFilter)
                    .build();

                // VV within the scope
                final SearchParameters searchNumOfVv = getSearchScopeBuilder(context, requestedStat)
                    .addSearchFilter(connectedToVVSearchFilter)
                    .build();

                Long numOfVvs = repositoryApi.newSearchRequest(searchNumOfVv).count();
                Long numOfAttachedVvInScope;
                if (context.isGlobalScope()) {
                    numOfAttachedVvInScope = repositoryApi.newSearchRequest(searchVvAttachedToVMInScope).count();
                } else {
                    Set<Long> vvAttachedToVM = repositoryApi.newSearchRequest(searchVvAttachedToVMInScope).getOids();
                    // This step is required because vvAttachedToVM can include VVs outside the current scope-
                    // we get VMs connected from VVs in scope, then VVs connected to those VMs- this has the potential to be
                    // a superset of the VVs we started with (VMs can be connected to more than one VV simultaneously)
                    numOfAttachedVvInScope = Long.valueOf(Sets.intersection(context.getQueryScope().getEntities(), vvAttachedToVM).size());
                }

                Map<String, Long> vvAttachmentCountMap = ImmutableMap.<String, Long>builder()
                    .put(StringConstants.ATTACHED, numOfAttachedVvInScope)
                    .put(StringConstants.UNATTACHED, numOfVvs - numOfAttachedVvInScope)
                    .build();

                List<StatApiDTO> stats = toCountStatApiDtos(requestedStat, StringConstants.ATTACHMENT, vvAttachmentCountMap);

                results.addAll(stats);

                // when request for stat group by storage tier of Virtual volume
            } else if (requestedStat.getName().equals(NUM_VOL) &&
                requestedStat.getGroupBy() != null &&
                requestedStat.getGroupBy().contains(UIEntityType.STORAGE_TIER.apiStr()) &&
                requestedStat.getRelatedEntityType().equals(UIEntityType.VIRTUAL_VOLUME.apiStr())) {
                // Get all the storage tier in the scope
                // Storage tier is for Cloud only

                final SearchParameters vvInScopeSearchParams = getSearchScopeBuilder(context, requestedStat)
                    .addSearchFilter(SearchFilter.newBuilder()
                        .setPropertyFilter(
                            SearchProtoUtil.stringPropertyFilterExact(
                                SearchableProperties.ENVIRONMENT_TYPE,
                                Collections.singletonList(UIEnvironmentType.CLOUD.name()))
                        )
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

                List<StatApiDTO> stats = toCountStatApiDtos(requestedStat, UIEntityType.STORAGE_TIER.apiStr(), storageTierCountMap);

                results.addAll(stats);

            } else {
                final SearchParameters searchParameters =
                    getSearchScopeBuilder(context, requestedStat).build();
                final long total = repositoryApi.newSearchRequest(searchParameters).count();
                results.add(makeCountStatDto(requestedStat, total));
            }
        }

        if (!results.isEmpty()) {
            retStats.put(context.getCurTime(), results);
        }
        return retStats;
    }

    /**
     * Create SearchParameters Builder based on search request and context.
     *
     * @param context {@link StatsQueryContext}
     * @param requestStat {@link StatApiInputDTO}
     * @return {@link SearchParameters.Builder}
     */
    @Nonnull
    private static SearchParameters.Builder getSearchScopeBuilder(@Nonnull final StatsQueryContext context,
                                                                  @Nonnull final StatApiInputDTO requestStat) {
        if (context.isGlobalScope()) {
            SearchParameters.Builder builder = SearchProtoUtil.makeSearchParameters(SearchProtoUtil.entityTypeFilter(
                context.getQueryScope().getGlobalScope().get().entityTypes().stream()
                    .map(et -> et.apiStr())
                    .collect(Collectors.toList())));

            // Apply environment type filter explicitly if exists
            Optional<StatFilterApiDTO> environmentTypeFilterDto = requestStat.getFilters() == null || requestStat.getFilters().isEmpty() ? Optional.empty() :
                requestStat.getFilters().stream().filter(filter -> filter.getType().equals(StringConstants.ENVIRONMENT_TYPE)).findFirst();

            // explicitly add search filter for CLOUD and ONPREM only
            environmentTypeFilterDto.flatMap(envFilter -> UIEnvironmentType.fromString(envFilter.getValue()).toEnvType())
                .ifPresent(targetEnvType -> {
                    if (environmentTypeFilterAllowed.contains(targetEnvType)) {
                        builder.addSearchFilter(
                            SearchFilter.newBuilder()
                                .setPropertyFilter(
                                    SearchProtoUtil.stringPropertyFilterExact(SearchableProperties.ENVIRONMENT_TYPE,
                                        Collections.singletonList(environmentTypeFilterDto.get().getValue()))
                                )
                                .build()
                        );
                    }
                });

            return builder;
        } else {
            return SearchProtoUtil
                .makeSearchParameters(SearchProtoUtil.idFilter(context.getQueryScope().getEntities()));
        }
    }

    /**
     * Helper method to create SearchFilter with a SearchTraversalFilter.
     *
     * @param direction {@link TraversalDirection}
     * @param uiEntityType {@link UIEntityType}
     * @return {@link SearchFilter.Builder}
     */
    @Nonnull
    private static SearchFilter.Builder createSearchTraversalFilter(@Nonnull final TraversalDirection direction,
                                                                    @Nonnull final UIEntityType uiEntityType) {
        return SearchFilter.newBuilder()
            .setTraversalFilter(SearchProtoUtil.traverseToType(direction, uiEntityType.apiStr()));
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
