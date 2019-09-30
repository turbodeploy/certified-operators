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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory.StatsQueryContext;
import com.vmturbo.api.component.external.api.util.stats.query.StatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.SubQuerySupportedStats;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
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

    /**
     * Supported stat.
     */
    public static final String NUM_VOL = "numVolumes";

    /**
     * Allowed filters for environment type for global scope.
     */
    private static final Set<String> environmentTypeFilterAllowed =
        Sets.newHashSet(EnvironmentType.CLOUD.name(), EnvironmentType.ONPREM.name());

    private final RepositoryApi repositoryApi;

    public StorageStatsSubQuery(@Nonnull final RepositoryApi repositoryApi) {
        this.repositoryApi = repositoryApi;
    }

    @Override
    public boolean applicableInContext(@Nonnull final StatsQueryContext context) {
        if (context.getInputScope().isPlan()) {
            return false;
        }
        return true;
    }

    @Override
    public SubQuerySupportedStats getHandledStats(@Nonnull final StatsQueryContext context) {
        // Add any supported stats type here.
        Set<StatApiInputDTO> supportedStatsType = context.findStats(Collections.singleton(NUM_VOL));

        // Only support attachment OR storage tier for now
        Predicate<StatApiInputDTO> supportedGroupBy = dto -> dto.getGroupBy() != null && dto.getGroupBy().size() == 1 &&
            (dto.getGroupBy().contains(StringConstants.ATTACHMENT) || dto.getGroupBy().contains(UIEntityType.STORAGE_TIER.apiStr()));

        Set<StatApiInputDTO> supportedStats = supportedStatsType.stream()
            .filter(supportedGroupBy)
            .collect(Collectors.toSet());

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
            if (requestedStat.getGroupBy() == null || requestedStat.getGroupBy().isEmpty()) {
                // only support group by attachment and storage tier for now
                break;
            }

            // When requested for groupBy Virtual Volume attachment status
            if (requestedStat.getGroupBy().contains(StringConstants.ATTACHMENT) &&
                requestedStat.getRelatedEntityType().equals(UIEntityType.VIRTUAL_VOLUME.apiStr())) {
                final SearchFilter.Builder connectedToVVSearchFilter =
                    createSearchTraversalFilter(TraversalDirection.CONNECTED_TO,
                                                UIEntityType.VIRTUAL_VOLUME);

                final SearchFilter.Builder connectedFromVMSearchFilter =
                    createSearchTraversalFilter(TraversalDirection.CONNECTED_FROM,
                                                UIEntityType.VIRTUAL_MACHINE);

                // VV which are attached to VM within the scope
                final SearchParameters searchNumOfVvAttachedToVMInScope = getSearchScopeBuilder(context, requestedStat)
                    .addSearchFilter(connectedFromVMSearchFilter)
                    .addSearchFilter(connectedToVVSearchFilter)
                    .build();

                // VV within the scope
                final SearchParameters searchNumOfVv = getSearchScopeBuilder(context, requestedStat)
                    .addSearchFilter(connectedToVVSearchFilter)
                    .build();


                if (requestedStat.getName().equals(NUM_VOL)) {
                    Long numOfVvs = repositoryApi.newSearchRequest(searchNumOfVv).count();
                    Long numOfVvAttachedToVM = repositoryApi.newSearchRequest(searchNumOfVvAttachedToVMInScope).count();

                    Map<String, Long> tempMap = ImmutableMap.<String, Long>builder()
                        .put(StringConstants.ATTACHED, numOfVvAttachedToVM)
                        .put(StringConstants.UNATTACHED, numOfVvs - numOfVvAttachedToVM)
                        .build();

                    List<StatApiDTO> stats = toCountStatApiDtos(requestedStat, StringConstants.ATTACHMENT, tempMap);

                    results.addAll(stats);
                }
                // when request for stat group by storage tier of Virtual volume
            } else if (requestedStat.getGroupBy().contains(UIEntityType.STORAGE_TIER.apiStr()) &&
                requestedStat.getRelatedEntityType().equals(UIEntityType.VIRTUAL_VOLUME.apiStr())) {
                // Get all the storage tier in the scope
                // Storage tier is for Cloud only

                final SearchParameters vvInScopeSearchParams = getSearchScopeBuilder(context, requestedStat)
                    .addSearchFilter(SearchFilter.newBuilder()
                        .setPropertyFilter(SearchProtoUtil.stringPropertyFilterExact(SearchableProperties.ENVIRONMENT_TYPE,
                            Collections.singletonList(EnvironmentType.CLOUD.name()))))
                    .build();

                List<ApiPartialEntity> vvInScope = repositoryApi.newSearchRequest(vvInScopeSearchParams)
                    .getEntities()
                    .collect(Collectors.toList());

                final Function<ApiPartialEntity, String> getStorageTierOidFromVV = (vvPartialEntity) ->
                    vvPartialEntity.getConnectedToList().stream().filter(relatedEntity -> relatedEntity.getEntityType() == EntityType.STORAGE_TIER.getNumber())
                        .findFirst()
                        .map(RelatedEntity::getOid)
                        .get().toString();

                if (requestedStat.getName().equals(NUM_VOL)) {
                    Map<String, Long> tempMap = vvInScope.stream()
                        .collect(Collectors.groupingBy(getStorageTierOidFromVV, Collectors.counting()));

                    List<StatApiDTO> stats = toCountStatApiDtos(requestedStat, UIEntityType.STORAGE_TIER.apiStr(), tempMap);

                    results.addAll(stats);
                }
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
    private SearchParameters.Builder getSearchScopeBuilder(@Nonnull final StatsQueryContext context,
                                                           @Nonnull final StatApiInputDTO requestStat) {
        if (context.isGlobalScope()) {
            SearchParameters.Builder builder = SearchProtoUtil.makeSearchParameters(SearchProtoUtil.entityTypeFilter(
                context.getQueryScope().getGlobalScope().get().entityTypes().stream()
                    .map(et -> et.apiStr())
                    .collect(Collectors.toList())));

            // Apply environment type filter explicitly if exists

            Optional<StatFilterApiDTO> environmentTypeFilterDto = requestStat.getFilters() == null || requestStat.getFilters().isEmpty() ? Optional.empty() :
                requestStat.getFilters().stream().filter(filter -> filter.getType().equals(StringConstants.ENVIRONMENT_TYPE)).findFirst();

            // add search filter for CLOUD and ONPERM only
            environmentTypeFilterDto.flatMap(envFilter -> UIEnvironmentType.fromString(envFilter.getValue()).toEnvType())
                .ifPresent(targetEnvType -> builder.addSearchFilter(
                    SearchFilter.newBuilder()
                        .setPropertyFilter(SearchProtoUtil.stringPropertyFilterExact(SearchableProperties.ENVIRONMENT_TYPE,
                            Collections.singleton(targetEnvType.name()))
                )));

            return builder;
        } else {
            return SearchProtoUtil
                .makeSearchParameters(SearchProtoUtil.idFilter(context.getQueryScope().getEntities()));
        }
    }

    /**
     * Helper method to create SearchTraversalFilter.
     *
     * @param direction {@link TraversalDirection}
     * @param uiEntityType {@link UIEntityType}
     * @return {@link SearchFilter.Builder}
     */
    private SearchFilter.Builder createSearchTraversalFilter(@Nonnull final TraversalDirection direction,
                                                             @Nonnull final UIEntityType uiEntityType) {
        return SearchFilter.newBuilder()
            .setTraversalFilter(
                SearchProtoUtil.traverseToType(direction, uiEntityType.apiStr()));
    }

    /**
     * Convert count stat to StatApiDTOs based on grouping.
     *
     * @param statApiInputDTO {@link StatApiInputDTO}
     * @param groupBy group by type
     * @param countsByGroup {@link Map} Map for Group to Count
     * @return list of StatApiDTO with the grouping and stat information
     */
    private List<StatApiDTO> toCountStatApiDtos(@Nonnull StatApiInputDTO statApiInputDTO,
                                                @Nonnull String groupBy,
                                                Map<String, Long> countsByGroup) {

         return countsByGroup.entrySet().stream().map(entry -> {
            final StatApiDTO statApiDTO = new StatApiDTO();
            statApiDTO.setName(statApiInputDTO.getName());

            final StatValueApiDTO converted = new StatValueApiDTO();
            float countInFloat = entry.getValue().floatValue();
            converted.setAvg(countInFloat);
            converted.setMax(countInFloat);
            converted.setMin(countInFloat);
            converted.setTotal(countInFloat);

            statApiDTO.setValues(converted);
            statApiDTO.setValue(countInFloat);

            final List<StatFilterApiDTO> filters = new ArrayList<>();
            final StatFilterApiDTO resultsTypeFilter = new StatFilterApiDTO();
            resultsTypeFilter.setType(groupBy);
            resultsTypeFilter.setValue(entry.getKey());
            filters.add(resultsTypeFilter);

            statApiDTO.setFilters(filters);
            if (statApiInputDTO.getRelatedEntityType() != null) {
                statApiDTO.setRelatedEntityType(statApiInputDTO.getRelatedEntityType());
            }


            return statApiDTO;
        }).collect(Collectors.toList());
    }
}
