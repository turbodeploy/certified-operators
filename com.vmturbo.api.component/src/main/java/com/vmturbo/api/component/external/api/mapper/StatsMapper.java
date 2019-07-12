package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.component.external.api.service.TargetsService;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.EntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.history.schema.RelationType;

/**
 * Maps stats snapshots between their API DTO representation and their protobuf representation.
 */
public class StatsMapper {

    private static final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    public static final String RELATION_FILTER_TYPE = "relation";

    private static final String STAT_RECORD_PREFIX_CURRENT = "current";
    public static final String FILTER_NAME_KEY = "key";
    private final ConcurrentHashMap<Long, TargetApiDTO> uuidToTargetApiDtoMap = new ConcurrentHashMap<>();

    private static final ImmutableMap<String, Optional<String>> dbToUiStatTypes = ImmutableMap.of(
               RelationType.COMMODITIES.getLiteral(), Optional.of("sold"),
               RelationType.COMMODITIESBOUGHT.getLiteral(), Optional.of("bought"),
               // (June 12, 2017): This is not a relation that the UI understands,
               // so don't map it to any relation type.
               RelationType.METRICS.getLiteral(), Optional.empty(),
               // (June 8, 2017): "plan" is not valid relation type from the UI's point of view,
               // so don't map it to any relation type when constructing results for the UI.
               "plan", Optional.empty());

    /**
     * The UI distinguishes between "metrics" and "commodities". Commodities are expected to contain
     * information about things like capacities and reservation. Metrics are not allowed to.
     *
     * Providing a capacity or reserved in a stat request for a metric makes no sense. Doing so
     * causes the UI to render the metric as a commodity (ie render a donut chart and provide a
     * utilization percentage instead of a raw number). Uses these metric names
     * to decide whether to supply commodity-related fields in the stats response or not.
     *
     * There is no canonical list of metrics or commodities nor does either have an attribute
     * on the stat indicating which class of stat they belong to. Akhand provided this partial list,
     * but it may be incomplete and it is likely more will be added in the future.
     */
    public static final Set<String> METRIC_NAMES = ImmutableSet.of(
        StringConstants.PRICE_INDEX, StringConstants.NUM_VMS, StringConstants.NUM_HOSTS,
        StringConstants.NUM_STORAGES, StringConstants.NUM_DBS, StringConstants.NUM_CONTAINERS,
        StringConstants.NUM_VDCS, StringConstants.NUM_VMS_PER_HOST,
        StringConstants.NUM_VMS_PER_STORAGE, StringConstants.NUM_CNT_PER_HOST,
        StringConstants.NUM_CNT_PER_STORAGE, StringConstants.NUM_CNT_PER_VM,
        StringConstants.HEADROOM_VMS, StringConstants.CURRENT_HEADROOM, StringConstants.DESIREDVMS,
        StringConstants.PRODUCES, StringConstants.NUM_RI, StringConstants.RI_COUPON_COVERAGE,
        StringConstants.RI_COUPON_UTILIZATION, StringConstants.RI_DISCOUNT,
        StringConstants.NUM_CPUS, StringConstants.NUM_SOCKETS, StringConstants.NUM_VCPUS
    );
    private static final String UNKNOWN = "UNKNOWN";

    /**
     * Cloud cost component constant, a filter type for bottom-up cost
     */
    private static final String COST_COMPONENT = "costComponent";

    private final PaginationMapper paginationMapper;

    public StatsMapper(@Nonnull final PaginationMapper paginationMapper) {
        this.paginationMapper = Objects.requireNonNull(paginationMapper);
    }

    /**
     * Convert a protobuf Stats.StatSnapshot to an API DTO StatSnapshotApiDTO.
     *
     * A Snapshot consists of a date, a date range, and a collection of SnapshotRecords.
     * If the date is not set in the StatSnapshot, then do not return a date in the resulting
     * StatSnapshotApiDTO.
     *
     * The collection may be zero length.
     *
     * @param statSnapshot a {@link StatSnapshot} protobuf to be converted to a {@link StatSnapshotApiDTO} for
     *                     return to the REST API caller (e.g. UX)
     * @return a {@link StatSnapshotApiDTO} with fields initialized from the given StatSnapshot
     *
     **/
    @Nonnull
    public StatSnapshotApiDTO toStatSnapshotApiDTO(StatSnapshot statSnapshot) {
        final StatSnapshotApiDTO dto = new StatSnapshotApiDTO();
        if (statSnapshot.hasSnapshotDate()) {
            dto.setDate(statSnapshot.getSnapshotDate());
        }
        dto.setStatistics(statSnapshot.getStatRecordsList().stream()
                .map(this::toStatApiDto)
                .collect(Collectors.toList()));
        return dto;
    }


    /**
     * Convert a protobuf Stats.StatSnapshot to an API DTO StatSnapshotApiDTO.
     *
     * A Snapshot consists of a date, a date range, and a collection of SnapshotRecords.
     * If the date is not set in the StatSnapshot, then do not return a date in the resulting
     * StatSnapshotApiDTO.
     *
     * The collection may be zero length.
     *
     * @param statSnapshot a {@link StatSnapshot} protobuf to be converted to a {@link StatSnapshotApiDTO} for
     *                     return to the REST API caller (e.g. UX)
     * @param targetApiDTOs target API DTOs
     * @param typeFunction function to populate the statistics -> filters -> type for API DTO
     * @param valueFunction function to populate the statistics -> filters -> value for API DTO
     *
     * @param targetsService
     * @return a {@link StatSnapshotApiDTO} with fields initialized from the given StatSnapshot
     *
     **/
    @Nonnull
    public StatSnapshotApiDTO toStatSnapshotApiDTO(@Nonnull final CloudCostStatRecord statSnapshot,
                                                   @Nonnull final List<TargetApiDTO> targetApiDTOs,
                                                   @Nonnull final Function<TargetApiDTO, String> typeFunction,
                                                   @Nonnull final Function<TargetApiDTO, String> valueFunction,
                                                   @Nonnull final Collection<BaseApiDTO> cloudServiceDTOs,
                                                   @Nonnull final TargetsService targetsService) {
        // construct target UUID -> targetApiDTO map
        targetApiDTOs.forEach(targetApiDTO -> {
            uuidToTargetApiDtoMap.putIfAbsent(Long.parseLong(targetApiDTO.getUuid()), targetApiDTO);
        });
        final StatSnapshotApiDTO dto = new StatSnapshotApiDTO();
        if (statSnapshot.hasSnapshotDate()) {
            dto.setDate(statSnapshot.getSnapshotDate());
        }
        // for Cloud service, search for all the discovered Cloud services ,and match the expenses
        if (!cloudServiceDTOs.isEmpty()) {
            dto.setStatistics(statSnapshot.getStatRecordsList().stream()
                    .map(statApiDTO -> toStatApiDtoWithTargets(statApiDTO,
                            typeFunction,
                            valueFunction,
                            cloudServiceDTOs))
                    .collect(Collectors.toList()));
        } else {
            // for groupBy CSP and target
            dto.setStatistics(statSnapshot.getStatRecordsList().stream()
                    .filter(statRecord -> isAssociatedWithTarget(targetsService, statRecord.getAssociatedEntityId()))
                    .map(statApiDTO ->
                            toStatApiDtoWithTargets(statApiDTO,
                                    typeFunction,
                                    valueFunction,
                                    Collections.emptyList()))
                    .collect(Collectors.toList()));
        }
        return dto;
    }

    // determine if the associated entity id matches with target ID
    private boolean isAssociatedWithTarget(final TargetsService targetsService, final long associatedEntityId) {
        if (uuidToTargetApiDtoMap.containsKey(associatedEntityId)
                && uuidToTargetApiDtoMap.get(associatedEntityId).getUuid() != null) {
            return true;
        }
        // handle hidden targets, e.g. AWS billing
        try {
            final TargetApiDTO targetApiDTO = targetsService.getTarget(String.valueOf(associatedEntityId));
            uuidToTargetApiDtoMap.putIfAbsent(associatedEntityId, targetApiDTO);
            return true;
        } catch (UnknownObjectException e) {
            uuidToTargetApiDtoMap.putIfAbsent(associatedEntityId, new TargetApiDTO());
            return false;
        }
    }

    /**
     * Convert a protobuf Stats.EntityStats to a list of StatSnapshotApiDTO.
     *
     * EntityStats includes information about an entity as well as a list of StatsSnapshots
     * related to that entity. This list of StatsSnapshots must be converted to StatSnapshotApiDTO.
     *
     * @param entityStats the Stats.EntityStats to convert
     * @return a List of StatSnapshotApiDTO populated from the EntityStats
     */
    @Nonnull
    public List<StatSnapshotApiDTO> toStatsSnapshotApiDtoList(EntityStats entityStats) {
        return entityStats.getStatSnapshotsList().stream()
                .map(this::toStatSnapshotApiDTO)
                .collect(Collectors.toList());
    }

    private StatApiDTO toStatApiDtoWithTargets(@Nonnull final CloudCostStatRecord.StatRecord statRecord,
                                               @Nonnull final Function<TargetApiDTO, String> typeFunction,
                                               @Nonnull final Function<TargetApiDTO, String> valueFunction,
                                               @Nonnull final Collection<BaseApiDTO> cloudServiceDTOs) {
        final StatApiDTO statApiDTO = new StatApiDTO();
        statApiDTO.setName(statRecord.getName());
        statApiDTO.setUnits(statRecord.getUnits());

        final StatValueApiDTO converted = new StatValueApiDTO();
        converted.setAvg(statRecord.getValues().getAvg());
        converted.setMax(statRecord.getValues().getMax());
        converted.setMin(statRecord.getValues().getMin());
        converted.setTotal(statRecord.getValues().getTotal());

        statApiDTO.setValues(converted);
        statApiDTO.setValue(statRecord.getValues().getAvg());


        final BaseApiDTO provider = new BaseApiDTO();
        provider.setUuid(String.valueOf(statRecord.getAssociatedEntityId()));

        statApiDTO.setRelatedEntity(provider);

        // Build filters
        final List<StatFilterApiDTO> filters = new ArrayList<>();
        final TargetApiDTO targetApiDTO = uuidToTargetApiDtoMap.get(statRecord.getAssociatedEntityId());
        final StatFilterApiDTO resultsTypeFilter = new StatFilterApiDTO();
        resultsTypeFilter.setType(typeFunction.apply(targetApiDTO));
        resultsTypeFilter.setValue(cloudServiceDTOs.isEmpty() ?
                valueFunction.apply(targetApiDTO) : popluateCloudServiceName(cloudServiceDTOs, statApiDTO)
                .orElse(UNKNOWN));
        filters.add(resultsTypeFilter);
        statApiDTO.setFilters(filters);
        return statApiDTO;
    }

    // populate Cloud service name based on uuid match
    private Optional<String> popluateCloudServiceName(@Nonnull final Collection<BaseApiDTO> finalCloudServiceOids,
                                                      @Nonnull final StatApiDTO statApiDTO) {
       return finalCloudServiceOids.stream()
               .filter(service -> statApiDTO.getRelatedEntity() != null
                       && service.getUuid().equals(statApiDTO.getRelatedEntity().getUuid()))
               .map(baseApiDTO -> baseApiDTO.getDisplayName())
               .findFirst();
    }

    /**
     * Convert a protobuf for a {@link StatRecord} to a {@link StatApiDTO} for return to the REST API caller (e.g. UX).
     *
     * A StatRecord contains information about a single statistic.
     *
     * @param statRecord the {@link StatRecord} protobuf to be converted to the {@link StatApiDTO} for return to the
     *                   REST API caller.
     * @return a new instance of {@link StatApiDTO} initialized from given protobuf.
     */
    @Nonnull
    private StatApiDTO toStatApiDto(StatRecord statRecord) {
        final StatApiDTO statApiDTO = new StatApiDTO();
        if (statRecord.getName().startsWith(STAT_RECORD_PREFIX_CURRENT)) {
            // The UI requires the name for both before and after plan values to be the same.
            // Remove the prefix "current".  e.g. currentNumVMs => numVMs
            statApiDTO.setName(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL,
                    statRecord.getName().substring(STAT_RECORD_PREFIX_CURRENT.length())));
        } else {
            statApiDTO.setName(statRecord.getName());
        }

        if (statRecord.hasProviderUuid() || statRecord.hasProviderDisplayName()) {
            final BaseApiDTO provider = new BaseApiDTO();
            provider.setDisplayName(statRecord.getProviderDisplayName());
            provider.setUuid(statRecord.getProviderUuid());
            statApiDTO.setRelatedEntity(provider);
        }

        if (statRecord.hasRelatedEntityType()) {
            statApiDTO.setRelatedEntityType(statRecord.getRelatedEntityType());
        }

        statApiDTO.setUnits(statRecord.getUnits());
        // Only add capacity and reservation values when the stat is NOT a metric (ie when it is
        // a commodity)
        if (!METRIC_NAMES.contains(statRecord.getName())) {
            statApiDTO.setCapacity(toStatValueApiDTO(statRecord.getCapacity()));
            statApiDTO.setReserved(buildStatDTO(statRecord.getReserved()));
        }

        // Set display name for this stat
        statApiDTO.setDisplayName(buildStatDiplayName(statRecord));

        // The "values" should be equivalent to "used".
        statApiDTO.setValues(toStatValueApiDTO(statRecord.getUsed()));
        statApiDTO.setValue(statRecord.getUsed().getAvg());

        // Build filters
        final List<StatFilterApiDTO> filters = new ArrayList<>();
        if (statRecord.hasRelation()) {
            relationFilter(statRecord.getRelation()).ifPresent(filters::add);
        }
        if (statRecord.getName().startsWith(STAT_RECORD_PREFIX_CURRENT)) {
            StatFilterApiDTO resultsTypeFilter = new StatFilterApiDTO();
            resultsTypeFilter.setType(com.vmturbo.components.common.utils.StringConstants.RESULTS_TYPE);
            resultsTypeFilter.setValue(com.vmturbo.components.common.utils.StringConstants.BEFORE_PLAN);
            filters.add(resultsTypeFilter);
        }

        if (statRecord.hasStatKey()) {
            StatFilterApiDTO keyFilter = new StatFilterApiDTO();
            keyFilter.setType(FILTER_NAME_KEY);
            keyFilter.setValue(statRecord.getStatKey());
            filters.add(keyFilter);
        }

        if (filters.size() > 0) {
            statApiDTO.setFilters(filters);
        }
        return statApiDTO;
    }

    @Nonnull
    private String buildStatDiplayName(@Nonnull StatRecord statRecord) {
        // if this is a flow commodity, just display "FLOW"
        if (UICommodityType.FLOW.apiStr().equals(statRecord.getName())) {
            return "FLOW";
        }

        final StringBuilder resultBuilder = new StringBuilder();

        // if bought, add provider information
        if (RelationType.COMMODITIESBOUGHT.getLiteral().equals(statRecord.getRelation())
                && !StringUtils.isEmpty(statRecord.getProviderDisplayName())) {
            resultBuilder.append("FROM: ").append(statRecord.getProviderDisplayName()).append(" ");
        }

        // add key information
        if (!StringUtils.isEmpty(statRecord.getStatKey())) {
            resultBuilder.append("KEY: ").append(statRecord.getStatKey());
        }

        return resultBuilder.toString();
    }

    @Nonnull
    private Optional<StatFilterApiDTO> relationFilter(@Nonnull final String relation) {
        return getUIValue(relation).map(uiRelation -> {
            final StatFilterApiDTO filter = new StatFilterApiDTO();
            filter.setType(RELATION_FILTER_TYPE);
            filter.setValue(uiRelation);
            return filter;
        });
    }

    /**
     * Convert a protobuf for a {@link StatValue} to a {@link StatValueApiDTO} for return to the REST API
     * caller (e.g. UX).
     *
     * @param statValue a {@link StatValue} protobuf to convert
     * @return a new instance of {@link StatValueApiDTO} initialized from the given protobuf
     */
    @Nonnull
    private StatValueApiDTO toStatValueApiDTO(@Nonnull final StatValue statValue) {
        final StatValueApiDTO converted = new StatValueApiDTO();
        converted.setAvg(statValue.getAvg());
        converted.setMax(statValue.getMax());
        converted.setMin(statValue.getMin());
        converted.setTotal(statValue.getTotal());
        return converted;
    }

    /**
     * Create a {@link GetAveragedEntityStatsRequest} for a group of UUIDs.
     *
     * @param entityIds gather stats for the entities with these IDs.
     * @param statApiInput a {@link StatApiInputDTO} specifying query options for this /stats query
     * @param globalTempGroupEntityType a optional entity type of a global temp group. if present, means
     *                            it will query stats from market stats table to speed up query.
     * @return a new instance of {@link GetAveragedEntityStatsRequest} protobuf with fields set from the given statApiInput
     */
    @Nonnull
    public GetAveragedEntityStatsRequest toAveragedEntityStatsRequest(
                final Set<Long> entityIds,
                @Nullable final StatPeriodApiInputDTO statApiInput,
                @Nonnull final Optional<Integer> globalTempGroupEntityType) {
        final GetAveragedEntityStatsRequest.Builder entityStatsRequest =
            GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(newPeriodStatsFilter(statApiInput, globalTempGroupEntityType));
        if (entityIds != null && !entityIds.isEmpty()) {
            entityStatsRequest.addAllEntities(entityIds);
        }
        globalTempGroupEntityType.ifPresent(entityType ->
            entityStatsRequest.setRelatedEntityType(UIEntityType.fromType(entityType).apiStr()));
        return entityStatsRequest.build();
    }

    /**
     * Create a {@link GetEntityStatsRequest}.
     *
     * @param entityStatsScope The {@link EntityStatsScope} for the request.
     * @param statApiInput A {@link StatApiInputDTO} specifying query options.
     * @param paginationRequest A {@link EntityStatsPaginationRequest} specifying the pagination
     *                          parameters.
     * @return The {@link GetEntityStatsRequest} to use to call the history component.
     */
    @Nonnull
    public GetEntityStatsRequest toEntityStatsRequest(
            @Nonnull final EntityStatsScope entityStatsScope,
            @Nullable final StatPeriodApiInputDTO statApiInput,
            @Nonnull final EntityStatsPaginationRequest paginationRequest) {
        return GetEntityStatsRequest.newBuilder()
                .setFilter(newPeriodStatsFilter(statApiInput, Optional.empty()))
                .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                .setScope(entityStatsScope)
                .build();
    }

    /**
     * Create a {@link ProjectedEntityStatsRequest}.
     *
     * @param entityIds The IDs to get stats for.
     * @param statApiInput A {@link StatApiInputDTO} specifying query options.
     * @param paginationRequest A {@link EntityStatsPaginationRequest} specifying the pagination
     *                          parameters.
     * @return The {@link ProjectedEntityStatsRequest} to use to call the history component.
     */
    @Nonnull
    public ProjectedEntityStatsRequest toProjectedEntityStatsRequest(
            @Nonnull final Set<Long> entityIds,
            @Nullable final StatPeriodApiInputDTO statApiInput,
            @Nonnull final EntityStatsPaginationRequest paginationRequest) {
        // fetch the projected stats for each of the given entities
        final ProjectedEntityStatsRequest.Builder requestBuilder =
                ProjectedEntityStatsRequest.newBuilder()
                        .addAllEntities(entityIds)
                        .setPaginationParams(paginationMapper.toProtoParams(paginationRequest));
        Optional.ofNullable(statApiInput)
                .map(input -> input.getStatistics())
                .orElse(Collections.emptyList())
                .stream()
                .filter(statApiInputDto -> statApiInputDto.getName() != null)
                .map(StatApiInputDTO::getName)
                .forEach(requestBuilder::addCommodityName);
        return requestBuilder.build();
    }

    /**
     * Convert a {@link StatPeriodApiInputDTO} request from the REST API into a protobuf
     * {@link StatsFilter} to send via gRPC to the History Service.
     *
     * The StatApiInputDTO specifies the details of the /stats request, including a date range,
     * commodities to query, and one of entity names / scopes / entity-type to query.
     *
     * It also specifies filter clauses to use as in SQL 'where' clauses and 'group by' clauses
     *
     * @param statApiInput a {@link StatPeriodApiInputDTO} specifying query options for
     *                     this /stats query.
     * @param globalTempGroupEntityType a optional represent the entity type of global temporary group.
     * @return a new instance of {@link StatsFilter} protobuf with fields set from the
     *        given statApiInput
     */
    @Nonnull
    public StatsFilter newPeriodStatsFilter(
            @Nullable final StatPeriodApiInputDTO statApiInput,
            @Nonnull final Optional<Integer> globalTempGroupEntityType) {
        final StatsFilter.Builder filterRequestBuilder = StatsFilter.newBuilder();
        if (statApiInput != null) {

            final String inputStartDate = statApiInput.getStartDate();
            if (inputStartDate != null) {
                final Long aLong = Long.valueOf(inputStartDate);
                filterRequestBuilder.setStartDate(aLong);
            }

            final String inputEndDate = statApiInput.getEndDate();
            if (inputEndDate != null) {
                final Long aLong = DateTimeUtil.parseTime(inputEndDate);
                filterRequestBuilder.setEndDate(aLong);
            }
            if (statApiInput.getStatistics() != null) {
                for (StatApiInputDTO stat : statApiInput.getStatistics()) {
                    CommodityRequest.Builder commodityRequestBuilder = CommodityRequest.newBuilder();
                    if (stat.getName() != null) {
                        commodityRequestBuilder.setCommodityName(stat.getName());
                    } else if (stat.getRelatedEntityType() == null) {
                        // too little information
                        final String errorMessage =
                            "Statistics commodity request contains neither commodity name nor "
                                + "related entity type";
                        logger.error(errorMessage);
                        throw new IllegalArgumentException(errorMessage);
                    }
                    // Pass filters, relatedEntityType, and groupBy as part of the request
                    if (stat.getFilters() != null && !stat.getFilters().isEmpty()) {
                        stat.getFilters().forEach(statFilterApiDto ->
                                commodityRequestBuilder.addPropertyValueFilter(
                                        StatsFilter.PropertyValueFilter.newBuilder()
                                                .setProperty(statFilterApiDto.getType())
                                                .setValue(statFilterApiDto.getValue())
                                                .build()));
                    }
                    if (stat.getGroupBy() != null && !stat.getGroupBy().isEmpty()) {
                        commodityRequestBuilder.addAllGroupBy(stat.getGroupBy());
                    }
                    globalTempGroupEntityType.ifPresent(globalType ->
                        commodityRequestBuilder.setRelatedEntityType(
                            UIEntityType.fromType(globalType).apiStr()));
                    if (stat.getRelatedEntityType() != null) {
                        if (commodityRequestBuilder.hasRelatedEntityType()
                            && !areRelatedEntityTypesCompatible(
                            commodityRequestBuilder.getRelatedEntityType(),
                            stat.getRelatedEntityType())) {
                            logger.error(
                                "Api input related entity type: {} is not consistent with "
                                        + "group entity type: {}",
                                    stat.getRelatedEntityType(),
                                    UIEntityType.fromType(globalTempGroupEntityType.get()).apiStr());
                            throw new IllegalArgumentException(
                                "Related entity type is not same as group entity type");
                        }
                        commodityRequestBuilder.setRelatedEntityType(stat.getRelatedEntityType());
                    }
                    filterRequestBuilder.addCommodityRequests(commodityRequestBuilder.build());
                }
            }
        }
        return filterRequestBuilder.build();
    }

    private boolean areRelatedEntityTypesCompatible(@Nonnull final String relatedEntityType,
                                                    @Nonnull final String otherEntityType) {
        return normalizeRelatedType(relatedEntityType).equals(normalizeRelatedType(otherEntityType));
    }

    @Nonnull
    private StatValueApiDTO buildStatDTO(float value) {
        // TODO: This conversion is a hack. Implement properly.
        final StatValueApiDTO stat = new StatValueApiDTO();
        stat.setAvg(value);
        stat.setTotal(value);
        stat.setMin(value);
        stat.setMax(value);

        return stat;
    }

    @Nonnull
    private Optional<String> getUIValue(@Nonnull final String dbValue) {
        final Optional<String> uiValue = dbToUiStatTypes.get(dbValue);

        if (uiValue == null) {
            throw new IllegalArgumentException("Illegal statistic type [" + dbValue + "]");
        }

        return uiValue;
    }

    /**
     * Create a request to fetch Projected Stats from the History Component.
     *
     * @param uuid a set of {@link ServiceEntityApiDTO} UUIDs to query
     * @param inputDto parameters for the query, especially the requested stats
     * @return a {@link ProjectedStatsRequest} protobuf which encapsulates the given uuid list
     * and stats names to be queried.
     */
    @Nonnull
    public ProjectedStatsRequest toProjectedStatsRequest(
            @Nonnull final Set<Long> uuid,
            @Nullable final StatPeriodApiInputDTO inputDto) {
        ProjectedStatsRequest.Builder builder = ProjectedStatsRequest.newBuilder().addAllEntities(uuid);
        Optional.ofNullable(inputDto)
                .map(StatPeriodApiInputDTO::getStatistics)
                .orElse(Collections.emptyList())
                .forEach(statApiInputDTO -> {
                    // If necessary we can add support for other parts of the StatPeriodApiInputDTO,
                    // and extend the Projected Stats API to serve the additional functionality.
                    if (statApiInputDTO.getName() != null) {
                        builder.addCommodityName(statApiInputDTO.getName());
                    }
                });
        return builder.build();
    }

    /**
     * Create a ClusterStatsRequest object from a cluster UUID and a StatPeriodApiInputDTO object.
     *
     * @param uuid UUID of a cluster
     * @param inputDto input DTO containing details of the request.
     * @return a ClusterStatsRequest object contain details from the input DTO.
     */
    @Nonnull
    public ClusterStatsRequest toClusterStatsRequest(
            @Nonnull final String uuid,
            @Nullable final StatPeriodApiInputDTO inputDto) {
        return ClusterStatsRequest.newBuilder()
                .setClusterId(Long.parseLong(uuid))
                .setStats(newPeriodStatsFilter(inputDto, Optional.empty()))
                .build();
    }

    /**
     * Format a {@link PlanTopologyStatsRequest} used to fetch stats for a Plan Topology.
     * The 'startDate' and 'endDate' will determine if the request will be satisfied from
     * the plan source topology or the projected plan topology.
     * We also pass along a 'relatedEntityType', if specified, which will be used to limit
     * the results to the given type.
     *
     * @param planInstance the plan instance to request the stats from
     * @param inputDto a description of what stats to request from this plan, including time range,
     *                 stats types, etc
     * @return a request to fetch the plan stats from the Repository
     */
    @Nonnull
    public PlanTopologyStatsRequest toPlanTopologyStatsRequest(
            @Nonnull final PlanInstance planInstance,
            @Nonnull final StatScopesApiInputDTO inputDto,
            @Nonnull final EntityStatsPaginationRequest paginationRequest) {

        final Stats.StatsFilter.Builder planStatsFilter = Stats.StatsFilter.newBuilder();
        if (inputDto.getPeriod() != null) {
            if (inputDto.getPeriod().getStartDate() != null) {
                planStatsFilter.setStartDate(Long.valueOf(inputDto.getPeriod().getStartDate()));
            }
            if (inputDto.getPeriod().getEndDate() != null) {
                planStatsFilter.setEndDate(Long.valueOf(inputDto.getPeriod().getEndDate()));
            }
            if (inputDto.getPeriod().getStatistics() != null) {
                inputDto.getPeriod().getStatistics().forEach(statApiInputDTO -> {
                    // If necessary we can add support for other parts of the StatPeriodApiInputDTO,
                    // and extend the Projected Stats API to serve the additional functionality.
                    if (statApiInputDTO.getName() != null) {
                        planStatsFilter.addCommodityRequests(CommodityRequest.newBuilder()
                                .setCommodityName(statApiInputDTO.getName())
                                .build());
                    }
                });
            }
        }

        final PlanTopologyStatsRequest.Builder requestBuilder = PlanTopologyStatsRequest.newBuilder()
                .setTopologyId(planInstance.getProjectedTopologyId())
                .setFilter(planStatsFilter);

        final String relatedType = inputDto.getRelatedType();
        if (relatedType != null) {
            requestBuilder.setRelatedEntityType(normalizeRelatedType(relatedType));
        }

        // If there are scopes, set the entity filter.
        // Note - right now if you set an entity filter but do not add any entity ids, there will
        // be no results.
        if (!CollectionUtils.isEmpty(inputDto.getScopes())) {
            requestBuilder.setEntityFilter(EntityFilter.newBuilder()
                .addAllEntityIds(Collections2.transform(inputDto.getScopes(), Long::parseLong)));
        }

        requestBuilder.setPaginationParams(paginationMapper.toProtoParams(paginationRequest));

        return requestBuilder.build();
    }

    /**
     * Handle the special exception where related type "Cluster" is mapped to "PhysicalMachine".
     *
     * @param relatedType the input type from the request.
     * @return the original 'relatedType' except that 'Cluster' is replaced by 'PhysicalMachine'
     */
    @Nonnull
    public String normalizeRelatedType(@Nonnull String relatedType) {
        return relatedType.equals(StringConstants.CLUSTER) ||
                relatedType.equals(UIEntityType.DATACENTER.apiStr()) ||
                relatedType.equals(UIEntityType.PHYSICAL_MACHINE.apiStr()) ?
            UIEntityType.PHYSICAL_MACHINE.apiStr() : relatedType;
    }

    /**
     * Convert Cloud related stat snap shot to StatSnapshotApiDTO
     * @param statSnapshot stat snap shot
     * @return StatSnapshotApiDTO
     */
    public StatSnapshotApiDTO toCloudStatSnapshotApiDTO(final CloudCostStatRecord statSnapshot) {
        final StatSnapshotApiDTO dto = new StatSnapshotApiDTO();
        if (statSnapshot.hasSnapshotDate()) {
            dto.setDate(statSnapshot.getSnapshotDate());
        }
        dto.setStatistics(statSnapshot.getStatRecordsList().stream()
                .map(statRecord -> {
                    final StatApiDTO statApiDTO = new StatApiDTO();
                    statApiDTO.setName(statRecord.getName());
                    statApiDTO.setUnits(statRecord.getUnits());

                    final StatValueApiDTO converted = new StatValueApiDTO();
                    converted.setAvg(statRecord.getValues().getAvg());
                    converted.setMax(statRecord.getValues().getMax());
                    converted.setMin(statRecord.getValues().getMin());
                    converted.setTotal(statRecord.getValues().getTotal());

                    statApiDTO.setValues(converted);
                    statApiDTO.setValue(statRecord.getValues().getAvg());

                    if (statRecord.hasCategory()) {
                        // Build filters
                        final List<StatFilterApiDTO> filters = new ArrayList<>();
                        final StatFilterApiDTO resultsTypeFilter = new StatFilterApiDTO();
                        resultsTypeFilter.setType(COST_COMPONENT);
                        switch(statRecord.getCategory()) {
                            case ON_DEMAND_COMPUTE:
                                resultsTypeFilter.setValue(CostCategory.ON_DEMAND_COMPUTE.name());
                                break;
                            case IP:
                                resultsTypeFilter.setValue(CostCategory.IP.name());
                                break;
                            case LICENSE:
                                resultsTypeFilter.setValue(CostCategory.LICENSE.name());
                                break;
                            case STORAGE:
                                resultsTypeFilter.setValue(CostCategory.STORAGE.name());
                                break;
                            case RI_COMPUTE:
                                resultsTypeFilter.setValue(CostCategory.RI_COMPUTE.name());
                                break;
                        }
                        filters.add(resultsTypeFilter);

                        if (filters.size() > 0) {
                            statApiDTO.setFilters(filters);
                        }
                    }
                    // set related entity type
                    if (statRecord.hasAssociatedEntityType()) {
                        statApiDTO.setRelatedEntityType(UIEntityType.fromType(
                            statRecord.getAssociatedEntityType()).apiStr());
                    }
                    return statApiDTO;
                })
                .collect(Collectors.toList()));
        return dto;
    }
}
