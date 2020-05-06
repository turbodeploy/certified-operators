package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.service.TargetsService;
import com.vmturbo.api.component.external.api.util.StatsUtils;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander.GlobalScope;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatFilterApiDTO;
import com.vmturbo.api.dto.statistic.StatHistUtilizationApiDTO;
import com.vmturbo.api.dto.statistic.StatPercentileApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.enums.Epoch;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.EntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanCombinedStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RequestDetails;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.stats.Stats.ClusterStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter;
import com.vmturbo.common.protobuf.stats.Stats.GlobalFilter.Builder;
import com.vmturbo.common.protobuf.stats.Stats.ProjectedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.HistUtilizationValue;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.schema.RelationType;

/**
 * Maps stats snapshots between their API DTO representation and their protobuf representation.
 */
public class StatsMapper {

    private static final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    public static final String RELATION_FILTER_TYPE = "relation";

    private static final String BYTE_PER_SEC = "Byte/sec";
    private static final String BIT_PER_SEC = "bit/sec";

    public static final String FILTER_NAME_KEY = "key";
    /**
     * This is a special "group by" sent in by the probe and transferred directly to/from history
     * component.
     */
    private final ConcurrentHashMap<Long, TargetApiDTO> uuidToTargetApiDtoMap = new ConcurrentHashMap<>();

    private static final ImmutableSet<String> apiRelationTypes = ImmutableSet.of(
        StringConstants.RELATION_SOLD,
        StringConstants.RELATION_BOUGHT);

    private static final ImmutableMap<String, Optional<String>> historyToApiStatTypes = ImmutableMap.of(
        // Map the relation types specific to the History component
        RelationType.COMMODITIES.getLiteral(), Optional.of(StringConstants.RELATION_SOLD),
        RelationType.COMMODITIESBOUGHT.getLiteral(), Optional.of(StringConstants.RELATION_BOUGHT),
        // (June 12, 2017): This is not a relation that the UI understands,
        // so don't map it to any relation type.
        RelationType.METRICS.getLiteral(), Optional.empty());

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

    /**
     * The related type in the api request which should be normalized to another type.
     */
    private static final Map<String, String> RELATED_TYPES_TO_NORMALIZE = ImmutableMap.of(
        ApiEntityType.DATACENTER.apiStr(), ApiEntityType.PHYSICAL_MACHINE.apiStr(),
        StringConstants.CLUSTER, ApiEntityType.PHYSICAL_MACHINE.apiStr()
    );

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
            dto.setDate(DateTimeUtil.toString(statSnapshot.getSnapshotDate()));
        }
        if (statSnapshot.hasStatEpoch()) {
            dto.setEpoch(toApiEpoch(statSnapshot.getStatEpoch()));
        }
        dto.setStatistics(statSnapshot.getStatRecordsList().stream()
                .map(this::toStatApiDto)
                .collect(Collectors.toList()));
        return dto;
    }

    private Epoch toApiEpoch(@Nonnull final StatEpoch statEpoch) {
        switch (statEpoch) {
            case HISTORICAL: return Epoch.HISTORICAL;
            case CURRENT: return Epoch.CURRENT;
            case PROJECTED: return Epoch.PROJECTED;
            case PLAN_SOURCE: return Epoch.PLAN_SOURCE;
            case PLAN_PROJECTED: return Epoch.PLAN_PROJECTED;
            default:
                final String errorMessage = "Unable to map unknown StatEpoch: " + statEpoch;
                logger.error(errorMessage);
                throw new IllegalArgumentException(errorMessage);
        }
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
            dto.setDate(DateTimeUtil.toString(statSnapshot.getSnapshotDate()));
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
        CloudCostStatRecord.StatRecord.StatValue value = statRecord.getValues();
        if (value.hasAvg()) {
            converted.setAvg(value.getAvg());
        }
        if (value.hasMax()) {
            converted.setMax(value.getMax());
        }
        if (value.hasMin()) {
            converted.setMin(value.getMin());
        }
        if (value.hasTotal()) {
            converted.setTotal(value.getTotal());
        }

        statApiDTO.setValues(converted);
        statApiDTO.setValue(value.getAvg());


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
    public StatApiDTO toStatApiDto(StatRecord statRecord) {
        // for some records, we want to convert the units before returning them
        // in particular, data transfer records may be coming in Byte/sec (or multiples)
        // but we want to send them in bit/sec (or multiples)
        final StatRecord convertedStatRecord = convertUnitsIfNeeded(statRecord);

        final StatApiDTO statApiDTO = new StatApiDTO();
        if (convertedStatRecord.getName().startsWith(StringConstants.STAT_PREFIX_CURRENT)) {
            // The UI requires the name for both before and after plan values to be the same.
            // Remove the prefix "current" (This is added to plan source aggregated stats)
            // Check if a UICommodityType can be matched, otherwise just return camelCaseFormat

            final String removedCurrentPrefix =
                    convertedStatRecord
                            .getName()
                            .substring(StringConstants.STAT_PREFIX_CURRENT.length());

            UICommodityType commodityType = UICommodityType.fromStringIgnoreCase(removedCurrentPrefix);
            if (commodityType.equals(UICommodityType.UNKNOWN)) {
                 //Things that can be unknown include {@link StatsMapper.METRIC_NAMES}.
                 //i.e. numCPU, numHosts
                statApiDTO.setName(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, removedCurrentPrefix));
            } else {
                statApiDTO.setName(commodityType.apiStr()); //Match to commodityType was found
            }

        } else {
            statApiDTO.setName(convertedStatRecord.getName());
        }

        if (convertedStatRecord.hasProviderUuid() || convertedStatRecord.hasProviderDisplayName()) {
            final BaseApiDTO provider = new BaseApiDTO();
            provider.setDisplayName(convertedStatRecord.getProviderDisplayName());
            provider.setUuid(convertedStatRecord.getProviderUuid());
            statApiDTO.setRelatedEntity(provider);
        }

        if (convertedStatRecord.hasRelatedEntityType()) {
            statApiDTO.setRelatedEntityType(convertedStatRecord.getRelatedEntityType());
        }

        // do not set empty string if there is no units
        if (convertedStatRecord.hasUnits()) {
            statApiDTO.setUnits(convertedStatRecord.getUnits());
        }

        // Only add capacity and reservation values when the stat is NOT a metric (ie when it is
        // a commodity)
        if (!METRIC_NAMES.contains(convertedStatRecord.getName())) {
            statApiDTO.setCapacity(toStatValueApiDTO(convertedStatRecord.getCapacity()));
            statApiDTO.setReserved(buildStatDTO(convertedStatRecord.getReserved()));
        }

        // Set display name for this stat, do not set if it's empty
        String displayName = buildStatDisplayName(convertedStatRecord);
        if (!StringUtils.isEmpty(displayName)) {
            statApiDTO.setDisplayName(displayName);
        }

        // The "values" should be equivalent to "used".
        statApiDTO.setValues(toStatValueApiDTO(convertedStatRecord.getUsed()));
        statApiDTO.setValue(convertedStatRecord.getUsed().getAvg());

        // Build filters
        final List<StatFilterApiDTO> filters = new ArrayList<>();
        if (convertedStatRecord.hasRelation()) {
            relationFilter(convertedStatRecord.getRelation()).ifPresent(filters::add);
        }
        if (convertedStatRecord.getName().startsWith(StringConstants.STAT_PREFIX_CURRENT)) {
            StatFilterApiDTO resultsTypeFilter = new StatFilterApiDTO();
            resultsTypeFilter.setType(StringConstants.RESULTS_TYPE);
            resultsTypeFilter.setValue(StringConstants.BEFORE_PLAN);
            filters.add(resultsTypeFilter);
        }

        if (convertedStatRecord.hasStatKey()) {
            StatFilterApiDTO keyFilter = new StatFilterApiDTO();
            keyFilter.setType(FILTER_NAME_KEY);
            keyFilter.setValue(convertedStatRecord.getStatKey());
            filters.add(keyFilter);
        }

        if (!convertedStatRecord.getHistUtilizationValueList().isEmpty()) {
            final Optional<HistUtilizationValue> percentileValue =
                            convertedStatRecord.getHistUtilizationValueList().stream()
                                            .filter(value -> StringConstants.PERCENTILE.equals(value.getType()))
                                            .findAny();
            percentileValue.map(StatsMapper::calculatePercentile)
                            .map(StatsMapper::createPercentileApiDto)
                            .ifPresent(statApiDTO::setPercentile);
            final List<StatHistUtilizationApiDTO> histUtilizationValues =
                            convertedStatRecord.getHistUtilizationValueList().stream()
                                            .map(StatsMapper::createHistUtilizationApiDto)
                                            .collect(Collectors.toList());
            statApiDTO.setHistUtilizations(histUtilizationValues);
        }

        if (filters.size() > 0) {
            statApiDTO.setFilters(filters);
        }
        return statApiDTO;
    }

    @Nonnull
    private static StatPercentileApiDTO createPercentileApiDto(@Nullable Float percentileUtilization) {
        final StatPercentileApiDTO result = new StatPercentileApiDTO();
        result.setPercentileUtilization(percentileUtilization);
        return result;
    }

    @Nonnull
    private static StatHistUtilizationApiDTO createHistUtilizationApiDto(
                    @Nonnull HistUtilizationValue histUtilizationValue) {
        final StatHistUtilizationApiDTO result = new StatHistUtilizationApiDTO();
        result.setType(histUtilizationValue.getType());
        result.setCapacity(histUtilizationValue.getCapacity().getAvg());
        result.setUsage(histUtilizationValue.getUsage().getAvg());
        return result;
    }

    @Nullable
    private static Float calculatePercentile(@Nonnull HistUtilizationValue value) {
        final StatValue usage = value.getUsage();
        final StatValue capacity = value.getCapacity();
        if (usage.hasAvg() && capacity.hasAvg()) {
            return usage.getAvg() / capacity.getAvg();
        }
        return null;
    }

    @Nonnull
    private String buildStatDisplayName(@Nonnull StatRecord statRecord) {
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
            resultBuilder.append(StatsUtils.COMMODITY_KEY_PREFIX).append(statRecord.getStatKey());
        }

        return resultBuilder.toString();
    }

    @Nonnull
    private Optional<StatFilterApiDTO> relationFilter(@Nonnull final String relation) {
        return getApiRelationValue(relation).map(apiRelation -> {
            final StatFilterApiDTO filter = new StatFilterApiDTO();
            filter.setType(RELATION_FILTER_TYPE);
            filter.setValue(apiRelation);
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
        if (statValue.hasAvg()) {
            converted.setAvg(statValue.getAvg());
            converted.setTotal(statValue.getTotal());
        }
        if (statValue.hasMax()) {
            converted.setMax(statValue.getMax());
            converted.setTotalMax(statValue.getTotalMax());
        }
        if (statValue.hasMin()) {
            converted.setMin(statValue.getMin());
            converted.setTotalMin(statValue.getTotalMin());
        }
        return converted;
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
                .setFilter(newPeriodStatsFilter(statApiInput))
                .setPaginationParams(paginationMapper.toProtoParams(paginationRequest))
                .setScope(entityStatsScope)
                .build();
    }

    /**
     * Create a {@link ProjectedEntityStatsRequest}.
     *
     * @param entityStatsScope The aggregated entities to get stats for.
     * @param statApiInput A {@link StatApiInputDTO} specifying query options.
     * @param paginationRequest A {@link EntityStatsPaginationRequest} specifying the pagination
     *                          parameters.
     * @return The {@link ProjectedEntityStatsRequest} to use to call the history component.
     */
    @Nonnull
    public ProjectedEntityStatsRequest toProjectedEntityStatsRequest(
            @Nonnull final EntityStatsScope entityStatsScope,
            @Nullable final StatPeriodApiInputDTO statApiInput,
            @Nonnull final EntityStatsPaginationRequest paginationRequest) {
        // fetch the projected stats for each of the given entities
        final ProjectedEntityStatsRequest.Builder requestBuilder =
                ProjectedEntityStatsRequest.newBuilder()
                    .setScope(entityStatsScope)
                    .setPaginationParams(paginationMapper.toProtoParams(paginationRequest));
        Optional.ofNullable(statApiInput)
                .map(StatPeriodApiInputDTO::getStatistics)
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
     * @return a new instance of {@link StatsFilter} protobuf with fields set from the
     *        given statApiInput
     */
    @Nonnull
    public StatsFilter newPeriodStatsFilter(@Nullable final StatPeriodApiInputDTO statApiInput) {
        return newPeriodStatsFilter(statApiInput, false);
    }

    /**
     * Convert a {@link StatPeriodApiInputDTO} request from the REST API into a protobuf
     * {@link StatsFilter} to send via gRPC to the History Service.
     *
     * <p>The StatApiInputDTO specifies the details of the /stats request, including a date range,
     * commodities to query, and one of entity names / scopes / entity-type to query.
     *
     * <p>It also specifies filter clauses to use as in SQL 'where' clauses and 'group by' clauses
     *
     * @param statApiInput a {@link StatPeriodApiInputDTO} specifying query options for
     *                     this /stats query.
     * @param requestProjectedHeadroom request projected headroom or not
     * @return a new instance of {@link StatsFilter} protobuf with fields set from the
     *        given statApiInput
     */
    @Nonnull
    StatsFilter newPeriodStatsFilter(@Nullable final StatPeriodApiInputDTO statApiInput,
                                             final boolean requestProjectedHeadroom) {
        final StatsFilter.Builder filterRequestBuilder = StatsFilter.newBuilder()
            .setRequestProjectedHeadroom(requestProjectedHeadroom);
        if (statApiInput != null) {

            final String inputStartDate = statApiInput.getStartDate();
            if (inputStartDate != null) {
                final Long startDateMillis = DateTimeUtil.parseTime(inputStartDate);
                filterRequestBuilder.setStartDate(startDateMillis);
            }

            final String inputEndDate = statApiInput.getEndDate();
            if (inputEndDate != null) {
                final Long endDateMillis = DateTimeUtil.parseTime(inputEndDate);
                filterRequestBuilder.setEndDate(endDateMillis);
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
                    if (stat.getRelatedEntityType() != null) {
                        // since we've expanded DC to PMs, we should also set related entity type
                        // of commodity request to PhysicalMachine, otherwise history component
                        // will not return required data
                        commodityRequestBuilder.setRelatedEntityType(
                            normalizeRelatedType(stat.getRelatedEntityType()));
                    }
                    filterRequestBuilder.addCommodityRequests(commodityRequestBuilder.build());
                }
            }
        }
        return filterRequestBuilder.build();
    }

    /**
     * Create a global filter based on a provided global scope.
     *
     * @param globalScope a global scope that applies to an entire API stats request
     * @return a GlobalFilter that can be used as part of a Stats query
     */
    @Nonnull
    public GlobalFilter newGlobalFilter(@Nonnull final GlobalScope globalScope) {
        final Builder globalFilter = GlobalFilter.newBuilder();
        globalScope.environmentType().ifPresent(globalFilter::setEnvironmentType);
        // since we've expanded DC to PMs, we should also set related entity type to
        // PhysicalMachine, otherwise history component will not return required data
        globalScope.entityTypes().forEach(type -> globalFilter.addRelatedEntityType(
            normalizeRelatedType(type.apiStr())));
        return globalFilter.build();
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
    private Optional<String> getApiRelationValue(@Nonnull final String relationValue) {
        // Preserve the relation types if they are already in the API format
        if (apiRelationTypes.contains(relationValue)) {
            return Optional.of(relationValue);
        }
        // Otherwise, try to convert from the History format
        final Optional<String> apiValue = historyToApiStatTypes.get(relationValue);
        if (apiValue == null) {
            throw new IllegalArgumentException("Illegal statistic type [" + apiValue + "]");
        }
        return apiValue;
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
        return toClusterStatsRequest(uuid, inputDto, false);
    }

    /**
     * Create a ClusterStatsRequest object from a cluster UUID and a StatPeriodApiInputDTO object.
     *
     * @param uuid UUID of a cluster
     * @param inputDto input DTO containing details of the request.
     * @param requestProjectedHeadroom request projected headroom or not
     * @return a ClusterStatsRequest object contain details from the input DTO.
     */
    @Nonnull
    public ClusterStatsRequest toClusterStatsRequest(
            @Nonnull final String uuid,
            @Nullable final StatPeriodApiInputDTO inputDto,
            final boolean requestProjectedHeadroom) {
        final ClusterStatsRequest.Builder resultBuilder =
                ClusterStatsRequest.newBuilder()
                    .setStats(newPeriodStatsFilter(inputDto, requestProjectedHeadroom));
        if (!"Market".equalsIgnoreCase(uuid)) {
            resultBuilder.addClusterIds(Long.valueOf(uuid));
        }
        return resultBuilder.build();
    }

    /**
     * Format a {@link PlanTopologyStatsRequest} used to fetch stats for a Plan Topology.
     * The 'startDate' and 'endDate' will determine if the request will be satisfied from
     * the plan source topology or the projected plan topology.
     * We also pass along a 'relatedEntityType', if specified, which will be used to limit
     * the results to the given type.
     *
     * @param topologyId the id of a single (source or projected) plan topology for which to
     *                   retrieve stats
     * @param inputDto a description of what stats to request from this plan, including time range,
     *                 stats types, etc
     * @param paginationRequest controls the pagination of the response
     * @return a request to fetch the plan stats from the Repository
     */
    @Nonnull
    public PlanTopologyStatsRequest toPlanTopologyStatsRequest(final long topologyId,
            @Nonnull final StatScopesApiInputDTO inputDto,
            @Nonnull final EntityStatsPaginationRequest paginationRequest) {
        final Stats.StatsFilter.Builder planStatsFilter = Stats.StatsFilter.newBuilder();
        if (inputDto.getPeriod() != null) {
            if (inputDto.getPeriod().getStartDate() != null) {
                planStatsFilter.setStartDate(DateTimeUtil.parseTime(inputDto.getPeriod().getStartDate()));
            }
            if (inputDto.getPeriod().getEndDate() != null) {
                planStatsFilter.setEndDate(DateTimeUtil.parseTime(inputDto.getPeriod().getEndDate()));
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
                .setTopologyId(topologyId);
        final RequestDetails.Builder detailsBuilder = RequestDetails.newBuilder()
                .setFilter(planStatsFilter);

        final String relatedType = inputDto.getRelatedType();
        if (relatedType != null) {
            detailsBuilder.setRelatedEntityType(normalizeRelatedType(relatedType));
        }

        // If there are scopes, set the entity filter.
        // Note - right now if you set an entity filter but do not add any entity ids, there will
        // be no results.
        if (!CollectionUtils.isEmpty(inputDto.getScopes())) {
            detailsBuilder.setEntityFilter(EntityFilter.newBuilder()
                .addAllEntityIds(Collections2.transform(inputDto.getScopes(), Long::parseLong)));
        }

        detailsBuilder.setReturnType(PartialEntity.Type.API);
        detailsBuilder.setPaginationParams(paginationMapper.toProtoParams(paginationRequest));

        requestBuilder.setRequestDetails(detailsBuilder);

        return requestBuilder.build();
    }

    /**
     * Format a {@link PlanCombinedStatsRequest} used to fetch combined stats for a Plan.
     *
     * @param topologyContextId the id of the contextId (plan ID) for which to retrieve combined stats
     * @param topologyTypeToSortOn the topology type to use for sorting the response
     * @param inputDto a description of what stats to request from this plan, including time range,
     *                 stats types, etc
     * @param paginationRequest controls the pagination of the response
     * @return a request to fetch the plan combined stats from the Repository
     */
    @Nonnull
    public PlanCombinedStatsRequest toPlanCombinedStatsRequest(final long topologyContextId,
                                                                @Nonnull final TopologyType topologyTypeToSortOn,
                                                                @Nonnull final StatScopesApiInputDTO inputDto,
                                                                @Nonnull final EntityStatsPaginationRequest paginationRequest) {
        final PlanCombinedStatsRequest.Builder requestBuilder = PlanCombinedStatsRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setTopologyToSortOn(topologyTypeToSortOn);
        final RequestDetails.Builder detailsBuilder = RequestDetails.newBuilder();

        if (inputDto.getPeriod() != null) {
            final Stats.StatsFilter planStatsFilter = newPeriodStatsFilter(inputDto.getPeriod());
            detailsBuilder.setFilter(planStatsFilter);
        }

        final String relatedType = inputDto.getRelatedType();
        if (relatedType != null) {
            detailsBuilder.setRelatedEntityType(normalizeRelatedType(relatedType));
        }

        // If there are scopes, set the entity filter.
        // Note - right now if you set an entity filter but do not add any entity ids, there will
        // be no results.
        if (!CollectionUtils.isEmpty(inputDto.getScopes())) {
            detailsBuilder.setEntityFilter(EntityFilter.newBuilder()
                .addAllEntityIds(Collections2.transform(inputDto.getScopes(), Long::parseLong)));
        }

        detailsBuilder.setReturnType(PartialEntity.Type.API);
        detailsBuilder.setPaginationParams(paginationMapper.toProtoParams(paginationRequest));

        requestBuilder.setRequestDetails(detailsBuilder);

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
        return RELATED_TYPES_TO_NORMALIZE.getOrDefault(relatedType, relatedType);
    }

    /**
     * Whether or not the relatedType should be normalized, for example: normalize DC to PM.
     *
     * @param relatedType type of the entity to check
     * @return true if the original 'relatedType' should be normalized, otherwise false
     */
    public boolean shouldNormalize(@Nonnull String relatedType) {
        return RELATED_TYPES_TO_NORMALIZE.containsKey(relatedType);
    }

    /**
     * Convert Cloud related stat snap shot to StatSnapshotApiDTO
     * @param statSnapshot stat snap shot
     * @return StatSnapshotApiDTO
     */
    public StatSnapshotApiDTO toCloudStatSnapshotApiDTO(final CloudCostStatRecord statSnapshot) {
        final StatSnapshotApiDTO dto = new StatSnapshotApiDTO();
        if (statSnapshot.hasSnapshotDate()) {
            dto.setDate(DateTimeUtil.toString(statSnapshot.getSnapshotDate()));
        }
        dto.setStatistics(statSnapshot.getStatRecordsList().stream()
                .map(statRecord -> {
                    final StatApiDTO statApiDTO = new StatApiDTO();
                    statApiDTO.setName(statRecord.getName());
                    statApiDTO.setUnits(statRecord.getUnits());

                    final StatValueApiDTO converted = new StatValueApiDTO();
                    CloudCostStatRecord.StatRecord.StatValue value = statRecord.getValues();
                    if (value.hasAvg()) {
                        converted.setAvg(value.getAvg());
                    }
                    if (value.hasMax()) {
                        converted.setMax(statRecord.getValues().getMax());
                    }
                    if (value.hasMin()) {
                        converted.setMin(statRecord.getValues().getMin());
                    }
                    if (value.hasTotal()) {
                        converted.setTotal(statRecord.getValues().getTotal());
                    }
                    statApiDTO.setValues(converted);
                    statApiDTO.setValue(value.getAvg());

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
                            case ON_DEMAND_LICENSE:
                                resultsTypeFilter.setValue(CostCategory.ON_DEMAND_LICENSE.name());
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
                        statApiDTO.setRelatedEntityType(ApiEntityType.fromType(
                            statRecord.getAssociatedEntityType()).apiStr());
                    }
                    return statApiDTO;
                })
                .collect(Collectors.toList()));
        return dto;
    }

    /**
     * Convert units in a {@link StatRecord}.
     * <p/>
     * Used to change data-transfer units from Byte/sec to bit/sec,
     * because the UI does not expect Byte/sec values.
     * Other conversions may be added here as necessary.
     *
     * @param statRecord a {@link StatRecord} whose units are to be converted.
     * @return an equivalent {@link StatRecord} with the units converted.
     */
    @Nonnull
    private StatRecord convertUnitsIfNeeded(@Nonnull StatRecord statRecord) {
        if (StringUtils.isEmpty(statRecord.getUnits())
                || !statRecord.getUnits().contains(BYTE_PER_SEC)) {
            return statRecord;
        }

        // convert units from Byte/sec to bit/sec (resp. KByte/sec to Kbit/sec etc.)
        final StatRecord.Builder resultBuilder =
                StatRecord.newBuilder(statRecord)
                    .setUnits(statRecord.getUnits().replace(BYTE_PER_SEC, BIT_PER_SEC));
        if (statRecord.hasCurrentValue()) {
            resultBuilder.setCurrentValue(statRecord.getCurrentValue() * 8);
        }
        if (statRecord.hasCapacity()) {
            resultBuilder.setCapacity(bytesToBits(statRecord.getCapacity()));
        }
        if (statRecord.hasUsed()) {
            resultBuilder.setUsed(bytesToBits(statRecord.getUsed()));
        }
        if (statRecord.hasPeak()) {
            resultBuilder.setPeak(bytesToBits(statRecord.getPeak()));
        }
        if (statRecord.hasValues()) {
            resultBuilder.setValues(bytesToBits(statRecord.getValues()));
        }
        return resultBuilder.build();
    }

    @Nonnull
    private StatValue bytesToBits(@Nonnull StatValue statValue) {
        final StatValue.Builder resultBuilder = StatValue.newBuilder();
        if (statValue.hasAvg()) {
            resultBuilder.setAvg(statValue.getAvg() * 8);
        }
        if (statValue.hasMax()) {
            resultBuilder.setMax(statValue.getMax() * 8);
        }
        if (statValue.hasMin()) {
            resultBuilder.setMin(statValue.getMin() * 8);
        }
        if (statValue.hasTotal()) {
            resultBuilder.setTotal(statValue.getTotal() * 8);
        }
        return resultBuilder.build();
    }

    /**
     * This method uses data from an api entity and populates data
     * in the output EntityStatsApiDTO.
     * @param apiId an api id which contains basic info about an entity
     *              this is the input
     * @param entityStatsApiDTO the output entity stats api dto to copy data into.
     * @return the output entity stats api dto.
     */
    public static EntityStatsApiDTO populateEntityDataEntityStatsApiDTO(ApiId apiId,
            EntityStatsApiDTO entityStatsApiDTO) {
        entityStatsApiDTO.setUuid(apiId.uuid());
        entityStatsApiDTO.setDisplayName(apiId.getDisplayName());
        entityStatsApiDTO.setClassName(apiId.getClassName());
        final Optional<EnvironmentType> envType =
                Optional.of(EnvironmentTypeMapper.fromXLToApi(apiId.getEnvironmentType()));
        if (envType.isPresent()) {
            entityStatsApiDTO.setEnvironmentType(envType.get());
        }
        return entityStatsApiDTO;
    }

    /**
     * This method uses data from an api entity and populates data
     * in the output EntityStatsApiDTO.
     * @param minimalEntity a minimal entity dto which contains basic info about an entity
     *                      this is the input
     * @param entityStatsApiDTO the output entity stats api dto to copy data into.
     * @return the output entity stats api dto.
     */
    public static EntityStatsApiDTO populateEntityDataEntityStatsApiDTO(MinimalEntity minimalEntity,
            EntityStatsApiDTO entityStatsApiDTO) {
        entityStatsApiDTO.setUuid(Long.toString(minimalEntity.getOid()));
        entityStatsApiDTO.setClassName(
                ApiEntityType.fromType(minimalEntity.getEntityType()).apiStr());
        entityStatsApiDTO.setDisplayName(minimalEntity.getDisplayName());
        if (minimalEntity.hasEnvironmentType()) {
            final Optional<EnvironmentType> envType = Optional.of(
                    EnvironmentTypeMapper.fromXLToApi(minimalEntity.getEnvironmentType()));
            if (envType.isPresent()) {
                entityStatsApiDTO.setEnvironmentType(envType.get());
            }
        }
        return entityStatsApiDTO;
    }

    /**
     * This method uses data from an api entity and populates data
     * in the output EntityStatsApiDTO.
     * @param apiPartialEntity a partial entity dto which contains basic info about an entity
     *                         this is the input
     * @param entityStatsApiDTO the output entity stats api dto to copy data into.
     * @return the output entity stats api dto.
     */
    public static EntityStatsApiDTO populateEntityDataEntityStatsApiDTO(ApiPartialEntity apiPartialEntity,
            EntityStatsApiDTO entityStatsApiDTO) {
        entityStatsApiDTO.setUuid(Long.toString(apiPartialEntity.getOid()));
        entityStatsApiDTO.setClassName(
                ApiEntityType.fromType(apiPartialEntity.getEntityType()).apiStr());
        entityStatsApiDTO.setDisplayName(apiPartialEntity.getDisplayName());
        if (apiPartialEntity.hasEnvironmentType()) {
            final Optional<EnvironmentType> envType = Optional.of(
                    EnvironmentTypeMapper.fromXLToApi(apiPartialEntity.getEnvironmentType()));
            if (envType.isPresent()) {
                entityStatsApiDTO.setEnvironmentType(envType.get());
            }
        }
        return entityStatsApiDTO;
    }
}
