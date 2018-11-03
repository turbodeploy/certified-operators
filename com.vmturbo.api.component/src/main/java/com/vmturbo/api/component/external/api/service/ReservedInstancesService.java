package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.api.component.external.api.util.ApiUtils.isGlobalScope;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.ReservedInstanceMapper;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.dto.statistic.EntityStatsApiDTO;
import com.vmturbo.api.dto.statistic.StatApiInputDTO;
import com.vmturbo.api.dto.statistic.StatScopesApiInputDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest;
import com.vmturbo.api.pagination.EntityStatsPaginationRequest.EntityStatsPaginationResponse;
import com.vmturbo.api.serviceinterfaces.IReservedInstancesService;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc.ReservedInstanceSpecServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.reports.db.StringConstants;

public class ReservedInstancesService implements IReservedInstancesService {

    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceBoughtServiceBlockingStub reservedInstanceService;

    private final ReservedInstanceSpecServiceBlockingStub reservedInstanceSpecService;

    private final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService;

    private final ReservedInstanceMapper reservedInstanceMapper;

    private final RepositoryApi repositoryApi;

    private final GroupExpander groupExpander;

    public ReservedInstancesService(
            @Nonnull final ReservedInstanceBoughtServiceBlockingStub reservedInstanceService,
            @Nonnull final ReservedInstanceSpecServiceBlockingStub reservedInstanceSpecService,
            @Nonnull final ReservedInstanceUtilizationCoverageServiceBlockingStub riUtilizationCoverageService,
            @Nonnull final ReservedInstanceMapper reservedInstanceMapper,
            @Nonnull final RepositoryApi repositoryApi,
            @Nonnull final GroupExpander groupExpander) {
        this.reservedInstanceService = Objects.requireNonNull(reservedInstanceService);
        this.reservedInstanceSpecService = Objects.requireNonNull(reservedInstanceSpecService);
        this.riUtilizationCoverageService = Objects.requireNonNull(riUtilizationCoverageService);
        this.reservedInstanceMapper = Objects.requireNonNull(reservedInstanceMapper);
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.groupExpander = Objects.requireNonNull(groupExpander);
    }

    @Override
    public List<ReservedInstanceApiDTO> getReservedInstances(@Nullable String scope) throws Exception {
        final List<ReservedInstanceBought> reservedInstancesBought = getReservedInstancesBought(scope);
        final Set<Long> reservedInstanceSpecIds = reservedInstancesBought.stream()
                .map(ReservedInstanceBought::getReservedInstanceBoughtInfo)
                .map(ReservedInstanceBoughtInfo::getReservedInstanceSpec)
                .collect(Collectors.toSet());

        final List<ReservedInstanceSpec> reservedInstanceSpecs =
                reservedInstanceSpecService.getReservedInstanceSpecByIds(
                        GetReservedInstanceSpecByIdsRequest.newBuilder()
                                .addAllReservedInstanceSpecIds(reservedInstanceSpecIds)
                                .build())
                .getReservedInstanceSpecList();
        final Map<Long, ReservedInstanceSpec> reservedInstanceSpecMap = reservedInstanceSpecs.stream()
                .collect(Collectors.toMap(ReservedInstanceSpec::getId, Function.identity()));
        final Set<Long> relatedEntityIds = getRelatedEntityIds(reservedInstancesBought, reservedInstanceSpecMap);
        // Get full service entity information for RI related entity(such as account, region,
        // availability zones, tier...).
        final Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap = repositoryApi.getServiceEntitiesById(
                ServiceEntitiesRequest.newBuilder(relatedEntityIds).build()).entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()));
        final List<ReservedInstanceApiDTO> results = new ArrayList<>();
        for (ReservedInstanceBought reservedInstanceBought : reservedInstancesBought) {
            results.add(reservedInstanceMapper.mapToReservedInstanceApiDTO(reservedInstanceBought,
                    reservedInstanceSpecMap.get(reservedInstanceBought.getReservedInstanceBoughtInfo()
                            .getReservedInstanceSpec()), serviceEntityApiDTOMap));
        }
        return results;
    }

    @Override
    public ReservedInstanceApiDTO getReservedInstanceByUuid(@Nonnull String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public EntityStatsPaginationResponse getReservedInstancesStats(
                           @Nonnull StatScopesApiInputDTO inputDto,
                           final EntityStatsPaginationRequest paginationRequest) throws Exception {
        if (!isValidInputArgument(inputDto)) {
            throw new InvalidOperationException("Input dto data is not supported: " + inputDto);
        }
        //TODO: support multiple scopes.
        final String scope = inputDto.getScopes().get(0);
        final Optional<Group> groupOptional = groupExpander.getGroup(scope);
        // it has already checked statistics can only has one element.
        final String riStatsName = inputDto.getPeriod().getStatistics().get(0).getName();
        if (riStatsName.equals(StringConstants.NUM_RI)) {
            final Map<Long, Long> reservedInstanceCountMap = getReservedInstanceCountMap(scope, groupOptional);
            final Map<Long, ServiceEntityApiDTO> riServiceEntityApiDtoMap = repositoryApi.getServiceEntitiesById(
                    ServiceEntitiesRequest.newBuilder(reservedInstanceCountMap.keySet()).build())
                    .entrySet().stream()
                    .filter(entry -> entry.getValue().isPresent())
                    .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()));
            final EntityStatsApiDTO result =
                    reservedInstanceMapper.riCountMapToEntityStatsApiDTO(reservedInstanceCountMap,
                            riServiceEntityApiDtoMap);
            return paginationRequest.allResultsResponse(Lists.newArrayList(result));
        } else if (riStatsName.equals(StringConstants.RI_COUPON_UTILIZATION)) {
            // TODO: add the projected reserved instance utilization stats.
            final List<ReservedInstanceStatsRecord> riStatsRecords =
                    getRiUtilizationStats(Long.valueOf(inputDto.getPeriod().getStartDate()),
                            Long.valueOf(inputDto.getPeriod().getEndDate()), scope, groupOptional);
            final EntityStatsApiDTO result =
                    reservedInstanceMapper.convertRIUtilizationStatsRecordsToEntityStatsApiDTO(riStatsRecords,
                            scope, groupOptional);
            return paginationRequest.allResultsResponse(Lists.newArrayList(result));
        } else {
            logger.error("Not support reserved instance stats type: " + riStatsName);
            throw new UnknownObjectException(riStatsName + " is not supported query type.");
        }
    }

    /**
     * Get a list of {@link ReservedInstanceBought} which belong to the input scope.
     *
     * @param scope The scope could be global market, a group, a region, a availability zone or a account.
     * @return a list of {@link ReservedInstanceBought}.
     * @throws UnknownObjectException if the input scope type is not supported.
     */
    private List<ReservedInstanceBought> getReservedInstancesBought(@Nullable final String scope)
            throws UnknownObjectException {
        final Optional<Group> groupOptional = groupExpander.getGroup(scope);
        if (isGlobalScope(scope, groupOptional)) {
            return reservedInstanceService.getReservedInstanceBoughtByFilter(
                    GetReservedInstanceBoughtByFilterRequest.newBuilder().build())
                    .getReservedInstanceBoughtsList();
        } else if (groupOptional.isPresent()) {
            final int groupEntityType = GroupProtoUtil.getEntityType(groupOptional.get());
            final Set<Long> expandedOidsList = groupExpander.expandUuid(scope);
            final GetReservedInstanceBoughtByFilterRequest request =
                    createGetReservedInstanceBoughtByFilterRequest(expandedOidsList, groupEntityType);
            return reservedInstanceService.getReservedInstanceBoughtByFilter(request)
                    .getReservedInstanceBoughtsList();
        } else {
            final ServiceEntityApiDTO scopeEntity = repositoryApi.getServiceEntityForUuid(Long.valueOf(scope));
            final int scopeEntityType = ServiceEntityMapper.fromUIEntityType(scopeEntity.getClassName());
            final GetReservedInstanceBoughtByFilterRequest request =
                    createGetReservedInstanceBoughtByFilterRequest(Sets.newHashSet(Long.valueOf(scope)),
                            scopeEntityType);
            return reservedInstanceService.getReservedInstanceBoughtByFilter(request)
                    .getReservedInstanceBoughtsList();
        }
    }

    /**
     * Get a list of entity id which needs to get full entity information from repository component,
     * such as region, availability zone, account, compute tier entities.
     *
     * @param reservedInstanceBoughts a list of {@link ReservedInstanceBought}.
     * @param reservedInstanceSpecMap a Map which key is spec id, value is {@link ReservedInstanceSpec}.
     * @return a list of entity ids.
     */
    private Set<Long> getRelatedEntityIds(@Nonnull final List<ReservedInstanceBought> reservedInstanceBoughts,
                                          @Nonnull final Map<Long, ReservedInstanceSpec> reservedInstanceSpecMap) {
        final Set<Long> relateEntityIds = new HashSet<>();
        for (ReservedInstanceBought reservedInstanceBought : reservedInstanceBoughts) {
            final ReservedInstanceBoughtInfo reservedInstanceBoughtInfo =
                    reservedInstanceBought.getReservedInstanceBoughtInfo();
            final long riSpecId = reservedInstanceBoughtInfo.getReservedInstanceSpec();
            relateEntityIds.add(reservedInstanceBoughtInfo.getBusinessAccountId());
            if (reservedInstanceBoughtInfo.hasAvailabilityZoneId()) {
                relateEntityIds.add(reservedInstanceBoughtInfo.getAvailabilityZoneId());
            }
            // The reserved instance spec must be exist.
            final ReservedInstanceSpecInfo reservedInstanceSpecInfo =
                    reservedInstanceSpecMap.get(riSpecId).getReservedInstanceSpecInfo();
            relateEntityIds.add(reservedInstanceSpecInfo.getRegionId());
            relateEntityIds.add(reservedInstanceSpecInfo.getTierId());
        }
        return relateEntityIds;
    }

    /**
     * Get the reserved instance count map which belongs to the input scope.
     *
     * @param scope The scope could be global market, a group, a region, a availability zone or a account.
     * @param groupOptional The optional of {@link Group}.
     * @return a Map which key is computer tier id, value is the count of reserved instance bought.
     * @throws UnknownObjectException if scope type is not supported.
     */
    private Map<Long, Long> getReservedInstanceCountMap(@Nonnull final String scope,
                                                        @Nonnull final Optional<Group> groupOptional)
            throws UnknownObjectException {
        if (isGlobalScope(scope, groupOptional)) {
            return reservedInstanceService.getReservedInstanceBoughtCount(
                    GetReservedInstanceBoughtCountRequest.newBuilder().build())
                    .getReservedInstanceCountMapMap();
        } else if (groupOptional.isPresent()) {
            final int groupEntityType = GroupProtoUtil.getEntityType(groupOptional.get());
            final Set<Long> expandedOidsList = groupExpander.expandUuid(scope);
            final GetReservedInstanceBoughtCountRequest request =
                    createGetReservedInstanceBoughtCountRequest(expandedOidsList, groupEntityType);
            return reservedInstanceService.getReservedInstanceBoughtCount(request)
                    .getReservedInstanceCountMapMap();
        } else {
            final ServiceEntityApiDTO scopeEntity = repositoryApi.getServiceEntityForUuid(Long.valueOf(scope));
            final int scopeEntityType = ServiceEntityMapper.fromUIEntityType(scopeEntity.getClassName());
            final GetReservedInstanceBoughtCountRequest request =
                    createGetReservedInstanceBoughtCountRequest(Sets.newHashSet(Long.valueOf(scope)),
                            scopeEntityType);
            return reservedInstanceService.getReservedInstanceBoughtCount(request)
                    .getReservedInstanceCountMapMap();
        }
    }

    /**
     * Create a {@link GetReservedInstanceBoughtCountRequest} based on input filter type and filter ids.
     *
     * @param filterIds a list of ids need to filter by.
     * @param filterType the filter entity type.
     * @return {@link GetReservedInstanceBoughtCountRequest}.
     * @throws UnknownObjectException
     */
    private GetReservedInstanceBoughtCountRequest createGetReservedInstanceBoughtCountRequest(
            @Nonnull final Set<Long> filterIds,
            final int filterType) throws UnknownObjectException {
        final GetReservedInstanceBoughtCountRequest.Builder request =
                GetReservedInstanceBoughtCountRequest.newBuilder();
        if (filterType == EntityDTO.EntityType.REGION_VALUE) {
            request.setRegionFilter(RegionFilter.newBuilder()
                    .addAllRegionId(filterIds));
        } else if (filterType == EntityDTO.EntityType.AVAILABILITY_ZONE_VALUE) {
            request.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                    .addAllAvailabilityZoneId(filterIds));
        } else if (filterType == EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE) {
            request.setAccountFilter(AccountFilter.newBuilder()
                    .addAllAccountId(filterIds));
        } else {
            throw new UnknownObjectException("filter type: "  + filterType + " is not supported.");
        }
        return request.build();
    }

    /**
     * Create a {@link GetReservedInstanceBoughtByFilterRequest} based on input filter type and filter ids.
     *
     * @param filterIds a list of ids need to filter by.
     * @param filterType the filter entity type.
     * @return {@link GetReservedInstanceBoughtByFilterRequest}
     * @throws UnknownObjectException
     */
    private GetReservedInstanceBoughtByFilterRequest createGetReservedInstanceBoughtByFilterRequest(
            @Nonnull final Set<Long> filterIds,
            final int filterType) throws UnknownObjectException {
        final GetReservedInstanceBoughtByFilterRequest.Builder request =
                GetReservedInstanceBoughtByFilterRequest.newBuilder();
        if (filterType == EntityDTO.EntityType.REGION_VALUE) {
            request.setRegionFilter(RegionFilter.newBuilder()
                    .addAllRegionId(filterIds));
        } else if (filterType == EntityDTO.EntityType.AVAILABILITY_ZONE_VALUE) {
            request.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                    .addAllAvailabilityZoneId(filterIds));
        } else if (filterType == EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE) {
            request.setAccountFilter(AccountFilter.newBuilder()
                    .addAllAccountId(filterIds));
        } else {
            throw new UnknownObjectException("filter type: "  + filterType + " is not supported.");
        }
        return request.build();
    }

    /**
     * Get a list of {@link ReservedInstanceStatsRecord} for reserved instance utilization stats request.
     *
     * @param startDateMillis the request start date millis.
     * @param endDateMillis the request end date millis.
     * @param scope the scope of the request.
     * @param groupOptional a optional of {@link Group}.
     * @return a list of {@link ReservedInstanceStatsRecord}.
     * @throws UnknownObjectException if the scope entity is unknown.
     */
    private List<ReservedInstanceStatsRecord> getRiUtilizationStats(
            final long startDateMillis,
            final long endDateMillis,
            @Nonnull final String scope,
            @Nonnull final Optional<Group> groupOptional) throws UnknownObjectException {
        if (isGlobalScope(scope, groupOptional)) {
            return riUtilizationCoverageService.getReservedInstanceUtilizationStats(
                    GetReservedInstanceUtilizationStatsRequest.newBuilder()
                            .setStartDate(startDateMillis)
                            .setEndDate(endDateMillis)
                            .build())
                    .getReservedInstanceStatsRecordsList();
        } else if (groupOptional.isPresent()) {
            final int groupEntityType = GroupProtoUtil.getEntityType(groupOptional.get());
            final Set<Long> expandedOidsList = groupExpander.expandUuid(scope);
            final GetReservedInstanceUtilizationStatsRequest request =
                    createGetReservedInstanceUtilizationStatsRequest(startDateMillis, endDateMillis,
                            expandedOidsList, groupEntityType);
            return riUtilizationCoverageService.getReservedInstanceUtilizationStats(request)
                    .getReservedInstanceStatsRecordsList();
        } else {
            final ServiceEntityApiDTO scopeEntity = repositoryApi.getServiceEntityForUuid(Long.valueOf(scope));
            final int scopeEntityType = ServiceEntityMapper.fromUIEntityType(scopeEntity.getClassName());
            final GetReservedInstanceUtilizationStatsRequest request =
                    createGetReservedInstanceUtilizationStatsRequest(startDateMillis, endDateMillis,
                            Sets.newHashSet(Long.valueOf(scope)), scopeEntityType);
            return riUtilizationCoverageService.getReservedInstanceUtilizationStats(request)
                    .getReservedInstanceStatsRecordsList();
        }
    }

    /**
     * Create a {@link GetReservedInstanceUtilizationStatsRequest} based on input parameters.
     *
     * @param startDateMillis the request start date millis.
     * @param endDateMillis the request end date millis.
     * @param filterIds a list of filter ids.
     * @param filterType the filter type.
     * @return a {@link GetReservedInstanceUtilizationStatsRequest}.
     * @throws UnknownObjectException if the filter type is unknown.
     */
    private GetReservedInstanceUtilizationStatsRequest createGetReservedInstanceUtilizationStatsRequest(
            final long startDateMillis,
            final long endDateMillis,
            @Nonnull final Set<Long> filterIds,
            final int filterType)  throws UnknownObjectException {
        final GetReservedInstanceUtilizationStatsRequest.Builder request =
                GetReservedInstanceUtilizationStatsRequest.newBuilder()
                        .setStartDate(startDateMillis)
                        .setEndDate(endDateMillis);
        if (filterType == EntityDTO.EntityType.REGION_VALUE) {
            request.setRegionFilter(RegionFilter.newBuilder()
                    .addAllRegionId(filterIds));
        } else if (filterType == EntityDTO.EntityType.AVAILABILITY_ZONE_VALUE) {
            request.setAvailabilityZoneFilter(AvailabilityZoneFilter.newBuilder()
                    .addAllAvailabilityZoneId(filterIds));
        } else if (filterType == EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE) {
            request.setAccountFilter(AccountFilter.newBuilder()
                    .addAllAccountId(filterIds));
        } else {
            throw new UnknownObjectException("filter type: "  + filterType + " is not supported.");
        }
        return request.build();
    }

    /**
     * Check if input dto is invalid or not.
     *
     * @param inputDto {@link StatScopesApiInputDTO}.
     * @return a true if it is valid, otherwise return false;
     */
    private boolean isValidInputArgument( @Nonnull StatScopesApiInputDTO inputDto) {
        if (inputDto.getScopes() != null && inputDto.getScopes().size() > 0
                && inputDto.getPeriod() != null && inputDto.getPeriod().getStatistics() != null) {
            final List<StatApiInputDTO> statApiInputDTOS = inputDto.getPeriod().getStatistics();
            if (statApiInputDTOS.size() == 1
                    && statApiInputDTOS.get(0).getName().equals(StringConstants.NUM_RI)
                    && statApiInputDTOS.get(0).getGroupBy() != null
                    && statApiInputDTOS.get(0).getGroupBy().size() == 1
                    && statApiInputDTOS.get(0).getGroupBy().get(0).equals(StringConstants.TEMPLATE)) {
                return true;
            }
            if (statApiInputDTOS.size() == 1
                    && statApiInputDTOS.get(0).getName().equals(StringConstants.RI_COUPON_UTILIZATION)) {
                return true;
            }
        }
        return false;
    }
}