package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.common.protobuf.utils.StringConstants.CLUSTER;
import static com.vmturbo.common.protobuf.utils.StringConstants.GROUP;
import static com.vmturbo.common.protobuf.utils.StringConstants.STORAGE_CLUSTER;
import static com.vmturbo.common.protobuf.utils.StringConstants.VIRTUAL_MACHINE_CLUSTER;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.aspect.EntityAspectMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory.SupplychainApiDTOFetcherBuilder;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.supplychain.SupplyChainStatsApiInputDTO;
import com.vmturbo.api.dto.supplychain.SupplychainApiDTO;
import com.vmturbo.api.enums.EntitiesCountCriteria;
import com.vmturbo.api.enums.EntityDetailType;
import com.vmturbo.api.enums.EntityState;
import com.vmturbo.api.enums.EnvironmentType;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.serviceinterfaces.ISupplyChainsService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessException;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.auth.api.licensing.LicenseFeature;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainGroupBy;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainStat;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainStat.StatGroup;
import com.vmturbo.common.protobuf.topology.UIEntityState;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.plan.orchestrator.api.PlanUtils;

/**
 * XL implementation of {@link ISupplyChainsService} (for the supply chain APIs).
 */
public class SupplyChainsService implements ISupplyChainsService {

    private static final Logger logger = LogManager.getLogger();

    private static final Set<String> GROUP_TYPES = Sets.newHashSet(GROUP, CLUSTER, STORAGE_CLUSTER,
            VIRTUAL_MACHINE_CLUSTER);

    private final SupplyChainFetcherFactory supplyChainFetcherFactory;
    private final long realtimeTopologyContextId;
    private final GroupExpander groupExpander;
    private final EntityAspectMapper entityAspectMapper;
    private final PlanServiceBlockingStub planRpcService;
    private final SupplyChainStatMapper supplyChainStatMapper;
    private final LicenseCheckClient licenseCheckClient;
    private final Clock clock;

    /**
     * Constructor to create instances of the service.
     *
     * @param supplyChainFetcherFactory Utility to fetch the actual supply chains.
     * @param planRpcService gRPC stub to help determine when an ID refers to a plan.
     * @param realtimeTopologyContextId The realtime context ID to identify "the global realtime topology".
     * @param groupExpander Utility to expand IDs that refer to groups.
     * @param entityAspectMapper Mapper to fill out entities with entity-specific aspect information.
     * @param clock Clock mainly to allow tests to control the time.
     */
    SupplyChainsService(@Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                        @Nonnull final PlanServiceBlockingStub planRpcService,
                        final long realtimeTopologyContextId,
                        @Nonnull final GroupExpander groupExpander,
                        @Nonnull final EntityAspectMapper entityAspectMapper,
                        @Nonnull final LicenseCheckClient licenseCheckClient,
                        @Nonnull final Clock clock) {
        this(supplyChainFetcherFactory, planRpcService, realtimeTopologyContextId,
            groupExpander, entityAspectMapper, clock, new SupplyChainStatMapper(), licenseCheckClient);
    }


    @VisibleForTesting
    SupplyChainsService(@Nonnull final SupplyChainFetcherFactory supplyChainFetcherFactory,
                        @Nonnull final PlanServiceBlockingStub planRpcService,
                        final long realtimeTopologyContextId,
                        @Nonnull final GroupExpander groupExpander,
                        @Nonnull final EntityAspectMapper entityAspectMapper,
                        @Nonnull final Clock clock,
                        @Nonnull final SupplyChainStatMapper supplyChainStatMapper,
                        @Nonnull final LicenseCheckClient licenseCheckClient) {
        this.supplyChainFetcherFactory = supplyChainFetcherFactory;
        this.planRpcService = planRpcService;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.entityAspectMapper = entityAspectMapper;
        this.clock = clock;
        this.supplyChainStatMapper = supplyChainStatMapper;
        this.licenseCheckClient = licenseCheckClient;
    }

    @Override
    public SupplychainApiDTO getSupplyChainByUuids(@Nonnull List<String> uuids,
                                                   @Nullable List<String> entityTypes,
                                                   @Nullable List<EntityState> entityStates,
                                                   @Nullable EnvironmentType environmentType,
                                                   @Nullable EntityDetailType entityDetailType,
                                                   @Nullable List<String> aspectNames,
                                                   @Nullable Boolean includeHealthSummary)
                                                   throws Exception {
        if (uuids.isEmpty()) {
            throw new RuntimeException("UUIDs list is empty");
        }

        // request the supply chain for the items, including expanding groups and clusters
        final SupplychainApiDTOFetcherBuilder fetcherBuilder =
            supplyChainFetcherFactory.newApiDtoFetcher()
                .entityTypes(entityTypes)
                .apiEnvironmentType(environmentType)
                .entityDetailType(entityDetailType)
                .entityStates(entityStates)
                .aspectsToInclude(aspectNames)
                .entityAspectMapper(entityAspectMapper)
                .includeHealthSummary(includeHealthSummary);

        //if the request is for a plan supply chain, the "seed uuid" should instead be used as the topology context ID.
        Optional<PlanInstance> possiblePlan = getPlanIfRequestIsPlan(uuids);
        if (possiblePlan.isPresent()) {
            // this is a plan. Before we go any further, we need to validate that the planner feature is available.
            licenseCheckClient.checkFeatureAvailable(LicenseFeature.PLANNER);
            PlanInstance plan = possiblePlan.get();
            // if we have a user id in the context, we may prevent access to the plan supply chain
            // if the user is either not an admin user or does not own the plan
            if (!PlanUtils.canCurrentUserAccessPlan(plan)) {
                throw new UserAccessException("User does not have access to plan.");
            } else {
                // turn off the access scope filter on the supply chain -- a user can see all entities
                // in a plan they have access to.
                fetcherBuilder.enforceUserScope(false);
            }

            fetcherBuilder.topologyContextId(Long.valueOf(uuids.iterator().next()));
            if (isPlanScoped(plan)) {
                Set<Long> planSeedOids = getSeedIdsForPlan(possiblePlan.get());
                for (Long oid : planSeedOids) {
                    fetcherBuilder.addSeedUuid(String.valueOf(oid));
                }
                if (planSeedOids.size() == 0) {
                    logger.warn("Scoped plan {} did not have any entities in scope.", plan.getPlanId());
                }
            }
        } else {
            fetcherBuilder.topologyContextId(realtimeTopologyContextId).addSeedUuids(uuids);
        }

        return fetcherBuilder.fetch();
    }

    /**
     * check if a PlanInstance is scoped. It's scoped if the plan instance contains a scenario with
     * a scope definition inside.
     * @param planInstance the PlanInstance to check
     * @return true if the plan is scoped, false otherwise
     */
    private boolean isPlanScoped(PlanInstance planInstance) {
        // does this plan have a scope?
        return (planInstance.hasScenario()
                && planInstance.getScenario().hasScenarioInfo()
                && planInstance.getScenario().getScenarioInfo().hasScope());
        }

    /**
     * Given a plan instance, extract the set of seed entities based on the plan scope. If a plan
     * is not scoped, this will be an empty set.
     *
     * @param planInstance the PlanInstance to check the scope of.
     * @return The set of unique seed entities based on the plan scope. Will be empty for an
     * unscoped plan.
     */
    private Set<Long> getSeedIdsForPlan(PlanInstance planInstance) {
        // does this plan have a scope?
        if (!isPlanScoped(planInstance)) {
            // nope, no scope
            return Collections.emptySet();
        }
        Set<Long> seedEntities = new HashSet(); // seed entities to return
        PlanScope scope = planInstance.getScenario().getScenarioInfo().getScope();
        for (PlanScopeEntry scopeEntry : scope.getScopeEntriesList()) {
            // if it's an entity, add it right to the seed set. Otherwise queue it for
            // group resolution.
            if (GROUP_TYPES.contains(scopeEntry.getClassName())) {
                // needs expansion
                groupExpander.expandUuid(String.valueOf(scopeEntry.getScopeObjectOid()))
                        .forEach(seedEntities::add);
            } else {
                // this is an entity -- add it right to the seedEntities
                seedEntities.add(scopeEntry.getScopeObjectOid());
            }
        }

        return seedEntities;
    }

    /**
     * Attempt to retrieve a PlanInstance if the supply chain request refers specifically to a plan.
     *
     * ASSUMPTIONS: A supply chain request refers to a plan if:
     *      - the seed UUID list has exactly one element
     *      - the uuid is a string of numerals
     *      - there exists a plan with that uuid
     *
     * These assumptions may change if the UI/API does.
     *
     * @param uuids the supplied seed UUID list
     * @return an Optional PlanInstance, which will be provided if the uuids did, in fact, represent
     * a plan request. It will be empty if this is not identified as a plan request.
     */
    private Optional<PlanInstance> getPlanIfRequestIsPlan(List<String> uuids) {
        if (uuids.size() != 1) {
            return Optional.empty();
        }
        final long prospectivePlanId;
        try {
            prospectivePlanId = Long.valueOf(uuids.iterator().next());
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        try {
            final OptionalPlanInstance possiblePlan =
                    planRpcService.getPlan(PlanId.newBuilder().setPlanId(prospectivePlanId).build());
            return possiblePlan.hasPlanInstance() ? Optional.of(possiblePlan.getPlanInstance()) : Optional.empty();
        } catch (RuntimeException e) {
            if (e instanceof StatusRuntimeException) {
                // This is a gRPC StatusRuntimeException
                Status status = ((StatusRuntimeException)e).getStatus();
                logger.warn("Unable to get plan {}: {} caused by {}.",
                        prospectivePlanId, status.getDescription(), status.getCause());
            } else {
                logger.error("Error when getting plan {}.", prospectivePlanId, e);
            }
            return Optional.empty();
        }
    }

    /**
     * Return the stats for a supplychain; expand the supplychain to SE's and use those for
     * the Stats query; if requested by the 'groupBy' field, aggregate counts for a
     * list of criteria.
     *
     * @param supplyChainStatsApiInputDTO a description of the supplychain seed uuids
     *                                    and the stats query to execute over that supplychain
     * @return the list of stats request for each snapshot time in the result
     * @throws Exception if the supplychain fetch() operation fails
     */
    @Override
    public List<StatSnapshotApiDTO> getSupplyChainStats(
            final SupplyChainStatsApiInputDTO supplyChainStatsApiInputDTO) throws Exception {

        final SupplychainApiDTOFetcherBuilder fetcherBuilder =
            supplyChainFetcherFactory.newApiDtoFetcher()
                .addSeedUuids(supplyChainStatsApiInputDTO.getUuids())
                .entityTypes(supplyChainStatsApiInputDTO.getTypes())
                .entityStates(supplyChainStatsApiInputDTO.getStates())
                .apiEnvironmentType(supplyChainStatsApiInputDTO.getEnvironmentType());
        final List<SupplyChainGroupBy> groupByCriteria = new ArrayList<>();
        for (EntitiesCountCriteria criteria : CollectionUtils.emptyIfNull(supplyChainStatsApiInputDTO.getGroupBy())) {
            supplyChainStatMapper.countCriteriaToGroupBy(criteria).ifPresent(groupByCriteria::add);
        }

        final List<SupplyChainStat> scStats = fetcherBuilder.fetchStats(groupByCriteria);
        StatSnapshotApiDTO snapshot = new StatSnapshotApiDTO();
        snapshot.setDate(DateTimeUtil.toString(clock.millis()));
        snapshot.setStatistics(scStats.stream()
            .map(supplyChainStatMapper::supplyChainStatToApi)
            .collect(Collectors.toList()));
        return Collections.singletonList(snapshot);
    }

    /**
     * Utility class to help map classes related to supply chain stats.
     * Mainly used to help test things in isolation.
     */
    @VisibleForTesting
    static class SupplyChainStatMapper {

        @Nonnull
        StatApiDTO supplyChainStatToApi(@Nonnull final SupplyChainStat stat) {
            final StatApiDTO apiStat = new StatApiDTO();
            apiStat.setName(StringConstants.ENTITIES);
            apiStat.setValue((float)stat.getNumEntities());
            final StatGroup statGroup = stat.getStatGroup();

            // We don't add null-value filters right now. We can do it if necessary.
            // i.e. if we ask to group by account ID (or other things) and get a SupplyChainStat
            // counting entities NOT owned by an account, we won't add a "businessUnit" filter
            // with an empty/0/null value.

            if (statGroup.hasAccountId()) {
                apiStat.addFilter(EntitiesCountCriteria.businessUnit.name(),
                    Long.toString(statGroup.getAccountId()));
            }
            if (statGroup.hasActionCategory()) {
                apiStat.addFilter(EntitiesCountCriteria.riskSubCategory.name(),
                    ActionSpecMapper.mapXlActionCategoryToApi(statGroup.getActionCategory()));
            }
            if (statGroup.hasTemplate()) {
                apiStat.addFilter(EntitiesCountCriteria.template.name(),
                   statGroup.getTemplate());
            }
            if (statGroup.hasEntityState()) {
                apiStat.addFilter(EntitiesCountCriteria.state.name(),
                    UIEntityState.fromEntityState(statGroup.getEntityState()).apiStr());
            }
            if (statGroup.hasEntityType()) {
                apiStat.addFilter(EntitiesCountCriteria.entityType.name(),
                    ApiEntityType.fromType(statGroup.getEntityType()).apiStr());
            }
            if (statGroup.hasSeverity()) {
                apiStat.addFilter(EntitiesCountCriteria.severity.name(),
                    ActionDTOUtil.getSeverityName(statGroup.getSeverity()));
            }
            if (statGroup.hasTargetId()) {
                apiStat.addFilter(EntitiesCountCriteria.target.name(),
                    Long.toString(statGroup.getTargetId()));
            }
            if (statGroup.hasResourceGroupId()) {
                apiStat.addFilter(EntitiesCountCriteria.resourceGroup.name(),
                        Long.toString(statGroup.getResourceGroupId()));
            }
            return apiStat;
        }

        @Nonnull
        Optional<SupplyChainGroupBy> countCriteriaToGroupBy(@Nonnull final EntitiesCountCriteria criteria)
            throws InvalidOperationException {
            switch (criteria) {
                case entityType:
                    return Optional.of(SupplyChainGroupBy.ENTITY_TYPE);
                case state:
                    return Optional.of(SupplyChainGroupBy.ENTITY_STATE);
                case severity:
                    return Optional.of(SupplyChainGroupBy.SEVERITY);
                case riskSubCategory:
                    return Optional.of(SupplyChainGroupBy.ACTION_CATEGORY);
                case target:
                    return Optional.of(SupplyChainGroupBy.TARGET);
                case businessUnit:
                    return Optional.of(SupplyChainGroupBy.BUSINESS_ACCOUNT_ID);
                case template:
                    return Optional.of(SupplyChainGroupBy.TEMPLATE);
                case resourceGroup:
                    return Optional.of(SupplyChainGroupBy.RESOURCE_GROUP);
                default:
                    logger.error("Unexpected entities count criteria: {}", criteria);
                    throw new InvalidOperationException("Invalid criteria: " + criteria);
            }
        }
    }
}
