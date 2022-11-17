package com.vmturbo.cost.component.savings;

import java.io.IOException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.SetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopology;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesResponse;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.cost.component.db.tables.records.EntityCloudScopeRecord;
import com.vmturbo.cost.component.entity.scope.SQLEntityCloudScopedStore;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.savings.calculator.Calculator;
import com.vmturbo.cost.component.savings.calculator.SavingsValues;
import com.vmturbo.cost.component.savings.calculator.StorageAmountResolver;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Processes a chunk of entity stats for a set of given time periods.
 */
public class SavingsTracker extends SQLEntityCloudScopedStore implements ScenarioDataHandler {
    private final Logger logger = LogManager.getLogger();

    /**
     * For billing record queries.
     */
    private final BillingRecordStore billingRecordStore;

    /**
     * Action chain interface.
     */
    private final ActionChainStore actionChainStore;

    /**
     * Stats writing interface.
     */
    private final SavingsStore savingsStore;

    /**
     * Clock.
     */
    private final Clock clock;

    /**
     * Bill-based savings calculator.
     */
    private Calculator calculator;

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    private final RepositoryClient repositoryClient;

    private final long realtimeTopologyContextId;

    /**
     * Set of supported cloud service providers.
     */
    private final Set<String> supportedCSPs;

    /**
     * OIDs of supported cloud service providers.
     */
    private final Set<Long> supportedCspOids = new HashSet<>();

    /**
     * Search service client, used for name to OID resolution.
     */
    private final SearchServiceBlockingStub searchServiceStub;

    private final Set<EntityType> supportedBillingEntityTypes;

    /**
     * Creates a new tracker.
     *
     * @param billingRecordStore Store for billing records.
     * @param actionChainStore Action chain store.
     * @param savingsStore Writer for final stats.
     * @param supportedBillingEntityTypes supports entity types
     * @param supportedBillingCSPs supported CPSs
     * @param deleteActionRetentionMs length of time for accruing savings for delete actions in milliseconds
     * @param clock clock
     * @param cloudTopologyFactory cloud topology factory
     * @param repositoryClient repository client
     * @param dsl Jooq DSL context
     * @param priceTableKeyStore price table key store
     * @param priceTableStore price table store
     * @param searchServiceStub search service
     * @param realtimeTopologyContextId realtime topology context ID
     * @param chunkSize chunk size
     */
    public SavingsTracker(@Nonnull final BillingRecordStore billingRecordStore,
            @Nonnull ActionChainStore actionChainStore,
            @Nonnull final SavingsStore savingsStore,
            @Nonnull Set<EntityType> supportedBillingEntityTypes,
            @Nonnull Set<String> supportedBillingCSPs,
            long deleteActionRetentionMs,
            @Nonnull Clock clock,
            @Nonnull TopologyEntityCloudTopologyFactory cloudTopologyFactory,
            @Nonnull RepositoryClient repositoryClient,
            @Nonnull final DSLContext dsl,
            @Nonnull BusinessAccountPriceTableKeyStore priceTableKeyStore,
            @Nonnull PriceTableStore priceTableStore,
            @Nonnull SearchServiceBlockingStub searchServiceStub,
            long realtimeTopologyContextId,
            final int chunkSize) {
        super(dsl, chunkSize);
        this.billingRecordStore = billingRecordStore;
        this.actionChainStore = actionChainStore;
        this.savingsStore = savingsStore;
        this.clock = clock;
        this.cloudTopologyFactory = cloudTopologyFactory;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.repositoryClient = repositoryClient;
        this.searchServiceStub = searchServiceStub;
        StorageAmountResolver storageAmountResolver = new StorageAmountResolver(priceTableKeyStore, priceTableStore);
        this.calculator = new Calculator(deleteActionRetentionMs, clock, storageAmountResolver);
        this.supportedBillingEntityTypes = supportedBillingEntityTypes;
        this.supportedCSPs = supportedBillingCSPs;
    }

    /**
     * Allow test cases to pass in a different calculator for mocking purpose.
     *
     * @param calculator storage amount resolver
     */
    void setCalculator(Calculator calculator) {
        this.calculator = calculator;
    }

    /**
     * Process savings for a given list of entity OIDs. A chunk of entities is processed at a time.
     *
     * @param entityIds OIDs of entities to be processed.
     * @param savingsTimes Contains timing related info used for query, stores responses as well.
     * @param chunkCounter Counter for current chunk, for logging.
     * @throws EntitySavingsException Thrown on DB error.
     */
    void processSavings(@Nonnull final Set<Long> entityIds,
            @Nonnull final SavingsTimes savingsTimes, @Nonnull final AtomicInteger chunkCounter)
            throws EntitySavingsException {
        long previousLastUpdated = savingsTimes.getPreviousLastUpdatedTime();
        long lastUpdatedEndTime = savingsTimes.getLastUpdatedEndTime();
        logger.trace("{}: Processing savings for a chunk of {} entities with last updated time between {} and {}.",
                () -> chunkCounter, entityIds::size, () -> previousLastUpdated,
                () -> lastUpdatedEndTime);

        // Get billing records in this time range, mapped by entity id.
        final Map<Long, Set<BillingRecord>> billingRecords = new HashMap<>();

        // For this set of billing records, see if we have any last_updated times that are newer.
        final AtomicLong newLastUpdated = new AtomicLong(savingsTimes.getCurrentLastUpdatedTime());
        billingRecordStore.getUpdatedBillRecords(previousLastUpdated, lastUpdatedEndTime, entityIds)
                .filter(BillingRecord::isValid)
                .filter(this::isSupportedBillingEntityType)
                .filter(this::isSupportedCSP)
                .forEach(record -> {
                    if (record.getLastUpdated() != null
                            && record.getLastUpdated() > newLastUpdated.get()) {
                        newLastUpdated.set(record.getLastUpdated());
                    }
                    billingRecords.computeIfAbsent(record.getEntityId(), e -> new HashSet<>())
                            .add(record);
                });
        savingsTimes.setCurrentLastUpdatedTime(newLastUpdated.get());

        // Get map of entity id to sorted list of actions for it, starting with first executed.
        final Map<Long, NavigableSet<ExecutedActionsChangeWindow>> actionChains = actionChainStore
                .getActionChains(entityIds);

        // Get the timestamp of the day (beginning of the day) that was last processed.
        // Need this date for delete action savings calculation.
        long lastProcessedDate = savingsTimes.getLastRollupTimes().getLastTimeByDay();
        final Set<Long> statTimes = processSavings(entityIds, billingRecords, actionChains,
                lastProcessedDate, TimeUtil.millisToLocalDateTime(lastUpdatedEndTime, clock));

        // Save off the day stats timestamps for all stats written this time, used for rollups.
        savingsTimes.addAllDayStatsTimes(statTimes);
    }

    private Set<Long> processSavings(@Nonnull final Set<Long> entityOids,
            Map<Long, Set<BillingRecord>> billingRecords,
            Map<Long, NavigableSet<ExecutedActionsChangeWindow>> actionChains,
            long lastProcessedDate, LocalDateTime periodEndTime) throws EntitySavingsException {
        final List<SavingsValues> allSavingsValues = new ArrayList<>();
        entityOids.forEach(entityId -> {
            Set<BillingRecord> entityBillingRecords = billingRecords.getOrDefault(entityId,
                    Collections.emptySet());
            NavigableSet<ExecutedActionsChangeWindow> entityActionChain = actionChains.get(entityId);
            if (SetUtils.emptyIfNull(entityActionChain).isEmpty()) {
                return;
            }
            final List<SavingsValues> values = calculator.calculate(entityId, entityBillingRecords,
                    entityActionChain, lastProcessedDate, periodEndTime);
            logger.trace("{} savings values for entity {}, {} bill records, {} actions.",
                    values::size, () -> entityId, entityBillingRecords::size,
                    entityActionChain::size);
            allSavingsValues.addAll(values);
            logger.trace("Savings stats for entity {}:\n{}\n{}", () -> entityId,
                    SavingsValues::toCsvHeader,
                    () -> values.stream()
                            .sorted(Comparator.comparing(SavingsValues::getTimestamp))
                            .map(SavingsValues::toCsv)
                            .collect(Collectors.joining("\n")));

        });
        //Check if the record is already present or insert a new record into cloud scope table
        insertCloudScopeRecords(entityOids);

        // Once we are done processing all the states for this period, we write stats.
        return savingsStore.writeDailyStats(allSavingsValues);
    }

    /**
     * Process given list of entity states. This can only be invoked when the
     * ENABLE_SAVINGS_TEST_INPUT feature flag is enabled.
     *
     * @param participatingUuids list of UUIDs involved in the injected scenario
     * @param startTime starting time of the injected scenario
     * @param endTime ending time of the injected scenario
     * @param actionChains action chain
     * @param billRecordsByEntity bill records of each entity
     * @throws EntitySavingsException Errors with generating or writing stats
     */
    @Override
    public void processSavings(@Nonnull Set<Long> participatingUuids,
            @Nonnull LocalDateTime startTime, @Nonnull LocalDateTime endTime,
            @Nonnull final Map<Long, NavigableSet<ExecutedActionsChangeWindow>> actionChains,
            @Nonnull final Map<Long, Set<BillingRecord>> billRecordsByEntity)
            throws EntitySavingsException {
        logger.info("Scenario generator invoked for the period of {} to {} on UUIDs: {}",
                startTime, endTime, participatingUuids);
        final Map<Long, NavigableSet<ExecutedActionsChangeWindow>> actions = new HashMap<>(actionChains);
        final Map<Long, Set<BillingRecord>> billRecords = new HashMap<>(billRecordsByEntity);
        if (actions.isEmpty()) {
            logger.info("No actions are defined in the scenario. Get action and bill data from the database.");
            // If no action chains are passed in, we will use the data in the database.
            // Get billing records in this time range, mapped by entity id.
            billingRecordStore.getBillRecords(startTime, endTime, participatingUuids)
                    .filter(BillingRecord::isValid)
                    .filter(this::isSupportedBillingEntityType)
                    .filter(this::isSupportedCSP)
                    .forEach(record -> billRecords.computeIfAbsent(record.getEntityId(), e -> new HashSet<>())
                            .add(record));

            // Get map of entity id to sorted list of actions for it, starting with first executed.
            actions.putAll(actionChainStore.getActionChains(participatingUuids));
        }

        processSavings(participatingUuids, billRecords, actions,
                TimeUtil.localTimeToMillis(startTime.truncatedTo(ChronoUnit.DAYS).minusDays(1),
                        Clock.systemUTC()), endTime);
    }

    /**
     * Purge savings stats for the indicated UUIDs in preparation for processing injected data.  This can
     * only be invoked when the ENABLE_SAVINGS_TEST_INPUT feature flag is enabled.
     *
     * @param uuids UUIDs to purge.
     */
    @Override
    public void purgeState(Set<Long> uuids) {
        logger.debug("Purge savings stats for UUIDs in preparation for data injection: {}",
                uuids);
        if (!uuids.isEmpty()) {
            logger.info("Purging savings stats for UUIDs: {}", uuids);
            savingsStore.deleteStats(uuids);
        }
    }

    /**
     * Create EntityCloud Topology.
     * @param entityOids entityOids
     * @return EntityTopology for a set of Oids .
     */
    public TopologyEntityCloudTopology createCloudTopology(Set<Long> entityOids) {
        // The cloud topology requires the list of OIDs of entities (VMs, volumes, DBs, DBSs) and
        // their associated accounts, availability zones (if applicable), regions and service providers.

        // Find all availability zones and business accounts associated with the entities.
        Stream<TopologyEntityDTO> workloadEntities =
                repositoryClient.retrieveTopologyEntities(new ArrayList<>(entityOids), realtimeTopologyContextId);
        List<Long> availabilityZoneOids = workloadEntities.flatMap(entity -> entity.getConnectedEntityListList().stream())
                .filter(connEntity -> connEntity.getConnectedEntityType() == EntityType.AVAILABILITY_ZONE_VALUE)
                .map(ConnectedEntity::getConnectedEntityId)
                .distinct()
                .collect(Collectors.toList());

        // Find all related accounts.
        Set<Long> accountOids = repositoryClient.getAllBusinessAccountOidsInScope(entityOids);

        // Get all regions and service provider entities.
        // Note that we get all regions and all service providers instead of only those
        // associated with the entities.
        // It is because the number of regions and service providers is finite.
        // The logic to find the connected regions of availability zones requires all regions anyways.
        List<Long> regionAndAServiceProviderOids =
                repositoryClient.getEntitiesByType(Arrays.asList(EntityType.REGION, EntityType.SERVICE_PROVIDER))
                        .map(TopologyEntityDTO::getOid)
                        .collect(Collectors.toList());

        List<Long> entityOidList = new ArrayList<>(entityOids);
        entityOidList.addAll(availabilityZoneOids);
        entityOidList.addAll(accountOids);
        entityOidList.addAll(regionAndAServiceProviderOids);

        return cloudTopologyFactory.newCloudTopology(
                repositoryClient.retrieveTopologyEntities(entityOidList, realtimeTopologyContextId));
    }

    private void insertCloudScopeRecords(Set<Long> entityOids) throws EntitySavingsException {
        List<EntityCloudScopeRecord> scopeRecords;
        if (Objects.isNull(repositoryClient)) {
            // For Unit Tests
            scopeRecords = entityOids.stream()
                    .map(entityOid -> createCloudScopeRecord(entityOid, null))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } else {
            // Check if the record is present in the table or create a new record
            TopologyEntityCloudTopology cloudTopology = createCloudTopology(entityOids);
            scopeRecords = entityOids.stream()
                    .map(entityOid -> createCloudScopeRecord(entityOid, cloudTopology))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }

        try {
            insertCloudScopeRecords(scopeRecords);
        } catch (IOException e) {
            throw new EntitySavingsException("Error occurred when writing to entity_cloud_scope table.", e);
        }
    }

    /**
     * Create the EntityCloudScopeRecord to be inserted or updated in the entity_cloud_scope table.
     * If the method is called by a test, the cloudTopology parameter is null and a record with
     * dummy values will be returned. For the production workflow, the cloudTopology parameter
     * should be provided.
     *
     * @param entityOid Entity OID
     * @param cloudTopology cloud topology
     * @return EntityCloudScopeRecord to be inserted or updated
     */
    @Nullable
    private EntityCloudScopeRecord createCloudScopeRecord(Long entityOid,
            @Nullable TopologyEntityCloudTopology cloudTopology) {
        Integer entityType;
        Long serviceProviderOid;
        Long regionOid;
        Optional<Long> availabilityZoneOid = Optional.empty();
        Long accountOid;
        Optional<Long> resourceGroupOid;
        if (Objects.isNull(cloudTopology)) {
            /*
             * To implement scope for the test entities, create scope based upon the entity's UUID,
             * divide the UUID by the following:
             *  - service provider  100,000
             *  - region            10,000
             *  - account           1,000
             *  - resource group    100
             */
            entityType = EntityType.VIRTUAL_MACHINE_VALUE;
            serviceProviderOid = entityOid / 100000L;
            regionOid = entityOid / 10000L;
            accountOid = entityOid / 1000L;
            resourceGroupOid = Optional.of(entityOid / 100L);
        } else if (cloudTopology.getEntity(entityOid).isPresent()) {
            entityType = cloudTopology.getEntity(entityOid).map(TopologyEntityDTO::getEntityType).orElse(null);

            // Get the service provider OID.
            Optional<TopologyEntityDTO> serviceProvider = cloudTopology.getServiceProvider(entityOid);
            serviceProviderOid = serviceProvider.map(TopologyEntityDTO::getOid).orElse(null);

            // Get the region OID.
            Optional<TopologyEntityDTO> region = cloudTopology.getConnectedRegion(entityOid);
            regionOid = region.map(TopologyEntityDTO::getOid).orElse(null);

            // Get the availability zone OID.
            Optional<TopologyEntityDTO> availabilityZone = cloudTopology.getConnectedAvailabilityZone(entityOid);
            availabilityZoneOid = availabilityZone.map(TopologyEntityDTO::getOid);

            // Get the account OID.
            Optional<TopologyEntityDTO> businessAccount = cloudTopology.getOwner(entityOid);
            accountOid = businessAccount.map(TopologyEntityDTO::getOid).orElse(null);

            // Get the resource group OID.
            Optional<GroupAndMembers> resourceGroup = cloudTopology.getResourceGroup(entityOid);
            resourceGroupOid = resourceGroup.map(groupAndMembers -> groupAndMembers.group().getId());
        } else {
            // The entity is not in the topology.
            logger.debug("Cannot create entity cloud scope record for entity {} because it is not found in topology.",
                    entityOid);
            return null;
        }

        if (entityType != null && serviceProviderOid != null && regionOid != null && accountOid != null) {
            return createCloudScopeRecord(entityOid, entityType, accountOid, regionOid,
                    availabilityZoneOid, serviceProviderOid, resourceGroupOid, LocalDateTime.now());
        }
        logger.warn("Cannot create entity cloud scope record because required information is missing."
                        + " EntityOid={}, EntityType={}, serviceProviderOid={}, regionOid={}, accountOid={}",
                entityOid, entityType, serviceProviderOid, regionOid, accountOid);

        return null;
    }

    /**
     * Check if CSP of a bill record is one that is supported for savings calculation.
     *
     * @param record a bill record
     * @return true is it is one of the supported CSPs, false otherwise.
     */
    boolean isSupportedCSP(BillingRecord record) {
        if (supportedCspOids.isEmpty()) {
            // Populate the support OID set only if it has not been initialized.
            populateSupportedCspOids();
        }
        long cspId = record.getServiceProviderId();
        return supportedCspOids.contains(cspId);
    }

    /**
     * Get the OIDs of the supported CSPs and save the OIDs in memory.
     */
    void populateSupportedCspOids() {
        Set<String> entityTypes = ImmutableSet.of(StringConstants.SERVICE_PROVIDER);
        PropertyFilter entityTypeFilter = SearchProtoUtil.entityTypeFilter(entityTypes);

        PropertyFilter cspFilter = SearchProtoUtil
                .stringPropertyFilterExact(SearchableProperties.DISPLAY_NAME, supportedCSPs);
        SearchFilter searchFilter = SearchProtoUtil.searchFilterProperty(cspFilter);

        SearchParameters.Builder parametersBuilder = SearchParameters.newBuilder()
                .setStartingFilter(entityTypeFilter)
                .addSearchFilter(searchFilter);

        SearchQuery.Builder searchQueryBuilder = SearchQuery.newBuilder()
                .addSearchParameters(parametersBuilder);

        Search.SearchEntitiesRequest searchRequest = SearchEntitiesRequest.newBuilder()
                .setSearch(searchQueryBuilder)
                .setReturnType(Type.MINIMAL)
                .build();

        SearchEntitiesResponse response = searchServiceStub.searchEntities(searchRequest);
        if (response != null) {
            for (PartialEntity partialEntity : response.getEntitiesList()) {
                MinimalEntity entity = partialEntity.getMinimal();
                supportedCspOids.add(entity.getOid());
            }
        }
    }

    private boolean isSupportedBillingEntityType(BillingRecord record) {
        return supportedBillingEntityTypes.stream().anyMatch(entityType -> (entityType.getNumber() == record.getEntityType()));
    }
}
