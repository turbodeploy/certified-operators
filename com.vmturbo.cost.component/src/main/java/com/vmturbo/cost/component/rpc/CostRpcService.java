package com.vmturbo.cost.component.rpc;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.DeletePlanEntityCostsRequest;
import com.vmturbo.common.protobuf.cost.Cost.DeletePlanEntityCostsResponse;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsRecord.SavingsRecord;
import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest.GroupByType;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntitySavingsStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesResponse;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceImplBase;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.discount.DiscountNotFoundException;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.discount.DuplicateAccountIdException;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.entity.cost.PlanProjectedEntityCostStore;
import com.vmturbo.cost.component.entity.cost.ProjectedEntityCostStore;
import com.vmturbo.cost.component.expenses.AccountExpensesStore;
import com.vmturbo.cost.component.savings.AggregatedSavingsStats;
import com.vmturbo.cost.component.savings.EntitySavingsException;
import com.vmturbo.cost.component.savings.EntitySavingsStore;
import com.vmturbo.cost.component.savings.EntitySavingsTracker;
import com.vmturbo.cost.component.util.AccountExpensesFilter.AccountExpenseFilterBuilder;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.cost.component.util.CostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter.EntityCostFilterBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.sql.utils.DbException;

/**
 * Implements the RPC calls supported by the cost component for retrieving Cloud cost data.
 */
public class CostRpcService extends CostServiceImplBase {

    private static final String ERR_MSG = "Invalid discount deletion input: No associated account ID or discount ID specified";

    private static final String NO_ASSOCIATED_ACCOUNT_ID_OR_DISCOUNT_INFO_PRESENT = "No discount info present.";

    private static final String INVALID_ARGUMENTS_FOR_DISCOUNT_UPDATE = "Invalid arguments for discount update";

    private static final String CREATING_A_DISCOUNT_WITH_ASSOCIATED_ACCOUNT_ID = "Creating a discount: {} with associated account id: {}";

    private static final String DELETING_A_DISCOUNT = "Deleting a discount {}: {}";

    private static final String UPDATING_A_DISCOUNT = "Updating a discount: {}";

    private static final String FAILED_TO_UPDATE_DISCOUNT = "Failed to update discount ";

    private static final String FAILED_TO_FIND_THE_UPDATED_DISCOUNT = "Failed to find the updated discount";

    private static final int PROJECTED_STATS_TIME_IN_FUTURE_HOURS = 1;

    private static final Logger logger = LogManager.getLogger();

    private final DiscountStore discountStore;

    private final AccountExpensesStore accountExpensesStore;

    private final EntityCostStore entityCostStore;

    private final ProjectedEntityCostStore projectedEntityCostStore;

    private final PlanProjectedEntityCostStore planProjectedEntityCostStore;

    private final BusinessAccountHelper businessAccountHelper;

    private final TimeFrameCalculator timeFrameCalculator;

    private final Clock clock;

    private final long realtimeTopologyContextId;

    private final int maxNumberOfInnerStatRecords;

    private final EntitySavingsStore entitySavingsStore;

    /**
     * Create a new RIAndExpenseUploadRpcService.
     *
     * @param discountStore The store containing account discounts
     * @param accountExpensesStore The store containing account expenses
     * @param costStoreHouse Entity cost store
     * @param projectedEntityCostStore Projected entity cost store
     * @param planProjectedEntityCostStore Plan projected entity cost store
     * @param timeFrameCalculator Time frame calculator
     * @param businessAccountHelper Business account helper
     * @param clock A clock providing access to the current instant, date and time using a time-zone
     * @param realtimeTopologyContextId real-time topology context ID
     * @param maxNumberOfInnerStatRecords maximum number of stats transferred in a single GRPC message
     * @param savingsStore Entity Savings DB store.
     */
    public CostRpcService(@Nonnull final DiscountStore discountStore,
                          @Nonnull final AccountExpensesStore accountExpensesStore,
                          @Nonnull final EntityCostStore costStoreHouse,
                          @Nonnull final ProjectedEntityCostStore projectedEntityCostStore,
                          @Nonnull final PlanProjectedEntityCostStore planProjectedEntityCostStore,
                          @Nonnull final TimeFrameCalculator timeFrameCalculator,
                          @Nonnull final BusinessAccountHelper businessAccountHelper,
                          @Nonnull final Clock clock,
                          final long realtimeTopologyContextId,
                          final int maxNumberOfInnerStatRecords,
                          @Nonnull final EntitySavingsStore savingsStore) {
        this.discountStore = Objects.requireNonNull(discountStore);
        this.accountExpensesStore = Objects.requireNonNull(accountExpensesStore);
        this.entityCostStore = Objects.requireNonNull(costStoreHouse);
        this.projectedEntityCostStore = Objects.requireNonNull(projectedEntityCostStore);
        this.planProjectedEntityCostStore = Objects.requireNonNull(planProjectedEntityCostStore);
        this.businessAccountHelper = Objects.requireNonNull(businessAccountHelper);
        this.timeFrameCalculator = Objects.requireNonNull(timeFrameCalculator);
        this.clock = Objects.requireNonNull(clock);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.maxNumberOfInnerStatRecords = maxNumberOfInnerStatRecords;
        this.entitySavingsStore = savingsStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createDiscount(CreateDiscountRequest request,
                               StreamObserver<CreateDiscountResponse> responseObserver) {
        logger.info(CREATING_A_DISCOUNT_WITH_ASSOCIATED_ACCOUNT_ID,
                request.getDiscountInfo(), request.getId());
        if (!request.hasId() || !request.hasDiscountInfo()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(NO_ASSOCIATED_ACCOUNT_ID_OR_DISCOUNT_INFO_PRESENT).asRuntimeException());
            return;
        }
        try {
            final Discount discount = discountStore.persistDiscount(request.getId(), request.getDiscountInfo());
            responseObserver.onNext(CreateDiscountResponse.newBuilder()
                    .setDiscount(discount)
                    .build());
            responseObserver.onCompleted();
        } catch (DuplicateAccountIdException e) {
            responseObserver.onError(Status.ALREADY_EXISTS
                    .withDescription(e.getMessage())
                    .asException());
        } catch (DbException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    @Override
    public void getTierPriceForEntities(GetTierPriceForEntitiesRequest request, StreamObserver<GetTierPriceForEntitiesResponse> responseObserver) {
        Set<Long> entityOids = new HashSet<>(request.getOidsList());
        final boolean isPlanRequest = request.hasTopologyContextId()
                && request.getTopologyContextId() != realtimeTopologyContextId;
        CostCategory category = request.getCostCategory();
        try {
            Map<Long, Map<Long, EntityCost>> queryResult =
                entityCostStore.getEntityCosts(EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST,
                        realtimeTopologyContextId)
                    .entityIds(entityOids)
                    .costCategoryFilter(CostCategoryFilter.newBuilder()
                            .setExclusionFilter(false)
                            .addCostCategory(category)
                            .build())
                    .latestTimestampRequested(true)
                    .costSources(false,
                        Collections.singleton(CostSource.ON_DEMAND_RATE.getNumber()))
                    .build());
            Map<Long, EntityCost> beforeEntityCostbyOid = new HashMap<>();
            // Because we requested latest timestamp, every entity will have only one record.
            queryResult.forEach((timestamp, costByIdMap) ->
                    costByIdMap.forEach((entityId, entityCost) -> {
                        beforeEntityCostbyOid.put(entityId, entityCost);
            }));
            final Map<Long, EntityCost> afterEntityCostbyOid;
            if (isPlanRequest) {
                afterEntityCostbyOid = planProjectedEntityCostStore
                        .getPlanProjectedEntityCosts(entityOids, request.getTopologyContextId());
            } else {
                final EntityCostFilterBuilder filterBuilder = createEntityCostFilter(request);
                afterEntityCostbyOid = projectedEntityCostStore.getProjectedEntityCosts(filterBuilder.build());
            }
            Map<Long, CurrencyAmount> beforeCurrencyAmountByOid = new HashMap<>();
            Map<Long, CurrencyAmount> afterCurrencyAmountByOid = new HashMap<>();
            for (Map.Entry<Long, EntityCost> entry : beforeEntityCostbyOid.entrySet()) {
                Long id = entry.getKey();
                EntityCost cost = entry.getValue();
                if (!CollectionUtils.isEmpty(cost.getComponentCostList())) {
                    // Since the entity cost here is queried via the request cost category and
                    // the on demand rate cost source, the firs index in the component cost list
                    // should give us the correct cost for the category and in demand rate,
                    CurrencyAmount amount = cost.getComponentCost(0).getAmount();
                    beforeCurrencyAmountByOid.put(id, amount);
                } else {
                    logger.error("No costs could be retrieved from database for entity having oid {}" +
                            " .This may result in the on demand rate not being displayed on the UI.", request.getOidsList());
                }
            }
            for (Map.Entry<Long, EntityCost> entry : afterEntityCostbyOid.entrySet()) {
                Long id = entry.getKey();
                EntityCost cost = entry.getValue();
                // Since we don't query the cost here via any cost category or cost source filters, we
                // need to filter out for the on Demand rate source and the cost category of the input request.
                ComponentCost componentCost = cost.getComponentCostList().stream().filter(s -> s.getCategory().equals(category)
                        && s.getCostSource().equals(CostSource.ON_DEMAND_RATE)).findFirst().orElse(null);
                if (componentCost != null) {
                    afterCurrencyAmountByOid.put(id, componentCost.getAmount());
                } else {
                    logger.debug("Projected on demand rate could not be found for entity having oid {}." +
                            " This may result in the on demand rate not being displayed on the UI.", request.getOidsList());
                }
            }
            responseObserver.onNext(GetTierPriceForEntitiesResponse.newBuilder()
                    .putAllBeforeTierPriceByEntityOid(beforeCurrencyAmountByOid).putAllAfterTierPriceByEntityOid(afterCurrencyAmountByOid).build());
            responseObserver.onCompleted();
        } catch (DbException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDiscount(DeleteDiscountRequest request,
                               StreamObserver<DeleteDiscountResponse> responseObserver) {
        // ensure request has either associated account or discount id
        if ((!request.hasAssociatedAccountId() && !request.hasDiscountId())) {
            logger.error(ERR_MSG);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(ERR_MSG).asRuntimeException());
            return;
        }
        try {
            if (request.hasDiscountId()) {
                logger.info(DELETING_A_DISCOUNT, "by ID", request.getDiscountId());
                discountStore.deleteDiscountByDiscountId(request.getDiscountId());
            } else {
                logger.info(DELETING_A_DISCOUNT, "by associated account ID", request.getDiscountId());
                discountStore.deleteDiscountByAssociatedAccountId(request.getAssociatedAccountId());
            }
            responseObserver.onNext(DeleteDiscountResponse.newBuilder()
                    .setDeleted(true)
                    .build());
            responseObserver.onCompleted();
        } catch (DiscountNotFoundException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getMessage())
                    .asException());
        } catch (DbException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getDiscounts(GetDiscountRequest request,
                             StreamObserver<Discount> responseObserver) {
        try {
            if (request.hasFilter()) {
                request.getFilter().getAssociatedAccountIdList().forEach(id -> {
                    try {
                        discountStore.getDiscountByAssociatedAccountId(id).forEach(responseObserver::onNext);
                    } catch (DbException e) {
                        throw new RuntimeException(e);
                    }
                });
            } else {
                discountStore.getAllDiscount().forEach(responseObserver::onNext);
            }
            responseObserver.onCompleted();
        } catch (DbException | RuntimeException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDiscount(UpdateDiscountRequest request,
                               StreamObserver<UpdateDiscountResponse> responseObserver) {
        // require new discount info
        if (!request.hasNewDiscountInfo() || (!request.hasAssociatedAccountId() && !request.hasDiscountId())) {
            logger.error(INVALID_ARGUMENTS_FOR_DISCOUNT_UPDATE);
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(INVALID_ARGUMENTS_FOR_DISCOUNT_UPDATE).asRuntimeException());
            return;
        }
        logger.info(UPDATING_A_DISCOUNT, request);

        try {
            final Discount existingDiscount = getDiscount(request);
            // copy the original discountInfo as base
            final DiscountInfo.Builder newBuilder = DiscountInfo.newBuilder(existingDiscount.getDiscountInfo());
            final DiscountInfo discountInfo = request.getNewDiscountInfo();

            if (discountInfo.hasServiceLevelDiscount()) {
                newBuilder.setServiceLevelDiscount(discountInfo.getServiceLevelDiscount());
            }

            if (discountInfo.hasTierLevelDiscount()) {
                newBuilder.setTierLevelDiscount(discountInfo.getTierLevelDiscount());
            }

            if (discountInfo.hasAccountLevelDiscount()) {
                newBuilder.setAccountLevelDiscount(discountInfo.getAccountLevelDiscount());
            }

            if (discountInfo.hasDisplayName()) {
                newBuilder.setDisplayName(discountInfo.getDisplayName());
            }
            if (request.hasAssociatedAccountId()) {
                discountStore.updateDiscountByAssociatedAccount(request.getAssociatedAccountId(), newBuilder.build());
            } else {
                discountStore.updateDiscount(request.getDiscountId(), newBuilder.build());

            }

            responseObserver.onNext(UpdateDiscountResponse.newBuilder()
                    .setUpdatedDiscount(getDiscount(request))
                    .build());
            responseObserver.onCompleted();
        } catch (DiscountNotFoundException e) {
            logger.error(FAILED_TO_UPDATE_DISCOUNT + getId(request), e);
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(e.getMessage()).asException());
        } catch (DbException e) {
            logger.error(FAILED_TO_UPDATE_DISCOUNT + getId(request), e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void getCurrentAccountExpenses(GetCurrentAccountExpensesRequest request,
                                          StreamObserver<GetCurrentAccountExpensesResponse> responseObserver) {
        try {
            AccountExpenseFilterBuilder builder = AccountExpenseFilterBuilder
                .newBuilder(TimeFrame.LATEST, realtimeTopologyContextId)
                .latestTimestampRequested(true);

            if (!request.getScope().getAllAccounts()) {
                builder.accountIds(request
                    .getScope()
                    .getSpecificAccounts()
                    .getAccountIdsList());
            }

            Map<Long, Map<Long, AccountExpenses>> expensesByTimeAndAccountId = accountExpensesStore
                .getAccountExpenses(builder.build());
            final List<AccountExpenses> expenses = new ArrayList<>();
            expensesByTimeAndAccountId.values().forEach(accountExpensesMap ->
                expenses.addAll(accountExpensesMap.values()));

            responseObserver.onNext(GetCurrentAccountExpensesResponse.newBuilder()
                .addAllAccountExpense(expenses)
                .build());
            responseObserver.onCompleted();
        } catch (DbException e) {
            logger.error("DB Exception when querying for account expenses.", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    /**
     * Fetch a sequence of StatSnapshot's based on the time range and filters in the StatsRequest.
     * The stats will be provided from the Cost database.
     *
     * <p>Currently (Sep 26, 2019), it only supports requests for account expense with GroupBy
     * with CSP, target, and cloudService.
     *
     * <p>The 'entitiesList' in the {@link GetCloudExpenseStatsRequest} is not currently used.
     *
     * @param request          a set of parameters describing how to fetch the snapshot and what entities.
     * @param responseObserver the sync for each result value {@link StatSnapshot}
     */
    @Override
    public void getAccountExpenseStats(GetCloudExpenseStatsRequest request,
                                          StreamObserver<GetCloudCostStatsResponse> responseObserver) {
        if (!request.hasGroupBy()) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("The request includes unsupported group by type: " + request
                            + ", currently only group by CSP, CloudService and target (Account) are supported")
                    .asException());
            return;
        }

        final TimeFrame timeFrame = timeFrameCalculator.millis2TimeFrame(request.getStartDate());
        final AccountExpenseFilterBuilder filterBuilder = createAccountExpenseFilter(request);
        final CostFilter accountCostFilter = filterBuilder.build();

        final Map<Long, Map<Long, AccountExpenses>> expensesByTimeAndAccountId;
        try {
            expensesByTimeAndAccountId = accountExpensesStore.getAccountExpenses(accountCostFilter);
        } catch (DbException e) {
            logger.error("Error getting stats snapshots for {}", request);
            logger.error("    ", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal Error fetching stats for: " + request + ", cause: "
                            + e.getMessage())
                    .asException());
            return;
        }

        Map<Long, Map<Long, Float>> historicData = new HashMap<>();
        Map<Long, Set<Long>> accountIdToEntitiesMap = new HashMap<>();
        final List<CloudCostStatRecord> cloudStatRecords = Lists.newArrayList();

        boolean groupByCloudService = request.getGroupBy().equals(GroupByType.CLOUD_SERVICE);
        boolean groupByBusinessAccount = request.getGroupBy().equals(GroupByType.BUSINESS_UNIT);
        boolean groupByCloudProvider = request.getGroupBy().equals(GroupByType.CSP);

        // create cost stat records from the account expenses from the DB
        expensesByTimeAndAccountId.forEach((snapshotTime, accountIdToExpensesMap) -> {
            final CloudCostStatRecord.Builder snapshotBuilder = CloudCostStatRecord.newBuilder();
            snapshotBuilder.setSnapshotDate(snapshotTime);
            final List<AccountExpenseStat> accountExpenseStats = Lists.newArrayList();
            accountIdToExpensesMap.forEach((accountId, accountExpenses) -> {
                if (!businessAccountHelper.isAccountDiscovered(accountId)) {
                    return;
                }
                accountExpenses.getAccountExpensesInfo().getServiceExpensesList()
                        .forEach(serviceExpenses -> {
                            final double amount = serviceExpenses.getExpenses().getAmount();
                            if (groupByCloudService) {
                                // create stat with associated entity ID = service ID
                                updateAccountExpenses(accountExpenseStats,
                                        historicData,
                                        accountIdToEntitiesMap,
                                        snapshotTime,
                                        serviceExpenses.getAssociatedServiceId(), amount);
                            } else if (groupByBusinessAccount) {
                                // create stat with associated entity ID = business account ID
                                updateAccountExpenses(accountExpenseStats,
                                        historicData,
                                        accountIdToEntitiesMap,
                                        snapshotTime,
                                        accountId, amount);
                            } else if (groupByCloudProvider) {
                                // create stat with associated entity ID = target ID
                                businessAccountHelper
                                        .resolveTargetId(accountId)
                                        .stream()
                                        .findAny()
                                        .ifPresent(targetId -> updateAccountExpenses(
                                                accountExpenseStats,
                                                historicData,
                                                accountIdToEntitiesMap,
                                                snapshotTime,
                                                targetId, amount));
                            }
                        });
            });
            snapshotBuilder.addAllStatRecords(aggregateStatRecords(accountExpenseStats, timeFrame));
            cloudStatRecords.add(snapshotBuilder.build());
        });

        GetCloudCostStatsResponse response = GetCloudCostStatsResponse.newBuilder()
                .addAllCloudStatRecord(cloudStatRecords)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private AccountExpenseFilterBuilder createAccountExpenseFilter(GetCloudExpenseStatsRequest request) {
        final AccountExpenseFilterBuilder filterBuilder;

        // If start and end date is set we get the cost for that duration
        if (request.hasStartDate() && request.hasEndDate()) {
            filterBuilder = AccountExpenseFilterBuilder.newBuilder(
                timeFrameCalculator.millis2TimeFrame(request.getStartDate()),
                    realtimeTopologyContextId)
                .duration(request.getStartDate(), request.getEndDate());
            // if we don't have both start and end date, we will assume the latest
            // cost is requested
        } else {
            if (request.hasStartDate() || request.hasEndDate()) {
                logger.warn("Request for account expense stats should have both start and end date" +
                    " or neither of them. Ignoring the duration. Request : {}", request);
            }

            filterBuilder = AccountExpenseFilterBuilder.newBuilder(TimeFrame.LATEST, realtimeTopologyContextId)
                .latestTimestampRequested(true);
        }

        if (request.hasEntityFilter()) {
            filterBuilder.entityIds(request.getEntityFilter().getEntityIdList());
        }

        if (request.hasEntityTypeFilter()) {
            filterBuilder.entityTypes(request.getEntityTypeFilter().getEntityTypeIdList());
        }

        if (request.hasScope() && !request.getScope().getAllAccounts()) {
            filterBuilder.accountIds(request
                    .getScope()
                    .getSpecificAccounts()
                    .getAccountIdsList());
        }

        return filterBuilder;
    }

    private void updateAccountExpenses(@Nonnull final List<AccountExpenseStat> accountExpenseStats,
                                       @Nonnull final Map<Long, Map<Long, Float>> historicData,
                                       @Nonnull final Map<Long, Set<Long>> accountIdToEntitiesMap,
                                       @Nonnull final Long snapshotTime,
                                       final long associatedServiceId,
                                       final double amount) {
        final AccountExpenseStat accountExpenseStat = new AccountExpenseStat(associatedServiceId, amount);
        // add this record to the snapshot for this timestamp
        accountExpenseStats.add(accountExpenseStat);
        historicData.computeIfAbsent(accountExpenseStat.getAssociatedEntityId(),
                k -> new TreeMap<>()).put(snapshotTime, (float)amount);
        accountIdToEntitiesMap.computeIfAbsent(accountExpenseStat.getAssociatedEntityId(),
                k -> new HashSet<>()).add(associatedServiceId);
    }

    // aggregate account expense stats based to either targetId, or serviceId
    @VisibleForTesting
    Iterable<? extends StatRecord> aggregateStatRecords(
            @Nonnull final List<AccountExpenseStat> accountExpenseStats, TimeFrame timeFrame) {
        String costUnits = timeFrame.getUnits();
        double costMultiplier = timeFrame.getMultiplier();
        final Map<Long, List<AccountExpenseStat>> aggregatedMap = accountExpenseStats.stream()
                .collect(groupingBy(AccountExpenseStat::getAssociatedEntityId));
        final List<StatRecord> aggregatedStatRecords = Lists.newArrayList();
        aggregatedMap.forEach((id, stats) -> {
            final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder()
                    .setName(StringConstants.COST_PRICE);
            statRecordBuilder.setAssociatedEntityId(id);
            statRecordBuilder.setAssociatedEntityType(EntityType.CLOUD_SERVICE_VALUE);
            statRecordBuilder.setUnits(costUnits);
            statRecordBuilder.setValues(CloudCostStatRecord.StatRecord.StatValue.newBuilder()
                    .setAvg((float)getAmountsStream(stats, costMultiplier).average().orElse(0f))
                    .setMax((float)getAmountsStream(stats, costMultiplier).max().orElse(0f))
                    .setMin((float)getAmountsStream(stats, costMultiplier).min().orElse(0f))
                    .setTotal((float)getAmountsStream(stats, costMultiplier).sum())
                    .build());
            aggregatedStatRecords.add(statRecordBuilder.build());
        });
        return aggregatedStatRecords;
    }

    /**
     * This method returns a stream of costs, after being converted to the correct amount for
     * the time frame.
     * The amounts in all tables (daily/monthly) is stored in "/h" units, so for example:
     * if we got the records from the account_expenses_by_month table, we need to convert
     * the amount to a monthly price.
     * This stream is used later for calculating average, max, min and sum.
     *
     * @param accountExpenseStats a list of stats
     * @param costMultiplier the multiplier to use, based on the time frame
     * @return a stream of doubles
     */
    private DoubleStream getAmountsStream(List<AccountExpenseStat> accountExpenseStats,
                                          double costMultiplier) {
        return accountExpenseStats.stream()
                .map(accountExpenseStat -> accountExpenseStat.getValue() * costMultiplier)
                .mapToDouble(v -> v);
    }

    @Override
    public void getCloudCostStats(GetCloudCostStatsRequest getCloudCostStatsRequest,
                                  StreamObserver<GetCloudCostStatsResponse> responseObserver) {
        try {
            final List<CloudCostStatRecord> cloudStatRecords = Lists.newArrayList();
            if (getCloudCostStatsRequest.getCloudCostStatsQueryList().isEmpty()) {

                responseObserver.onError(Status.INTERNAL
                        .withDescription("The request does not contain any queries.")
                        .asException());
            }
            for (CloudCostStatsQuery request : getCloudCostStatsRequest.getCloudCostStatsQueryList()) {
                final EntityCostFilterBuilder filterBuilder = createEntityCostFilter(request);
                final EntityCostFilter entityCostFilter = filterBuilder.build();

                Map<Long, Collection<StatRecord>> snapshotToEntityCostMap = entityCostStore
                        .getEntityCostStats(entityCostFilter);

                Long projectedStatTime = null;
                if (request.getRequestProjected()) {
                    projectedStatTime = (request.hasEndDate() ? request.getEndDate() : clock.millis())
                            + TimeUnit.HOURS.toMillis(PROJECTED_STATS_TIME_IN_FUTURE_HOURS);
                    Collection<StatRecord> projectedStatRecords = new ArrayList<>();
                    boolean projectCostStoreReady = projectedEntityCostStore.isStoreReady();
                    final boolean isPlanRequest = request.hasTopologyContextId()
                        && request.getTopologyContextId() != realtimeTopologyContextId;
                    if (isPlanRequest) {
                        projectedStatRecords = planProjectedEntityCostStore.getPlanProjectedStatRecordsByGroup(
                                        request.getGroupByList(), entityCostFilter,
                                        request.getTopologyContextId());
                    } else if (projectCostStoreReady ) {
                        projectedStatRecords = request.getGroupByList().isEmpty() ?
                            projectedEntityCostStore.getProjectedStatRecords(entityCostFilter) :
                            projectedEntityCostStore.getProjectedStatRecordsByGroup(request.getGroupByList(),
                                    entityCostFilter);
                    }
                    if (projectCostStoreReady || isPlanRequest) {
                        snapshotToEntityCostMap.put(projectedStatTime, projectedStatRecords);
                    } else {
                        // Change the request to only get the the latest timestamp info
                        // we will use that as projected cost
                        EntityCostFilter latestFilter = filterBuilder
                                .removeDuration()
                                .timeFrame(TimeFrame.LATEST)
                                .latestTimestampRequested(true)
                                .build();
                        final Map<Long, Collection<StatRecord>> latestEntityCostMapWithTimestamp =
                                entityCostStore.getEntityCostStats(latestFilter);
                        final Collection<Collection<StatRecord>> values =
                                latestEntityCostMapWithTimestamp.values();

                        try {
                            Collection<StatRecord> entityCostMap = Iterables.getOnlyElement(values);
                            snapshotToEntityCostMap.put(projectedStatTime, entityCostMap);
                        } catch (IllegalArgumentException ex) {
                            logger.warn("Found more than one entry for latest entity cost for " +
                                            "following filter {}. Setting projected entity cost to empty",
                                    latestFilter);
                            snapshotToEntityCostMap.put(projectedStatTime, Collections.emptyList());
                        } catch (NoSuchElementException ex) {
                            logger.warn("Unable to find latest entity cost for filter {}. Setting" +
                                            " projected entity cost to empty",
                                    latestFilter);
                            snapshotToEntityCostMap.put(projectedStatTime, Collections.emptyList());
                        }
                    }
                }
                // if this is not a grouping request; everything else.
                for (Map.Entry<Long, Collection<StatRecord>> entry
                        : snapshotToEntityCostMap.entrySet()) {
                    Long time = entry.getKey();
                    Collection<StatRecord> statRecords = entry.getValue();
                    final CloudCostStatRecord.Builder outerRecordBuilder =
                        CloudCostStatRecord.newBuilder()
                                .setSnapshotDate(time)
                                .setIsProjected(time.equals(projectedStatTime))
                                .setQueryId(request.getQueryId());
                    if (statRecords.isEmpty()) {
                        cloudStatRecords.add(outerRecordBuilder.build());
                    } else {
                        Iterators.partition(statRecords.iterator(), maxNumberOfInnerStatRecords)
                            .forEachRemaining(innerStatChunk ->
                                cloudStatRecords.add(outerRecordBuilder.clone()
                                    .addAllStatRecords(innerStatChunk)
                                    .build())
                            );
                    }
                }
            }
            cloudStatRecords.sort(Comparator.comparingLong(CloudCostStatRecord::getSnapshotDate));

            GetCloudCostStatsResponse.Builder responseBuilder = GetCloudCostStatsResponse.newBuilder();
            int innerRecordCount = 0;
            for (final CloudCostStatRecord outerRecord : cloudStatRecords) {
                if (innerRecordCount + outerRecord.getStatRecordsCount() <= maxNumberOfInnerStatRecords) {
                    responseBuilder.addCloudStatRecord(outerRecord);
                    innerRecordCount += outerRecord.getStatRecordsCount();
                } else {
                    logger.debug("Sending response chunk containing {} StatsRecords within " +
                        "{} CloudCostStatRecords", innerRecordCount,
                        responseBuilder.getCloudStatRecordCount());
                    responseObserver.onNext(responseBuilder.build());
                    responseBuilder.clear().addCloudStatRecord(outerRecord);
                    innerRecordCount = outerRecord.getStatRecordsCount();
                }
            }
            if (innerRecordCount > 0) {
                logger.debug("Sending response chunk containing {} StatsRecords within " +
                        "{} CloudCostStatRecords", innerRecordCount,
                    responseBuilder.getCloudStatRecordCount());
                responseObserver.onNext(responseBuilder.build());
            }
            responseObserver.onCompleted();
        } catch (DbException e) {
            logger.error("Error getting stats snapshots for {}", getCloudCostStatsRequest);
            logger.error("    ", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal Error fetching stats for: " + getCloudCostStatsRequest + ", cause: "
                            + e.getMessage())
                    .asException());
        }
    }

    private EntityCostFilterBuilder createEntityCostFilter(CloudCostStatsQuery request) {
        EntityCostFilterBuilder filterBuilder;
        // If start and end date is set we get the cost for that duration
        if (request.hasStartDate() && request.hasEndDate()) {
            filterBuilder = EntityCostFilterBuilder.newBuilder(
                timeFrameCalculator.millis2TimeFrame(request.getStartDate()), realtimeTopologyContextId)
                .duration(request.getStartDate(), request.getEndDate());
            // if we don't have both start and end date, we will assume the latest
            // cost is requested
        } else {
            if (request.hasStartDate() || request.hasEndDate()) {
                logger.warn("Request for cloud cost stats should have both start and end date" +
                    " or neither of them. Ignoring the duration. Request : {}", request);
            }

            filterBuilder = EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST, realtimeTopologyContextId)
                .latestTimestampRequested(true);
        }

        if (request.hasTopologyContextId()) {
            filterBuilder.topologyContextId(request.getTopologyContextId());
        }

        if (request.hasEntityFilter()) {
            filterBuilder.entityIds(request.getEntityFilter().getEntityIdList());
        }

        if (request.hasCostCategoryFilter()) {
            filterBuilder.costCategoryFilter(request.getCostCategoryFilter());
        }

        if (request.hasEntityTypeFilter()) {
            filterBuilder.entityTypes(request.getEntityTypeFilter().getEntityTypeIdList());
        }
        if (!request.getGroupByList().isEmpty()) {
            final Set<String> groupByColumn = request.getGroupByList().stream()
                    .map(item -> item.getValueDescriptor().getName()).collect(toSet());
            filterBuilder.groupByFields(groupByColumn);
        }

        if (request.hasCostSourceFilter()) {
            filterBuilder.costSources(request.getCostSourceFilter().getExclusionFilter(),
                request.getCostSourceFilter().getCostSourcesList()
                    .stream()
                    .map(CostSource::getNumber)
                    .collect(Collectors.toSet()));
        }

        if (request.hasCostCategoryFilter()) {
            filterBuilder.costCategoryFilter(request.getCostCategoryFilter());
        }

        // Add account filters if present
        if (request.hasAccountFilter()) {
            filterBuilder.accountIds(request.getAccountFilter().getAccountIdList());
        }

        // Add availability zone filter if present
        if (request.hasAvailabilityZoneFilter()) {
            filterBuilder.availabilityZoneIds(request.getAvailabilityZoneFilter()
                .getAvailabilityZoneIdList());
        }

        // Add region filter if present
        if (request.hasRegionFilter()) {
            filterBuilder.regionIds(request.getRegionFilter().getRegionIdList());
        }

        return filterBuilder;
    }

    @Nonnull
    private EntityCostFilterBuilder createEntityCostFilter(@Nonnull final GetTierPriceForEntitiesRequest request) {
        EntityCostFilterBuilder filterBuilder = EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST, request.getTopologyContextId());
        filterBuilder.entityIds(request.getOidsList());
        if (request.hasCostCategory()) {
            CostCategoryFilter costCategoryFilter = CostCategoryFilter.newBuilder()
                    .addCostCategory(request.getCostCategory()).build();
            filterBuilder.costCategoryFilter(costCategoryFilter);
        }
        return filterBuilder;
    }

    @Override
    public void deletePlanEntityCosts(DeletePlanEntityCostsRequest request, StreamObserver<DeletePlanEntityCostsResponse> responseObserver) {
        final long planId = request.getPlanId();
        try {
            final int rowsDeleted = planProjectedEntityCostStore.deletePlanProjectedCosts(planId);
            final DeletePlanEntityCostsResponse response =
                            DeletePlanEntityCostsResponse.newBuilder().setDeleted(rowsDeleted > 0).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                            .withDescription("Failed to delete plan entity costs for plan ID: " + planId)
                            .asException());
        }
    }

    private long getId(final UpdateDiscountRequest request) {
        return request.hasDiscountId() ? request.getDiscountId() : request.getAssociatedAccountId();
    }

    private Discount getDiscount(final UpdateDiscountRequest request) throws DiscountNotFoundException, DbException {
        final List<Discount> discounts = request.hasAssociatedAccountId() ?
                discountStore.getDiscountByAssociatedAccountId(request.getAssociatedAccountId()) :
                discountStore.getDiscountByDiscountId(request.getDiscountId());
        return discounts.stream()
                .findFirst()
                .orElseThrow(() -> new DiscountNotFoundException(FAILED_TO_FIND_THE_UPDATED_DISCOUNT));
    }

    /**
     * Query savings stats records.
     *
     * @param request Stats request.
     * @param responseObserver Response with stats records.
     */
    @Override
    public void getEntitySavingsStats(@Nonnull final GetEntitySavingsStatsRequest request,
            @Nonnull final StreamObserver<EntitySavingsStatsRecord> responseObserver) {
        try {
            final Set<EntitySavingsStatsType> statsTypes = new HashSet<>(request.getStatsTypesList());

            final TimeFrame timeFrame = timeFrameCalculator.millis2TimeFrame(request.getStartDate());
            final List<AggregatedSavingsStats> stats = entitySavingsStore.getSavingsStats(
                    timeFrame, statsTypes, request.getStartDate(), request.getEndDate(),
                    request.getEntityFilter().getEntityIdList());

            final Set<EntitySavingsStatsRecord> records = createSavingsStatsRecords(stats);
            records.forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (EntitySavingsException | RuntimeException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    /**
     * Converts stats coming from DB query response, into stats records that API needs.
     *
     * @param stats Stats from DB query, one per stats type, per VM and timestamp.
     * @return Stats records, each with VM id and timestamp, having all stats type as records.
     */
    private Set<EntitySavingsStatsRecord> createSavingsStatsRecords(
            @Nonnull final List<AggregatedSavingsStats> stats) {
        // Group stats by timestamp.
        final Map<Long, List<AggregatedSavingsStats>> groupedStats =
                stats.stream()
                        .collect(groupingBy(oneStat -> oneStat.getTimestamp()));

        // Now make up the stats records to be returned.
        final Set<EntitySavingsStatsRecord> records = new HashSet<>();
        groupedStats.forEach((timestamp, statsList) -> {
            final EntitySavingsStatsRecord.Builder recBuilder = EntitySavingsStatsRecord.newBuilder()
                    .setSnapshotDate(timestamp);
            statsList.forEach(aStat -> {
                recBuilder.addStatRecords(SavingsRecord.newBuilder()
                        .setName(aStat.getType().name())
                        .setValue(aStat.getValue().floatValue())
                        .build());
            });
            records.add(recBuilder.build());
        });
        return records;
    }

    /**
     * Helper class to do account expense aggregation calculation per timestamp.
     */
    @VisibleForTesting
    static class AccountExpenseStat {
        private final long associatedEntityId;
        private final double value;

        AccountExpenseStat(long associatedEntityId, double value) {
            this.associatedEntityId = associatedEntityId;
            this.value = value;
        }

        public long getAssociatedEntityId() {
            return associatedEntityId;
        }

        public double getValue() {
            return value;
        }
    }
}
