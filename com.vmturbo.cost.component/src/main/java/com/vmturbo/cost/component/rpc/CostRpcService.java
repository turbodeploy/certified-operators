package com.vmturbo.cost.component.rpc;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest.GroupByType;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesResponse;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountResponse;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceImplBase;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.commons.forecasting.ForecastingContext;
import com.vmturbo.commons.forecasting.ForecastingStrategyNotProvidedException;
import com.vmturbo.commons.forecasting.InvalidForecastingDateRangeException;
import com.vmturbo.commons.forecasting.RegressionForecastingStrategy;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.discount.DiscountNotFoundException;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.discount.DuplicateAccountIdException;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.entity.cost.ProjectedEntityCostStore;
import com.vmturbo.cost.component.expenses.AccountExpensesStore;
import com.vmturbo.cost.component.stats.ReservedInstanceStatCleanupScheduler;
import com.vmturbo.cost.component.util.AccountExpensesFilter.AccountExpenseFilterBuilder;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.cost.component.util.CostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter.EntityCostFilterBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
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

    private final BusinessAccountHelper businessAccountHelper;

    private final TimeFrameCalculator timeFrameCalculator;

    private final Clock clock;

    private ForecastingContext forecastingContext;


    /**
     * Create a new RIAndExpenseUploadRpcService.
     *
     * @param discountStore        The store containing account discounts
     * @param accountExpensesStore The store containing account expenses
     */
    public CostRpcService(@Nonnull final DiscountStore discountStore,
                          @Nonnull final AccountExpensesStore accountExpensesStore,
                          @Nonnull final EntityCostStore costStoreHouse,
                          @Nonnull final ProjectedEntityCostStore projectedEntityCostStore,
                          @Nonnull final TimeFrameCalculator timeFrameCalculator,
                          @Nonnull final BusinessAccountHelper businessAccountHelper,
                          @Nonnull final Clock clock) {
        this.discountStore = Objects.requireNonNull(discountStore);
        this.accountExpensesStore = Objects.requireNonNull(accountExpensesStore);
        this.entityCostStore = Objects.requireNonNull(costStoreHouse);
        this.projectedEntityCostStore = Objects.requireNonNull(projectedEntityCostStore);
        this.businessAccountHelper = Objects.requireNonNull(businessAccountHelper);
        this.timeFrameCalculator = Objects.requireNonNull(timeFrameCalculator);
        this.forecastingContext = new ForecastingContext(new RegressionForecastingStrategy());
        this.clock = Objects.requireNonNull(clock);
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
        Long oid = request.getOid();
        CostCategory category = request.getCostCategory();
        try {
            Map<Long, Map<Long, EntityCost>> queryResult =
                entityCostStore.getEntityCosts(EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST)
                    .entityIds(Collections.singleton(oid))
                    .costCategories(Collections.singleton(category.getNumber()))
                    .latestTimestampRequested(true)
                    .costSources(false,
                        Collections.singleton(CostSource.ON_DEMAND_RATE.getNumber()))
                    .build());
            Map<Long, EntityCost> beforeEntityCostbyOid =
                Iterables.getOnlyElement(queryResult.values(), Collections.emptyMap());
            Map<Long, EntityCost> afterEntityCostbyOid = projectedEntityCostStore.getProjectedEntityCosts(new HashSet<>(Arrays.asList(oid)));
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
                            " .This may result in the on demand rate not being displayed on the UI.", oid);
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
                    logger.error("Projected costs could not be found for entity having oid {}." +
                            " This may result in the on demand rate not being displayed on the UI.", oid);
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
                request.getFilter().getAssociatedAccountIdList().forEach(id ->
                {
                    try {
                        discountStore.getDiscountByAssociatedAccountId(id)
                                .stream().forEach(responseObserver::onNext);
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
                .newBuilder(TimeFrame.LATEST)
                .latestTimestampRequested(true);

            if (!request.getScope().getAll()) {
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
        if (request.hasGroupBy()) {
            try {
                final TimeFrame timeFrame = timeFrameCalculator.millis2TimeFrame(request.getStartDate());

                final AccountExpenseFilterBuilder filterBuilder =
                    createAccountExpenseFilter(request);
                final CostFilter accountCostFilter = filterBuilder.build();

                final Map<Long, Map<Long, AccountExpenses>> expensesByTimeAndAccountId =
                    accountExpensesStore.getAccountExpenses(accountCostFilter);

                // Mapping from AccountId -> {Timestamp -> Stat Value}
                Map<Long, Map<Long, Float>> historicData = new HashMap<>();
                Map<Long, Set<Long>> accountIdToEntitiesMap = new HashMap<>();
                final List<CloudCostStatRecord> cloudStatRecords = Lists.newArrayList();
                boolean isCloudServiceRequest = request.getGroupBy().equals(GroupByType.CLOUD_SERVICE);
                boolean isGroupByTargetRequest  = request.getGroupBy().equals(GroupByType.TARGET);
                expensesByTimeAndAccountId.forEach((snapshotTime, accountIdToExpensesMap) -> {
                    final CloudCostStatRecord.Builder snapshotBuilder = CloudCostStatRecord.newBuilder();
                    snapshotBuilder.setSnapshotDate(snapshotTime);
                    final List<AccountExpenseStat> accountExpenseStats = Lists.newArrayList();
                    accountIdToExpensesMap.values().forEach(accountExpenses -> {
                        accountExpenses.getAccountExpensesInfo().getServiceExpensesList().forEach(serviceExpenses -> {
                            final double amount = serviceExpenses.getExpenses().getAmount();
                            // build the record for this stat (commodity type)
                            if (isCloudServiceRequest) {
                                updateAccountExpenses(accountExpenseStats,
                                        historicData,
                                        accountIdToEntitiesMap,
                                        snapshotTime,
                                        serviceExpenses.getAssociatedServiceId(), amount);
                            } else {
                                businessAccountHelper
                                        .resolveTargetId(accountExpenses.getAssociatedAccountId())
                                        .forEach(targetId -> updateAccountExpenses(accountExpenseStats,
                                                historicData,
                                                accountIdToEntitiesMap,
                                                snapshotTime,
                                                targetId, amount));
                            }
                        });

                        // TODO: understand what is the use-case for group by target
                        if (isGroupByTargetRequest) {
                            accountExpenses.getAccountExpensesInfo()
                                    .getTierExpensesList().forEach(tierExpenses -> {
                                businessAccountHelper
                                        .resolveTargetId(accountExpenses.getAssociatedAccountId())
                                        .forEach(targetId -> {
                                            double existingStatAmount = 0.0f;
                                            if (historicData.containsKey(targetId)) {
                                                existingStatAmount = historicData.get(targetId)
                                                        .getOrDefault(snapshotTime, 0.0f);
                                            }
                                            final double amount = tierExpenses.getExpenses()
                                                    .getAmount() + existingStatAmount;
                                            updateAccountExpenses(accountExpenseStats,
                                                    historicData,
                                                    accountIdToEntitiesMap,
                                                    snapshotTime,
                                                    targetId, amount);
                                        });
                            });
                        }
                    });

                    snapshotBuilder.addAllStatRecords(aggregateStatRecords(accountExpenseStats, timeFrame));
                    cloudStatRecords.add(snapshotBuilder.build());
                });

                // For the projected stats, forecast data from the historic stats.
                if (request.hasStartDate() && request.hasEndDate()) {
                    final CloudCostStatRecord.Builder projectedSnapshotBuilder = CloudCostStatRecord.newBuilder();
                    long projectedTime = request.getEndDate() + TimeUnit.HOURS.toMillis(PROJECTED_STATS_TIME_IN_FUTURE_HOURS);
                    projectedSnapshotBuilder.setSnapshotDate(projectedTime);
                    final List<AccountExpenseStat> accountExpenseStats = Lists.newArrayList();
                    Map<Long, EntityCost> projecteEntitiesCosts = projectedEntityCostStore.getAllProjectedEntitiesCosts();

                    for (Entry<Long, Map<Long, Float>> stats : historicData.entrySet()) {
                        try {
                            SortedMap<Long, Float> forecast =
                                    (SortedMap)forecastingContext.computeForecast(request.getStartDate(),
                                            projectedTime,
                                            com.vmturbo.commons.TimeFrame.valueOf(accountCostFilter
                                                .getTimeFrame().name()),
                                            stats.getValue());
                            accountExpenseStats.add(new AccountExpenseStat(stats.getKey(),
                                    Math.round(forecast.get(forecast.lastKey()))));
                        } catch (InvalidForecastingDateRangeException invalidDateRangeException) {
                            logger.error("Error getting stats snapshots for {}." +
                                    "Forecast requested for an invalid time range", request, invalidDateRangeException);
                            responseObserver.onError(Status.INTERNAL
                                    .withDescription("Internal Error fetching stats for: " + request + ", cause: "
                                            + invalidDateRangeException.getMessage())
                                    .asException());
                            return;
                        } catch (ForecastingStrategyNotProvidedException strategyNotProvidedException) {
                            logger.error("Error getting stats snapshots for {}." +
                                            "Forecast requested but forecasting strategy not specified",
                                    request, strategyNotProvidedException);
                            responseObserver.onError(Status.INTERNAL
                                    .withDescription("Internal Error fetching stats for: " + request + ", cause: "
                                            + strategyNotProvidedException.getMessage())
                                    .asException());
                            return;
                        }
                    }
                    projectedSnapshotBuilder.addAllStatRecords(aggregateStatRecords(accountExpenseStats, timeFrame));
                    cloudStatRecords.add(projectedSnapshotBuilder.build());
                }

                GetCloudCostStatsResponse response =
                        GetCloudCostStatsResponse.newBuilder()
                                .addAllCloudStatRecord(cloudStatRecords)
                                .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } catch (DbException e) {
                logger.error("Error getting stats snapshots for {}", request);
                logger.error("    ", e);
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Internal Error fetching stats for: " + request + ", cause: "
                                + e.getMessage())
                        .asException());
            }
        } else {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("The request includes unsupported group by type: " + request
                            + ", currently only group by CSP, CloudService and target (Account) are supported")
                    .asException());
        }
    }

    private AccountExpenseFilterBuilder createAccountExpenseFilter(GetCloudExpenseStatsRequest request) {
        final AccountExpenseFilterBuilder filterBuilder;

        // If start and end date is set we get the cost for that duration
        if (request.hasStartDate() && request.hasEndDate()) {
            filterBuilder = AccountExpenseFilterBuilder.newBuilder(
                timeFrameCalculator.millis2TimeFrame(request.getStartDate()))
                .duration(request.getStartDate(), request.getEndDate());
            // if we don't have both start and end date, we will assume the latest
            // cost is requested
        } else {
            if (request.hasStartDate() || request.hasEndDate()) {
                logger.warn("Request for account expense stats should have both start and end date" +
                    " or neither of them. Ignoring the duration. Request : {}", request);
            }

            filterBuilder = AccountExpenseFilterBuilder.newBuilder(TimeFrame.LATEST)
                .latestTimestampRequested(true);
        }

        if (request.hasEntityFilter()) {
            filterBuilder.entityIds(request.getEntityFilter().getEntityIdList());
        }

        if (request.hasEntityTypeFilter()) {
            filterBuilder.entityTypes(request.getEntityTypeFilter().getEntityTypeIdList());
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
                .collect(Collectors.groupingBy(AccountExpenseStat::getAssociatedEntityId));
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
    public void getCloudCostStats(GetCloudCostStatsRequest request,
                                  StreamObserver<GetCloudCostStatsResponse> responseObserver) {
        try {
            if (!request.hasCostCategoryFilter()) {
                final EntityCostFilterBuilder filterBuilder = createEntityCostFilter(request);
                final EntityCostFilter entityCostFilter = filterBuilder.build();

                Map<Long, Map<Long, EntityCost>> snapshotToEntityCostMap = entityCostStore
                    .getEntityCosts(entityCostFilter);

                if (request.getRequestProjected()) {
                    final long projectedStatTime = (request.hasEndDate() ? request.getEndDate() : clock.millis())
                        + TimeUnit.HOURS.toMillis(PROJECTED_STATS_TIME_IN_FUTURE_HOURS);
                    final Map<Long, EntityCost> projectedEntityCostMap =
                                    projectedEntityCostStore.getProjectedEntityCosts(entityCostFilter);
                    if (projectedEntityCostMap.isEmpty()) {
                        // Change the request to only get the the latest timestamp info
                        // we will use that as projected cost
                        EntityCostFilter latestFilter = filterBuilder
                            .removeDuration()
                            .timeFrame(TimeFrame.LATEST)
                            .latestTimestampRequested(true)
                            .build();
                        final Map<Long, Map<Long, EntityCost>> latestEntityCostMapWithTimestamp =
                            entityCostStore.getEntityCosts(latestFilter);
                        final Collection<Map<Long, EntityCost>> values =
                                        latestEntityCostMapWithTimestamp.values();

                        try {
                            Map<Long, EntityCost> entityCostMap = Iterables.getOnlyElement(values);
                            snapshotToEntityCostMap.put(projectedStatTime, entityCostMap);
                        } catch (IllegalArgumentException ex) {
                            logger.warn("Found more than one entry for latest entity cost for " +
                                    "following filter {}. Setting projected entity cost to empty",
                                latestFilter);
                            snapshotToEntityCostMap.put(projectedStatTime, Collections.emptyMap());
                        } catch (NoSuchElementException ex) {
                            logger.warn("Unable to find latest entity cost for filter {}. Setting" +
                                    " projected entity cost to empty",
                                latestFilter);
                            snapshotToEntityCostMap.put(projectedStatTime, Collections.emptyMap());
                        }
                    } else {
                        snapshotToEntityCostMap.put(projectedStatTime, projectedEntityCostMap);
                    }
                }

                final List<CloudCostStatRecord> cloudStatRecords = Lists.newArrayList();
                snapshotToEntityCostMap.forEach((time, costsByEntity) -> {
                            final List<CloudCostStatRecord.StatRecord> statRecords = Lists.newArrayList();
                            // GroupBy Cost components, e.g. compute, IP, storage, license
                            if (request.hasGroupBy()
                                    && request.getGroupBy().equals(GetCloudCostStatsRequest.GroupByType.COSTCOMPONENT)) {
                                final CloudCostStatRecord.StatRecord.Builder builder = CloudCostStatRecord.StatRecord.newBuilder();
                                aggregateEntityCostByCostType(costsByEntity.values())
                                        .forEach(componentCost -> {
                                            builder.setAssociatedEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
                                            builder.setCategory(componentCost.costCategory);
                                            builder.setName(StringConstants.COST_PRICE);
                                            builder.setUnits("$/h");
                                            builder.setValues(CloudCostStatRecord.StatRecord.StatValue.newBuilder()
                                                    .setAvg((float) componentCost.geAvg().orElse(0))
                                                    .setMax((float) componentCost.getMax().orElse(0))
                                                    .setMin((float) componentCost.getMin().orElse(0))
                                                    .setTotal((float) componentCost.getTotal())
                                                    .build());
                                            statRecords.add(builder.build());
                                        });
                            } else {  // populate cost per entity, and up to the caller to aggregate as needed.
                                costsByEntity.values().forEach(entityCost -> {
                                    entityCost.getComponentCostList().forEach(componentCost -> {
                                                final CloudCostStatRecord.StatRecord.Builder builder = CloudCostStatRecord.StatRecord.newBuilder();
                                                final float amount = (float) componentCost.getAmount().getAmount();
                                                builder.setAssociatedEntityId(entityCost.getAssociatedEntityId());
                                                builder.setAssociatedEntityType(entityCost.getAssociatedEntityType());
                                                builder.setCategory(componentCost.getCategory());
                                                builder.setName(StringConstants.COST_PRICE);
                                                builder.setUnits("$/h");
                                                builder.setValues(CloudCostStatRecord.StatRecord.StatValue.newBuilder()
                                                        .setAvg(amount)
                                                        .setMax(amount)
                                                        .setMin(amount)
                                                        .setTotal(amount)
                                                        .build());
                                                statRecords.add(builder.build());
                                            }
                                    );
                                });
                            }
                            final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                                    .setSnapshotDate(time)
                                    .addAllStatRecords(statRecords)
                                    .build();
                            cloudStatRecords.add(cloudStatRecord);

                        }
                );
                cloudStatRecords.sort(Comparator.comparingLong(CloudCostStatRecord::getSnapshotDate));
                GetCloudCostStatsResponse response =
                        GetCloudCostStatsResponse.newBuilder()
                                .addAllCloudStatRecord(cloudStatRecords)
                                .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.INTERNAL
                        .withDescription("The request includes unsupported filter type: " + request
                                + ", currently only workload type is supported")
                        .asException());
            }
        } catch (DbException e) {
            logger.error("Error getting stats snapshots for {}", request);
            logger.error("    ", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal Error fetching stats for: " + request + ", cause: "
                            + e.getMessage())
                    .asException());
        }
    }

    private EntityCostFilterBuilder createEntityCostFilter(GetCloudCostStatsRequest request) {
        EntityCostFilterBuilder filterBuilder;
        // If start and end date is set we get the cost for that duration
        if (request.hasStartDate() && request.hasEndDate()) {
            filterBuilder = EntityCostFilterBuilder.newBuilder(
                timeFrameCalculator.millis2TimeFrame(request.getStartDate()))
                .duration(request.getStartDate(), request.getEndDate());
            // if we don't have both start and end date, we will assume the latest
            // cost is requested
        } else {
            if (request.hasStartDate() || request.hasEndDate()) {
                logger.warn("Request for cloud cost stats should have both start and end date" +
                    " or neither of them. Ignoring the duration. Request : {}", request);
            }

            filterBuilder = EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST)
                .latestTimestampRequested(true);
        }

        if (request.hasEntityFilter()) {
            filterBuilder.entityIds(request.getEntityFilter().getEntityIdList());
        }

        if (request.hasEntityTypeFilter()) {
            filterBuilder.entityTypes(request.getEntityTypeFilter().getEntityTypeIdList());
        }

        if (request.hasCostCategoryFilter()) {
            filterBuilder.costCategories(Collections
                .singleton(request.getCostCategoryFilter().getNumber()));
        }

        if (request.hasCostSourceFilter()) {
            filterBuilder.costSources(request.getCostSourceFilter().getExclusionFilter(),
                request.getCostSourceFilter().getCostSourcesList()
                    .stream()
                    .map(CostSource::getNumber)
                    .collect(Collectors.toSet()));
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

    //TODO move them to helper class
    // aggregate all the value per cost type, e.g. IP, Compute, License
    private Set<AggregatedEntityCost> aggregateEntityCostByCostType(final Collection<EntityCost> values) {
        final AggregatedEntityCost costIP = new AggregatedEntityCost(CostCategory.IP);
        final AggregatedEntityCost costLicense = new AggregatedEntityCost(CostCategory.ON_DEMAND_LICENSE);
        final AggregatedEntityCost costOnDemandCompute = new AggregatedEntityCost(CostCategory.ON_DEMAND_COMPUTE);
        final AggregatedEntityCost costStorage = new AggregatedEntityCost(CostCategory.STORAGE);
        final AggregatedEntityCost costRICompute = new AggregatedEntityCost(CostCategory.RI_COMPUTE);
        values.forEach(entityCost -> {
            entityCost.getComponentCostList().forEach(componentCost -> {
                switch(componentCost.getCategory()) {
                    case ON_DEMAND_COMPUTE:
                        costOnDemandCompute.addCost(componentCost.getAmount().getAmount());
                        break;
                    case IP:
                        costIP.addCost(componentCost.getAmount().getAmount());
                        break;
                    case ON_DEMAND_LICENSE:
                        costLicense.addCost(componentCost.getAmount().getAmount());
                        break;
                    case STORAGE:
                        costStorage.addCost(componentCost.getAmount().getAmount());
                        break;
                    case RI_COMPUTE:
                        costRICompute.addCost(componentCost.getAmount().getAmount());
                        break;
                }
            });
        });
        return ImmutableSet.of(costIP, costOnDemandCompute, costLicense, costStorage, costRICompute);
    }

    private CloudCostStatRecord.StatRecord buildStatRecord(@Nullable final Long producerId,
                                                           @Nullable final Float avgValue) {
        final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder()
                .setName(StringConstants.COST_PRICE);
        if (producerId != null && producerId != 0) {
            // providerUuid, it's associated accountId except CloudService type which is serviceId
            statRecordBuilder.setAssociatedEntityId(producerId);
        }
        // hardcoded for now
        statRecordBuilder.setUnits("$/h");

        // values, used, peak
        StatValue.Builder statValueBuilder = StatValue.newBuilder();
        if (avgValue != null) {
            statValueBuilder.setAvg(avgValue);
        }

        statValueBuilder.setTotal(avgValue);

        statRecordBuilder.setValues(CloudCostStatRecord.StatRecord.StatValue.newBuilder()
                .setAvg(avgValue)
                .setMax(avgValue)
                .setMin(avgValue)
                .setTotal(avgValue)
                .build());
        return statRecordBuilder.build();
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
     * Convert date time to local date time.
     *
     * @param dateTime date time with long type.
     * @return local date time with LocalDateTime type.
     */
    private LocalDateTime getLocalDateTime(long dateTime) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(dateTime),
                TimeZone.getDefault().toZoneId());
    }

    // helper class to do entity cost aggregation calculation per timestamp
    private class AggregatedEntityCost {
        final CostCategory costCategory;
        final List<Double> costList = Lists.newArrayList();

        AggregatedEntityCost(CostCategory costCategory) {
            this.costCategory = costCategory;
        }

        void addCost(double amount) {
            costList.add(amount);
        }

        OptionalDouble getMin() {
            return costList.stream()
                    .mapToDouble(v -> v)
                    .min();
        }

        OptionalDouble getMax() {
            return costList.stream()
                    .mapToDouble(v -> v)
                    .max();
        }

        OptionalDouble geAvg() {
            return costList.stream()
                    .mapToDouble(v -> v)
                    .average();
        }

        double getTotal() {
            return costList.stream()
                    .mapToDouble(v -> v)
                    .sum();
        }
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

    /**
     *  Get the savings for the list of entities.
     *
     * @param entityCostMap Mapping from EntityId -> EntityCost
     * @param entityIds
     * @return The total savings for all the entities in the input list.
     */
    private double getSavingsForEntities(Map<Long, EntityCost> entityCostMap,
                                         Set<Long> entityIds) {
        return entityIds.stream()
                .filter(entityId -> entityCostMap.containsKey(entityId))
                .map(entityId -> entityCostMap.get(entityId))
                .map(this::getSavingsFromEntityCost)
                .mapToDouble(Double::doubleValue)
                .sum();
    }

    /**
     * Return the total savings for the given entity.
     *
     * @param entityCost The cost details of the entity.
     * @return Total savings for the entity.
     */
    private double getSavingsFromEntityCost(EntityCost entityCost) {
        return entityCost.getComponentCostList().stream()
                .filter(cost -> cost.hasAmount() && cost.getAmount().getAmount() <=0)
                .map(ComponentCost::getAmount)
                .map(CurrencyAmount::getAmount)
                .map(Math::abs)
                .mapToDouble(Double::doubleValue)
                .sum();
    }
}
