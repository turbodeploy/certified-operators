package com.vmturbo.cost.component.rpc;

import static java.util.stream.Collectors.toSet;

import java.time.Clock;
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
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
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
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse.Builder;
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
import com.vmturbo.cost.component.util.AccountExpensesFilter;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.cost.component.util.CostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter;
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
            Map<Long, EntityCost> beforeEntityCostbyOid = entityCostStore.getLatestEntityCost(oid, category,
                    new HashSet<>(Arrays.asList(CostSource.ON_DEMAND_RATE)));
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
            final Collection<AccountExpenses> expenses = accountExpensesStore.getCurrentAccountExpenses(
                request.getScope());
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
                final Set<Long> filterIds = new HashSet<>(request.getEntityFilter().getEntityIdList());
                final Set<Integer> entityTypeFilterIds = new HashSet<>(request.getEntityTypeFilter().getEntityTypeIdList());
                final CostFilter entityCostFilter =
                        new AccountExpensesFilter(filterIds, entityTypeFilterIds, request.getStartDate(), request.getEndDate(),
                                timeFrame, Collections.singleton(request.getGroupBy().getValueDescriptor().getName()));
                final Map<Long, Map<Long, AccountExpenses>> snapshotToExpenseMap =
                        (request.hasStartDate() && request.hasEndDate()) ?
                                accountExpensesStore.getAccountExpenses(entityCostFilter) :
                                accountExpensesStore.getLatestExpenses(filterIds, entityTypeFilterIds);

                // Mapping from AccountId -> {Timestamp -> Stat Value}
                Map<Long, Map<Long, Float>> historicData = new HashMap<>();
                Map<Long, Set<Long>> accountIdToEntitiesMap = new HashMap<>();
                final List<CloudCostStatRecord> cloudStatRecords = Lists.newArrayList();
                boolean isCloudServiceRequest = request.getGroupBy().equals(GroupByType.CLOUD_SERVICE);
                boolean isGroupByTargetRequest  = request.getGroupBy().equals(GroupByType.TARGET);
                snapshotToExpenseMap.forEach((snapshotTime, accountIdToExpensesMap) -> {
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
                                        .forEach(associatedServiceId -> updateAccountExpenses(accountExpenseStats,
                                                historicData,
                                                accountIdToEntitiesMap,
                                                snapshotTime,
                                                associatedServiceId, amount));
                            }
                        });

                        if (isGroupByTargetRequest) {
                            accountExpenses.getAccountExpensesInfo()
                                    .getTierExpensesList().forEach(tierExpenses -> {
                                businessAccountHelper
                                        .resolveTargetId(accountExpenses.getAssociatedAccountId())
                                        .forEach(associatedServiceId -> {
                                            double existingStatAmount = 0.0f;
                                            if (historicData.containsKey(associatedServiceId)) {
                                                existingStatAmount = historicData.get(associatedServiceId)
                                                        .getOrDefault(snapshotTime, 0.0f);
                                            }
                                            final double amount = tierExpenses.getExpenses()
                                                    .getAmount() + existingStatAmount;
                                            updateAccountExpenses(accountExpenseStats,
                                                    historicData,
                                                    accountIdToEntitiesMap,
                                                    snapshotTime,
                                                    associatedServiceId, amount);
                                        });
                            });
                        }
                    });


                    snapshotBuilder.addAllStatRecords(aggregateStatRecords(accountExpenseStats));
                    cloudStatRecords.add(snapshotBuilder.build());
                });

                // For the projected stats, forecast data from the historic stats.
                if (request.hasStartDate() && request.hasEndDate()) {
                    final CloudCostStatRecord.Builder projectedSnapshotBuilder = CloudCostStatRecord.newBuilder();
                    long projectedTime = request.getEndDate() + TimeUnit.HOURS.toMillis(PROJECTED_STATS_TIME_IN_FUTURE_HOURS);
                    projectedSnapshotBuilder.setSnapshotDate(projectedTime);
                    final List<AccountExpenseStat> accountExpenseStats = Lists.newArrayList();

                    for (Entry<Long, Map<Long, Float>> stats : historicData.entrySet()) {
                        try {
                            SortedMap<Long, Float> forecast =
                                    (SortedMap)forecastingContext.computeForecast(request.getStartDate(),
                                            projectedTime,
                                            com.vmturbo.commons.TimeFrame.valueOf(timeFrame.name()),
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
                    projectedSnapshotBuilder.addAllStatRecords(aggregateStatRecords(accountExpenseStats));
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
    private Iterable<? extends StatRecord> aggregateStatRecords(@Nonnull final List<AccountExpenseStat> accountExpenseStats) {
        final Map<Long, List<AccountExpenseStat>> aggregatedMap = accountExpenseStats.stream()
                .collect(Collectors.groupingBy(statRecord -> statRecord.getAssociatedEntityId()));
        final List<StatRecord> aggregatedStatRecords = Lists.newArrayList();
        aggregatedMap.forEach((id, stats) -> {
            final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder()
                    .setName(StringConstants.COST_PRICE);
            statRecordBuilder.setAssociatedEntityId(id);
            statRecordBuilder.setAssociatedEntityType(EntityType.CLOUD_SERVICE_VALUE);
            statRecordBuilder.setUnits("$/h");
            statRecordBuilder.setValues(CloudCostStatRecord.StatRecord.StatValue.newBuilder()
                    .setAvg((float)stats.stream().map(AccountExpenseStat::getValue).mapToDouble(v -> v).average().orElse(0f))
                    .setMax((float)stats.stream().map(AccountExpenseStat::getValue).mapToDouble(v -> v).max().orElse(0f))
                    .setMin((float)stats.stream().map(AccountExpenseStat::getValue).mapToDouble(v -> v).min().orElse(0f))
                    .setTotal((float)stats.stream().map(AccountExpenseStat::getValue).mapToDouble(v -> v).sum())
                    .build());
            aggregatedStatRecords.add(statRecordBuilder.build());
        });
        return aggregatedStatRecords;
    }

    @Override
    public void getCloudCostStats(GetCloudCostStatsRequest getCloudCostStatsRequest,
                                  StreamObserver<GetCloudCostStatsResponse> responseObserver) {
        try {
            Builder response =
                    GetCloudCostStatsResponse.newBuilder();
            if (getCloudCostStatsRequest.getCloudCostStatsQueryList().isEmpty()) {

                responseObserver.onError(Status.INTERNAL
                        .withDescription("The request does not contain any queries.")
                        .asException());
            }
            for (CloudCostStatsQuery request : getCloudCostStatsRequest.getCloudCostStatsQueryList()) {
                final TimeFrame timeFrame = timeFrameCalculator.millis2TimeFrame(request.getStartDate());
                final Set<Long> filterIds = new HashSet<>(request.getEntityFilter().getEntityIdList());
                final Set<Integer> entityTypeFilterIds = new HashSet<>(request.getEntityTypeFilter().getEntityTypeIdList());
                final Set<String> groupByColumn = request.getGroupByList().stream()
                        .map(item -> item.getValueDescriptor().getName()).collect(toSet());
                final CostFilter entityCostFilter =
                        new EntityCostFilter(filterIds, entityTypeFilterIds,
                                request.getStartDate(), request.getEndDate(), timeFrame, groupByColumn);
                final  Map<Long, Collection<StatRecord>> snapshotToStatRecordMap = Maps.newHashMap();

                final List<CloudCostStatRecord> cloudStatRecords = Lists.newArrayList();
                cloudStatRecords.sort(Comparator.comparingLong(CloudCostStatRecord::getSnapshotDate));
                // if this is not a grouping request; everything else.
                snapshotToStatRecordMap.putAll(entityCostStore.getEntityCostStats(entityCostFilter));
                calculateProjectedCost(request, filterIds, entityTypeFilterIds, timeFrame, snapshotToStatRecordMap);
                snapshotToStatRecordMap.forEach((time, statRecords) -> {
                    final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                            .setSnapshotDate(time)
                            .setQueryId(request.getQueryId())
                            .addAllStatRecords(statRecords)
                            .build();
                    cloudStatRecords.add(cloudStatRecord);
                });
                //add all the records to the final response.
                response.addAllCloudStatRecord(cloudStatRecords);
            }
            responseObserver.onNext(response.build());
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

    private void calculateProjectedCost(@Nonnull final CloudCostStatsQuery request,
                                        @Nonnull final Set<Long> filterIds,
                                        @Nonnull final Set<Integer> entityTypeFilterIds,
                                        @Nonnull final TimeFrame timeFrame,
                                        @Nonnull final Map<Long, Collection<StatRecord>> snapshotToEntityCostMap)
            throws DbException {
        //add projected data.
        if (request.getRequestProjected()) {
            final long projectedStatTime = (request.hasEndDate() ? request.getEndDate() : clock.millis())
                    + TimeUnit.HOURS.toMillis(PROJECTED_STATS_TIME_IN_FUTURE_HOURS);
            final Collection<StatRecord> projectedStatRecords = request.getGroupByList().isEmpty() ?
                    projectedEntityCostStore.getProjectedStatRecords(filterIds) :
                    projectedEntityCostStore.getProjectedStatRecordsByGroup(request.getGroupByList());
            final Set<String> groupByColumn = request.getGroupByList().stream()
                    .map(item -> item.getValueDescriptor().getName()).collect(toSet());
            final CostFilter entityCostFilter =
                    new EntityCostFilter(filterIds, entityTypeFilterIds,
                            0, 0, timeFrame, groupByColumn);
            final Map<Long, Collection<StatRecord>> latestEntityCostMapWithTimestamp;
            try {
                if (projectedStatRecords.isEmpty()) {
                    latestEntityCostMapWithTimestamp = entityCostStore.getEntityCostStats(entityCostFilter);
                    Collection<StatRecord> statRecordCollections = Iterables
                            .getOnlyElement(latestEntityCostMapWithTimestamp.values());
                    snapshotToEntityCostMap.put(projectedStatTime, statRecordCollections);
                } else {
                    snapshotToEntityCostMap.put(projectedStatTime, projectedStatRecords);
                }
            } catch (IllegalArgumentException ex) {
                logger.warn("Found more than one entry for latest entity cost for " +
                                "entityCostFilter: {} " +
                                "Setting projected entity cost to empty",
                        entityCostFilter);
                snapshotToEntityCostMap.put(projectedStatTime, Collections.emptySet());
            } catch (NoSuchElementException ex) {
                logger.warn("Unable to find latest entity cost for entityCostFilter " +
                        "{} .Setting projected entity cost to empty", entityCostFilter);
                snapshotToEntityCostMap.put(projectedStatTime, Collections.emptySet());
            }
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
     * Helper class to do account expense aggregation calculation per timestamp.
     */
    private class AccountExpenseStat {
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
