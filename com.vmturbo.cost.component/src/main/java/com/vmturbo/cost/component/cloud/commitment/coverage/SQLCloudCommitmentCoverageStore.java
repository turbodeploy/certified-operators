package com.vmturbo.cost.component.cloud.commitment.coverage;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.GroupField;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.impl.DSL;

import com.vmturbo.cloud.common.commitment.CloudCommitmentUtils;
import com.vmturbo.cloud.common.stat.CloudGranularityCalculator;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData.CloudCommitmentDataBucket;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData.CloudCommitmentDataBucket.CloudCommitmentDataPoint;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord.StatValue;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCommitmentCoverageStatsRequest.GroupByCondition;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudStatGranularity;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.ServiceProviderFilter;
import com.vmturbo.cost.component.cloud.commitment.coverage.AccountCoverageTable.DailyTable;
import com.vmturbo.cost.component.cloud.commitment.coverage.AccountCoverageTable.HourlyTable;
import com.vmturbo.cost.component.cloud.commitment.coverage.AccountCoverageTable.MonthlyTable;

/**
 * A SQL implementation of {@link CloudCommitmentCoverageStore}.
 */
public class SQLCloudCommitmentCoverageStore implements CloudCommitmentCoverageStore {

    private static final String SAMPLE_COUNT = "sample_count";

    private static final String MIN_COVERAGE_AMOUNT = "min_coverage_amount";
    private static final String MAX_COVERAGE_AMOUNT = "max_coverage_amount";
    private static final String SUM_COVERAGE_AMOUNT = "sum_coverage_amount";
    private static final String AVG_COVERAGE_AMOUNT = "avg_coverage_amount";

    private static final String MIN_COVERAGE_CAPACITY = "min_coverage_capacity";
    private static final String MAX_COVERAGE_CAPACITY = "max_coverage_capacity";
    private static final String SUM_COVERAGE_CAPACITY = "sum_coverage_capacity";
    private static final String AVG_COVERAGE_CAPACITY = "avg_coverage_capacity";

    private final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;

    private final CloudGranularityCalculator granularityCalculator;

    /**
     * Constructs a new {@link SQLCloudCommitmentCoverageStore} instance.
     * @param dslContext The {@link DSLContext}.
     * @param granularityCalculator The granularity calculator.
     */
    public SQLCloudCommitmentCoverageStore(@Nonnull DSLContext dslContext,
                                           @Nonnull CloudGranularityCalculator granularityCalculator) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.granularityCalculator = Objects.requireNonNull(granularityCalculator);
    }

    @Override
    public void persistCoverageSamples(@Nonnull final Collection<CloudCommitmentDataBucket> coverageBuckets) {

        Preconditions.checkNotNull(coverageBuckets, "Coverage buckets collection cannot be null");

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final ListMultimap<CloudStatGranularity, CloudCommitmentDataBucket> bucketsByGranularity =
                coverageBuckets.stream()
                        .collect(ImmutableListMultimap.toImmutableListMultimap(
                                CloudCommitmentDataBucket::getGranularity,
                                Function.identity()));

        bucketsByGranularity.asMap().forEach(((granularity, buckets) -> {
            final AccountCoverageTable<?, ?> accountTable = createAccountCoverageTable(granularity);

            // for now, we assume all data points are aggregated at the account level
            buckets.forEach(bucket -> persistDataBucket(accountTable, bucket));
        }));

        logger.info("Persisted {} coverage buckets in {}", coverageBuckets.size(), stopwatch);
    }

    @Override
    public List<CloudCommitmentDataBucket> getCoverageBuckets(@Nonnull final AccountCoverageFilter filter) {

        Preconditions.checkNotNull(filter, "Account coverage filter cannot be null");

        final AccountCoverageTable<?, ?> tableWrapper = createAccountCoverageTable(determineGranularity(filter));


        try (Stream<AccountCoverageRecord<?>> accountCoverageStream =
                     dslContext.selectFrom(tableWrapper.coverageTable())
                             .where(generateConditionsFromFilter(tableWrapper, filter))
                             .stream()
                             .map(tableWrapper::createRecordAccessor)) {

            final ListMultimap<Instant, CloudCommitmentDataPoint> dataPointsByTime =
                    accountCoverageStream.collect(ImmutableListMultimap.toImmutableListMultimap(
                            AccountCoverageRecord::sampleTimeInstant,
                            this::convertRecordToDataPoint));

            return dataPointsByTime.asMap().entrySet()
                    .stream()
                    .map(e -> CloudCommitmentDataBucket.newBuilder()
                            .setGranularity(tableWrapper.granularity())
                            .setTimestampMillis(e.getKey().toEpochMilli())
                            .addAllSample(e.getValue())
                            .build())
                    .sorted(Comparator.comparing(CloudCommitmentDataBucket::getTimestampMillis))
                    .collect(ImmutableList.toImmutableList());
        }
    }

    @Override
    public Stream<CloudCommitmentStatRecord> streamCoverageStats(@Nonnull final AccountCoverageStatsFilter coverageFilter) {

        Preconditions.checkNotNull(coverageFilter, "Account coverage stats filter cannot be null");

        final AccountCoverageTable<?, ?> tableWrapper = createAccountCoverageTable(determineGranularity(coverageFilter));

        logger.debug("Streaming {} coverage stats filtered by {}", tableWrapper::granularity, coverageFilter::toString);

        return dslContext.select(generateSelectFromStatsFilter(tableWrapper, coverageFilter))
                .from(tableWrapper.coverageTable())
                .where(generateConditionsFromFilter(tableWrapper, coverageFilter))
                .groupBy(generateGroupByFields(tableWrapper, coverageFilter))
                .orderBy(tableWrapper.sampleTimeField(),
                        tableWrapper.coverageTypeField(),
                        tableWrapper.coverageSubTypeField())
                .stream()
                .map(tableWrapper::createRecordAccessor)
                .map(this::convertRecordToStat);
    }

    /**
     * Generates a dynamic select based on the group by condition. The select will include min/max/sum/avg
     * of the used and capacity columns, as well as the sample time, coverage type, and coverage subtype (as
     * these are always included in the group by condition).
     * @param coverageTable The granular account coverage table.
     * @param statsFilter The stats filter.
     * @return The list of column selects.
     */
    private Set<SelectFieldOrAsterisk> generateSelectFromStatsFilter(@Nonnull AccountCoverageTable<?, ?> coverageTable,
                                                                     @Nonnull AccountCoverageStatsFilter statsFilter) {

        final ImmutableSet.Builder<SelectFieldOrAsterisk> selectFields = ImmutableSet.<SelectFieldOrAsterisk>builder()
                .add(coverageTable.sampleTimeField())
                .add(coverageTable.coverageTypeField())
                .add(coverageTable.coverageSubTypeField())
                .add(DSL.count().as(SAMPLE_COUNT))
                .add(DSL.min(coverageTable.coverageAmountField()).as(MIN_COVERAGE_AMOUNT))
                .add(DSL.max(coverageTable.coverageAmountField()).as(MAX_COVERAGE_AMOUNT))
                .add(DSL.sum(coverageTable.coverageAmountField()).as(SUM_COVERAGE_AMOUNT))
                .add(DSL.avg(coverageTable.coverageAmountField()).as(AVG_COVERAGE_AMOUNT))
                .add(DSL.min(coverageTable.coverageCapacityField()).as(MIN_COVERAGE_CAPACITY))
                .add(DSL.max(coverageTable.coverageCapacityField()).as(MAX_COVERAGE_CAPACITY))
                .add(DSL.sum(coverageTable.coverageCapacityField()).as(SUM_COVERAGE_CAPACITY))
                .add(DSL.avg(coverageTable.coverageCapacityField()).as(AVG_COVERAGE_CAPACITY));

        if (statsFilter.groupByList().size() > 0) {
            for (GroupByCondition groupByCondition : statsFilter.groupByList()) {
                switch (groupByCondition) {
                    case ACCOUNT:
                        selectFields.add(coverageTable.accountIdField());
                        selectFields.add(coverageTable.serviceProviderIdField());
                        break;
                    case REGION:
                        selectFields.add(coverageTable.regionIdField());
                        selectFields.add(coverageTable.serviceProviderIdField());
                        break;
                    case CLOUD_SERVICE:
                        selectFields.add(coverageTable.cloudServiceIdField());
                        selectFields.add(coverageTable.serviceProviderIdField());
                        break;
                    case SERVICE_PROVIDER:
                        selectFields.add(coverageTable.serviceProviderIdField());
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                String.format("Group by condition not supported: %s", groupByCondition));
                }
            }

        }

        return selectFields.build();
    }

    private Collection<GroupField> generateGroupByFields(@Nonnull AccountCoverageTable<?, ?> coverageTable,
                                                         @Nonnull AccountCoverageStatsFilter statsFilter) {

        final ImmutableList.Builder<GroupField> groupFields = ImmutableList.<GroupField>builder()
                .add(coverageTable.sampleTimeField())
                .add(coverageTable.coverageTypeField())
                .add(coverageTable.coverageSubTypeField());

        statsFilter.groupByList().forEach(groupByCondition -> {
            switch (groupByCondition) {
                case ACCOUNT:
                    groupFields.add(coverageTable.accountIdField());
                    break;
                case REGION:
                    groupFields.add(coverageTable.regionIdField());
                    break;
                case SERVICE_PROVIDER:
                    groupFields.add(coverageTable.serviceProviderIdField());
                    break;
                case CLOUD_SERVICE:
                    groupFields.add(coverageTable.cloudServiceIdField());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format("Group by condition is not supported: %s", groupByCondition));
            }
        });

        return groupFields.build();
    }

    private List<Condition> generateConditionsFromFilter(@Nonnull AccountCoverageTable<?, ?> table,
                                                         @Nonnull AccountCoverageFilter filter) {

        final ImmutableList.Builder<Condition> conditions = ImmutableList.builder();

        // Add start and end time filters, if present
        filter.startTime()
                .map(table::startTimeConditionInclusive)
                .ifPresent(conditions::add);

        filter.endTime()
                .map(table::endTimeConditionExclusive)
                .ifPresent(conditions::add);

        // Add filters for scoping attributes of the coverage data

        final RegionFilter regionFilter = filter.regionFilter();
        if (regionFilter.getRegionIdCount() > 0) {
            conditions.add(
                    table.regionIdField().in(regionFilter.getRegionIdList()));
        }

        final AccountFilter accountFilter = filter.accountFilter();
        if (accountFilter.getAccountIdCount() > 0) {
            conditions.add(
                    table.accountIdField().in(accountFilter.getAccountIdList()));
        }

        final ServiceProviderFilter serviceProviderFilter = filter.serviceProviderFilter();
        if (serviceProviderFilter.getServiceProviderIdCount() > 0) {
            conditions.add(
                    table.serviceProviderIdField().in(serviceProviderFilter.getServiceProviderIdList()));
        }

        return conditions.build();
    }

    private CloudCommitmentDataPoint convertRecordToDataPoint(@Nonnull AccountCoverageRecord<?> record) {
        return CloudCommitmentDataPoint.newBuilder()
                .setAccountId(record.accountId())
                .setRegionId(record.regionId())
                .setCloudServiceId(record.cloudServiceId())
                .setServiceProviderId(record.serviceProviderId())
                .setUsed(CloudCommitmentUtils.buildCommitmentAmount(
                        record.coverageType(),
                        record.coverageSubType(),
                        record.coverageAmount()))
                .setCapacity(CloudCommitmentUtils.buildCommitmentAmount(
                        record.coverageType(),
                        record.coverageSubType(),
                        record.coverageCapacity()))
                .build();
    }

    private CloudCommitmentStatRecord convertRecordToStat(@Nonnull AccountCoverageRecord<?> record) {

        final Instant sampleTime = record.sampleTimeInstant();
        final CloudCommitmentCoverageType coverageType = record.coverageType();
        final int coverageSubType = record.coverageSubType();

        final long sampleCount = record.get(SAMPLE_COUNT, Long.class);
        final double minCoverageAmount = record.get(MIN_COVERAGE_AMOUNT, Double.class);
        final double maxCoverageAmount = record.get(MAX_COVERAGE_AMOUNT, Double.class);
        final double sumCoverageAmount = record.get(SUM_COVERAGE_AMOUNT, Double.class);
        final BigDecimal avgCoverageAmount = record.get(AVG_COVERAGE_AMOUNT, BigDecimal.class);
        final double minCoverageCapacity = record.get(MIN_COVERAGE_CAPACITY, Double.class);
        final double maxCoverageCapacity = record.get(MAX_COVERAGE_CAPACITY, Double.class);
        final double sumCoverageCapacity = record.get(SUM_COVERAGE_CAPACITY, Double.class);
        final BigDecimal avgCoverageCapacity = record.get(AVG_COVERAGE_CAPACITY, BigDecimal.class);

        final CloudCommitmentStatRecord.Builder statRecord = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(sampleTime.toEpochMilli())
                .setSampleCount(sampleCount)
                .setValues(StatValue.newBuilder()
                        .setMin(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, minCoverageAmount))
                        .setMax(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, maxCoverageAmount))
                        .setTotal(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, sumCoverageAmount))
                        .setAvg(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, avgCoverageAmount.doubleValue()))
                        .build())
                .setCapacity(StatValue.newBuilder()
                        .setMin(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, minCoverageCapacity))
                        .setMax(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, maxCoverageCapacity))
                        .setTotal(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, sumCoverageCapacity))
                        .setAvg(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, avgCoverageCapacity.doubleValue()))
                        .build());

        // Add scoping information
        if (record.hasAccountId()) {
            statRecord.setAccountId(record.accountId());
        }

        if (record.hasRegionId()) {
            statRecord.setRegionId(record.regionId());
        }

        if (record.hasCloudServiceId()) {
            statRecord.setCloudServiceId(record.cloudServiceId());
        }

        if (record.hasServiceProviderId()) {
            statRecord.setServiceProviderId(record.serviceProviderId());
        }

        return statRecord.build();
    }

    private CloudStatGranularity determineGranularity(@Nonnull AccountCoverageFilter filter) {
        return filter.granularity()
                .orElseGet(() -> filter.startTime()
                        // If an explicit granularity was not provided, we determine the granularity
                        // base on the start time and retention periods.
                        .map(startTime -> granularityCalculator.startTimeToGranularity(startTime))
                        // If neither a granularity nor start time have been specified, we default
                        // to returning hourly data.
                        .orElse(CloudStatGranularity.HOURLY));
    }

    private AccountCoverageTable<?, ?> createAccountCoverageTable(@Nonnull CloudStatGranularity granularity) {
        switch (granularity) {
            case HOURLY:
                return new HourlyTable();
            case DAILY:
                return new DailyTable();
            case MONTHLY:
                return new MonthlyTable();
            default:
                throw new UnsupportedOperationException(
                        String.format("Cloud state granularity '%s' is not supported", granularity));
        }
    }

    private void persistDataBucket(@Nonnull AccountCoverageTable<?, ?> accountCoverageTable,
                                   @Nonnull CloudCommitmentDataBucket coverageBucket) {

        logger.trace("Persisting coverage data bucket: {}", coverageBucket::toString);

        final CloudCommitmentDataBucket validatedBucket = coverageBucket.toBuilder()
                .clearSample()
                .addAllSample(coverageBucket.getSampleList().stream()
                        .filter(this::isValidAccountSample)
                        .collect(ImmutableList.toImmutableList()))
                .build();

        dslContext
                .batch(accountCoverageTable.toRecords(validatedBucket)
                        .map(record -> dslContext.insertInto(accountCoverageTable.coverageTable())
                                .set(record)
                                .onDuplicateKeyUpdate()
                                .set(record))
                        .collect(ImmutableList.toImmutableList()))
                .execute();

    }

    private boolean isValidAccountSample(@Nonnull CloudCommitmentDataPoint sample) {

        final boolean isValid = sample.hasAccountId()
                && sample.hasRegionId()
                && sample.hasCloudServiceId()
                && sample.hasServiceProviderId()
                && sample.hasUsed()
                && sample.hasCapacity()
                && sample.getUsed().getValueCase() == sample.getCapacity().getValueCase();

        if (!isValid) {
            logger.warn("Dropping invalid account coverage data point: {}", sample);
        }

        return isValid;
    }
}
