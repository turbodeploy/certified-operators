package com.vmturbo.cost.component.cloud.commitment.utilization;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.GroupField;
import org.jooq.Record;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.impl.DSL;

import com.vmturbo.cloud.common.commitment.CloudCommitmentUtils;
import com.vmturbo.cloud.common.stat.CloudGranularityCalculator;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData.CloudCommitmentDataBucket;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData.CloudCommitmentDataBucket.CloudCommitmentDataPoint;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord.StatValue;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCloudCommitmentUtilizationRequest.GroupByCondition;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudCommitmentFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudStatGranularity;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.ServiceProviderFilter;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationTable.DailyUtilizationTable;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationTable.HourlyUtilizationTable;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationTable.MonthlyUtilizationTable;

/**
 * A SQL implementation of {@link CloudCommitmentUtilizationStore}.
 */
public class SQLCloudCommitmentUtilizationStore implements CloudCommitmentUtilizationStore {

    private static final String SAMPLE_COUNT = "sample_count";

    private static final String MIN_UTILIZATION_AMOUNT = "min_utilization_amount";
    private static final String MAX_UTILIZATION_AMOUNT = "max_utilization_amount";
    private static final String SUM_UTILIZATION_AMOUNT = "sum_utilization_amount";
    private static final String AVG_UTILIZATION_AMOUNT = "avg_utilization_amount";

    private static final String MIN_COMMITMENT_CAPACITY = "min_utilization_capacity";
    private static final String MAX_COMMITMENT_CAPACITY = "max_utilization_capacity";
    private static final String SUM_COMMITMENT_CAPACITY = "sum_utilization_capacity";
    private static final String AVG_COMMITMENT_CAPACITY = "avg_utilization_capacity";


    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;

    private final CloudGranularityCalculator granularityCalculator;

    /**
     * Constructs a new {@link SQLCloudCommitmentUtilizationStore} instance.
     * @param dslContext The {@link DSLContext} to use in queries.
     * @param granularityCalculator The granularity calculator.
     */
    public SQLCloudCommitmentUtilizationStore(@Nonnull DSLContext dslContext,
                                              @Nonnull CloudGranularityCalculator granularityCalculator) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.granularityCalculator = Objects.requireNonNull(granularityCalculator);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void persistUtilizationSamples(@Nonnull final Collection<CloudCommitmentDataBucket> utilizationBuckets) {
        Preconditions.checkNotNull(utilizationBuckets, "Utilization buckets collection cannot be null");

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final ListMultimap<CloudStatGranularity, CloudCommitmentDataBucket> utilizationBucketsByGranularity =
                utilizationBuckets.stream()
                        .collect(ImmutableListMultimap.toImmutableListMultimap(
                                CloudCommitmentDataBucket::getGranularity,
                                Function.identity()));

        utilizationBucketsByGranularity.asMap().forEach((granularity, buckets) -> {
            logger.info("Persisting {} utilization buckets to the {} table", buckets::size, granularity::toString);

            final UtilizationTable<?, ?> utilizationTable = createUtilizationTable(granularity);
            buckets.forEach(bucket -> persistDataBucket(utilizationTable, bucket));
        });

        logger.info("Persisted {} utilization buckets in {}", utilizationBuckets.size(), stopwatch);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<CloudCommitmentDataBucket> getUtilizationBuckets(@Nonnull final CloudCommitmentUtilizationFilter filter) {

        Preconditions.checkNotNull(filter, "Utilization filter cannot be null");

        final UtilizationTable<?, ?> utilizationTable = createUtilizationTable(determineGranularity(filter));

        logger.debug("Querying {} utilization buckets filtered by {}", utilizationTable::granularity, filter::toString);

        try (Stream<? extends Record> stream = dslContext.selectFrom(utilizationTable.wrappedTable())
                        .where(generateConditionsFromFilter(utilizationTable, filter))
                        .stream()) {

            final ListMultimap<Instant, CloudCommitmentDataPoint> dataPointsByTime = stream
                            .map(utilizationTable::createRecordAccessor)
                            .collect(ImmutableListMultimap.toImmutableListMultimap(
                            UtilizationRecord::sampleTimeInstant,
                            this::convertRecordToDataPoint));

            return dataPointsByTime.asMap().entrySet()
                            .stream()
                            .map(e -> CloudCommitmentDataBucket.newBuilder()
                                    .setGranularity(utilizationTable.granularity())
                                    .setTimestampMillis(e.getKey().toEpochMilli())
                                    .addAllSample(e.getValue())
                                    .build())
                            .sorted(Comparator.comparing(CloudCommitmentDataBucket::getTimestampMillis))
                            .collect(ImmutableList.toImmutableList());
        }
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Stream<CloudCommitmentStatRecord> streamUtilizationStats(@Nonnull final CloudCommitmentUtilizationStatsFilter filter) {

        Preconditions.checkNotNull(filter, "Utilization stats filter cannot be null");

        final UtilizationTable<?, ?> utilizationTable = createUtilizationTable(determineGranularity(filter));

        logger.debug("Streaming {} utilization stats filtered by {}", utilizationTable::granularity, filter::toString);

        Stream<CloudCommitmentStatRecord>
                        statRecordStream =
                        dslContext.select(generateSelectFromStatsFilter(utilizationTable, filter))
                                        .from(utilizationTable.wrappedTable())
                                        .where(generateConditionsFromFilter(utilizationTable,
                                                                            filter))
                                        .groupBy(generateGroupByFields(utilizationTable, filter))
                                        .orderBy(utilizationTable.sampleTimeField(),
                                                 utilizationTable.coverageTypeField(),
                                                 utilizationTable.coverageSubTypeField())
                                        .stream()
                                        .map(utilizationTable::createRecordAccessor)
                                        .map(this::convertRecordToStat);

        logger.debug("Streaming {} {} utilization stat records", statRecordStream::count, utilizationTable::granularity);

        return statRecordStream;
    }

    private List<SelectFieldOrAsterisk> generateSelectFromStatsFilter(@Nonnull UtilizationTable<?, ?> utilizationTable,
                    @Nonnull CloudCommitmentUtilizationStatsFilter statsFilter) {

        final ImmutableList.Builder<SelectFieldOrAsterisk> selectFieldList = ImmutableList.<SelectFieldOrAsterisk>builder()
                .add(utilizationTable.sampleTimeField())
                .add(utilizationTable.coverageTypeField())
                .add(utilizationTable.coverageSubTypeField())
                .add(DSL.count().as(SAMPLE_COUNT))
                .add(DSL.min(utilizationTable.utilizationAmountField()).as(MIN_UTILIZATION_AMOUNT))
                .add(DSL.max(utilizationTable.utilizationAmountField()).as(MAX_UTILIZATION_AMOUNT))
                .add(DSL.sum(utilizationTable.utilizationAmountField()).as(SUM_UTILIZATION_AMOUNT))
                .add(DSL.avg(utilizationTable.utilizationAmountField()).as(AVG_UTILIZATION_AMOUNT))
                .add(DSL.min(utilizationTable.capacityField()).as(MIN_COMMITMENT_CAPACITY))
                .add(DSL.max(utilizationTable.capacityField()).as(MAX_COMMITMENT_CAPACITY))
                .add(DSL.sum(utilizationTable.capacityField()).as(SUM_COMMITMENT_CAPACITY))
                .add(DSL.avg(utilizationTable.capacityField()).as(AVG_COMMITMENT_CAPACITY));

        if (statsFilter.groupByList().size() > 0) {
            for (GroupByCondition groupByCondition : statsFilter.groupByList()) {
                switch (groupByCondition) {
                    case CLOUD_COMMITMENT:
                        selectFieldList.add(utilizationTable.cloudCommitmentIdField());
                        // Intentionally fall through to include account and service provider.
                    case ACCOUNT:
                        selectFieldList.add(utilizationTable.accountIdField());
                        // grouping by account can still provide service provider ID
                    case SERVICE_PROVIDER:
                        selectFieldList.add(utilizationTable.serviceProviderIdField());
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                        String.format("Group by condition not supported: %s", groupByCondition));
                }
            }

        }

        return selectFieldList.build();
    }

    private Collection<GroupField> generateGroupByFields(@Nonnull UtilizationTable<?, ?> utilizationTable,
                                                         @Nonnull CloudCommitmentUtilizationStatsFilter statsFilter) {

        final ImmutableList.Builder<GroupField> groupFields = ImmutableList.<GroupField>builder()
                .add(utilizationTable.sampleTimeField())
                .add(utilizationTable.coverageTypeField())
                .add(utilizationTable.coverageSubTypeField());

        statsFilter.groupByList().forEach(groupByCondition -> {
            switch (groupByCondition) {
                case CLOUD_COMMITMENT:
                    groupFields.add(utilizationTable.cloudCommitmentIdField());
                    break;
                case SERVICE_PROVIDER:
                    groupFields.add(utilizationTable.serviceProviderIdField());
                    break;
                case ACCOUNT:
                    groupFields.add(utilizationTable.accountIdField());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format("Group by condition is not supported: %s", groupByCondition));
            }
        });

        return groupFields.build();
    }

    private Collection<Condition> generateConditionsFromFilter(@Nonnull UtilizationTable<?, ?> utilizationTable,
                                                               @Nonnull CloudCommitmentUtilizationFilter filter) {

        final ImmutableList.Builder<Condition> conditions = ImmutableList.builder();

        // Add start and end time filters, if present
        filter.startTime()
                .map(utilizationTable::startTimeConditionInclusive)
                .ifPresent(conditions::add);

        filter.endTime()
                .map(utilizationTable::endTimeConditionExclusive)
                .ifPresent(conditions::add);

        // Add filters for scoping attributes of the utilization data

        final CloudCommitmentFilter cloudCommitmentFilter = filter.cloudCommitmentFilter();
        if (cloudCommitmentFilter.getCloudCommitmentIdCount() > 0) {
            conditions.add(utilizationTable
                    .cloudCommitmentIdField()
                    .in(cloudCommitmentFilter.getCloudCommitmentIdList()));
        }

        final RegionFilter regionFilter = filter.regionFilter();
        if (regionFilter.getRegionIdCount() > 0) {
            conditions.add(
                    utilizationTable.regionIdField().in(regionFilter.getRegionIdList()));
        }

        final AccountFilter accountFilter = filter.accountFilter();
        if (accountFilter.getAccountIdCount() > 0) {
            conditions.add(
                    utilizationTable.accountIdField().in(accountFilter.getAccountIdList()));
        }

        final ServiceProviderFilter serviceProviderFilter = filter.serviceProviderFilter();
        if (serviceProviderFilter.getServiceProviderIdCount() > 0) {
            conditions.add(
                    utilizationTable.serviceProviderIdField().in(serviceProviderFilter.getServiceProviderIdList()));
        }

        return conditions.build();
    }

    private CloudCommitmentDataPoint convertRecordToDataPoint(@Nonnull UtilizationRecord<?> record) {

        return CloudCommitmentDataPoint.newBuilder()
                .setCommitmentId(record.cloudCommitmentId())
                .setAccountId(record.accountId())
                .setRegionId(record.regionId())
                .setServiceProviderId(record.serviceProviderId())
                .setUsed(CloudCommitmentUtils.buildCommitmentAmount(
                        record.coverageType(),
                        record.coverageSubType(),
                        record.utilizationAmount()))
                .setCapacity(CloudCommitmentUtils.buildCommitmentAmount(
                        record.coverageType(),
                        record.coverageSubType(),
                        record.capacity()))
                .build();
    }

    private CloudCommitmentStatRecord convertRecordToStat(@Nonnull UtilizationRecord<?> record) {

        final Instant sampleTime = record.sampleTimeInstant();
        final CloudCommitmentCoverageType coverageType = record.coverageType();
        final int coverageSubType = record.coverageSubType();

        final long sampleCount = record.get(SAMPLE_COUNT, Long.class);
        final double minUtilizationAmount = record.get(MIN_UTILIZATION_AMOUNT, Double.class);
        final double maxUtilizationAmount = record.get(MAX_UTILIZATION_AMOUNT, Double.class);
        final double sumUtilizationAmount = record.get(SUM_UTILIZATION_AMOUNT, Double.class);
        final BigDecimal avgUtilizationAmount = record.get(AVG_UTILIZATION_AMOUNT, BigDecimal.class);
        final double minCommitmentCapacity = record.get(MIN_COMMITMENT_CAPACITY, Double.class);
        final double maxCommitmentCapacity = record.get(MAX_COMMITMENT_CAPACITY, Double.class);
        final double sumCommitmentCapacity = record.get(SUM_COMMITMENT_CAPACITY, Double.class);
        final BigDecimal avgCommitmentCapacity = record.get(AVG_COMMITMENT_CAPACITY, BigDecimal.class);

        final CloudCommitmentStatRecord.Builder statRecord = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(sampleTime.toEpochMilli())
                .setSampleCount(sampleCount)
                .setValues(StatValue.newBuilder()
                       .setMin(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, minUtilizationAmount))
                       .setMax(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, maxUtilizationAmount))
                       .setTotal(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, sumUtilizationAmount))
                       .setAvg(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, avgUtilizationAmount.doubleValue()))
                       .build())
                .setCapacity(StatValue.newBuilder()
                        .setMin(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, minCommitmentCapacity))
                        .setMax(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, maxCommitmentCapacity))
                        .setTotal(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, sumCommitmentCapacity))
                        .setAvg(CloudCommitmentUtils.buildCommitmentAmount(coverageType, coverageSubType, avgCommitmentCapacity.doubleValue()))
                        .build());

        // Add scoping information
        if (record.hasCloudCommitmentId()) {
            statRecord.setCommitmentId(record.cloudCommitmentId());
        }

        if (record.hasAccountId()) {
            statRecord.setAccountId(record.accountId());
        }

        if (record.hasRegionId()) {
            statRecord.setRegionId(record.regionId());
        }

        if (record.hasServiceProviderId()) {
            statRecord.setServiceProviderId(record.serviceProviderId());
        }

        return statRecord.build();
    }

    private CloudStatGranularity determineGranularity(@Nonnull CloudCommitmentUtilizationFilter filter) {
        return filter.granularity()
                .orElseGet(() -> filter.startTime()
                        // If an explicit granularity was not provided, we determine the granularity
                        // base on the start time and retention periods.
                        .map(startTime -> granularityCalculator.startTimeToGranularity(startTime))
                        // If neither a granularity nor start time have been specified, we default
                        // to returning hourly data.
                        .orElse(CloudStatGranularity.HOURLY));
    }

    /**
     * Constructs and returns a new {@link UtilizationTable} instance for the corresponding
     * {@code granularity}.
     * @param granularity The target utilization stat granularity.
     * @return The newly constructed {@link UtilizationTable} instance.
     */
    @Nonnull
    public static UtilizationTable<?, ?> createUtilizationTable(@Nonnull CloudStatGranularity granularity) {
        switch (granularity) {
            case HOURLY:
                return new HourlyUtilizationTable();
            case DAILY:
                return new DailyUtilizationTable();
            case MONTHLY:
                return new MonthlyUtilizationTable();
            default:
                throw new UnsupportedOperationException(
                                String.format("Unsupported granularity: %s", granularity));
        }
    }

    /**
     * Corresponds to {@link CloudCommitmentUtilizationStore#persistUtilizationSamples(Collection)}.
     * @param utilizationBucket The utilization buckets to persist.
     * @param utilizationTable The utilization table.
     */
    public void persistDataBucket(@Nonnull UtilizationTable<?, ?> utilizationTable,
                                  @Nonnull final CloudCommitmentDataBucket utilizationBucket) {

        logger.trace("Persisting utilization data bucket: {}", utilizationBucket);

        final CloudCommitmentDataBucket validatedBucket = utilizationBucket.toBuilder()
                .clearSample()
                .addAllSample(utilizationBucket.getSampleList().stream()
                          .filter(this::isValidDataPoint)
                          .collect(ImmutableList.toImmutableList()))
                .build();

        dslContext
                .batch(utilizationTable.toRecords(validatedBucket)
                        .map(record -> dslContext.insertInto(utilizationTable.wrappedTable())
                                .set(record)
                                .onDuplicateKeyUpdate()
                                .set(record))
                        .collect(ImmutableList.toImmutableList()))
                .execute();
    }

    private boolean isValidDataPoint(@Nonnull CloudCommitmentDataPoint dataPoint) {

        final boolean isValid = dataPoint.hasCommitmentId()
                                && dataPoint.hasAccountId()
                                && dataPoint.hasServiceProviderId()
                                && dataPoint.hasUsed()
                                && dataPoint.hasCapacity()
                                && dataPoint.getUsed().getValueCase() == dataPoint.getCapacity().getValueCase();

        if (!isValid) {
            logger.warn("Dropping invalid data point: {}", dataPoint);
        }

        return isValid;
    }

}
