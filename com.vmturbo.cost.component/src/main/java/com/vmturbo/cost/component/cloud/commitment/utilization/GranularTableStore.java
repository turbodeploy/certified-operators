package com.vmturbo.cost.component.cloud.commitment.utilization;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.GroupField;
import org.jooq.Record;
import org.jooq.SelectFieldOrAsterisk;
import org.jooq.Table;
import org.jooq.UpdatableRecord;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
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
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore.CloudCommitmentUtilizationFilter;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore.CloudCommitmentUtilizationStatsFilter;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.DailyCloudCommitmentUtilizationRecord;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * A store specific to a single {@link CloudStatGranularity} of a cloud commitment utilization table. This
 * class is intended to abstract the table specific columns from the {@link SQLCloudCommitmentUtilizationStore}.
 * @param <WrappedTableRecordT> The jooq record type.
 * @param <SampleTemporalTypeT> The sample time type (e.g. LocalDateTime or LocalDate).
 */
class GranularTableStore<
        WrappedTableRecordT extends UpdatableRecord<WrappedTableRecordT>,
        SampleTemporalTypeT extends Temporal> {

    private static final String SAMPLE_COUNT = "sample_count";

    private static final String MIN_UTILIZATION_AMOUNT = "min_utilization_amount";
    private static final String MAX_UTILIZATION_AMOUNT = "max_utilization_amount";
    private static final String SUM_UTILIZATION_AMOUNT = "sum_utilization_amount";
    private static final String AVG_UTILIZATION_AMOUNT = "avg_utilization_amount";

    private static final String MIN_COMMITMENT_CAPACITY = "min_commitment_capacity";
    private static final String MAX_COMMITMENT_CAPACITY = "max_commitment_capacity";
    private static final String SUM_COMMITMENT_CAPACITY = "sum_commitment_capacity";
    private static final String AVG_COMMITMENT_CAPACITY = "avg_commitment_capacity";

    private final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;

    private final UtilizationTable<WrappedTableRecordT, SampleTemporalTypeT> utilizationTable;

    /**
     * Constructs a new {@link GranularTableStore} instance.
     * @param dslContext The {@link DSLContext} used in queries to the DB.
     * @param utilizationTable The target utilization table to query.
     */
    GranularTableStore(@Nonnull DSLContext dslContext,
                       @Nonnull UtilizationTable<WrappedTableRecordT, SampleTemporalTypeT> utilizationTable) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.utilizationTable = Objects.requireNonNull(utilizationTable);
    }

    /**
     * Constructs and returns a new {@link GranularTableStore} instance for the corresponding
     * {@code granularity}.
     * @param granularity The target utilization stat granularity.
     * @param dslContext The {@link DSLContext} used to construct the {@link GranularTableStore} instance.
     * @return The newly constructed {@link GranularTableStore} instance.
     */
    @Nonnull
    public static GranularTableStore<?, ?> createTableStore(@Nonnull CloudStatGranularity granularity,
                                                            @Nonnull DSLContext dslContext) {
        switch (granularity) {

            case HOURLY:
                throw new NotImplementedException("Hourly  granularity is not supported");
            case DAILY:
                return new GranularTableStore<>(dslContext, new DailyUtilizationTable());
            case MONTHLY:
                throw new NotImplementedException("Monthly  granularity is not supported");
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported granularity: %s", granularity));
        }
    }

    /**
     * Corresponds to {@link CloudCommitmentUtilizationStore#getUtilizationBuckets}.
     * @param filter The utilization filter.
     * @return The list of utilization data buckets, sorted by sample time.
     */
    @Nonnull
    public List<CloudCommitmentDataBucket> getUtilizationBuckets(@Nonnull CloudCommitmentUtilizationFilter filter) {

        logger.debug("Querying {} utilization buckets filtered by {}", utilizationTable::granularity, filter::toString);

        final ListMultimap<Instant, CloudCommitmentDataPoint> dataPointsByTime =
                dslContext.selectFrom(utilizationTable.wrappedTable())
                        .where(generateConditionsFromFilter(filter))
                        .stream()
                        .map(utilizationTable::createRecordAccessor)
                        .collect(ImmutableListMultimap.toImmutableListMultimap(
                                RecordAccessor::sampleTimeInstant,
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

    /**
     * Corresponds to {@link CloudCommitmentUtilizationStore#streamUtilizationStats(CloudCommitmentUtilizationStatsFilter)}.
     * @param filter The stats filter.
     * @return The stat records, sorted by sample time.
     */
    @Nonnull
    public Stream<CloudCommitmentStatRecord> streamUtilizationStats(@Nonnull final CloudCommitmentUtilizationStatsFilter filter) {

        logger.debug("Streaming {} utilization stats filtered by {}", utilizationTable::granularity, filter::toString);

        return dslContext.select(generateSelectFromStatsFilter(filter))
                .from(utilizationTable.wrappedTable())
                .where(generateConditionsFromFilter(filter))
                .groupBy(generateGroupByFields(filter))
                .orderBy(utilizationTable.sampleTimeField(),
                        utilizationTable.coverageTypeField(),
                        utilizationTable.coverageSubTypeField())
                .stream()
                .map(this::convertRecordToStat);
    }

    /**
     * Corresponds to {@link CloudCommitmentUtilizationStore#persistUtilizationSamples(Collection)}.
     * @param utilizationBucket The utilization buckets to persist.
     */
    public void persistDataBucket(@Nonnull final CloudCommitmentDataBucket utilizationBucket) {

        logger.trace("Persisting utilization data bucket: {}", utilizationBucket);

        final CloudCommitmentDataBucket validatedBucket = utilizationBucket.toBuilder()
                .clearSample()
                .addAllSample(utilizationBucket.getSampleList().stream()
                        .filter(this::isValidDataPoint)
                        .collect(ImmutableList.toImmutableList()))
                .build();

        dslContext
                .batch(
                        utilizationTable.toRecords(validatedBucket)
                                .map(record -> dslContext.insertInto(utilizationTable.wrappedTable())
                                        .set(record)
                                        .onDuplicateKeyUpdate()
                                        .set(record))
                                .collect(ImmutableList.toImmutableList()))
                .execute();
    }

    private Collection<Condition> generateConditionsFromFilter(@Nonnull CloudCommitmentUtilizationFilter filter) {
        final ImmutableList.Builder<Condition> conditions = ImmutableList.builder();

        // Add start and end time filters, if present
        final Field<SampleTemporalTypeT> sampleTimeField = utilizationTable.sampleTimeField();
        filter.startTime()
                .map(utilizationTable::toSampleTime)
                .ifPresent(startTime -> conditions.add(sampleTimeField.greaterOrEqual(startTime)));

        filter.endTime()
                .map(utilizationTable::toSampleTime)
                .ifPresent(endTime -> conditions.add(sampleTimeField.lessThan(endTime)));

        // Add filters for scoping attributes of the utilization data

        final CloudCommitmentFilter cloudCommitmentFilter = filter.cloudCommitmentFilter();
        if (cloudCommitmentFilter.getCloudCommitmentIdCount() > 0) {
            conditions.add(
                    utilizationTable.cloudCommitmentIdField()
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

    /**
     * Generates a dynamic select based on the group by condition. The select will include min/max/sum/avg
     * of the used and capacity columns, as well as the sample time, coverage type, and coverage subtype (as
     * these are always included in the group by condition).
     * @param statsFilter The stats filter.
     * @return The list of column selects.
     */
    private List<SelectFieldOrAsterisk> generateSelectFromStatsFilter(@Nonnull CloudCommitmentUtilizationStatsFilter statsFilter) {

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

    private Collection<GroupField> generateGroupByFields(@Nonnull CloudCommitmentUtilizationStatsFilter statsFilter) {

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

    private CloudCommitmentStatRecord convertRecordToStat(@Nonnull Record record) {

        final RecordAccessor<SampleTemporalTypeT> recordAccessor = utilizationTable.createRecordAccessor(record);

        final Instant sampleTime = recordAccessor.sampleTimeInstant();
        final CloudCommitmentCoverageType coverageType = recordAccessor.coverageType();
        final int coverageSubType = recordAccessor.coverageSubType();

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
                        .setMin(buildCommitmentAmount(coverageType, coverageSubType, minUtilizationAmount))
                        .setMax(buildCommitmentAmount(coverageType, coverageSubType, maxUtilizationAmount))
                        .setTotal(buildCommitmentAmount(coverageType, coverageSubType, sumUtilizationAmount))
                        .setAvg(buildCommitmentAmount(coverageType, coverageSubType, avgUtilizationAmount.doubleValue()))
                        .build())
                .setCapacity(StatValue.newBuilder()
                        .setMin(buildCommitmentAmount(coverageType, coverageSubType, minCommitmentCapacity))
                        .setMax(buildCommitmentAmount(coverageType, coverageSubType, maxCommitmentCapacity))
                        .setTotal(buildCommitmentAmount(coverageType, coverageSubType, sumCommitmentCapacity))
                        .setAvg(buildCommitmentAmount(coverageType, coverageSubType, avgCommitmentCapacity.doubleValue()))
                        .build());

        // Add scoping information
        if (recordAccessor.hasCloudCommitmentId()) {
            statRecord.setCommitmentId(recordAccessor.cloudCommitmentId());
        }

        if (recordAccessor.hasAccountId()) {
            statRecord.setAccountId(recordAccessor.accountId());
        }

        if (recordAccessor.hasRegionId()) {
            statRecord.setRegionId(recordAccessor.regionId());
        }

        if (recordAccessor.hasServiceProviderId()) {
            statRecord.setServiceProviderId(recordAccessor.serviceProviderId());
        }

        return statRecord.build();
    }

    private CloudCommitmentDataPoint convertRecordToDataPoint(
            @Nonnull RecordAccessor<SampleTemporalTypeT> record) {

        return CloudCommitmentDataPoint.newBuilder()
                .setCommitmentId(record.cloudCommitmentId())
                .setAccountId(record.accountId())
                .setRegionId(record.regionId())
                .setServiceProviderId(record.serviceProviderId())
                .setUsed(buildCommitmentAmount(
                        record.coverageType(),
                        record.coverageSubType(),
                        record.utilizationAmount()))
                .setCapacity(buildCommitmentAmount(
                        record.coverageType(),
                        record.coverageSubType(),
                        record.capacity()))
                .build();
    }

    private CloudCommitmentAmount buildCommitmentAmount(@Nonnull CloudCommitmentCoverageType coverageType,
                                                        int coverageSubType,
                                                        double amount) {
        if (coverageType == CloudCommitmentCoverageType.SPEND_COMMITMENT) {

            return CloudCommitmentAmount.newBuilder()
                    .setAmount(CurrencyAmount.newBuilder()
                            .setCurrency(coverageSubType)
                            .setAmount(amount))
                    .build();
        } else { // assume coupons for now
            return CloudCommitmentAmount.newBuilder()
                    .setCoupons(amount)
                    .build();
        }
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

    /**
     * A wrapper around a jooq table representing utilization for a specific granularity.
     * @param <WrappedTableRecordT> The jooq record type.
     * @param <SampleTemporalTypeT> The sample time type (e.g. LocalDateTime or LocalDate).
     */
    protected abstract static class UtilizationTable<
            WrappedTableRecordT extends UpdatableRecord<WrappedTableRecordT>,
            SampleTemporalTypeT extends Temporal> {

        protected static final ZoneId UTC_ZONE_ID = ZoneId.from(ZoneOffset.UTC);

        private static final String SAMPLE_TIME_COLUMN = "sample_time";

        private static final String ACCOUNT_ID_COLUMN = "account_id";

        private static final String REGION_ID_COLUMN = "region_id";

        private static final String SERVICE_PROVIDER_ID_COLUMN = "service_provider_id";

        private static final String CLOUD_COMMITMENT_ID_COLUMN = "cloud_commitment_id";

        private static final String UTILIZATION_AMOUNT_COLUMN = "utilization_amount";

        private static final String CAPACITY_COLUMN = "commitment_capacity";

        private static final String COVERAGE_TYPE_COLUMN = "coverage_type";

        private static final String COVERAGE_SUB_TYPE_COLUMN = "coverage_sub_type";

        protected final Table<WrappedTableRecordT> wrappedTable;

        protected final CloudStatGranularity tableGranularity;

        protected final UtilizationRecordCreator<WrappedTableRecordT, SampleTemporalTypeT> recordCreator;


        public UtilizationTable(@Nonnull Table<WrappedTableRecordT> wrappedTable,
                                @Nonnull CloudStatGranularity tableGranularity,
                                @Nonnull UtilizationRecordCreator<WrappedTableRecordT, SampleTemporalTypeT> recordCreator) {
            this.wrappedTable = Objects.requireNonNull(wrappedTable);
            this.tableGranularity = Objects.requireNonNull(tableGranularity);
            this.recordCreator = Objects.requireNonNull(recordCreator);
        }


        public Table<WrappedTableRecordT> wrappedTable() {
            return wrappedTable;
        }

        public CloudStatGranularity granularity() {
            return tableGranularity;
        }

        public Field<SampleTemporalTypeT> sampleTimeField() {
            return (Field<SampleTemporalTypeT>)wrappedTable.field(SAMPLE_TIME_COLUMN);
        }

        public Field<Long> regionIdField() {
            return wrappedTable.field(REGION_ID_COLUMN, Long.class);
        }

        public Field<Long> accountIdField() {
            return wrappedTable.field(ACCOUNT_ID_COLUMN, Long.class);
        }

        public Field<Long> serviceProviderIdField() {
            return wrappedTable.field(SERVICE_PROVIDER_ID_COLUMN, Long.class);
        }

        public Field<Long> cloudCommitmentIdField() {
            return wrappedTable.field(CLOUD_COMMITMENT_ID_COLUMN, Long.class);
        }

        public Field<Double> utilizationAmountField() {
            return wrappedTable.field(UTILIZATION_AMOUNT_COLUMN, Double.class);
        }

        public Field<Double> capacityField() {
            return wrappedTable.field(CAPACITY_COLUMN, Double.class);
        }

        public Field<Byte> coverageTypeField() {
            return wrappedTable.field(COVERAGE_TYPE_COLUMN, Byte.class);
        }

        public Field<Integer> coverageSubTypeField() {
            return wrappedTable.field(COVERAGE_SUB_TYPE_COLUMN, Integer.class);
        }

        @Nonnull
        public RecordAccessor<SampleTemporalTypeT> createRecordAccessor(@Nonnull Record record) {
            return new RecordAccessor<>(this, record);
        }

        @Nonnull
        public Stream<WrappedTableRecordT> toRecords(@Nonnull CloudCommitmentDataBucket dataBucket) {

            final SampleTemporalTypeT sampleTime =
                    toSampleTime(Instant.ofEpochMilli(dataBucket.getTimestampMillis()));

            return dataBucket.getSampleList().stream()
                    .map(sample -> recordCreator.createRecord(
                            sampleTime,
                            sample.getCommitmentId(),
                            sample.getAccountId(),
                            // region ID may default to 0, if not set
                            sample.getRegionId(),
                            sample.getServiceProviderId(),
                            sample.getUsed().hasAmount()
                                    ? sample.getUsed().getAmount().getAmount()
                                    : sample.getUsed().getCoupons(),
                            sample.getCapacity().hasAmount()
                                    ? sample.getCapacity().getAmount().getAmount()
                                    : sample.getCapacity().getCoupons(),
                            (byte)(sample.getCapacity().hasAmount()
                                    ? CloudCommitmentCoverageType.SPEND_COMMITMENT_VALUE
                                    : CloudCommitmentCoverageType.COUPONS_VALUE),
                            sample.getCapacity().hasAmount()
                                    ? sample.getCapacity().getAmount().getCurrency()
                                    : 0));
        }

        public abstract SampleTemporalTypeT toSampleTime(@Nonnull Instant instant);
    }

    /**
     * A wrapper around the daily cloud commitment utilization table.
     */
    protected static class DailyUtilizationTable extends UtilizationTable<DailyCloudCommitmentUtilizationRecord, LocalDate> {

        private static final String SAMPLE_TIME_COLUMN = "sample_date";

        public DailyUtilizationTable() {
            super(Tables.DAILY_CLOUD_COMMITMENT_UTILIZATION,
                    CloudStatGranularity.DAILY,
                    DailyCloudCommitmentUtilizationRecord::new);
        }

        @Override
        public Field<LocalDate> sampleTimeField() {
            return wrappedTable.field(SAMPLE_TIME_COLUMN, LocalDate.class);
        }

        @Override
        public LocalDate toSampleTime(@Nonnull final Instant instant) {
            return instant.atZone(UTC_ZONE_ID).toLocalDate();
        }
    }

    /**
     * An accessor to the common fields of a wrapped utilization record.
     * @param <SampleTemporalTypeT> The type of the sample date/time attribute.
     */
    protected static class RecordAccessor<SampleTemporalTypeT extends Temporal> {

        private static final ZoneId UTC_ZONE_ID = ZoneId.from(ZoneOffset.UTC);

        private final UtilizationTable<?, SampleTemporalTypeT> utilizationTable;

        private final Record tableRecord;


        public RecordAccessor(@Nonnull UtilizationTable<?, SampleTemporalTypeT> utilizationTable,
                              Record tableRecord) {
            this.utilizationTable = Objects.requireNonNull(utilizationTable);
            this.tableRecord = Objects.requireNonNull(tableRecord);
        }

        public SampleTemporalTypeT sampleTime() {
            return tableRecord.get(utilizationTable.sampleTimeField());
        }

        public Instant sampleTimeInstant() {
            if (sampleTime() instanceof LocalDate) {
                return ((LocalDate)sampleTime()).atStartOfDay(UTC_ZONE_ID).toInstant();
            } else if (sampleTime() instanceof LocalDateTime) {
                return ((LocalDateTime)sampleTime()).toInstant(ZoneOffset.UTC);
            } else {
                // last resort
                return Instant.from(sampleTime());
            }
        }

        public boolean hasRegionId() {
            return tableRecord.indexOf(utilizationTable.regionIdField()) >= 0;
        }

        public long regionId() {
            return tableRecord.get(utilizationTable.regionIdField());
        }

        public boolean hasAccountId() {
            return tableRecord.indexOf(utilizationTable.accountIdField()) >= 0;
        }

        public long accountId() {
            return tableRecord.get(utilizationTable.accountIdField());
        }

        public boolean hasServiceProviderId() {
            return tableRecord.indexOf(utilizationTable.accountIdField()) >= 0;
        }

        public long serviceProviderId() {
            return tableRecord.get(utilizationTable.serviceProviderIdField());
        }

        public boolean hasCloudCommitmentId() {
            return tableRecord.indexOf(utilizationTable.cloudCommitmentIdField()) >= 0;
        }

        public long cloudCommitmentId() {
            return tableRecord.get(utilizationTable.cloudCommitmentIdField());
        }

        public double utilizationAmount() {
            return tableRecord.get(utilizationTable.utilizationAmountField());
        }

        public double capacity() {
            return tableRecord.get(utilizationTable.capacityField());
        }

        public CloudCommitmentCoverageType coverageType() {
            return CloudCommitmentCoverageType.forNumber(
                    tableRecord.get(utilizationTable.coverageTypeField()));
        }

        public int coverageSubType() {
            return tableRecord.get(utilizationTable.coverageSubTypeField());
        }
    }

    /**
     * A creator of jooq records, based on a common set of attributes.
     * @param <WrappedTableRecordT> The wrapped record type the creator constructs.
     * @param <SampleTemporalTypeT> The type of the sample date/time.
     */
    @FunctionalInterface
    protected interface UtilizationRecordCreator<
            WrappedTableRecordT extends UpdatableRecord<WrappedTableRecordT>,
            SampleTemporalTypeT extends Temporal> {

        WrappedTableRecordT createRecord(
                SampleTemporalTypeT sampleTime,
                long commitmentId,
                long accountId,
                long regionId,
                long serviceProviderId,
                double utilizationAmount,
                double capacity,
                byte coverageType,
                Integer coverageSubType);
    }
}
