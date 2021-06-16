package com.vmturbo.cost.component.cloud.commitment.coverage;

import static com.vmturbo.cost.component.db.Tables.DAILY_ACCOUNT_COMMITMENT_COVERAGE;
import static com.vmturbo.cost.component.db.Tables.HOURLY_ACCOUNT_COMMITMENT_COVERAGE;
import static com.vmturbo.cost.component.db.Tables.MONTHLY_ACCOUNT_COMMITMENT_COVERAGE;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.UpdatableRecord;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData.CloudCommitmentDataBucket;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudStatGranularity;
import com.vmturbo.cost.component.db.tables.records.DailyAccountCommitmentCoverageRecord;
import com.vmturbo.cost.component.db.tables.records.HourlyAccountCommitmentCoverageRecord;
import com.vmturbo.cost.component.db.tables.records.MonthlyAccountCommitmentCoverageRecord;

/**
 * A wrapper around an account-aggregated coverage table.
 * @param <WrappedTableRecordT> The wrapped jooq table record.
 * @param <SampleTemporalTypeT> The temporal type for the sample time column.
 */
abstract class AccountCoverageTable<
        WrappedTableRecordT extends UpdatableRecord<WrappedTableRecordT>,
        SampleTemporalTypeT extends Temporal> {

    private static final String ACCOUNT_ID_COLUMN = "account_id";

    private static final String REGION_ID_COLUMN = "region_id";

    private static final String CLOUD_SERVICE_ID_COLUMN = "cloud_service_id";

    private static final String SERVICE_PROVIDER_ID_COLUMN = "service_provider_id";

    private static final String COVERAGE_AMOUNT_COLUMN = "covered_amount";

    private static final String COVERAGE_CAPACITY_COLUMN = "coverage_capacity";

    private static final String COVERAGE_TYPE_COLUMN = "coverage_type";

    private static final String COVERAGE_SUB_TYPE_COLUMN = "coverage_sub_type";

    protected static final ZoneId UTC_ZONE_ID = ZoneId.from(ZoneOffset.UTC);

    protected final Table<WrappedTableRecordT> coverageTable;

    protected final CloudStatGranularity tableGranularity;

    protected final AccountCoverageRecord.Creator<WrappedTableRecordT, SampleTemporalTypeT> recordCreator;

    protected final String sampleTimeColumnName;

    AccountCoverageTable(@Nonnull Table<WrappedTableRecordT> coverageTable,
                         @Nonnull CloudStatGranularity tableGranularity,
                         @Nonnull AccountCoverageRecord.Creator<WrappedTableRecordT, SampleTemporalTypeT> recordCreator,
                         @Nonnull String sampleTimeColumnName) {
        this.coverageTable = Objects.requireNonNull(coverageTable);
        this.tableGranularity = Objects.requireNonNull(tableGranularity);
        this.recordCreator = Objects.requireNonNull(recordCreator);
        this.sampleTimeColumnName = Objects.requireNonNull(sampleTimeColumnName);
    }

    public Table<WrappedTableRecordT> coverageTable() {
        return coverageTable;
    }

    public CloudStatGranularity granularity() {
        return tableGranularity;
    }

    public Field<SampleTemporalTypeT> sampleTimeField() {
        return (Field<SampleTemporalTypeT>)coverageTable.field(sampleTimeColumnName);
    }

    public Field<Long> accountIdField() {
        return coverageTable.field(ACCOUNT_ID_COLUMN, Long.class);
    }

    public Field<Long> regionIdField() {
        return coverageTable.field(REGION_ID_COLUMN, Long.class);
    }

    public Field<Long> cloudServiceIdField() {
        return coverageTable.field(CLOUD_SERVICE_ID_COLUMN, Long.class);
    }

    public Field<Long> serviceProviderIdField() {
        return coverageTable.field(SERVICE_PROVIDER_ID_COLUMN, Long.class);
    }

    public Field<Double> coverageAmountField() {
        return coverageTable.field(COVERAGE_AMOUNT_COLUMN, Double.class);
    }

    public Field<Double> coverageCapacityField() {
        return coverageTable.field(COVERAGE_CAPACITY_COLUMN, Double.class);
    }

    public Field<Byte> coverageTypeField() {
        return coverageTable.field(COVERAGE_TYPE_COLUMN, Byte.class);
    }

    public Field<Integer> coverageSubTypeField() {
        return coverageTable.field(COVERAGE_SUB_TYPE_COLUMN, Integer.class);
    }

    @Nonnull
    public AccountCoverageRecord<SampleTemporalTypeT> createRecordAccessor(@Nonnull Record record) {
        return new AccountCoverageRecord(this, record);
    }

    @Nonnull
    public Stream<WrappedTableRecordT> toRecords(@Nonnull CloudCommitmentDataBucket dataBucket) {

        final SampleTemporalTypeT sampleTime =
                toSampleTime(Instant.ofEpochMilli(dataBucket.getTimestampMillis()));

        return dataBucket.getSampleList().stream()
                .map(sample -> recordCreator.createRecord(
                        sampleTime,
                        sample.getAccountId(),
                        // region ID may default to 0, if not set
                        sample.getRegionId(),
                        sample.getCloudServiceId(),
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

    public Condition startTimeConditionInclusive(@Nonnull Instant instant) {
        return sampleTimeField().greaterOrEqual(toSampleTime(instant));
    }

    public Condition endTimeConditionExclusive(@Nonnull Instant instant) {
        return sampleTimeField().lessThan(toSampleTime(instant));
    }

    public abstract SampleTemporalTypeT toSampleTime(@Nonnull Instant instant);

    /**
     * An implementation of {@link AccountCoverageTable} for hourly data.
     */
    static class HourlyTable extends AccountCoverageTable<HourlyAccountCommitmentCoverageRecord, LocalDateTime> {

        HourlyTable() {
            super(HOURLY_ACCOUNT_COMMITMENT_COVERAGE,
                    CloudStatGranularity.HOURLY,
                    HourlyAccountCommitmentCoverageRecord::new,
                    "sample_time");
        }

        @Override
        public LocalDateTime toSampleTime(@Nonnull final Instant instant) {
            return LocalDateTime.ofInstant(instant, UTC_ZONE_ID);
        }
    }

    /**
     * An implementation of {@link AccountCoverageTable} for daily data.
     */
    static class DailyTable extends AccountCoverageTable<DailyAccountCommitmentCoverageRecord, LocalDate> {

        DailyTable() {
            super(DAILY_ACCOUNT_COMMITMENT_COVERAGE,
                    CloudStatGranularity.DAILY,
                    DailyAccountCommitmentCoverageRecord::new,
                    "sample_date");
        }

        @Override
        public LocalDate toSampleTime(@Nonnull final Instant instant) {
            return instant.atZone(UTC_ZONE_ID).toLocalDate();
        }
    }

    /**
     * An implementation of {@link AccountCoverageTable} for monthly data.
     */
    static class MonthlyTable extends AccountCoverageTable<MonthlyAccountCommitmentCoverageRecord, LocalDate> {

        MonthlyTable() {
            super(MONTHLY_ACCOUNT_COMMITMENT_COVERAGE,
                    CloudStatGranularity.MONTHLY,
                    MonthlyAccountCommitmentCoverageRecord::new,
                    "sample_date");
        }

        @Override
        public LocalDate toSampleTime(@Nonnull final Instant instant) {
            return instant.atZone(UTC_ZONE_ID).toLocalDate();
        }
    }
}
