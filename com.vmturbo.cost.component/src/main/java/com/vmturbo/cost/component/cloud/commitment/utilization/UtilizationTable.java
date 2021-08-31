package com.vmturbo.cost.component.cloud.commitment.utilization;

import static com.vmturbo.cost.component.db.Tables.DAILY_CLOUD_COMMITMENT_UTILIZATION;
import static com.vmturbo.cost.component.db.Tables.HOURLY_CLOUD_COMMITMENT_UTILIZATION;
import static com.vmturbo.cost.component.db.tables.MonthlyCloudCommitmentUtilization.MONTHLY_CLOUD_COMMITMENT_UTILIZATION;

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
import com.vmturbo.cost.component.db.tables.records.DailyCloudCommitmentUtilizationRecord;
import com.vmturbo.cost.component.db.tables.records.HourlyCloudCommitmentUtilizationRecord;
import com.vmturbo.cost.component.db.tables.records.MonthlyCloudCommitmentUtilizationRecord;

/**
 * A wrapper around a cloud commitment utilization table.
 * @param <WrappedTableRecordT> The wrapped jooq table record.
 * @param <SampleTemporalTypeT> The temporal type for the sample time column.
 */

abstract class UtilizationTable<
                WrappedTableRecordT extends UpdatableRecord<WrappedTableRecordT>,
                SampleTemporalTypeT extends Temporal> {

    private static final String ACCOUNT_ID_COLUMN = "account_id";

    private static final String REGION_ID_COLUMN = "region_id";

    private static final String SERVICE_PROVIDER_ID_COLUMN = "service_provider_id";

    private static final String CLOUD_COMMITMENT_ID_COLUMN = "cloud_commitment_id";

    private static final String UTILIZATION_AMOUNT_COLUMN = "utilization_amount";

    private static final String CAPACITY_COLUMN = "commitment_capacity";

    private static final String COVERAGE_TYPE_COLUMN = "coverage_type";

    private static final String COVERAGE_SUB_TYPE_COLUMN = "coverage_sub_type";

    protected static final ZoneId UTC_ZONE_ID = ZoneId.from(ZoneOffset.UTC);

    protected final Table<WrappedTableRecordT> wrappedTable;

    protected final CloudStatGranularity tableGranularity;

    protected final UtilizationRecord.Creator<WrappedTableRecordT, SampleTemporalTypeT> recordCreator;

    protected final String sampleTimeColumnName;

    UtilizationTable(@Nonnull Table<WrappedTableRecordT> wrappedTable,
                            @Nonnull CloudStatGranularity tableGranularity,
                            @Nonnull UtilizationRecord.Creator<WrappedTableRecordT, SampleTemporalTypeT> recordCreator,
                            @Nonnull String sampleTimeColumnName) {
        this.wrappedTable = Objects.requireNonNull(wrappedTable);
        this.tableGranularity = Objects.requireNonNull(tableGranularity);
        this.recordCreator = Objects.requireNonNull(recordCreator);
        this.sampleTimeColumnName = Objects.requireNonNull(sampleTimeColumnName);
    }

    public Table<WrappedTableRecordT> wrappedTable() {
        return wrappedTable;
    }

    public CloudStatGranularity granularity() {
        return tableGranularity;
    }

    public Field<SampleTemporalTypeT> sampleTimeField() {
        return (Field<SampleTemporalTypeT>)wrappedTable.field(sampleTimeColumnName);
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
    public UtilizationRecord<SampleTemporalTypeT> createRecordAccessor(@Nonnull Record record) {
        return new UtilizationRecord<>(this, record);
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

    public Condition startTimeConditionInclusive(@Nonnull Instant instant) {
        return sampleTimeField().greaterOrEqual(toSampleTime(instant));
    }

    public Condition endTimeConditionExclusive(@Nonnull Instant instant) {
        return sampleTimeField().lessThan(toSampleTime(instant));
    }

    public abstract SampleTemporalTypeT toSampleTime(@Nonnull Instant instant);

    /**
     * An implementation of {@link UtilizationTable} for hourly data.
     */
    static class HourlyUtilizationTable extends UtilizationTable<HourlyCloudCommitmentUtilizationRecord, LocalDateTime> {

        HourlyUtilizationTable() {
            super(HOURLY_CLOUD_COMMITMENT_UTILIZATION,
                  CloudStatGranularity.HOURLY,
                  HourlyCloudCommitmentUtilizationRecord::new,
                  "sample_time");
        }

        @Override
        public LocalDateTime toSampleTime(@Nonnull final Instant instant) {
            return LocalDateTime.ofInstant(instant, UTC_ZONE_ID);
        }
    }

    /**
     * An implementation of {@link UtilizationTable} for daily data.
     */
    static class DailyUtilizationTable extends UtilizationTable<DailyCloudCommitmentUtilizationRecord, LocalDate> {

        DailyUtilizationTable() {
            super(DAILY_CLOUD_COMMITMENT_UTILIZATION,
                  CloudStatGranularity.DAILY,
                  DailyCloudCommitmentUtilizationRecord::new,
                  "sample_date");
        }

        @Override
        public LocalDate toSampleTime(@Nonnull final Instant instant) {
            return instant.atZone(UTC_ZONE_ID).toLocalDate();
        }
    }

    /**
     * An implementation of {@link UtilizationTable} for monthly data.
     */
    static class MonthlyUtilizationTable extends UtilizationTable<MonthlyCloudCommitmentUtilizationRecord, LocalDate> {

        MonthlyUtilizationTable() {
            super(MONTHLY_CLOUD_COMMITMENT_UTILIZATION,
                  CloudStatGranularity.MONTHLY,
                  MonthlyCloudCommitmentUtilizationRecord::new,
                  "sample_date");
        }

        @Override
        public LocalDate toSampleTime(@Nonnull final Instant instant) {
            return instant.atZone(UTC_ZONE_ID).toLocalDate();
        }
    }
}
