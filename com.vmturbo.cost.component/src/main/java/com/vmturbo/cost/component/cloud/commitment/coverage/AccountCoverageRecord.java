package com.vmturbo.cost.component.cloud.commitment.coverage;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.jooq.Record;
import org.jooq.UpdatableRecord;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;

/**
 * A wrapper around and account-aggregated jooq coverage record.
 * @param <SampleTemporalTypeT> The temporal type of the sample column.
 */
class AccountCoverageRecord<SampleTemporalTypeT extends Temporal> {

    private static final ZoneId UTC_ZONE_ID = ZoneId.from(ZoneOffset.UTC);

    private final AccountCoverageTable<?, SampleTemporalTypeT> coverageTable;

    private final Record tableRecord;

    AccountCoverageRecord(@Nonnull AccountCoverageTable<?, SampleTemporalTypeT> coverageTable,
                                 @Nonnull Record record) {
        this.coverageTable = Objects.requireNonNull(coverageTable);
        this.tableRecord = Objects.requireNonNull(record);
    }

    public SampleTemporalTypeT sampleTime() {
        return tableRecord.get(coverageTable.sampleTimeField());
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

    public boolean hasAccountId() {
        return tableRecord.indexOf(coverageTable.accountIdField()) >= 0;
    }

    public long accountId() {
        return tableRecord.get(coverageTable.accountIdField());
    }

    public boolean hasRegionId() {
        return tableRecord.indexOf(coverageTable.regionIdField()) >= 0;
    }

    public long regionId() {
        return tableRecord.get(coverageTable.regionIdField());
    }

    public boolean hasCloudServiceId() {
        return tableRecord.indexOf(coverageTable.cloudServiceIdField()) >= 0;
    }

    public long cloudServiceId() {
        return tableRecord.get(coverageTable.cloudServiceIdField());
    }

    public boolean hasServiceProviderId() {
        return tableRecord.indexOf(coverageTable.serviceProviderIdField()) >= 0;
    }

    public long serviceProviderId() {
        return tableRecord.get(coverageTable.serviceProviderIdField());
    }

    public double coverageAmount() {
        return tableRecord.get(coverageTable.coverageAmountField());
    }

    public double coverageCapacity() {
        return tableRecord.get(coverageTable.coverageCapacityField());
    }

    public CloudCommitmentCoverageType coverageType() {
        return CloudCommitmentCoverageType.forNumber(
                tableRecord.get(coverageTable.coverageTypeField()));
    }

    public int coverageSubType() {
        return tableRecord.get(coverageTable.coverageSubTypeField());
    }

    public <T> T get(String fieldName, Class<? extends T> classTypeT) {
        return tableRecord.get(fieldName, classTypeT);
    }

    /**
     * A creator of {@link AccountCoverageRecord} instances.
     * @param <WrappedTableRecordT> The wrapped jooq record type.
     * @param <SampleTemporalTypeT> The temporal type of the sample column.
     */
    @FunctionalInterface
    protected interface Creator<
            WrappedTableRecordT extends UpdatableRecord<WrappedTableRecordT>,
            SampleTemporalTypeT extends Temporal> {

        WrappedTableRecordT createRecord(
                SampleTemporalTypeT sampleTime,
                long accountId,
                long regionId,
                long cloudServiceId,
                long serviceProviderId,
                double coverageAmount,
                double coverageCapacity,
                byte coverageType,
                Integer coverageSubType);
    }
}
