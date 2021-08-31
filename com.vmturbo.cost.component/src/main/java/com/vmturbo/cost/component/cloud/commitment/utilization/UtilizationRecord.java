package com.vmturbo.cost.component.cloud.commitment.utilization;

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
 * A wrapper around a jooq cloud commitment utilization record.
 * @param <SampleTemporalTypeT> The temporal type of the sample column.
 */
class UtilizationRecord<SampleTemporalTypeT extends Temporal> {
    private static final ZoneId UTC_ZONE_ID = ZoneId.from(ZoneOffset.UTC);

    private final UtilizationTable<?, SampleTemporalTypeT> utilizationTable;

    private final Record tableRecord;

    UtilizationRecord(@Nonnull UtilizationTable<?, SampleTemporalTypeT> utilizationTable,
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

    public <T> T get(String fieldName, Class<? extends T> classTypeT) {
        return tableRecord.get(fieldName, classTypeT);
    }

    /**
     * A creator of jooq records, based on a common set of attributes.
     * @param <WrappedTableRecordT> The wrapped record type the creator constructs.
     * @param <SampleTemporalTypeT> The type of the sample date/time.
     */
    @FunctionalInterface
    protected interface Creator<
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
