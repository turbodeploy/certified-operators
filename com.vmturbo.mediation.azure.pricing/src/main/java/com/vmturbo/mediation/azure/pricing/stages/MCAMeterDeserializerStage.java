package com.vmturbo.mediation.azure.pricing.stages;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.azure.pricing.pipeline.PricingPipeline.Stage;
import com.vmturbo.mediation.util.target.status.ProbeStageEnum;

/**
 * Stage for opening InputStreams as Readers, with automatic BOM detection and character set
 * handling.
 *
 * @param <E> The enum for the probe discovery stages that apply to this particular kind
 *   of discovery.
 */
public class MCAMeterDeserializerStage<E extends ProbeStageEnum>
        extends Stage<Stream<CSVRecord>, Stream<AzureMeter>, E>
        implements AutoCloseable {
    private final Logger logger = LogManager.getLogger();

    private int processed = 0;
    private int errored = 0;

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(
            "M[M]/d[d]/yyyy hh:mm:ss a");

    /**
     * Create a deserializing stage for MCA meters.
     *
     * @param probeStage the enum value representing this probe discovery stage, used for reporting
     *   detailed discovery status.
     */
    public MCAMeterDeserializerStage(@Nonnull E probeStage) {
        super(probeStage);
    }

    @NotNull
    @Override
    protected StageResult<Stream<AzureMeter>> executeStage(@NotNull Stream<CSVRecord> records) {
        // As the pipeline stage returns a stream immediately where exceptions from deserializing
        // may happen later, the pipeline stage returns immediate success, but the probe
        // stage will report later on any failures.

        getContext().autoClose(this);

        return StageResult.withResult(records.map(this::deserializeRecord).filter(Objects::nonNull))
                .andStatus(Status.success("Deserializing mapper added"));
    }

    @Nonnull
    private AzureMeter deserializeRecord(@Nonnull CSVRecord record) {
        processed++;
        try {
            return new MCAMeter(record);
        } catch (Exception ex) {
            errored++;
            logger.info("{} Exception during deserialization of {}",
            getContext().getAccountName(), record.toString(), ex);

            return null;
        }
    }

    @Override
    public void close() {
        final Double failPercent = 100.0D * errored / processed;

        final String status = String.format("Processed %d meters of which %d (%g%%) had errors",
            processed, errored, failPercent);

        getStageInfo().ok(status);
    }

    /**
     * An implementatuion of AzureMeter, loaded from a CSV record.
     */
    public static class MCAMeter implements AzureMeter {
        private static final String METER_ID_COLUMN = "meterId";
        private static final String PRODUCT_ORDER_NAME = "productOrderName";
        private static final String TIER_MINIMUM_UNITS = "tierMinimumUnits";
        private static final String UNIT_OF_MEASURE = "unitOfMeasure";
        private static final String UNIT_PRICE = "unitPrice";
        private static final String EFFECTIVE_START_DATE = "effectiveStartDate";
        private static final String EFFECTIVE_END_DATE = "effectiveEndDate";

        private final String meterId;
        private final String planId;
        private final double tierMinimumUnits;
        private final String unitOfMeasure;
        private final double unitPrice;
        private final ZonedDateTime effectiveStartDate;
        private final ZonedDateTime effectiveEndDate;

        /**
         * Instantiate a meter from a CSVRecord from an MCA price sheet.
         *
         * @param row a row from an MCA price sheet CSV file.
         */
        public MCAMeter(@Nonnull CSVRecord row) {
            meterId = row.get(METER_ID_COLUMN);
            planId = row.get(PRODUCT_ORDER_NAME);
            tierMinimumUnits = Double.parseDouble(row.get(TIER_MINIMUM_UNITS));
            unitOfMeasure = row.get(UNIT_OF_MEASURE);
            unitPrice = Double.parseDouble(row.get(UNIT_PRICE));
            if (StringUtils.isNotBlank(row.get(EFFECTIVE_START_DATE))) {
                effectiveStartDate = LocalDateTime.parse(row.get(EFFECTIVE_START_DATE),
                        DATE_TIME_FORMATTER).atZone(ZoneOffset.UTC);
            } else {
                effectiveStartDate = Instant.ofEpochMilli(Long.MIN_VALUE).atZone(ZoneOffset.UTC);
            }
            if (StringUtils.isNotBlank(row.get(EFFECTIVE_END_DATE))) {
                effectiveEndDate = LocalDateTime.parse(row.get(EFFECTIVE_END_DATE),
                        DATE_TIME_FORMATTER).atZone(ZoneOffset.UTC);
            } else {
                effectiveEndDate = Instant.ofEpochMilli(Long.MAX_VALUE).atZone(ZoneOffset.UTC);
            }
        }

        @Override
        @Nonnull
        public String getMeterId() {
            return meterId;
        }

        @Override
        @Nonnull
        public String getPlanName() {
            return planId;
        }

        @Override
        @Nonnull
        public double getTierMinimumUnits() {
            return tierMinimumUnits;
        }

        @Nonnull
        public String getUnitOfMeasure() {
            return unitOfMeasure;
        }

        @Override
        public double getUnitPrice() {
            return unitPrice;
        }

        @Override
        public ZonedDateTime getEffectiveStartDate() {
            return effectiveStartDate;
        }

        @Override
        public ZonedDateTime getEffectiveEndDate() {
            return effectiveEndDate;
        }

        @Nonnull
        @Override
        public String toString() {
            return String.format("%s %s $%g / %s @ >= %g effective from %s to %s", getMeterId(),
                    getPlanName(), getUnitPrice(), getUnitOfMeasure(), getTierMinimumUnits(),
                    getEffectiveStartDate(), getEffectiveEndDate());
        }
    }
}