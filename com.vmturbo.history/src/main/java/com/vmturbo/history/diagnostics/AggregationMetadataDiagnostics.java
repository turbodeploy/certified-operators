package com.vmturbo.history.diagnostics;

import java.time.Clock;
import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.exception.DataAccessException;

import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.StringDiagnosable;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.pojos.AggregationMetaData;
import com.vmturbo.history.schema.abstraction.tables.records.AggregationMetaDataRecord;

/**
 * Responsible for collecting additional diagnostics from the history component.
 *
 * <p>Unlike the other components, we don't dump the actual contents of the various stat tables.
 * It would be too expensive. As a result, there is no real "restore" for the diagnostics.
 */
public class AggregationMetadataDiagnostics implements StringDiagnosable {

    /**
     * File name for the aggregation metadata - this is data about the duration and completion
     * time of the last aggregation for each latest table.
     */
    private static final String AGGREGATION_METADATA = "AggregationMetadata.txt";


    private static final Logger logger = LogManager.getLogger();

    private final Clock clock;
    private final DSLContext dsl;

    /**
     * Constructs aggregation metadata diagnostics.
     *
     * @param clock clock to use
     * @param dsl history DAO
     */
    public AggregationMetadataDiagnostics(@Nonnull final Clock clock,
            @Nonnull final DSLContext dsl) {
        this.clock = clock;
        this.dsl = dsl;
    }

    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
        final StringBuilder prefixBuilder = new StringBuilder().append("Fields : ");
        for (Field<?> field : Tables.AGGREGATION_META_DATA.fields()) {
            prefixBuilder.append(field.getName()).append(" , ");
        }
        prefixBuilder.append("\n---------------------\n");

        appender.appendString("Current clock time: " + LocalDateTime.now(clock));
        appender.appendString(prefixBuilder.toString());

        try {
            for (AggregationMetaDataRecord record : dsl.fetch(Tables.AGGREGATION_META_DATA)) {
                appender.appendString(record.into(AggregationMetaData.class).toString());
            }
        } catch (DataAccessException e) {
            logger.error("Failed to write aggregation metadata due to error.", e);
            throw new DiagnosticsException(e);
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return AGGREGATION_METADATA;
    }
}
