package com.vmturbo.history.diagnostics;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;

import com.vmturbo.components.common.diagnostics.StringDiagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.pojos.AggregationMetaData;

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

    private final HistorydbIO historydbIO;

    /**
     * Constructs aggregation metadata diagnostics.
     *
     * @param clock clock to use
     * @param historydbIO history DAO
     */
    public AggregationMetadataDiagnostics(@Nonnull final Clock clock,
                              @Nonnull final HistorydbIO historydbIO) {
        this.clock = clock;
        this.historydbIO = historydbIO;
    }

    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
        try {

            final StringBuilder prefixBuilder = new StringBuilder().append("Fields : ");
            for (Field<?> field : Tables.AGGREGATION_META_DATA.fields()) {
                prefixBuilder.append(field.getName()).append(" , ");
            }
            prefixBuilder.append("\n---------------------\n");

            appender.appendString("Current clock time: " + LocalDateTime.now(clock));
            appender.appendString(prefixBuilder.toString());

            try (Connection connection = historydbIO.connection()) {
                for (AggregationMetaData aggregationMetaData: historydbIO.using(connection)
                        .selectFrom(Tables.AGGREGATION_META_DATA)
                        .fetchInto(AggregationMetaData.class)) {
                    appender.appendString(aggregationMetaData.toString());
                }
            }
        } catch (RuntimeException | VmtDbException | SQLException e) {
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
