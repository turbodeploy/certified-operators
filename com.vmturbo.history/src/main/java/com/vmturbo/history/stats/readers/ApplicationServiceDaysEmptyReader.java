package com.vmturbo.history.stats.readers;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;

import com.vmturbo.history.component.api.HistoryComponentNotifications.ApplicationServiceHistoryNotification.DaysEmptyInfo;
import com.vmturbo.history.schema.abstraction.tables.ApplicationServiceDaysEmpty;

/**
 * Class that queries app_service_days_empty table.
 */
public class ApplicationServiceDaysEmptyReader {

    private static final Logger logger = LogManager.getLogger();
    private static final ApplicationServiceDaysEmpty ASDE = ApplicationServiceDaysEmpty.APPLICATION_SERVICE_DAYS_EMPTY;
    private final DSLContext dsl;

    /**
     * Creates an instance of ApplicationServiceDaysEmptyReader.
     *
     * @param dsl instance to execute queries.
     */
    public ApplicationServiceDaysEmptyReader(@Nonnull final DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Retrieves the map days empty by app service oid.
     *
     * @return map of days empty by app service oid.
     */
    @Nonnull
    public List<DaysEmptyInfo> getApplicationServiceDaysEmptyInfo() {
        try {
            final Result<Record3<Long, String, Timestamp>> result = dsl
                    .select(ASDE.ID, ASDE.NAME, ASDE.FIRST_DISCOVERED)
                    .from(ASDE)
                    .fetch();
            if (result != null) {
                return result.stream().map(
                        r -> DaysEmptyInfo.newBuilder()
                                .setAppSvcOid(r.component1())
                                .setAppSvcName(r.component2())
                                .setFirstDiscoveredTime(r.component3().getTime())
                                .build())
                        .collect(Collectors.toList());
            }
        } catch (DataAccessException e) {
            logger.error("Error retrieving application service days empty records", e);
        }
        return Collections.emptyList();
    }
}