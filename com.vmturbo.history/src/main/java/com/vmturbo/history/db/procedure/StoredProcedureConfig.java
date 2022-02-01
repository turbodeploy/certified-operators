package com.vmturbo.history.db.procedure;

import static com.vmturbo.history.schema.abstraction.Tables.AUDIT_LOG_ENTRIES;
import static com.vmturbo.history.schema.abstraction.Tables.AUDIT_LOG_RETENTION_POLICIES;
import static com.vmturbo.history.schema.abstraction.Tables.MOVING_STATISTICS_BLOBS;
import static com.vmturbo.history.schema.abstraction.Tables.PERCENTILE_BLOBS;
import static com.vmturbo.history.schema.abstraction.Tables.RETENTION_POLICIES;
import static com.vmturbo.history.schema.abstraction.Tables.SYSTEM_LOAD;

import java.sql.SQLException;

import org.jooq.DSLContext;
import org.jooq.DatePart;
import org.jooq.Record1;
import org.jooq.Select;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.history.db.DbAccessConfig;
import com.vmturbo.history.schema.RetentionUtil;
import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.jooq.PurgeEvent;
import com.vmturbo.sql.utils.jooq.PurgeProcedure;

/**
 * Configuration for all the stored procedures which were converted from mariadb to java. This is
 * disabled by default, and only enabled when FF is on.
 */
@Configuration
@Import({DbAccessConfig.class})
@Conditional(DbEndpointCondition.class)
public class StoredProcedureConfig {

    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Bean
    PurgeEvent purgeExpiredSystemloadData() {
        try {
            final PurgeProcedure procedure = new PurgeProcedure(dsl(),
                    SYSTEM_LOAD, SYSTEM_LOAD.SNAPSHOT_TIME, RETENTION_POLICIES,
                    RETENTION_POLICIES.POLICY_NAME, RETENTION_POLICIES.RETENTION_PERIOD,
                    RetentionUtil.SYSTEM_LOAD_RETENTION_POLICY_NAME, DatePart.DAY);
            return createPurgeEvent("purge_expired_systemload_data", procedure).schedule(0, 24);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create purgeExpiredSystemloadData", e);
        }
    }

    /**
     * Event for purging the data in percentile_blobs table regularly.
     *
     * @return purge event
     */
    @Bean
    PurgeEvent purgeExpiredPercentileData() {
        try {
            final DSLContext dsl = dsl();
            // temporary workaround due to the version of mariadb running on jenkins (5.5.68) is
            // different from production (10.5.12). It doesn't allow same table in both delete
            // source and target before 10.3.1.
            // see https://mariadb.com/kb/en/delete/#deleting-from-the-same-source-and-target
            // To avoid test failures on jenkins, we can nest the query deeper into a from clause
            final Select<Record1<Long>> startPointQuery = dsl.select(DSL.field("t").coerce(Long.class))
                    .from(dsl.selectDistinct(PERCENTILE_BLOBS.AGGREGATION_WINDOW_LENGTH.as("t"))
                            .from(PERCENTILE_BLOBS).where(PERCENTILE_BLOBS.START_TIMESTAMP.eq(0L)));

            final PurgeProcedure procedure = new PurgeProcedure(dsl,
                    PERCENTILE_BLOBS, PERCENTILE_BLOBS.START_TIMESTAMP, RETENTION_POLICIES,
                    RETENTION_POLICIES.POLICY_NAME, RETENTION_POLICIES.RETENTION_PERIOD,
                    RetentionUtil.PERCENTILE_RETENTION_POLICY_NAME, DatePart.DAY)
                    .withAdditionalConditions(PERCENTILE_BLOBS.START_TIMESTAMP.ne(0L))
                    .withStartPointQuery(startPointQuery);
            return createPurgeEvent("purge_expired_percentile_data", procedure).schedule(0, 24);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create purgeExpiredPercentileData", e);
        }
    }

    @Bean
    PurgeEvent purgeAuditLogExpiredEntries() {
        try {
            final PurgeProcedure procedure = new PurgeProcedure(dsl(),
                    AUDIT_LOG_ENTRIES, AUDIT_LOG_ENTRIES.SNAPSHOT_TIME, AUDIT_LOG_RETENTION_POLICIES,
                    AUDIT_LOG_RETENTION_POLICIES.POLICY_NAME, AUDIT_LOG_RETENTION_POLICIES.RETENTION_PERIOD,
                    RetentionUtil.AUDIT_LOG_RETENTION_POLICY_NAME, DatePart.DAY);
            return createPurgeEvent("purge_audit_log_expired_days", procedure).schedule(0, 24);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create purgeAuditLogExpiredEntries", e);
        }
    }

    @Bean
    PurgeEvent purgeExpiredMovingStatisticsData() {
        try {
            final DSLContext dsl = dsl();
            // temporary workaround due to the version of mariadb running on jenkins (5.5.68) is
            // different from production (10.5.12). It doesn't allow same table in both delete
            // source and target before 10.3.1.
            // see https://mariadb.com/kb/en/delete/#deleting-from-the-same-source-and-target
            // To avoid test failures on jenkins, we can nest the query deeper into a from clause
            final Select<Record1<Long>> startPointQuery = dsl.select(DSL.field("t").coerce(Long.class))
                    .from(dsl.select(DSL.max(MOVING_STATISTICS_BLOBS.START_TIMESTAMP).as("t"))
                            .from(MOVING_STATISTICS_BLOBS));

            final PurgeProcedure procedure = new PurgeProcedure(dsl, MOVING_STATISTICS_BLOBS,
                    MOVING_STATISTICS_BLOBS.START_TIMESTAMP, RETENTION_POLICIES,
                    RETENTION_POLICIES.POLICY_NAME, RETENTION_POLICIES.RETENTION_PERIOD,
                    RetentionUtil.MOVING_STATISTICS_RETENTION_POLICY_NAME, DatePart.DAY)
                    .withStartPointQuery(startPointQuery);
            return createPurgeEvent("purge_expired_moving_statistics_data", procedure).schedule(0, 24);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create purgeExpiredMovingStatisticsData", e);
        }
    }

    /**
     * Get the dsl context, which can be overridden by testing.
     *
     * @return {@link DSLContext}
     * @throws SQLException                if a DB initialization operation fails
     * @throws UnsupportedDialectException if the DbEndpoint is based on a bogus SLQDialect
     * @throws InterruptedException        if we're interrupted
     */
    public DSLContext dsl()
            throws SQLException, UnsupportedDialectException, InterruptedException {
        return dbAccessConfig.dsl();
    }

    /**
     * Create the purge event, which can be overridden by testing.
     *
     * @param name name of the event
     * @param runnable operation to perform
     * @return {@link PurgeEvent}
     */
    public PurgeEvent createPurgeEvent(String name, Runnable runnable) {
        return new PurgeEvent(name, runnable);
    }
}
