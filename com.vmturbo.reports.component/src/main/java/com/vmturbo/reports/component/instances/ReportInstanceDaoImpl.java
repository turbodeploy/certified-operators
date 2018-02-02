package com.vmturbo.reports.component.instances;

import static com.vmturbo.reports.component.db.tables.ReportInstance.REPORT_INSTANCE;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.reports.component.db.tables.pojos.ReportInstance;
import com.vmturbo.sql.utils.DbException;

/**
 * DB-backed report instance dao.
 */
public class ReportInstanceDaoImpl implements ReportInstanceDao {

    private final Logger logger = LogManager.getLogger(getClass());

    private final DSLContext dsl;

    /**
     * Constructs the DAO based on the specified Jooq context.
     *
     * @param dsl Jooq context to use
     */
    public ReportInstanceDaoImpl(@Nonnull DSLContext dsl) {
        this.dsl = Objects.requireNonNull(dsl);
    }

    @Nonnull
    @Override
    public ReportInstanceRecord createInstanceRecord(@Nonnull ReportType reportType, int reportTemplateId,
            @Nonnull ReportOutputFormat format) throws DbException {
        Objects.requireNonNull(reportType);
        Objects.requireNonNull(format);
        final long reportId = IdentityGenerator.next();
        logger.debug("Creating new report instance record {} for template {}", reportId,
                reportTemplateId);
        final Timestamp curTime = new Timestamp(System.currentTimeMillis());
        final ReportInstance instance =
                new ReportInstance(reportId, curTime, reportTemplateId, false, format, reportType);
        try {
            dsl.newRecord(REPORT_INSTANCE, instance).store();
        } catch (DataAccessException e) {
            throw new DbException("Could not create report record for template " + reportTemplateId,
                    e);
        }
        return new ReportInstanceRecordImpl(reportId);
    }

    private void rollbackInstanceRecord(long reportInstanceId) throws DbException {
        logger.debug("Rolling back report instance {}", reportInstanceId);
        try {
            if (dsl.deleteFrom(REPORT_INSTANCE)
                    .where(REPORT_INSTANCE.ID.eq(reportInstanceId)
                            .and(REPORT_INSTANCE.COMMITTED.eq(false)))
                    .execute() != 1) {
                throw new DbException("Dirty report instance with id " + reportInstanceId +
                        " not found. Could not delete");
            }
        } catch (DataAccessException e) {
            throw new DbException("Could not roll back report instance record " + reportInstanceId,
                    e);
        }
    }

    private void commitInstanceRecord(long reportInstanceId) throws DbException {
        logger.debug("Committing report instance {}", reportInstanceId);
        try {
            if (dsl.update(REPORT_INSTANCE)
                    .set(REPORT_INSTANCE.COMMITTED, true)
                    .where(REPORT_INSTANCE.ID.eq(reportInstanceId)
                            .and(REPORT_INSTANCE.COMMITTED.eq(false)))
                    .execute() != 1) {
                throw new DbException("Dirty report instance with id " + reportInstanceId +
                        " not found. Could not commit");
            }
        } catch (DataAccessException e) {
            throw new DbException("Could not commit report instance record " + reportInstanceId, e);
        }
    }

    @Nonnull
    @Override
    public Optional<ReportInstance> getInstanceRecord(long reportInstanceId) throws DbException {
        logger.debug("Getting report instance by id {}", reportInstanceId);
        final List<ReportInstance> records;
        try {
            records = dsl.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return context.selectFrom(REPORT_INSTANCE)
                        .where(REPORT_INSTANCE.ID.eq(reportInstanceId)
                                .and(REPORT_INSTANCE.COMMITTED.eq(true)))
                        .fetch()
                        .into(ReportInstance.class);
            });
        } catch (DataAccessException e) {
            throw new DbException("Error fetching report instance " + reportInstanceId, e);
        }
        return records.isEmpty() ? Optional.empty() : Optional.of(records.get(0));
    }

    @Nonnull
    @Override
    public Collection<ReportInstance> getAllInstances() throws DbException {
        logger.debug("Getting all the report instances");
        final List<ReportInstance> records;
        try {
            records = dsl.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return context.selectFrom(REPORT_INSTANCE)
                        .where(REPORT_INSTANCE.COMMITTED.eq(true))
                        .fetch()
                        .into(ReportInstance.class);
            });
        } catch (DataAccessException e) {
            throw new DbException("Error fetching report instances", e);
        }
        logger.debug("Successfully retrieved {} report instances", records::size);
        return records;
    }

    @Nonnull
    @Override
    public Collection<ReportInstance> getInstancesByTemplate(@Nonnull ReportType reportType,
            int templateId) throws DbException {
        Objects.requireNonNull(reportType);
        logger.debug("Getting all the report instances for template {}", templateId);
        final List<ReportInstance> records;
        try {
            records = dsl.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return context.selectFrom(REPORT_INSTANCE)
                        .where(REPORT_INSTANCE.COMMITTED.eq(true)
                                .and(REPORT_INSTANCE.TEMPLATE_ID.eq(templateId)
                                        .and(REPORT_INSTANCE.REPORT_TYPE.eq(reportType))))
                        .fetch()
                        .into(ReportInstance.class);
            });
        } catch (DataAccessException e) {
            throw new DbException("Error fetching report instances for template " + templateId, e);
        }
        logger.debug("Successfully retrieved {} report instances for template {}", records::size,
                () -> templateId);
        return records;
    }

    /**
     * Report instance record implementation.
     */
    private class ReportInstanceRecordImpl implements ReportInstanceRecord {
        private final long id;

        ReportInstanceRecordImpl(long id) {
            this.id = id;
        }

        @Override
        public long getId() {
            return id;
        }

        @Override
        public void commit() throws DbException {
            commitInstanceRecord(id);
        }

        @Override
        public void rollback() throws DbException {
            rollbackInstanceRecord(id);
        }
    }
}
