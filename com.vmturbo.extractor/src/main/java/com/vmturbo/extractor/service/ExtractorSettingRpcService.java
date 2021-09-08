package com.vmturbo.extractor.service;

import java.sql.SQLException;
import java.util.Set;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.extractor.ExtractorSettingServiceGrpc.ExtractorSettingServiceImplBase;
import com.vmturbo.common.protobuf.extractor.Reporting.UpdateRetentionSettingRequest;
import com.vmturbo.common.protobuf.extractor.Reporting.UpdateRetentionSettingResponse;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Service to help set retention settings, called by API component.
 */
public class ExtractorSettingRpcService extends ExtractorSettingServiceImplBase {
    private final Logger logger = LogManager.getLogger();

    /**
     * DB endpoint.
     */
    private final DbEndpoint ingestEndpoint;

    /**
     * Names of hyper tables, for which retention period is set.
     */
    private Set<String> hypertables;

    /**
     * Initializes.
     *
     * @param endpoint DB Endpoint.
     */
    public ExtractorSettingRpcService(@Nonnull final DbEndpoint endpoint) {
        ingestEndpoint = endpoint;
    }

    /**
     * Called by API component when retention setting 'embeddedReportingRetentionDays' is set
     * by user from UI/API.
     *
     * @param request Request containing retention days value to set.
     * @param responseObserver Response containing number of tables that update was done for.
     */
    @Override
    public void updateRetentionSettings(UpdateRetentionSettingRequest request,
            StreamObserver<UpdateRetentionSettingResponse> responseObserver) {
        if (!request.hasRetentionDays() || request.getRetentionDays() < 0) {
            responseObserver.onError(Status.ABORTED
                    .withDescription("Invalid request retention days specified.")
                    .asException());
            return;
        }
        int retentionDays = request.getRetentionDays();
        if (hypertables == null) {
            try {
                // Hyper table names are being fetched here lazily first time as otherwise
                // the DB endpoint would not be ready if we were to do this in constructor.
                hypertables = RetentionUtils.getHypertables(ingestEndpoint);
            } catch (DataAccessException | UnsupportedDialectException | SQLException e) {
                responseObserver.onError(Status.ABORTED.withCause(e)
                        .withDescription("Unable to fetch hyper table names.")
                                .asException());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                responseObserver.onError(Status.ABORTED.withCause(ie)
                        .withDescription("Interrupted while trying to fetch hyper table names.")
                        .asException());
            }
        }
        logger.info("Updating retention setting to {} days for {} tables.", retentionDays,
                hypertables.size());
        int updateCount = 0;
        for (String table : hypertables) {
            // Log any warnings in this loop, try and update all applicable hyper-tables.
            try {
                RetentionUtils.updateRetentionPeriod(table, ingestEndpoint, retentionDays);
                updateCount++;
            } catch (DataAccessException | UnsupportedDialectException | SQLException e) {
                logger.warn("Could not set retention period {} days for {}.", retentionDays,
                                table);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while setting retention period {} days for {}.",
                        retentionDays, table);
            }
        }
        if (updateCount < hypertables.size()) {
            responseObserver.onError(Status.ABORTED.withDescription(
                    String.format("Could only set retention period % days for %s tables out of %s",
                            retentionDays, updateCount, hypertables.size())).asException());
        } else {
            responseObserver.onNext(UpdateRetentionSettingResponse.newBuilder()
                    .setUpdateCount(updateCount).build());
            responseObserver.onCompleted();
        }
    }
}
