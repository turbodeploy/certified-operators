package com.vmturbo.extractor.service;

import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;

import io.grpc.StatusRuntimeException;

import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.extractor.schema.Extractor;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.sizemon.DbSizeAdapter;
import com.vmturbo.sql.utils.sizemon.PostgresSizeAdapter;

/**
 * Helper functions to set retention policy setting in extractor.
 */
public class RetentionUtils {
    /**
     * Private constructor for util class.
     */
    private RetentionUtils() {}

    /**
     * Sets the retention period for a particular hyper table.
     *
     * @param table Name of the hyper table.
     * @param endpoint DB Endpoint.
     * @param retentionDays Days to set retention to.
     * @throws InterruptedException When thread is interrupted.
     * @throws UnsupportedDialectException thrown on unknown SQL dialect.
     * @throws SQLException on SQL errors.
     * @throws DataAccessException thrown by JooQ.
     */
    static void updateRetentionPeriod(@Nonnull final String table, @Nonnull final DbEndpoint endpoint,
            int retentionDays) throws InterruptedException, UnsupportedDialectException,
            SQLException, DataAccessException {
        endpoint.getAdapter().setupRetentionPolicy(table, ChronoUnit.DAYS, retentionDays);
    }

    /**
     * Gets names of hyper tables.
     *
     * @param endpoint DB Endpoint.
     * @return Set of hyper table names, can be empty if not PG DB.
     * @throws InterruptedException When thread is interrupted.
     * @throws UnsupportedDialectException thrown on unknown SQL dialect.
     * @throws SQLException on SQL errors.
     * @throws DataAccessException thrown by JooQ.
     */
    @Nonnull
    static Set<String> getHypertables(@Nonnull final DbEndpoint endpoint)
            throws InterruptedException, UnsupportedDialectException, SQLException,
            DataAccessException {
        final DbSizeAdapter adapter = DbSizeAdapter.of(endpoint.dslContext(), Extractor.EXTRACTOR);
        if (!(adapter instanceof PostgresSizeAdapter)) {
            return Collections.emptySet();
        }
        final PostgresSizeAdapter pgAdapter = (PostgresSizeAdapter)adapter;
        return pgAdapter.getHypertables();
    }

    /**
     * Util method to fetch current retention setting days from group component.
     *
     * @param settingService Reference to settings service to request retention settings from.
     * @return Number of days to retain extractor data for.
     * @throws StatusRuntimeException Thrown on gRPC access error.
     * @throws IllegalStateException Thrown if could not read retention setting in response.
     */
    public static int fetchRetentionPeriod(@Nonnull final SettingServiceBlockingStub settingService)
            throws StatusRuntimeException, IllegalStateException {
        final SettingProto.GetSingleGlobalSettingRequest request =
                SettingProto.GetSingleGlobalSettingRequest.newBuilder()
                        .setSettingSpecName(GlobalSettingSpecs.EmbeddedReportingRetentionDays
                                .getSettingName())
                        .build();
        SettingProto.Setting setting = settingService.getGlobalSetting(request).getSetting();
        if (setting.hasNumericSettingValue() && setting.getNumericSettingValue().hasValue()) {
            return (int)setting.getNumericSettingValue().getValue();
        }
        throw new IllegalStateException("Could not find 'EmbeddedReportingRetentionDays' setting.");
    }
}
