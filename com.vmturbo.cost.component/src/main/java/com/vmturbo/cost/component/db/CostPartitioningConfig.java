package com.vmturbo.cost.component.db;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.components.common.utils.RollupTimeFrame;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.partition.IPartitionAdapter;
import com.vmturbo.sql.utils.partition.IPartitioningManager;
import com.vmturbo.sql.utils.partition.MariaDBPartitionAdapter;
import com.vmturbo.sql.utils.partition.PartitionProcessingException;
import com.vmturbo.sql.utils.partition.PartitioningManager;
import com.vmturbo.sql.utils.partition.PartitionsManager;
import com.vmturbo.sql.utils.partition.PostgresPartitionAdapter;
import com.vmturbo.sql.utils.partition.RetentionSettings;

/**
 * Cost component config for {@link IPartitioningManager}.
 */
@Configuration
public class CostPartitioningConfig {

    // Should be defined in CostDBConfig
    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Autowired
    private SettingServiceBlockingStub settingServiceBlockingStub;

    @Value("${cost.partitioning.hourlyInterval:PT12H}")
    private String hourlyPartitioningInterval;

    @Value("${cost.partitioning.dailyInterval:P1D}")
    private String dailyPartitioningInterval;

    @Value("${cost.partitioning.monthlyInterval:P1M}")
    private String monthlyPartitioningInterval;

    @Value("${retention.numRetainedMinutes:130}")
    private int latestRetentionMinutes;


    /**
     * Create a new {@link IPartitioningManager} instance.
     *
     * @return new instance
     */
    @Bean
    public IPartitioningManager partitionManager() {
        try {

            final Map<String, RollupTimeFrame> tableTimeFrameMap = ImmutableMap.<String, RollupTimeFrame>builder()
                    .put(Tables.CLOUD_COST_HOURLY.getName(), RollupTimeFrame.HOUR)
                    .put(Tables.CLOUD_COST_DAILY.getName(), RollupTimeFrame.DAY)
                    .build();

            return new PartitioningManager(dbAccessConfig.dsl(),
                    dbAccessConfig.getSchemaName(),
                    new PartitionsManager(partitioningAdapter()),
                    new RetentionSettings(settingServiceBlockingStub, latestRetentionMinutes),
                    partitionIntervals(),
                    (tableName) -> Optional.ofNullable(tableTimeFrameMap.get(tableName)));

        } catch (SQLException | UnsupportedDialectException | InterruptedException | PartitionProcessingException e) {
            throw new BeanCreationException("Failed to obtain DSLContext for partitioning manager", e);
        }
    }

    @Nonnull
    private Map<RollupTimeFrame, String> partitionIntervals() {
        return ImmutableMap.of(
                RollupTimeFrame.HOUR, hourlyPartitioningInterval,
                RollupTimeFrame.DAY, dailyPartitioningInterval,
                RollupTimeFrame.MONTH, monthlyPartitioningInterval
        );
    }

    @Nonnull
    private IPartitionAdapter partitioningAdapter()
            throws SQLException, UnsupportedDialectException, InterruptedException {

        final DSLContext dsl = dbAccessConfig.dsl();
        return dsl.dialect() == SQLDialect.POSTGRES
                ? new PostgresPartitionAdapter(dsl)
                : new MariaDBPartitionAdapter(dsl);
    }
}
