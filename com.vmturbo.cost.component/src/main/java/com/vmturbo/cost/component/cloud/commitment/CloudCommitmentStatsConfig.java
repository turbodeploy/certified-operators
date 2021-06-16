package com.vmturbo.cost.component.cloud.commitment;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentServicesREST.CloudCommitmentStatsServiceController;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServicesREST.CloudCommitmentUploadServiceController;
import com.vmturbo.cost.component.cloud.commitment.coverage.CloudCommitmentCoverageStore;
import com.vmturbo.cost.component.cloud.commitment.coverage.SQLCloudCommitmentCoverageStore;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore;
import com.vmturbo.cost.component.cloud.commitment.utilization.SQLCloudCommitmentUtilizationStore;

/**
 * A configuration file for cloud commitment statistics stores (coverage & utilization),
 * as well as the associated RPC classes.
 */
@Configuration
public class CloudCommitmentStatsConfig {

    // Should be defined in CostDBConfig
    @Autowired
    private DSLContext dslContext;

    @Value("${cloudCommitment.maxStatRecordsPerChunk:100}")
    private int maxStatRecordsPerChunk;

    /**
     * A bean for {@link CloudCommitmentUtilizationStore}.
     * @return A bean for {@link CloudCommitmentUtilizationStore}.
     */
    @Nonnull
    @Bean
    public CloudCommitmentUtilizationStore cloudCommitmentUtilizationStore() {
        return new SQLCloudCommitmentUtilizationStore(dslContext);
    }

    /**
     * A bean for {@link CloudCommitmentCoverageStore}.
     * @return A bean for {@link CloudCommitmentCoverageStore}.
     */
    @Nonnull
    @Bean
    public CloudCommitmentCoverageStore cloudCommitmentCoverageStore() {
        return new SQLCloudCommitmentCoverageStore(dslContext);
    }

    /**
     * A bean for {@link CloudCommitmentUploadRpcService}.
     * @return A bean for {@link CloudCommitmentUploadRpcService}.
     */
    @Nonnull
    @Bean
    public CloudCommitmentUploadRpcService cloudCommitmentUploadRpcService() {
        return new CloudCommitmentUploadRpcService(
                cloudCommitmentUtilizationStore(),
                cloudCommitmentCoverageStore());
    }

    /**
     * A bean for {@link CloudCommitmentStatsRpcService}.
     * @return A bean for {@link CloudCommitmentStatsRpcService}.
     */
    @Nonnull
    @Bean
    public CloudCommitmentStatsRpcService cloudCommitmentStatsRpcService() {
        return new CloudCommitmentStatsRpcService(
                cloudCommitmentCoverageStore(),
                cloudCommitmentUtilizationStore(),
                maxStatRecordsPerChunk);
    }

    /**
     * A bean for {@link CloudCommitmentUploadServiceController}.
     * @return A bean for {@link CloudCommitmentUploadServiceController}.
     */
    @Nonnull
    @Bean
    public CloudCommitmentUploadServiceController cloudCommitmentUploadServiceController() {
        return new CloudCommitmentUploadServiceController(cloudCommitmentUploadRpcService());
    }

    /**
     * A bean for {@link CloudCommitmentStatsServiceController}.
     * @return A bean for {@link CloudCommitmentStatsServiceController}.
     */
    @Nonnull
    @Bean
    public CloudCommitmentStatsServiceController cloudCommitmentStatsServiceController() {
        return new CloudCommitmentStatsServiceController(cloudCommitmentStatsRpcService());
    }
}
