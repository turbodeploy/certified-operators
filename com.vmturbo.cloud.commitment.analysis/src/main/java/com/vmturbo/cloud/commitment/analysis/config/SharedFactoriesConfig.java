package com.vmturbo.cloud.commitment.analysis.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory.DefaultBillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.group.api.GroupMemberRetriever;

/**
 * A configuration for shared factory classes between analysis stages.
 */
@Configuration
public class SharedFactoriesConfig {

    @Autowired
    private GroupClientConfig groupClientConfig;

    /**
     * The {@link ComputeTierFamilyResolverFactory}.
     * @return The {@link ComputeTierFamilyResolverFactory}.
     */
    @Bean
    public ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory() {
        return new ComputeTierFamilyResolverFactory();
    }

    /**
     * bean for the billing family retriever.
     *
     * @return An instance of the billing family retriever.
     */
    @Bean
    public BillingFamilyRetrieverFactory billingFamilyRetrieverFactory() {
        return new DefaultBillingFamilyRetrieverFactory(
                new GroupMemberRetriever(
                        GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel())));
    }
}
