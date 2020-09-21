package com.vmturbo.market.topology;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.licensing.LicenseCheckClientConfig;
import com.vmturbo.market.runner.MarketRunnerConfig;

/**
 * Configuration of listeners associated with topology-processor broadcasts. This configuration is
 * separate from {@link TopologyProcessorConfig} to allow listener dependency creation based on
 * creation of beans defined in {@link TopologyProcessorConfig}.
 */
@Configuration
@Import({
        MarketRunnerConfig.class,
        TopologyProcessorConfig.class,
        LicenseCheckClientConfig.class
})
public class TopologyListenerConfig {

    @Autowired
    private MarketRunnerConfig marketRunnerConfig;

    @Autowired
    private TopologyProcessorConfig tpConfig;

    @Autowired
    private LicenseCheckClientConfig licenseCheckClientConfig;

    @Value("${maxPlacementIterations:-1}")
    private int maxPlacementIterations;

    @Value("${useQuoteCacheDuringSNM:false}")
    private boolean useQuoteCacheDuringSNM;

    @Value("${replayProvisionsForRealTime:false}")
    private boolean replayProvisionsForRealTime;

    @Value("${rightsizeLowerWatermark:0.7}")
    private float rightsizeLowerWatermark;

    @Value("${rightsizeUpperWatermark:0.7}")
    private float rightsizeUpperWatermark;

    @Value("${discountedComputeCostFactor:4.0}")
    private float discountedComputeCostFactor;

    @Bean
    public Optional<Integer> maxPlacementsOverride() {
        return maxPlacementIterations > 0
                ? Optional.of(maxPlacementIterations)
                : Optional.empty();
    }

    @Bean
    public TopologyEntitiesListener topologyEntitiesListener() {
        final TopologyEntitiesListener topologyEntitiesListener = new TopologyEntitiesListener(
                marketRunnerConfig.marketRunner(), maxPlacementsOverride(), useQuoteCacheDuringSNM,
                replayProvisionsForRealTime, rightsizeLowerWatermark, rightsizeUpperWatermark,
                discountedComputeCostFactor, licenseCheckClientConfig.licenseCheckClient());
        tpConfig.topologyProcessor().addLiveTopologyListener(topologyEntitiesListener);
        tpConfig.topologyProcessor().addPlanTopologyListener(topologyEntitiesListener);
        return topologyEntitiesListener;
    }
}
