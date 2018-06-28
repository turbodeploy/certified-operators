package com.vmturbo.topology.processor.supplychain;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;

/**
 * Configuration for supply validation.
 */
@Configuration
@Import({TargetConfig.class, ProbeConfig.class})
public class SupplyChainValidationConfig {
    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Bean
    public SupplyChainValidator supplyChainValidator() {
        return new SupplyChainValidator(probeConfig.probeStore(), targetConfig.targetStore());
    }
}
