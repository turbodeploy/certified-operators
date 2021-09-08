package com.vmturbo.extractor.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.extractor.ReportingREST.ExtractorSettingServiceController;
import com.vmturbo.extractor.ExtractorDbConfig;

/**
 * RPC related config for extractor.
 */
@Configuration
@Import({ExtractorDbConfig.class})
public class ExtractorRpcConfig {
    @Autowired
    private ExtractorDbConfig dbConfig;

    /**
     * Gets extractor setting service.
     *
     * @return Instance of ExtractorSettingRpcService.
     */
    @Bean
    public ExtractorSettingRpcService extractorSettingService() {
        return new ExtractorSettingRpcService(dbConfig.ingesterEndpoint());
    }

    /**
     * Gets the controller.
     *
     * @return New ExtractorSettingServiceController instance.
     */
    @Bean
    public ExtractorSettingServiceController extractorSettingServiceController() {
        return new ExtractorSettingServiceController(extractorSettingService());
    }
}
