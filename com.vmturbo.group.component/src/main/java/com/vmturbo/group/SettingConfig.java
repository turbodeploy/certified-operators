package com.vmturbo.group;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.group.persistent.SettingStore;

@Configuration
public class SettingConfig {

    @Value("${settingSpecJsonFile:setting/setting-spec.json}")
    private String settingSpecJsonFile;

    @Bean
    public SettingStore settingStore() {
        return new SettingStore(settingSpecJsonFile);
    }

}
