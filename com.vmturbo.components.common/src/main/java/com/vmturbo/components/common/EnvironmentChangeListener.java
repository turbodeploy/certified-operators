package com.vmturbo.components.common;

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.context.environment.EnvironmentChangeEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * A listener for events of type {@link EnvironmentChangeEvent}.
 *
 */
@Component
public class EnvironmentChangeListener implements ApplicationListener<EnvironmentChangeEvent> {

    private Logger logger = LogManager.getLogger();

    @Autowired
    @Qualifier("theComponent")
    IVmtComponent theComponent;

    @Autowired
    Environment env;

    @Override
    public void onApplicationEvent(EnvironmentChangeEvent event) {
        Set<String> changedPropertykeys = event.getKeys();
        if (logger.isDebugEnabled()) {
            logger.debug("Environment changed, keys: " + changedPropertykeys);
            for (String changedKey : changedPropertykeys) {
                logger.debug("   " + changedKey + " -> " + env.getProperty(changedKey));
            }
        }
        theComponent.configurationPropertiesChanged(env, changedPropertykeys);
    }
}
