package com.vmturbo.topology.processor.discoverydumper;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.clustermgr.api.ComponentProperties;
import com.vmturbo.components.common.BaseVmtComponent;

/**
 * Discovery dump settings that obtain config values from the XL ClusterManager.
 *
 * <p>The dumpsToHold setting comes in two forms: discoveryDumpsToHold.default, and discoveryDumpsToHold.target.[targetName].
 * When the value is requested for a given target, the target-specific setting is used if it's available,
 * else the default setting is used.</p>
 *
 * <p>To reduce chatter to the cluster manager, the full set of component settings is retrieved and
 * kept locally. The settings are refreshed under two conditions:</p>
 * <ul>
 *     <li>The existing properties are stale, as defined by a refresh interval.</li>
 *     <li>The refresh method is invoked directly (e.g. because the discovery to be dumped was manually requested</li>
 * </ul>
 *
 */
public class ComponentBasedTargetDumpingSettings implements TargetDumpingSettings {
    private static Logger logger = LogManager.getLogger(ComponentBasedTargetDumpingSettings.class);


    // properties are considered stale if this much time has elapsed since last fetch
    private static final long PROPERTIES_REFRESH_INTERVAL = TimeUnit.MINUTES.toMillis(5L);
    private final String componentType;
    private final String componentId;
    // time (milliseconds since epoch) of last fetch
    private long lastFetchTime = 0L;
    // property values from most recent fetch
    private ComponentProperties componentProperties;

    public ComponentBasedTargetDumpingSettings(String componentType, String componentId) {
        this.componentType = componentType;
        this.componentId = componentId;
    }

    @Override
    public int getDumpsToHold(@Nonnull final String targetName) {
        refreshSettingsIfStale();
        int dumpsToHold = getDumpsToHoldProperty(targetName)
            .orElse(getDumpsToHoldProperty(null)
                .orElse(0));
        logger.info("Retaining {} discovery dumps for target {}", dumpsToHold, targetName);
        return dumpsToHold;
    }

    /**
     * Refresh local copy of properties if current properties are stale.
     */
    private void refreshSettingsIfStale() {
        if (System.currentTimeMillis() - lastFetchTime >= PROPERTIES_REFRESH_INTERVAL) {
            refreshSettings();
        }
    }

    /**
     * Refresh all properties for our component from the cluster manager.
     *
     * <p>This method can be used to force a refresh even if current properties are not yet stale.</p>
     */
    @Override
    public void refreshSettings() {
        this.componentProperties = BaseVmtComponent.getClusterMgrClient().getComponentInstanceProperties(componentType, componentId);
        this.lastFetchTime = System.currentTimeMillis();
    }

    /**
     * Obtain the integer value represented by the dumpsToHold property for the given target, or the default setting if no target specified.
     *
     * @param targetName name of target, as shown in UI, or null for default value
     * @return dumpsToHold property value for given target, or default value if no target tiven
     */
    private Optional<Integer> getDumpsToHoldProperty(@Nullable String targetName) {
        final String propertyName = "discoveryDumpsToHold" + (targetName != null ? ".target." + targetName : ".default");
        String value = componentProperties.get(propertyName);
        return Optional.ofNullable(value != null ? Integer.valueOf(value) : null);
    }
}
