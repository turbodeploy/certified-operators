package com.vmturbo.api.component.external.api.mapper;

import com.vmturbo.api.enums.CloudType;
import com.vmturbo.api.enums.EnvironmentType;

/**
 * Maps groups between their @EnvironmentType representation and their @CloudType representation.
 */
public class EntityEnvironment {
    private EnvironmentType environmentType;
    private CloudType cloudType;

    /**
     * Contructor for initializing the EnvCloudMapper object.
     * @param environmentType The environment type of the group.
     * @param cloudType The cloud type of the group.
     */
    public EntityEnvironment(EnvironmentType environmentType, CloudType cloudType) {
        this.environmentType = environmentType;
        this.cloudType = cloudType;
    }

    public EnvironmentType getEnvironmentType() {
        return environmentType;
    }

    public void setEnvironmentType(EnvironmentType environmentType) {
        this.environmentType = environmentType;
    }

    public CloudType getCloudType() {
        return cloudType;
    }

    public void setCloudType(CloudType cloudType) {
        this.cloudType = cloudType;
    }
}
