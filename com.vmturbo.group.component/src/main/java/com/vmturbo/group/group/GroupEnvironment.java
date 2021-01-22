package com.vmturbo.group.group;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.vmturbo.common.protobuf.common.CloudTypeEnum;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;

/**
 * Wrapper class for group's environment. Holds environment type & cloud type.
 *
 * <p>An instance of this class can hold multiple values of environment/cloud type. The accessor for
 * each type will return one, calculated type (e.g. if AWS & AZURE were added as cloud types, HYBRID
 * will be returned).
 */
public class GroupEnvironment {
    /**
     * The environment type of the group.
     */
    private final Set<EnvironmentTypeEnum.EnvironmentType> environmentTypes;

    /**
     * The cloud type for group. This is relevant when the environment type is CLOUD or HYBRID;
     * for ON_PREM, it is UNKNOWN.
     */
    private final Set<CloudTypeEnum.CloudType> cloudTypes;

    /**
     * Basic constructor.
     */
    public GroupEnvironment() {
        this.environmentTypes = EnumSet.noneOf(EnvironmentTypeEnum.EnvironmentType.class);
        this.cloudTypes = EnumSet.noneOf(CloudTypeEnum.CloudType.class);
    }

    /**
     * Constructor with initial values.
     *
     * @param envType initial value for environment type
     * @param cloudType initial value for cloud type
     */
    public GroupEnvironment(final EnvironmentTypeEnum.EnvironmentType envType,
            final CloudTypeEnum.CloudType cloudType) {
        this.environmentTypes = EnumSet.of(envType);
        this.cloudTypes = EnumSet.of(cloudType);
    }

    /**
     * Adds a new environment type to this group environment.
     * @param envType the environment type to add.
     */
    public void addEnvironmentType(EnvironmentTypeEnum.EnvironmentType envType) {
        environmentTypes.add(envType);
    }

    /**
     * Adds a new cloud type to this group environment.
     * @param cloudType the cloud type to add.
     */
    public void addCloudType(CloudTypeEnum.CloudType cloudType) {
        cloudTypes.add(cloudType);
    }

    /**
     * Accessor for environment type.
     * @return the environment type. If multiple types (excluding UNKNOWN) were added, it returns
     *         HYBRID.
     */
    public EnvironmentTypeEnum.EnvironmentType getEnvironmentType() {
        if (environmentTypes.isEmpty()) {
            return EnvironmentTypeEnum.EnvironmentType.UNKNOWN_ENV;
        } else if (environmentTypes.size() == 1) {
            return environmentTypes.iterator().next();
        } else {
            // if we have multiple values, check for unknowns before returning
            Set<EnvironmentTypeEnum.EnvironmentType> knownEnvTypes = environmentTypes.stream()
                    .filter(et -> et != EnvironmentTypeEnum.EnvironmentType.UNKNOWN_ENV)
                    .collect(Collectors.toSet());
            if (knownEnvTypes.size() == 1) {
                return knownEnvTypes.iterator().next();
            } else {
                // knownEnvTypes.size() > 1
                return EnvironmentTypeEnum.EnvironmentType.HYBRID;
            }
        }
    }

    /**
     * Accessor for cloud type.
     * @return the cloud type. If multiple types (excluding UNKNOWN) were added, it returns HYBRID.
     */
    public CloudTypeEnum.CloudType getCloudType() {
        if (cloudTypes.isEmpty()) {
            return CloudTypeEnum.CloudType.UNKNOWN_CLOUD;
        } else if (cloudTypes.size() == 1) {
            return cloudTypes.iterator().next();
        } else {
            // if we have multiple values, check for unknowns before returning
            Set<CloudTypeEnum.CloudType> knownCloudTypes = cloudTypes.stream()
                    .filter(ct -> ct != CloudTypeEnum.CloudType.UNKNOWN_CLOUD)
                    .collect(Collectors.toSet());
            if (knownCloudTypes.size() == 1) {
                return knownCloudTypes.iterator().next();
            } else {
                // knownCloudTypes.size() > 1
                return CloudTypeEnum.CloudType.HYBRID_CLOUD;
            }
        }
    }
}
