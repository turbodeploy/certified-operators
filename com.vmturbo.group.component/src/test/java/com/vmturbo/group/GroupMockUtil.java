package com.vmturbo.group;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.common.CloudTypeEnum.CloudType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.group.group.GroupEnvironment;

/**
 * Utilities to make mocking easier.
 */
public class GroupMockUtil {

    private GroupMockUtil() { }

    /**
     * Mock a {@link GroupEnvironment}.
     *
     * @param envType The environment type.
     * @param cloudType The cloud type.
     * @return The {@link GroupEnvironment}.
     */
    @Nonnull
    public static GroupEnvironment mockEnvironment(EnvironmentType envType, CloudType cloudType) {
        GroupEnvironment env = mock(GroupEnvironment.class);
        when(env.getEnvironmentType()).thenReturn(envType);
        when(env.getCloudType()).thenReturn(cloudType);
        return env;
    }

}
