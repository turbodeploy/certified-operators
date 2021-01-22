package com.vmturbo.group.group;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.common.CloudTypeEnum.CloudType;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;

/**
 * Unit tests for {@link GroupEnvironment}.
 */
public class GroupEnvironmentTest {
    /**
     * Tests that a {@link GroupEnvironment} with nothing added returns UNKNOWNs.
     */
    @Test
    public void testEmptyTypes() {
        GroupEnvironment groupEnvironment = new GroupEnvironment();
        Assert.assertEquals(EnvironmentType.UNKNOWN_ENV, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.UNKNOWN_CLOUD, groupEnvironment.getCloudType());
    }

    /**
     * Tests that a {@link GroupEnvironment} with single values for each type returns those values.
     */
    @Test
    public void testSingleTypes() {
        GroupEnvironment groupEnvironment = new GroupEnvironment(EnvironmentType.CLOUD, CloudType.AWS);
        Assert.assertEquals(EnvironmentType.CLOUD, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.AWS, groupEnvironment.getCloudType());
    }

    /**
     * Tests that a {@link GroupEnvironment} with multiple values for each type returns HYBRIDs.
     */
    @Test
    public void testMultipleTypes() {
        GroupEnvironment groupEnvironment = new GroupEnvironment();
        groupEnvironment.addEnvironmentType(EnvironmentType.CLOUD);
        groupEnvironment.addEnvironmentType(EnvironmentType.ON_PREM);
        groupEnvironment.addCloudType(CloudType.AWS);
        groupEnvironment.addCloudType(CloudType.AZURE);
        Assert.assertEquals(EnvironmentType.HYBRID, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.HYBRID_CLOUD, groupEnvironment.getCloudType());
    }

    /**
     * Tests (for both environment & cloud type) that if we have two values, where one is UNKNOWN
     * and the other is a "known" value, {@link GroupEnvironment} returns the latter.
     */
    @Test
    public void testTwoTypesWithUnknown() {
        GroupEnvironment groupEnvironment = new GroupEnvironment();
        groupEnvironment.addEnvironmentType(EnvironmentType.CLOUD);
        groupEnvironment.addEnvironmentType(EnvironmentType.UNKNOWN_ENV);
        groupEnvironment.addCloudType(CloudType.AWS);
        groupEnvironment.addCloudType(CloudType.UNKNOWN_CLOUD);
        Assert.assertEquals(EnvironmentType.CLOUD, groupEnvironment.getEnvironmentType());
        Assert.assertEquals(CloudType.AWS, groupEnvironment.getCloudType());
    }
}
