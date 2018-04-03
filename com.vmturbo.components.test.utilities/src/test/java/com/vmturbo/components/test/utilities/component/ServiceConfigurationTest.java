package com.vmturbo.components.test.utilities.component;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import com.vmturbo.api.serviceinterfaces.IClusterService;
import com.vmturbo.clustermgr.api.impl.ClusterMgrClient;

public class ServiceConfigurationTest {

    final IClusterService clusterMgr = mock(IClusterService.class);

    @Test
    public void testInstance() throws Exception {
        ServiceConfiguration.forService("market", "market_1")
            .withConfiguration("foo", "bar")
            .apply(clusterMgr);

        verify(clusterMgr).setPropertyForComponentInstance(eq("market"), eq("market_1"), eq("foo"), eq("bar"));
    }
}