package com.vmturbo.components.test.utilities.component;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import com.vmturbo.clustermgr.api.impl.ClusterMgrClient;

public class ServiceConfigurationTest {

    final ClusterMgrClient clusterMgr = mock(ClusterMgrClient.class);

    @Test
    public void testService() throws Exception {
        ServiceConfiguration.forService("market")
            .withConfiguration("foo", "bar")
            .withConfiguration("baz", "frob")
            .apply(clusterMgr);

        verify(clusterMgr).setPropertyForComponentType(eq("market"), eq("foo"), eq("bar"));
        verify(clusterMgr).setPropertyForComponentType(eq("market"), eq("baz"), eq("frob"));
    }

    @Test
    public void testInstance() throws Exception {
        ServiceConfiguration.forService("market")
            .instance("market_1")
            .withConfiguration("foo", "bar")
            .apply(clusterMgr);

        verify(clusterMgr).setPropertyForComponentInstance(eq("market"), eq("market_1"), eq("foo"), eq("bar"));
    }
}