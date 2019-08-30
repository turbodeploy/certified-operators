package com.vmturbo.components.test.utilities.component;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import com.vmturbo.clustermgr.api.ClusterMgrRestClient;

public class ServiceConfigurationTest {

    final ClusterMgrRestClient clusterMgr = mock(ClusterMgrRestClient.class);

    @Test
    public void testInstance() throws Exception {
        ServiceConfiguration.forService("market", "market_1")
            .withConfiguration("foo", "bar")
            .apply(clusterMgr);

        verify(clusterMgr).setComponentLocalProperty(eq("market"), eq("foo"), eq("bar"));
    }
}