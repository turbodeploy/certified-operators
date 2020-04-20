package com.vmturbo.mediation.gcp;

import com.vmturbo.mediation.gcp.client.GcpAccount;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import org.mockito.Mockito;

import javax.annotation.Nonnull;

import static org.junit.Assert.assertEquals;

public class GcpConversionProbeTest {

    private GcpAccount awsAccount = Mockito.mock(GcpAccount.class);
    private DiscoveryContextDTO discoveryContext = null;

    private void verifyOtherFieldsNotModified(@Nonnull DiscoveryResponse oldResponse,
                                              @Nonnull DiscoveryResponse newResponse) {
        assertEquals(oldResponse.getDiscoveredGroupList(), newResponse.getDiscoveredGroupList());
        assertEquals(oldResponse.getEntityProfileList(), newResponse.getEntityProfileList());
        assertEquals(oldResponse.getDeploymentProfileList(), newResponse.getDeploymentProfileList());
        assertEquals(oldResponse.getNotificationList(), newResponse.getNotificationList());
        assertEquals(oldResponse.getMetadataDTOList(), newResponse.getMetadataDTOList());
        assertEquals(oldResponse.getDerivedTargetList(), newResponse.getDerivedTargetList());
        assertEquals(oldResponse.getNonMarketEntityDTOList(), newResponse.getNonMarketEntityDTOList());
        assertEquals(oldResponse.getCostDTOList(), newResponse.getCostDTOList());
        assertEquals(oldResponse.getDiscoveryContext(), newResponse.getDiscoveryContext());
    }
}
