package com.vmturbo.topology.processor.probes.internal;

import static com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode.INTERNAL;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;

/**
 * Test class for {@link UserDefinedEntitiesProbeDefinition}.
 */
public class UserDefinedEntitiesProbeDefinitionTest {

    /**
     * Checks UDE probe properties for correctness.
     */
    @Test
    public void testProbeInfo() {
        UserDefinedEntitiesProbeRetrieval retrieval = Mockito.mock(UserDefinedEntitiesProbeRetrieval.class);
        UserDefinedEntitiesProbeDefinition udeProbeDefinition = new UserDefinedEntitiesProbeDefinition(retrieval);
        ProbeInfo probeInfo = udeProbeDefinition.getProbeInfo();
        Assert.assertEquals(INTERNAL, probeInfo.getCreationMode());
        Assert.assertTrue(probeInfo.getSupplyChainDefinitionSetCount() > 0);
        Assert.assertTrue(probeInfo.getEntityMetadataCount() > 0);
        Assert.assertEquals(1, probeInfo.getAccountDefinitionCount());
    }

    /**
     * Checks UDE target properties for correctness.
     */
    @Test
    public void testTargetSpec() {
        UserDefinedEntitiesProbeRetrieval retrieval = Mockito.mock(UserDefinedEntitiesProbeRetrieval.class);
        UserDefinedEntitiesProbeDefinition udeProbeDefinition = new UserDefinedEntitiesProbeDefinition(retrieval);
        long probeId = 1001L;
        TopologyProcessorDTO.TargetSpec target = udeProbeDefinition.getProbeTarget(probeId);
        Assert.assertTrue(target.getIsHidden());
        Assert.assertEquals(probeId, target.getProbeId());
        Assert.assertEquals(1, target.getAccountValueCount());
    }
}
