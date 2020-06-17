package com.vmturbo.topology.processor.targets;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.identity.attributes.IdentityMatchingAttribute;
import com.vmturbo.identity.attributes.IdentityMatchingAttributes;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * Test the class that extracts identifying attributes from a WorkflowItem.
 */
public class TargetSpecAttributeExtractorTest {

    public static final String PROBE_TYPE_IDENTIFIER = "probeType";
    public static final String PROBE_TYPE_NAME = "myProbeType";
    public static final String ADDR_NAME = "address";
    public static final String ADDR = "1.2.3.4";

    private final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
    private final org.apache.logging.log4j.Logger logger = LogManager.getLogger();

    /**
     * Test that the WorkflowInfo external name and target id fields are extracted into the
     * IdentityMatchingAttributes correctly.
     */
    @Test
    public void testExtractAttributes() {
        // arrange
        ProbeInfo probe = ProbeInfo.newBuilder().setProbeType(PROBE_TYPE_NAME).setProbeCategory("probeCategory")
                .addTargetIdentifierField(ADDR_NAME).setUiProbeCategory("probeCategory").build();
        TargetSpec testItem = TargetSpec.newBuilder()
            .setProbeId(1L)
            .addAccountValue(AccountValue.newBuilder().setKey(ADDR_NAME).setStringValue(ADDR))
            .build();
        // a workflow has two identifying attributes:  name and targetId
        IdentityMatchingAttribute nameAttr = new IdentityMatchingAttribute(
            PROBE_TYPE_IDENTIFIER, PROBE_TYPE_NAME);
        IdentityMatchingAttribute targetAttr = new IdentityMatchingAttribute(ADDR_NAME, ADDR);
        TargetSpecAttributeExtractor extractorToTest = new TargetSpecAttributeExtractor(probeStore);
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probe));
        // act
        IdentityMatchingAttributes attributesExtracted = extractorToTest.extractAttributes(testItem);
        // assert
        final Set<IdentityMatchingAttribute> matchingAttributes =
                attributesExtracted.getMatchingAttributes();
        assertThat(matchingAttributes.size(), equalTo(2));
        assertThat(matchingAttributes, containsInAnyOrder(nameAttr, targetAttr));
    }
}
