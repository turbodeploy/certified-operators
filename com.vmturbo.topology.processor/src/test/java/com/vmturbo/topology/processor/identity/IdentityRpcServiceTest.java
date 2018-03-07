package com.vmturbo.topology.processor.identity;


import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.Identity;
import com.vmturbo.common.protobuf.topology.Identity.GetAllProbeIdentityMetadataRequest;
import com.vmturbo.common.protobuf.topology.Identity.GetProbeIdentityMetadataRequest;
import com.vmturbo.common.protobuf.topology.Identity.ProbeIdentityMetadata;
import com.vmturbo.common.protobuf.topology.IdentityServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata.PropertyMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.util.Probes;

/**
 * Test for the gRPC API provided by IdentityRpcService
 */
public class IdentityRpcServiceTest {
    private IdentityServiceGrpc.IdentityServiceBlockingStub identityRpcClient;

    private final ProbeStore probeStore = Mockito.mock(ProbeStore.class);

    private final IdentityRpcService identityRpcServiceBackend = new IdentityRpcService(probeStore);
    /**
     * Counter to assign unique types/categories to generated ProbeInfos.
     */
    private AtomicInteger probeInfoCounter = new AtomicInteger(0);

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(identityRpcServiceBackend);

    @Before
    public void startup() throws Exception {
        identityRpcClient = IdentityServiceGrpc.newBlockingStub(server.getChannel());
    }

    /**
     * Test the call to GET a single probe's identity information.
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetIdentityInformation() throws Exception {
        final ProbeInfo probeInfo = createProbeInfo(2, 2);
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));
        final ProbeIdentityMetadata identityMetadata =
                identityRpcClient.getProbeIdentityMetadata(
                        GetProbeIdentityMetadataRequest.newBuilder().setProbeId(0).build()
                );
        Assert.assertTrue(identityMetadata.getError().isEmpty());
        Assert.assertEquals(0, identityMetadata.getProbeId());
        Assert.assertNotNull(identityMetadata.getEntityIdentityMetadataList());
        Assert.assertEquals(2, identityMetadata.getEntityIdentityMetadataCount());
        compareMetadataList(probeInfo.getEntityMetadataList(), identityMetadata.getEntityIdentityMetadataList());
    }

    /**
     * Test the call to GET a single probe's identity information for a probe that doesn't
     * have any properties defined for it's entity identity metadata.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetIdentityInformationEmptyPropertyList() throws Exception {
        final ProbeInfo probeInfo = createProbeInfo(1, 0);
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.of(probeInfo));
        final ProbeIdentityMetadata identityMetadata =
                identityRpcClient.getProbeIdentityMetadata(
                        GetProbeIdentityMetadataRequest.newBuilder().setProbeId(0).build()
                );
        Assert.assertTrue(identityMetadata.getError().isEmpty());
        Assert.assertEquals(0, identityMetadata.getProbeId());
        Assert.assertNotNull(identityMetadata.getEntityIdentityMetadataList());
        Assert.assertEquals(1, identityMetadata.getEntityIdentityMetadataCount());
        Assert.assertNotNull(identityMetadata.getEntityIdentityMetadata(0).getNonVolatilePropertiesList());
        Assert.assertNotNull(identityMetadata.getEntityIdentityMetadata(0).getVolatilePropertiesList());
        Assert.assertNotNull(identityMetadata.getEntityIdentityMetadata(0).getHeuristicPropertiesList());
    }

    /**
     * Test the call to GET all probes' identity information.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetAllIdentityInformation() throws Exception {
        final ProbeInfo probeInfo1 = createProbeInfo(2, 1);
        final ProbeInfo probeInfo2 = createProbeInfo(2, 1);
        Mockito.when(probeStore.getProbes()).thenReturn(ImmutableMap.of(1L, probeInfo1, 2L, probeInfo2));
        final Iterator<Identity.ProbeIdentityMetadata> identityMetadata =
                identityRpcClient.getAllProbeIdentityMetadata(GetAllProbeIdentityMetadataRequest.newBuilder().build());
        for (final ProbeIdentityMetadata metadata : Lists.newArrayList(identityMetadata)) {
            Assert.assertNotNull(metadata.getProbeId());
            compareMetadataList((metadata.getProbeId() == 1 ? probeInfo1 : probeInfo2).getEntityMetadataList(), metadata.getEntityIdentityMetadataList());
        }

    }

    /**
     * Test a GET to a probe ID that doesn't exist.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testProbeIdNotFound() throws Exception {
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenReturn(Optional.empty());
        final ProbeIdentityMetadata resp = identityRpcClient.getProbeIdentityMetadata(
                GetProbeIdentityMetadataRequest.newBuilder().setProbeId(1).build()
        );
        Assert.assertNotNull(resp.getError());
        Assert.assertEquals(resp.getError(), "Probe not registered.");
    }

    private ProbeInfo createProbeInfo(final int numEntityTypes, final int numProperties) {
        probeInfoCounter.getAndIncrement();
        final ProbeInfo.Builder probeInfo = Probes.createEmptyProbe();
        for (int entityNum = 0; entityNum < numEntityTypes; ++entityNum) {
            final EntityIdentityMetadata.Builder entityBuilder = EntityIdentityMetadata.newBuilder();
            entityBuilder.setEntityType(EntityType.valueOf(entityNum));
            for (int propertyNum = 0; propertyNum < numProperties; ++propertyNum) {
                entityBuilder.addNonVolatileProperties(createPropertyMetadata("nonVolatile" + Integer.toString(propertyNum)));
                entityBuilder.addVolatileProperties(createPropertyMetadata("volatile" + Integer.toString(propertyNum)));
                entityBuilder.addHeuristicProperties(createPropertyMetadata("heuristic" + Integer.toString(propertyNum)));
            }
            probeInfo.addEntityMetadata(entityBuilder);
        }
        return probeInfo.build();
    }

    private void compareMetadataList(final List<EntityIdentityMetadata> expected,
                                     final List<Identity.EntityIdentityMetadata> actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); ++i) {
            compareMetadata(expected.get(i), actual.get(i));
        }
    }

    private void compareMetadata(final EntityIdentityMetadata expected,
                                 final Identity.EntityIdentityMetadata actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.getEntityType(), EntityType.valueOf(actual.getEntityType()));
        Assert.assertEquals(expected.getHeuristicThreshold(), actual.getHeuristicThreshold());
        comparePropertyList(expected.getHeuristicPropertiesList(), actual.getHeuristicPropertiesList());
        comparePropertyList(expected.getNonVolatilePropertiesList(), actual.getNonVolatilePropertiesList());
        comparePropertyList(expected.getVolatilePropertiesList(), actual.getVolatilePropertiesList());
    }

    private void comparePropertyList(final List<PropertyMetadata> expected,
                                     final List<String> actual) {
        Assert.assertNotNull(expected);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); ++i) {
            Assert.assertEquals(expected.get(i).getName(), actual.get(i));
        }
    }

    private PropertyMetadata createPropertyMetadata(final String name) {
        return PropertyMetadata.newBuilder().setName(name).build();
    }
}
