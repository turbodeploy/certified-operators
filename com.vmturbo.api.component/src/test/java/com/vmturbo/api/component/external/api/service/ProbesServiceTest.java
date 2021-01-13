package com.vmturbo.api.component.external.api.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.Spy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.dto.probe.ProbePropertyNameValuePairApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.probe.ProbeDTO.DeleteProbePropertyRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.DeleteProbePropertyResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetAllProbePropertiesRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetAllProbePropertiesResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbeInfoRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbeInfoResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbePropertyValueRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbePropertyValueResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetTableOfProbePropertiesRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetTableOfProbePropertiesResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.ProbePropertyNameValuePair;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateOneProbePropertyRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateOneProbePropertyResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateProbePropertyTableRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateProbePropertyTableResponse;
import com.vmturbo.common.protobuf.probe.ProbeRpcServiceGrpc;
import com.vmturbo.common.protobuf.probe.ProbeRpcServiceGrpc.ProbeRpcServiceImplBase;
import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.topology.processor.api.AccountDefEntry;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Unit tests for the {@link ProbesService} class.
 */
public class ProbesServiceTest {
    private ProbesService probesService;

    @Spy
    private ProbeRpcServiceImplBase mockServer;

    private TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);

    /**
     * Set up a mock topology processor server and a {@link ProbesService} client and connects them.
     *
     * @throws Exception should not happen.
     */
    @Before
    public void setUp() throws Exception {
        // Mock server
        mockServer = mock(ProbeRpcServiceImplBase.class, delegatesTo(new MockProbeRpcService()));
        final String serverName = InProcessServerBuilder.generateName();
        InProcessServerBuilder
            .forName(serverName)
            .directExecutor()
            .addService(mockServer)
            .build()
            .start();

        // Create client
        probesService =
            new ProbesService(
                ProbeRpcServiceGrpc.newBlockingStub(
                    InProcessChannelBuilder
                        .forName(serverName)
                        .directExecutor()
                        .build()), topologyProcessor);
    }

    /**
     * Calling {@link ProbesService#getProbe(String)} on a string that can be parsed as a number,
     * will send a correct {@link GetProbeInfoRequest} to the topology processor.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetProbe() throws Exception {
        final ArgumentCaptor<GetProbeInfoRequest>
            captureRequest = ArgumentCaptor.forClass(GetProbeInfoRequest.class);

        probesService.getProbe("20");

        verify(mockServer).getProbeInfo(captureRequest.capture(), any());
        assertEquals(20L, captureRequest.getValue().getOid());
    }

    /**
     * Calling {@link ProbesService#getProbes()} will send a correct {@link GetProbeInfoRequest} to the topology processor.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetProbes() throws Exception {
        // Create our return values
        Set<ProbeInfo> probeInfoSet = ImmutableSet.of(
                createMockProbeInfo(1, "type", "category", "uiCategory", MediationMessage.ProbeInfo.CreationMode.DERIVED)
        );

        // Configure the mock TopologyProcessor
        when(topologyProcessor.getAllProbes()).thenReturn(probeInfoSet);

        // Execute the getProbes() method
        List<TargetApiDTO> targets = probesService.getProbes();
        assertEquals("Expected 1 target", 1, targets.size());
        TargetApiDTO target = targets.get(0);
        assertEquals("UUID is not correct", "1", target.getUuid());
        assertEquals("Type is not correct", "type", target.getType());
        assertEquals("Category is not correct", "uiCategory", target.getCategory());
    }

    private ProbeInfo createMockProbeInfo(long probeId, String type, String category, String uiCategory,
                                          MediationMessage.ProbeInfo.CreationMode creationMode, AccountDefEntry... entries) throws Exception {
        final ProbeInfo newProbeInfo = Mockito.mock(ProbeInfo.class);
        when(newProbeInfo.getId()).thenReturn(probeId);
        when(newProbeInfo.getType()).thenReturn(type);
        when(newProbeInfo.getCategory()).thenReturn(category);
        when(newProbeInfo.getUICategory()).thenReturn(uiCategory);
        when(newProbeInfo.getAccountDefinitions()).thenReturn(Arrays.asList(entries));
        when(newProbeInfo.getCreationMode()).thenReturn(creationMode);
        if (entries.length > 0) {
            when(newProbeInfo.getIdentifyingFields())
                    .thenReturn(Collections.singletonList(entries[0].getName()));
        } else {
            when(newProbeInfo.getIdentifyingFields()).thenReturn(Collections.emptyList());
        }
        return newProbeInfo;
    }

    /**
     * A string that cannot be parsed as a number should not be used as a probe oid.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testGetProbeWithBadInput() throws Exception {
        probesService.getProbe("somestring");
    }

    /**
     * Calling {@link ProbesService#getAllProbeProperties()} should send an empty
     * {@link GetAllProbePropertiesRequest} to the topology processor.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetAllProbeProperties() throws Exception {
        final ArgumentCaptor<GetAllProbePropertiesRequest>
            captureRequest = ArgumentCaptor.forClass(GetAllProbePropertiesRequest.class);

        probesService.getAllProbeProperties();

        verify(mockServer).getAllProbeProperties(captureRequest.capture(), any());
    }

    /**
     * Calling {@link ProbesService#getAllProbeSpecificProbeProperties} should send a
     * {@link GetTableOfProbePropertiesRequest} that only contains a probe id to the topology processor.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetAllProbeSpecificProbeProperties() throws Exception {
        final ArgumentCaptor<GetTableOfProbePropertiesRequest>
            captureRequest = ArgumentCaptor.forClass(GetTableOfProbePropertiesRequest.class);

        probesService.getAllProbeSpecificProbeProperties("10");

        verify(mockServer).getTableOfProbeProperties(captureRequest.capture(), any());
        assertEquals(10L, captureRequest.getValue().getProbePropertyTable().getProbeId());
        assertFalse(captureRequest.getValue().getProbePropertyTable().hasTargetId());
    }

    /**
     * A string that cannot be parsed as a number should not be used as a probe oid.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testGetAllProbeSpecificProbePropertiesWithBadInput() throws Exception {
        probesService.getAllProbeSpecificProbeProperties("");
    }

    /**
     * Calling {@link ProbesService#getAllTargetSpecificProbeProperties} should send a
     * {@link GetTableOfProbePropertiesRequest} that contains a probe id and a target id to the
     * topology processor.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetAllTargetSpecificProbeProperties() throws Exception {
        final ArgumentCaptor<GetTableOfProbePropertiesRequest>
            captureRequest = ArgumentCaptor.forClass(GetTableOfProbePropertiesRequest.class);

        probesService.getAllTargetSpecificProbeProperties("10", "20");

        verify(mockServer).getTableOfProbeProperties(captureRequest.capture(), any());
        assertEquals(10L, captureRequest.getValue().getProbePropertyTable().getProbeId());
        assertEquals(20L, captureRequest.getValue().getProbePropertyTable().getTargetId());
    }

    /**
     * A string that cannot be parsed as a number should not be used as a probe oid.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testGetAllTargetSpecificProbePropertiesWithBadInput1() throws Exception {
        probesService.getAllTargetSpecificProbeProperties("x", "20");
    }

    /**
     * A string that cannot be parsed as a number should not be used as a target oid.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testGetAllTargetSpecificProbePropertiesWithBadInput2() throws Exception {
        probesService.getAllTargetSpecificProbeProperties("10", "x");
    }

    /**
     * Calling {@link ProbesService#getProbeSpecificProbeProperty} should send a
     * {@link GetProbePropertyValueRequest} that contains a probe id and a property name
     * to the topology processor.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetProbeSpecificProbeProperty() throws Exception {
        final ArgumentCaptor<GetProbePropertyValueRequest>
            captureRequest = ArgumentCaptor.forClass(GetProbePropertyValueRequest.class);

        probesService.getProbeSpecificProbeProperty("10", "name");

        verify(mockServer).getProbePropertyValue(captureRequest.capture(), any());
        assertEquals(10L, captureRequest.getValue().getProbePropertyTable().getProbeId());
        assertFalse(captureRequest.getValue().getProbePropertyTable().hasTargetId());
        assertEquals("name", captureRequest.getValue().getName());
    }

    /**
     * A string that cannot be parsed as a number should not be used as a probe oid.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testGetProbeSpecificProbePropertyWithBadInput1() throws Exception {
        probesService.getProbeSpecificProbeProperty("x", "y");
    }

    /**
     * Probe property names should not be empty.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testGetProbeSpecificProbePropertyWithBadInput2() throws Exception {
        probesService.getProbeSpecificProbeProperty("10", "");
    }

    /**
     * Calling {@link ProbesService#getTargetSpecificProbeProperty} should send a
     * {@link GetProbePropertyValueRequest} that contains a probe id, a target name,
     * and a property name to the topology processor.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetTargetSpecificProbeProperty() throws Exception {
        final ArgumentCaptor<GetProbePropertyValueRequest>
            captureRequest = ArgumentCaptor.forClass(GetProbePropertyValueRequest.class);

        probesService.getTargetSpecificProbeProperty("10", "20", "name");

        verify(mockServer).getProbePropertyValue(captureRequest.capture(), any());
        assertEquals(10L, captureRequest.getValue().getProbePropertyTable().getProbeId());
        assertEquals(20L, captureRequest.getValue().getProbePropertyTable().getTargetId());
        assertEquals("name", captureRequest.getValue().getName());
    }

    /**
     * A string that cannot be parsed as a number should not be used as a probe oid.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testGetTargetSpecificProbePropertyWithBadInput1() throws Exception {
        probesService.getTargetSpecificProbeProperty("x", "20", "z");
    }

    /**
     * A string that cannot be parsed as a number should not be used as a target oid.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testGetTargetSpecificProbePropertyWithBadInput2() throws Exception {
        probesService.getTargetSpecificProbeProperty("10", "", "z");
    }

    /**
     * Probe property names should not be empty.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testGetTargetSpecificProbePropertyWithBadInput3() throws Exception {
        probesService.getTargetSpecificProbeProperty("10", "20", "");
    }

    /**
     * Update the probe property table specific to one probe.  This should send an
     * {@link UpdateProbePropertyTableRequest} with all the new probe properties to the server.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testPutAllProbeSpecificProperties() throws Exception {
        final ProbePropertyNameValuePairApiDTO property1 = new ProbePropertyNameValuePairApiDTO();
        property1.setName("name");
        property1.setValue("value");
        final ProbePropertyNameValuePairApiDTO property2 = new ProbePropertyNameValuePairApiDTO();
        property2.setName("name1");
        property2.setValue("value");

        probesService.putAllProbeSpecificProperties("10", ImmutableList.of(property1, property2));

        final ArgumentCaptor<UpdateProbePropertyTableRequest>
            captureRequest = ArgumentCaptor.forClass(UpdateProbePropertyTableRequest.class);

        verify(mockServer).updateProbePropertyTable(captureRequest.capture(), any());

        assertEquals(10L, captureRequest.getValue().getProbePropertyTable().getProbeId());
        assertFalse(captureRequest.getValue().getProbePropertyTable().hasTargetId());

        assertEquals(2, captureRequest.getValue().getNewProbePropertiesCount());
        for (ProbePropertyNameValuePair p : captureRequest.getValue().getNewProbePropertiesList()) {
            assertTrue(p.getName().startsWith("name"));
            assertEquals("value", p.getValue());
        }
    }

    /**
     * Update the probe property table specific to one target: No duplicate property names are permitted.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testPutAllProbePropertiesDuplicateEntry() throws Exception {
        final ProbePropertyNameValuePairApiDTO property1 = new ProbePropertyNameValuePairApiDTO();
        property1.setName("name");
        property1.setValue("value");
        final ProbePropertyNameValuePairApiDTO property2 = new ProbePropertyNameValuePairApiDTO();
        property2.setName("name");
        property2.setValue("value");
        probesService.putAllProbeSpecificProperties("10", ImmutableList.of(property1, property2));
    }

    /**
     * Update the probe property table specific to one probe: property names are mandatory.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testPutAllProbeSpecificPropertiesBadInput1() throws Exception {
        final ProbePropertyNameValuePairApiDTO property1 = new ProbePropertyNameValuePairApiDTO();
        property1.setName("");
        property1.setValue("value");
        final ProbePropertyNameValuePairApiDTO property2 = new ProbePropertyNameValuePairApiDTO();
        property2.setName("name");
        property2.setValue("value");
        probesService.putAllProbeSpecificProperties("10", ImmutableList.of(property1, property2));
    }

    /**
     * Update the probe property table specific to one probe: values are mandatory.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testPutAllProbeSpecificPropertiesBadInput2() throws Exception {
        final ProbePropertyNameValuePairApiDTO property = new ProbePropertyNameValuePairApiDTO();
        property.setName("name");
        probesService.putAllProbeSpecificProperties("10", ImmutableList.of(property));
    }

    /**
     * Update the probe property table specific to one target.  This should send an
     * {@link UpdateProbePropertyTableRequest} with all the new probe properties to the server.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testPutAllTargetSpecificProperties() throws Exception {
        final ProbePropertyNameValuePairApiDTO property1 = new ProbePropertyNameValuePairApiDTO();
        property1.setName("name");
        property1.setValue("value");
        final ProbePropertyNameValuePairApiDTO property2 = new ProbePropertyNameValuePairApiDTO();
        property2.setName("name1");
        property2.setValue("value");

        probesService.putAllTargetSpecificProperties("10", "20", ImmutableList.of(property1, property2));

        final ArgumentCaptor<UpdateProbePropertyTableRequest>
            captureRequest = ArgumentCaptor.forClass(UpdateProbePropertyTableRequest.class);

        verify(mockServer).updateProbePropertyTable(captureRequest.capture(), any());

        assertEquals(10L, captureRequest.getValue().getProbePropertyTable().getProbeId());
        assertEquals(20L, captureRequest.getValue().getProbePropertyTable().getTargetId());

        assertEquals(2, captureRequest.getValue().getNewProbePropertiesCount());
        for (ProbePropertyNameValuePair p : captureRequest.getValue().getNewProbePropertiesList()) {
            assertTrue(p.getName().startsWith("name"));
            assertEquals("value", p.getValue());
        }
    }

    /**
     * Update the probe property table specific to one target: No duplicate property names are permitted.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testPutAllTargetPropertiesDuplicateEntry() throws Exception {
        final ProbePropertyNameValuePairApiDTO property1 = new ProbePropertyNameValuePairApiDTO();
        property1.setName("name");
        property1.setValue("value");
        final ProbePropertyNameValuePairApiDTO property2 = new ProbePropertyNameValuePairApiDTO();
        property2.setName("name");
        property2.setValue("value");
        probesService.putAllTargetSpecificProperties("10", "20", ImmutableList.of(property1, property2));
    }

    /**
     * Update the probe property table specific to one target: property names are mandatory.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testPutAllTargetSpecificPropertiesBadInput1() throws Exception {
        final ProbePropertyNameValuePairApiDTO property1 = new ProbePropertyNameValuePairApiDTO();
        property1.setName("name");
        property1.setValue("value");
        final ProbePropertyNameValuePairApiDTO property2 = new ProbePropertyNameValuePairApiDTO();
        property2.setName("");
        property2.setValue("value");
        probesService.putAllTargetSpecificProperties("10", "20", ImmutableList.of(property1, property2));
    }

    /**
     * Update the probe property table specific to one target: values are mandatory.
     *
     * @throws Exception an illegal argument exception should be thrown.
     */
    @Test(expected = OperationFailedException.class)
    public void testPutAllTargetSpecificPropertiesBadInput2() throws Exception {
        final ProbePropertyNameValuePairApiDTO property = new ProbePropertyNameValuePairApiDTO();
        property.setName("name");
        probesService.putAllTargetSpecificProperties("10", "20", ImmutableList.of(property));
    }

    /**
     * Update a probe-specific probe property should send a {@link UpdateOneProbePropertyRequest}.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testPutProbeSpecificProperty() throws Exception {
        probesService.putProbeSpecificProperty("10", "name", "value");
        final ArgumentCaptor<UpdateOneProbePropertyRequest>
            captureRequest = ArgumentCaptor.forClass(UpdateOneProbePropertyRequest.class);
        verify(mockServer).updateOneProbeProperty(captureRequest.capture(), any());

        assertEquals(
            10L,
            captureRequest.getValue().getNewProbeProperty().getProbePropertyTable().getProbeId());
        assertFalse(captureRequest.getValue().getNewProbeProperty().getProbePropertyTable().hasTargetId());
        assertEquals(
            "name",
            captureRequest.getValue().getNewProbeProperty().getProbePropertyNameAndValue().getName());
        assertEquals(
            "value",
            captureRequest.getValue().getNewProbeProperty().getProbePropertyNameAndValue().getValue());
    }

    /**
     * Update a probe-specific probe property.  Value must not be empty.
     *
     * @throws Exception should not happen.
     */
    @Test(expected = OperationFailedException.class)
    public void testPutProbeSpecificPropertyBadInput() throws Exception {
        probesService.putProbeSpecificProperty("10", "name", "");
    }

    /**
     * Update a target-specific probe property should send a {@link UpdateOneProbePropertyRequest}.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testPutTargetSpecificProperty() throws Exception {
        probesService.putTargetSpecificProperty("10", "20", "name", "value");
        final ArgumentCaptor<UpdateOneProbePropertyRequest>
            captureRequest = ArgumentCaptor.forClass(UpdateOneProbePropertyRequest.class);
        verify(mockServer).updateOneProbeProperty(captureRequest.capture(), any());

        assertEquals(
            10L,
            captureRequest.getValue().getNewProbeProperty().getProbePropertyTable().getProbeId());
        assertEquals(
            20L,
            captureRequest.getValue().getNewProbeProperty().getProbePropertyTable().getTargetId());
        assertEquals(
            "name",
            captureRequest.getValue().getNewProbeProperty().getProbePropertyNameAndValue().getName());
        assertEquals(
            "value",
            captureRequest.getValue().getNewProbeProperty().getProbePropertyNameAndValue().getValue());
    }

    /**
     * Update a target-specific probe property.  Target id should parse to a number.
     *
     * @throws Exception should not happen.
     */
    @Test(expected = OperationFailedException.class)
    public void testPutTargetSpecificPropertyBadInput1() throws Exception {
        probesService.putTargetSpecificProperty("10", "","name", "value");
    }

    /**
     * Update a target-specific probe property.  Value must not be empty.
     *
     * @throws Exception should not happen.
     */
    @Test(expected = OperationFailedException.class)
    public void testPutTargetSpecificPropertyBadInput2() throws Exception {
        probesService.putTargetSpecificProperty("10", "20","name", "");
    }

    /**
     * Delete a probe-specific probe property should send a {@link DeleteProbePropertyRequest}.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testDeleteProbeSpecificProperty() throws Exception {
        probesService.deleteProbeSpecificProperty("10", "name");
        final ArgumentCaptor<DeleteProbePropertyRequest>
            captureRequest = ArgumentCaptor.forClass(DeleteProbePropertyRequest.class);
        verify(mockServer).deleteProbeProperty(captureRequest.capture(), any());

        assertEquals(10L, captureRequest.getValue().getProbePropertyTable().getProbeId());
        assertFalse(captureRequest.getValue().getProbePropertyTable().hasTargetId());
        assertEquals("name", captureRequest.getValue().getName());
    }

    /**
     * Delete a target-specific probe property should send a {@link DeleteProbePropertyRequest}.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testDeleteTargetSpecificProperty() throws Exception {
        probesService.deleteTargetSpecificProperty("10", "20", "name");
        final ArgumentCaptor<DeleteProbePropertyRequest>
                captureRequest = ArgumentCaptor.forClass(DeleteProbePropertyRequest.class);
        verify(mockServer).deleteProbeProperty(captureRequest.capture(), any());

        assertEquals(10L, captureRequest.getValue().getProbePropertyTable().getProbeId());
        assertEquals(20L, captureRequest.getValue().getProbePropertyTable().getTargetId());
        assertEquals("name", captureRequest.getValue().getName());
    }

    private static class MockProbeRpcService extends ProbeRpcServiceImplBase {
        @Override
        public void getProbeInfo(
                GetProbeInfoRequest request,
                StreamObserver<GetProbeInfoResponse> response) {
            response.onNext(GetProbeInfoResponse.newBuilder().build());
            response.onCompleted();
        }

        @Override
        public void getAllProbeProperties(
                GetAllProbePropertiesRequest request,
                StreamObserver<GetAllProbePropertiesResponse> response) {
            response.onNext(GetAllProbePropertiesResponse.newBuilder().build());
            response.onCompleted();
        }

        @Override
        public void getTableOfProbeProperties(
                GetTableOfProbePropertiesRequest request,
                StreamObserver<GetTableOfProbePropertiesResponse> response) {
            response.onNext(GetTableOfProbePropertiesResponse.newBuilder().build());
            response.onCompleted();
        }

        @Override
        public void getProbePropertyValue(
                GetProbePropertyValueRequest request,
                StreamObserver<GetProbePropertyValueResponse> response) {
            response.onNext(GetProbePropertyValueResponse.newBuilder().build());
            response.onCompleted();
        }

        @Override
        public void updateProbePropertyTable(
                UpdateProbePropertyTableRequest request,
                StreamObserver<UpdateProbePropertyTableResponse> response) {
            response.onNext(UpdateProbePropertyTableResponse.newBuilder().build());
            response.onCompleted();
        }

        @Override
        public void updateOneProbeProperty(
                UpdateOneProbePropertyRequest request,
                StreamObserver<UpdateOneProbePropertyResponse> response) {
            response.onNext(UpdateOneProbePropertyResponse.newBuilder().build());
            response.onCompleted();
        }

        @Override
        public void deleteProbeProperty(
                DeleteProbePropertyRequest request,
                StreamObserver<DeleteProbePropertyResponse> response) {
            response.onNext(DeleteProbePropertyResponse.newBuilder().build());
            response.onCompleted();
        }
    }
}
