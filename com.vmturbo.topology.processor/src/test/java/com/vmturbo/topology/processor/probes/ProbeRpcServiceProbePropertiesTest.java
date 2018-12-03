package com.vmturbo.topology.processor.probes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.probe.ProbeDTO.DeleteProbePropertyRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetAllProbePropertiesRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetAllProbePropertiesResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbePropertyValueRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbePropertyValueResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetTableOfProbePropertiesRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetTableOfProbePropertiesResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.ProbeOrTarget;
import com.vmturbo.common.protobuf.probe.ProbeDTO.ProbePropertyInfo;
import com.vmturbo.common.protobuf.probe.ProbeDTO.ProbePropertyNameValuePair;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateOneProbePropertyRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateProbePropertyTableRequest;
import com.vmturbo.topology.processor.probeproperties.ProbePropertiesTestBase;

/**
 * Tests for the functionality of {@link ProbeRpcService}, with respect to probe properties.
 */
public class ProbeRpcServiceProbePropertiesTest extends ProbePropertiesTestBase {
    // construct service
    private final ProbeRpcService service = new ProbeRpcService(probeStore, targetStore, keyValueStore);

    /**
     * Set up mock probes and targets.
     */
    @Override
    @Before
    public void setUp() {
        super.setUp();
    }

    /**
     * Cannot get probe-specific probe properties from a non-existent probe.
     */
    @Test
    public void testGetProbeSpecificPropertiesFromNonExistentProbe() throws Exception {
        service.getTableOfProbeProperties(
            GetTableOfProbePropertiesRequest.newBuilder()
                .setProbePropertyTable(
                    ProbeOrTarget.newBuilder().setProbeId(NON_EXISTENT_PROBE_ID).build())
                .build(),
            new StreamObserverForFailure<>(Status.NOT_FOUND));
    }

    /**
     * Cannot get target-specific probe properties from a non-existent probe.
     *
     * @throws Exception expected.
     */
    @Test
    public void testGetTargetSpecificPropertiesFromNonExistentProbe() throws Exception {
        service.getTableOfProbeProperties(
            GetTableOfProbePropertiesRequest.newBuilder()
                .setProbePropertyTable(
                    ProbeOrTarget.newBuilder()
                        .setProbeId(NON_EXISTENT_PROBE_ID)
                        .setTargetId(TARGET_ID_11)
                        .build())
                .build(),
            new StreamObserverForFailure<>(Status.NOT_FOUND));
    }

    /**
     * Cannot get target-specific probe properties from a non-existent target.
     */
    @Test
    public void testGetTargetSpecificPropertiesFromNonExistentTarget() {
        service.getTableOfProbeProperties(
            GetTableOfProbePropertiesRequest.newBuilder()
                .setProbePropertyTable(
                    ProbeOrTarget.newBuilder()
                        .setProbeId(PROBE_ID_1)
                        .setTargetId(NON_EXISTENT_TARGET_ID)
                        .build())
                .build(),
            new StreamObserverForFailure<>(Status.NOT_FOUND));
    }

    /**
     * Cannot get target-specific probe properties if the target is not discovered by the given probe.
     */
    @Test
    public void testGetTargetSpecificPropertiesFromNonMatching() {
        service.getTableOfProbeProperties(
            GetTableOfProbePropertiesRequest.newBuilder()
                .setProbePropertyTable(
                    ProbeOrTarget.newBuilder()
                        .setProbeId(PROBE_ID_1)
                        .setTargetId(TARGET_ID_2)
                        .build())
                .build(),
            new StreamObserverForFailure<>(Status.INVALID_ARGUMENT));
    }

    /**
     * Updating a target-specific probe property table should insert the appropriate properties in the
     * table of the specified target, replacing all previously existing properties.
     */
    @Test
    public void testPutTargetSpecificProperties() {
        // update probe properties under target11 with propertyMap1
        service.updateProbePropertyTable(
            UpdateProbePropertyTableRequest.newBuilder()
                .setProbePropertyTable(
                    ProbeOrTarget.newBuilder()
                        .setProbeId(PROBE_ID_1)
                        .setTargetId(TARGET_ID_11)
                        .build())
                .addAllNewProbeProperties(convertPropertyMapToProtobufMessage(probePropertyMap1))
                .build(),
            new StreamObserverForSuccess<>());

        // use get methods to observe that the probe properties are correctly updated
        checkMapInTarget11(probePropertyMap1, "C");

        // update probe properties under target11 with propertyMap2
        service.updateProbePropertyTable(
            UpdateProbePropertyTableRequest.newBuilder()
                .setProbePropertyTable(
                    ProbeOrTarget.newBuilder()
                        .setProbeId(PROBE_ID_1)
                        .setTargetId(TARGET_ID_11)
                        .build())
                .addAllNewProbeProperties(convertPropertyMapToProtobufMessage(probePropertyMap2))
                .build(),
            new StreamObserverForSuccess<>());

        // use get methods to observe that the probe properties are correctly updated
        checkMapInTarget11(probePropertyMap2, "B");
    }

    /**
     * Cannot update the probe property table of a non-existent target.
     */
    @Test
    public void testPutInNonExistentTarget() {
        service.updateProbePropertyTable(
            UpdateProbePropertyTableRequest.newBuilder()
                .setProbePropertyTable(
                    ProbeOrTarget.newBuilder()
                        .setProbeId(PROBE_ID_1)
                        .setTargetId(NON_EXISTENT_TARGET_ID)
                        .build())
                .addAllNewProbeProperties(convertPropertyMapToProtobufMessage(probePropertyMap1))
                .build(),
            new StreamObserverForFailure<>(Status.NOT_FOUND));
    }

    /**
     * Cannot update the probe property table of a target, if the target is not discovered by the
     * given probe.
     */
    @Test
    public void testPutInNonMatchingTarget() {
        service.updateProbePropertyTable(
            UpdateProbePropertyTableRequest.newBuilder()
                .setProbePropertyTable(
                    ProbeOrTarget.newBuilder()
                        .setProbeId(PROBE_ID_1)
                        .setTargetId(TARGET_ID_2)
                        .build())
                .addAllNewProbeProperties(convertPropertyMapToProtobufMessage(probePropertyMap1))
                .build(),
            new StreamObserverForFailure<>(Status.INVALID_ARGUMENT));
    }

    /**
     * This checks the methods that modify a specific probe property
     * {@link ProbeRpcService#updateOneProbeProperty(UpdateOneProbePropertyRequest, StreamObserver)} and
     * {@link ProbeRpcService#deleteProbeProperty(DeleteProbePropertyRequest, StreamObserver)}.
     */
    @Test
    public void testSpecificProbePropertyMethods() {
        final ProbeOrTarget
            table = ProbeOrTarget.newBuilder().setProbeId(PROBE_ID_1).setTargetId(TARGET_ID_11).build();

        // create and check property map 1
        service.updateOneProbeProperty(
            UpdateOneProbePropertyRequest.newBuilder()
                .setNewProbeProperty(
                    ProbePropertyInfo.newBuilder()
                        .setProbePropertyTable(table)
                        .setProbePropertyNameAndValue(
                            ProbePropertyNameValuePair.newBuilder()
                                .setName("A")
                                .setValue("Avalue")
                                .build())
                        .build())
                .build(),
            new StreamObserverForSuccess<>());
        service.updateOneProbeProperty(
            UpdateOneProbePropertyRequest.newBuilder()
                .setNewProbeProperty(
                    ProbePropertyInfo.newBuilder()
                        .setProbePropertyTable(table)
                        .setProbePropertyNameAndValue(
                            ProbePropertyNameValuePair.newBuilder()
                                .setName("B")
                                .setValue("Bvalue")
                                .build())
                        .build())
                .build(),
            new StreamObserverForSuccess<>());
        checkMapInTarget11(probePropertyMap1, "C");

        // modify to create property map 2 and check again
        service.deleteProbeProperty(
            DeleteProbePropertyRequest.newBuilder().setProbePropertyTable(table).setName("B").build(),
            new StreamObserverForSuccess<>());
        service.updateOneProbeProperty(
            UpdateOneProbePropertyRequest.newBuilder()
                .setNewProbeProperty(
                    ProbePropertyInfo.newBuilder()
                        .setProbePropertyTable(table)
                        .setProbePropertyNameAndValue(
                            ProbePropertyNameValuePair.newBuilder()
                                .setName("C")
                                .setValue("Cvalue")
                                .build())
                            .build())
                .build(),
            new StreamObserverForSuccess<>());
        service.updateOneProbeProperty(
            UpdateOneProbePropertyRequest.newBuilder()
                .setNewProbeProperty(
                    ProbePropertyInfo.newBuilder()
                        .setProbePropertyTable(table)
                        .setProbePropertyNameAndValue(
                            ProbePropertyNameValuePair.newBuilder()
                                .setName("A")
                                .setValue("Avalue1")
                                .build())
                        .build())
                .build(),
            new StreamObserverForSuccess<>());
        checkMapInTarget11(probePropertyMap2, "B");
    }

    /**
     * Cannot delete a non-existent probe property.
     */
    @Test
    public void testDeleteNonExistentProbeProperty() throws Exception {
        service.deleteProbeProperty(
            DeleteProbePropertyRequest.newBuilder()
                .setProbePropertyTable(ProbeOrTarget.newBuilder().setProbeId(PROBE_ID_1).build())
                .setName("B")
                .build(),
            new StreamObserverForFailure<>(Status.NOT_FOUND));
    }

    private List<ProbePropertyNameValuePair> convertPropertyMapToProtobufMessage(
            Map<String, String> probeProperties) {
        final List<ProbePropertyNameValuePair> result = new ArrayList<>();
        for (Entry<String, String> e : probeProperties.entrySet()) {
            result.add(
                ProbePropertyNameValuePair.newBuilder()
                    .setName(e.getKey())
                    .setValue(e.getValue())
                    .build());
        }
        return result;
    }

    /**
     * Checks that all probe properties of a given map are in a target-specific probe property table
     * of target11 and that no other table has any probe properties.  All possible getter methods of
     * {@link ProbeRpcService} are exercised.
     *
     * @param propertiesExpected the probe property names and values that are expected.
     * @param notInMap a specific string that is a key in {@code probePropertiesExpected} map.
     */
    private void checkMapInTarget11(Map<String, String> propertiesExpected, String notInMap) {
        // properties can be retrieved by looking at target11
        final StreamObserverForSuccess<GetTableOfProbePropertiesResponse>
            response1 = new StreamObserverForSuccess<>();
        service.getTableOfProbeProperties(
            GetTableOfProbePropertiesRequest.newBuilder()
                .setProbePropertyTable(
                    ProbeOrTarget.newBuilder()
                        .setProbeId(PROBE_ID_1)
                        .setTargetId(TARGET_ID_11)
                        .build())
                .build(),
            response1);
        final List<ProbePropertyNameValuePair>
            properties1 = response1.getResponse().getProbePropertiesList();
        Assert.assertEquals(propertiesExpected.size(), properties1.size());
        for (ProbePropertyNameValuePair nameValuePair : properties1) {
            Assert.assertEquals(
                propertiesExpected.get(nameValuePair.getName()),
                nameValuePair.getValue());
        }

        // properties can be retrieved individually
        for (Entry<String, String> e : propertiesExpected.entrySet()) {
            final StreamObserverForSuccess<GetProbePropertyValueResponse>
                response2 = new StreamObserverForSuccess<>();
            service.getProbePropertyValue(
                GetProbePropertyValueRequest.newBuilder()
                    .setProbePropertyTable(
                        ProbeOrTarget.newBuilder()
                            .setProbeId(PROBE_ID_1)
                            .setTargetId(TARGET_ID_11)
                            .build())
                    .setName(e.getKey())
                    .build(),
                response2);
            Assert.assertEquals(e.getValue(), response2.getResponse().getValue());
        }

        // if a property name is not in the map, then inquiring for that property
        // should return an empty string
        final StreamObserverForSuccess<GetProbePropertyValueResponse>
            response3 = new StreamObserverForSuccess<>();
        service.getProbePropertyValue(
            GetProbePropertyValueRequest.newBuilder()
                .setProbePropertyTable(
                    ProbeOrTarget.newBuilder()
                        .setProbeId(PROBE_ID_1)
                        .setTargetId(TARGET_ID_11)
                        .build())
                .setName(notInMap)
                .build(),
            response3);
        Assert.assertEquals("", response3.getResponse().getValue());

        // other probe property tables should be empty
        final StreamObserverForSuccess<GetTableOfProbePropertiesResponse>
            response4 = new StreamObserverForSuccess<>();
        service.getTableOfProbeProperties(
            GetTableOfProbePropertiesRequest.newBuilder()
                .setProbePropertyTable(
                    ProbeOrTarget.newBuilder()
                        .setProbeId(PROBE_ID_1)
                        .setTargetId(TARGET_ID_12)
                        .build())
                .build(),
            response4);
        Assert.assertEquals(0, response4.getResponse().getProbePropertiesCount());
        final StreamObserverForSuccess<GetTableOfProbePropertiesResponse>
            response5 = new StreamObserverForSuccess<>();
        service.getTableOfProbeProperties(
            GetTableOfProbePropertiesRequest.newBuilder()
                .setProbePropertyTable(
                    ProbeOrTarget.newBuilder()
                        .setProbeId(PROBE_ID_2)
                        .setTargetId(TARGET_ID_2)
                        .build())
                .build(),
            response5);
        Assert.assertEquals(0, response5.getResponse().getProbePropertiesCount());
        final StreamObserverForSuccess<GetTableOfProbePropertiesResponse>
            response6 = new StreamObserverForSuccess<>();
        service.getTableOfProbeProperties(
            GetTableOfProbePropertiesRequest.newBuilder()
                .setProbePropertyTable(ProbeOrTarget.newBuilder().setProbeId(PROBE_ID_1).build())
                .build(),
            response6);
        Assert.assertEquals(0, response6.getResponse().getProbePropertiesCount());
        final StreamObserverForSuccess<GetTableOfProbePropertiesResponse>
            response7 = new StreamObserverForSuccess<>();
        service.getTableOfProbeProperties(
            GetTableOfProbePropertiesRequest.newBuilder()
                .setProbePropertyTable(ProbeOrTarget.newBuilder().setProbeId(PROBE_ID_2).build())
                .build(),
            response7);
        Assert.assertEquals(0, response7.getResponse().getProbePropertiesCount());
        final StreamObserverForSuccess<GetProbePropertyValueResponse>
            response8 = new StreamObserverForSuccess<>();
        service.getProbePropertyValue(
            GetProbePropertyValueRequest.newBuilder()
                    .setProbePropertyTable(
                        ProbeOrTarget.newBuilder()
                            .setProbeId(PROBE_ID_1)
                            .setTargetId(TARGET_ID_12)
                            .build())
                    .setName("A")
                    .build(),
            response8);
        Assert.assertEquals("", response8.getResponse().getValue());

        // fetching all properties at once reflects the same situation
        final StreamObserverForSuccess<GetAllProbePropertiesResponse>
            response9 = new StreamObserverForSuccess<>();
        service.getAllProbeProperties(GetAllProbePropertiesRequest.newBuilder().build(), response9);
        final List<ProbePropertyInfo> properties9 = response9.getResponse().getProbePropertiesList();
        Assert.assertEquals(propertiesExpected.size(), properties9.size());
        for (ProbePropertyInfo info : properties9) {
            Assert.assertEquals(
                propertiesExpected.get(info.getProbePropertyNameAndValue().getName()),
                info.getProbePropertyNameAndValue().getValue());
            Assert.assertEquals(PROBE_ID_1, info.getProbePropertyTable().getProbeId());
            Assert.assertEquals(TARGET_ID_11, info.getProbePropertyTable().getTargetId());
        }
    }

    /**
     * A stream observer that expects failure.
     *
     * @param <T> type for the stream observer response.
     */
    private static class StreamObserverForFailure<T> implements StreamObserver<T> {
        final private Status expectedFailureStatus;

        /**
         * Construct the stream observer and specify the failure status.
         *
         * @param expectedFailureStatus expected failure status.
         */
        public StreamObserverForFailure(Status expectedFailureStatus) {
            this.expectedFailureStatus = expectedFailureStatus;
        }

        @Override
        public void onNext(T t) {
            Assert.fail();
        }

        @Override
        public void onError(Throwable throwable) {
            Assert.assertEquals(
                expectedFailureStatus.getCode(),
                ((StatusException)throwable).getStatus().getCode());
        }

        @Override
        public void onCompleted() {
            Assert.fail();
        }
    }

    /**
     * A stream observer that expects success and records the response.
     *
     * @param <T> type for the stream observer response.
     */
    private static class StreamObserverForSuccess<T> implements StreamObserver<T> {
        private T response;

        @Override
        public void onNext(T t) {
            response = t;
        }

        @Override
        public void onError(Throwable throwable) {
            Assert.fail();
        }

        @Override
        public void onCompleted() {}

        /**
         * Return the response.
         *
         * @return response.
         */
        public T getResponse() {
            return response;
        }
    }
}
