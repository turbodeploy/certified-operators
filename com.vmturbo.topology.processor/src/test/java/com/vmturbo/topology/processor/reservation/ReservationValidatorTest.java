package com.vmturbo.topology.processor.reservation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.Collections;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles.PolicyServiceMole;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationDTO.ConstraintInfoCollection;
import com.vmturbo.common.protobuf.plan.ReservationDTO.Reservation;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ReservationConstraintInfo.Type;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.topology.processor.reservation.ReservationValidator.ValidationErrors;

/**
 * Unit tests for {@link ReservationValidator}.
 */
public class ReservationValidatorTest {

    private GroupServiceMole groupBackend = spy(GroupServiceMole.class);

    private PolicyServiceMole policyBackend = spy(PolicyServiceMole.class);

    /**
     * Used to mock out gRPC dependencies.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(groupBackend, policyBackend);

    private ReservationValidator reservationValidator;

    /**
     * Common code before every test.
     */
    @Before
    public void setup() {
        reservationValidator = new ReservationValidator(
            GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            PolicyServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));
    }

    private static final long RESERVATION_ID = 7L;

    private Reservation makeReservation(@Nonnull final Type constraintType,
                                        final long constraintId) {
        ReservationConstraintInfo constraint = ReservationConstraintInfo.newBuilder()
            .setType(constraintType)
            .setConstraintId(constraintId)
            .build();
        return Reservation.newBuilder()
            .setId(RESERVATION_ID)
            .setConstraintInfoCollection(ConstraintInfoCollection.newBuilder()
                .addReservationConstraintInfo(constraint))
            .build();
    }

    /**
     * Invalid cluster constraint.
     */
    @Test
    public void testInvalidCluster() {
        final long clusterId = 1;
        Reservation invalidCluster = makeReservation(Type.CLUSTER, clusterId);
        final ValidationErrors errors = reservationValidator.validateReservations(Stream.of(invalidCluster), e -> false);
        assertFalse(errors.isEmpty());
        assertThat(errors.getErrorsByReservation().get(RESERVATION_ID).get(0), containsString("cluster"));
    }

    /**
     * Valid cluster constraint.
     */
    @Test
    public void testValidCluster() {
        final long clusterId = 1;
        doReturn(Collections.singletonList(Grouping.newBuilder()
            .setId(clusterId)
            .build())).when(groupBackend).getGroups(GetGroupsRequest.newBuilder()
                .setGroupFilter(GroupFilter.newBuilder()
                    .setIncludeHidden(true)
                    .addId(clusterId))
                .build());
        Reservation validCluster = makeReservation(Type.CLUSTER, clusterId);
        final ValidationErrors errors = reservationValidator.validateReservations(Stream.of(validCluster), e -> false);
        assertTrue(errors.isEmpty());
    }

    /**
     * Invalid network constraint.
     */
    @Test
    public void testInvalidNetwork() {
        final long networkId = 1;
        Reservation invalidNetwork = makeReservation(Type.NETWORK, networkId);
        final ValidationErrors errors = reservationValidator.validateReservations(Stream.of(invalidNetwork), e -> false);
        assertFalse(errors.isEmpty());
        assertThat(errors.getErrorsByReservation().get(RESERVATION_ID).get(0), containsString("NETWORK"));
    }

    /**
     * Valid network constraint.
     */
    @Test
    public void testValidNetwork() {
        final long networkId = 1;
        Reservation validNetwork = makeReservation(Type.NETWORK, networkId);
        final ValidationErrors errors = reservationValidator.validateReservations(Stream.of(validNetwork), e -> true);
        assertTrue(errors.isEmpty());
    }

    /**
     * Invalid datacenter constraint.
     */
    @Test
    public void testInvalidDc() {
        final long dcId = 1;
        Reservation invalidDc = makeReservation(Type.DATA_CENTER, dcId);
        final ValidationErrors errors = reservationValidator.validateReservations(Stream.of(invalidDc), e -> false);
        assertFalse(errors.isEmpty());
        assertThat(errors.getErrorsByReservation().get(RESERVATION_ID).get(0), containsString("DATACENTER"));
    }

    /**
     * Valid datacenter constraint.
     */
    @Test
    public void testValidDc() {
        final long dcId = 1;
        Reservation validDc = makeReservation(Type.NETWORK, dcId);
        final ValidationErrors errors = reservationValidator.validateReservations(Stream.of(validDc), e -> true);
        assertTrue(errors.isEmpty());
    }

    /**
     * Invalid virtual datacenter constraint.
     */
    @Test
    public void testInvalidVdc() {
        final long vdcId = 1;
        Reservation invalidVdc = makeReservation(Type.VIRTUAL_DATA_CENTER, vdcId);
        final ValidationErrors errors = reservationValidator.validateReservations(Stream.of(invalidVdc), e -> false);
        assertFalse(errors.isEmpty());
        assertThat(errors.getErrorsByReservation().get(RESERVATION_ID).get(0), containsString("VIRTUAL_DATACENTER"));
    }

    /**
     * Valid virtual datacenter constraint.
     */
    @Test
    public void testValidVdc() {
        final long vdcId = 1;
        Reservation validVdc = makeReservation(Type.NETWORK, vdcId);
        final ValidationErrors errors = reservationValidator.validateReservations(Stream.of(validVdc), e -> true);
        assertTrue(errors.isEmpty());
    }

    /**
     * Invalid policy constraint.
     */
    @Test
    public void testInvalidPolicy() {
        final long policyId = 1;
        Reservation invalidPolicy = makeReservation(Type.POLICY, policyId);
        final ValidationErrors errors = reservationValidator.validateReservations(Stream.of(invalidPolicy), e -> false);
        assertFalse(errors.isEmpty());
        assertThat(errors.getErrorsByReservation().get(RESERVATION_ID).get(0), containsString("policy"));
    }

    /**
     * Valid policy constraint.
     */
    @Test
    public void testValidPolicy() {
        final long policyId = 1;
        Policy policy = Policy.newBuilder()
            .setId(policyId)
            .build();
        doReturn(Collections.singletonList(PolicyResponse.newBuilder()
            .setPolicy(policy)
            .build())).when(policyBackend).getPolicies(any());

        Reservation validPolicy = makeReservation(Type.POLICY, policyId);
        final ValidationErrors errors = reservationValidator.validateReservations(Stream.of(validPolicy), e -> false);
        assertTrue(errors.isEmpty());
    }
}