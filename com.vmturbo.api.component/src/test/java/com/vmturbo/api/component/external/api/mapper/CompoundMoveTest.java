package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.component.external.api.util.ApiUtilsTest;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Test the construction of {@link ActionApiDTO} with compound moves.
 */
public class CompoundMoveTest {

    private ActionSpecMapper mapper;

    private PolicyDTOMoles.PolicyServiceMole policyMole;

    private RepositoryApi repositoryApi;

    private GrpcTestServer grpcServer;

    private PolicyServiceGrpc.PolicyServiceBlockingStub policyService;

    private static final long TARGET_ID = 10;
    private static final String TARGET_NAME = "vm-1";
    private static final long ST1_ID = 20;
    private static final String ST1_NAME = "storage-1";
    private static final long ST2_ID = 21;
    private static final String ST2_NAME = "storage-2";
    private static final long PM1_ID = 30;
    private static final String PM1_NAME = "host-1";
    private static final long PM2_ID = 31;
    private static final String PM2_NAME = "host-2";

    @Before
    public void setup() throws IOException {
        policyMole = Mockito.spy(PolicyDTOMoles.PolicyServiceMole.class);
        final List<PolicyResponse> policyResponses = ImmutableList.of(
            PolicyResponse.newBuilder().setPolicy(Policy.newBuilder()
                .setId(1)
                .setPolicyInfo(PolicyInfo.newBuilder()
                    .setName("policy")))
                .build());
        Mockito.when(policyMole.getAllPolicies(Mockito.any())).thenReturn(policyResponses);
        grpcServer = GrpcTestServer.newServer(policyMole);
        grpcServer.start();
        policyService = PolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        repositoryApi = Mockito.mock(RepositoryApi.class);
        mapper = new ActionSpecMapper(repositoryApi, policyService, Executors
                        .newCachedThreadPool(new ThreadFactoryBuilder().build()));
        IdentityGenerator.initPrefix(0);
        Mockito.when(repositoryApi.getServiceEntitiesById(any()))
            .thenReturn(oidToEntityMap(
                    entityApiDTO(TARGET_NAME, TARGET_ID, UIEntityType.VIRTUAL_MACHINE.getValue()),
                    entityApiDTO(PM1_NAME, PM1_ID, UIEntityType.PHYSICAL_MACHINE.getValue()),
                    entityApiDTO(PM2_NAME, PM2_ID, UIEntityType.PHYSICAL_MACHINE.getValue()),
                    entityApiDTO(ST1_NAME, ST1_ID, UIEntityType.STORAGE.getValue()),
                    entityApiDTO(ST2_NAME, ST2_ID, UIEntityType.STORAGE.getValue())));
    }

    private Map<Long, Optional<ServiceEntityApiDTO>> oidToEntityMap(
                    ServiceEntityApiDTO... dtos) {
        Map<Long, Optional<ServiceEntityApiDTO>> answer = new HashMap<>();
        for (ServiceEntityApiDTO dto : dtos) {
            answer.put(Long.valueOf(dto.getUuid()), Optional.of(dto));
        }
        return answer;
    }

    private ServiceEntityApiDTO entityApiDTO(@Nonnull final String displayName, long oid,
                    @Nonnull String className) {
        ServiceEntityApiDTO seDTO = new ServiceEntityApiDTO();
        seDTO.setDisplayName(displayName);
        seDTO.setUuid(Long.toString(oid));
        seDTO.setClassName(className);
        return seDTO;
    }

    /**
     * Test a compound move with one provider being changed.
     *
     * @throws UnknownObjectException not supposed to happen
     * @throws UnsupportedActionException  not supposed to happen
     */
    @Test
    public void testSimpleAction()
                    throws UnknownObjectException, UnsupportedActionException, ExecutionException,
                    InterruptedException {
        ActionDTO.Action moveStorage = makeAction(TARGET_ID, ST1_ID, ST2_ID);
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveStorage), 77L);
        assertSame(ActionType.CHANGE, apiDto.getActionType());
        assertEquals(1, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(ST1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(ST2_ID), apiDto.getNewValue());
        assertEquals(details("Virtual Machine", TARGET_NAME, "Storage", ST1_NAME, ST2_NAME),
            apiDto.getDetails());
    }

    /**
     * Test a compound move with two providers being changed, first
     * is Storage, second is PM.
     *
     * @throws UnknownObjectException not supposed to happen
     * @throws UnsupportedActionException  not supposed to happen
     */
    @Test
    public void testCompoundAction1()
                    throws UnknownObjectException, UnsupportedActionException, ExecutionException,
                    InterruptedException {
        ActionDTO.Action moveBoth1 = makeAction(TARGET_ID, ST1_ID, ST2_ID, PM1_ID, PM2_ID);
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveBoth1), 77L);
        assertSame(ActionType.MOVE, apiDto.getActionType());
        assertEquals(2, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(PM1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(PM2_ID), apiDto.getNewValue());
        assertEquals(details("Virtual Machine", TARGET_NAME, "Physical Machine", PM1_NAME, PM2_NAME),
            apiDto.getDetails());
    }

    /**
     * Test a compound move with two providers being changed, first
     * is PM, second is Storage.
     *
     * @throws UnknownObjectException not supposed to happen
     * @throws UnsupportedActionException  not supposed to happen
     */
    @Test
    public void testCompoundAction2()
                    throws UnknownObjectException, UnsupportedActionException, ExecutionException,
                    InterruptedException {
        ActionDTO.Action moveBoth2 = makeAction(TARGET_ID, PM1_ID, PM2_ID, ST1_ID, ST2_ID); // different order
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveBoth2), 77L);
        assertSame(ActionType.MOVE, apiDto.getActionType());
        assertEquals(2, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(PM1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(PM2_ID), apiDto.getNewValue());
        assertEquals(details("Virtual Machine", TARGET_NAME, "Physical Machine", PM1_NAME, PM2_NAME),
            apiDto.getDetails());
    }

    private static String details(String targetType, String targetName,
                    String providerType, String providerName1, String providerName2) {
        return "Move " + targetType + " "
                        + targetName
                        + " from " + providerType  + " "
                        + providerName1
                        + " to " + providerType + " "
                        + providerName2;
    }

    private ActionSpec buildActionSpec(ActionDTO.Action action) {
        return ActionSpec.newBuilder()
            .setRecommendationTime(System.currentTimeMillis())
            .setRecommendation(action)
            .setActionState(ActionState.READY)
            .setActionMode(ActionMode.MANUAL)
            .setIsExecutable(true)
            .setExplanation("default explanation")
            .build();
    }

    private static ActionDTO.Action makeAction(long t, long s1, long d1) {
        return genericActionStuff()
                        .setInfo(ActionInfo.newBuilder()
                            .setMove(Move.newBuilder()
                                .setTarget(ApiUtilsTest.createActionEntity(t))
                                .addChanges(makeChange(s1, d1))
                                .build())
                            .build())
                        .build();
    }

    private static ActionDTO.Action makeAction(long t, long s1, long d1, long s2, long d2) {
        return genericActionStuff()
                        .setInfo(ActionInfo.newBuilder()
                            .setMove(Move.newBuilder()
                                .setTarget(ApiUtilsTest.createActionEntity(t))
                                .addChanges(makeChange(s1, d1))
                                .addChanges(makeChange(s2, d2))
                                .build())
                            .build())
                        .build();
    }

    private static ActionDTO.Action.Builder genericActionStuff() {
        return ActionDTO.Action.newBuilder()
                        .setId(IdentityGenerator.next())
                        .setImportance(0)
                        .setExecutable(true)
                        .setSupportingLevel(SupportLevel.SUPPORTED)
                        .setExplanation(Explanation.newBuilder().build());
    }

    private static ChangeProvider makeChange(long source, long destination) {
        return ChangeProvider.newBuilder()
                        .setSource(ApiUtilsTest.createActionEntity(source))
                        .setDestination(ApiUtilsTest.createActionEntity(destination))
                        .build();
    }

}
