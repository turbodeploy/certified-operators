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
import org.mockito.Mockito;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.component.external.api.util.ApiUtilsTest;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider.Builder;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test the construction of {@link ActionApiDTO} with compound moves.
 */
public class CompoundMoveTest {

    private ActionSpecMapper mapper;

    private PolicyDTOMoles.PolicyServiceMole policyMole;

    private RepositoryApi repositoryApi;

    private GrpcTestServer grpcServer;

    private PolicyServiceGrpc.PolicyServiceBlockingStub policyService;

    private static final int VM = EntityType.VIRTUAL_MACHINE_VALUE;
    private static final int PM = EntityType.PHYSICAL_MACHINE_VALUE;
    private static final int ST = EntityType.STORAGE_VALUE;

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
    private static final long VOL1_ID = 40;
    private static final String VOL1_NAME = "vol-1";

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
                        .newCachedThreadPool(new ThreadFactoryBuilder().build()), 777777L);
        IdentityGenerator.initPrefix(0);
        Mockito.when(repositoryApi.getServiceEntitiesById(any()))
            .thenReturn(oidToEntityMap(
                    entityApiDTO(TARGET_NAME, TARGET_ID, UIEntityType.VIRTUAL_MACHINE.getValue()),
                    entityApiDTO(PM1_NAME, PM1_ID, UIEntityType.PHYSICAL_MACHINE.getValue()),
                    entityApiDTO(PM2_NAME, PM2_ID, UIEntityType.PHYSICAL_MACHINE.getValue()),
                    entityApiDTO(ST1_NAME, ST1_ID, UIEntityType.STORAGE.getValue()),
                    entityApiDTO(ST2_NAME, ST2_ID, UIEntityType.STORAGE.getValue()),
                    entityApiDTO(VOL1_NAME, VOL1_ID, UIEntityType.VIRTUAL_VOLUME.getValue())));
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
        ActionDTO.Action moveStorage = makeAction(TARGET_ID, VM, ST1_ID, ST, ST2_ID, ST, null);
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveStorage), 77L);
        assertSame(ActionType.CHANGE, apiDto.getActionType());
        assertEquals(1, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(ST1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(ST2_ID), apiDto.getNewValue());
        assertEquals(details("Virtual Machine", TARGET_NAME, ST1_NAME, ST2_NAME),
            apiDto.getDetails());
    }

    /**
     * Test a simple move with one provider being changed. This change is for a particular
     * resource of the target.
     *
     * @throws UnknownObjectException not supposed to happen
     * @throws UnsupportedActionException  not supposed to happen
     */
    @Test
    public void testSimpleActionWithResource()
            throws UnknownObjectException, UnsupportedActionException, ExecutionException,
            InterruptedException {
        ActionDTO.Action moveVolume = makeAction(TARGET_ID, VM, ST1_ID, ST, ST2_ID, ST, VOL1_ID);
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveVolume), 77L);
        assertSame(ActionType.CHANGE, apiDto.getActionType());
        assertEquals(1, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(ST1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(ST2_ID), apiDto.getNewValue());
        assertEquals(moveDetailsWithResource("Virtual Volume", VOL1_NAME, "Virtual Machine",
                TARGET_NAME, ST1_NAME, ST2_NAME),
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
        ActionDTO.Action moveBoth1 = makeAction(TARGET_ID, VM, ST1_ID, ST, ST2_ID, ST, PM1_ID, PM, PM2_ID, PM);
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveBoth1), 77L);
        assertSame(ActionType.MOVE, apiDto.getActionType());
        assertEquals(2, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(PM1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(PM2_ID), apiDto.getNewValue());
        assertEquals(details("Virtual Machine", TARGET_NAME, PM1_NAME, PM2_NAME),
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
        ActionDTO.Action moveBoth2 = makeAction(TARGET_ID, VM,
            PM1_ID, PM, PM2_ID, PM, ST1_ID, ST, ST2_ID, ST); // different order
        ActionApiDTO apiDto = mapper.mapActionSpecToActionApiDTO(buildActionSpec(moveBoth2), 77L);
        assertSame(ActionType.MOVE, apiDto.getActionType());
        assertEquals(2, apiDto.getCompoundActions().size());
        assertEquals(String.valueOf(PM1_ID), apiDto.getCurrentValue());
        assertEquals(String.valueOf(PM2_ID), apiDto.getNewValue());
        assertEquals(details("Virtual Machine", TARGET_NAME, PM1_NAME, PM2_NAME),
            apiDto.getDetails());
    }

    private static String details(String targetType, String targetName, String providerName1,
                                  String providerName2) {
        return "Move " + targetType + " "
                        + targetName
                        + " from "
                        + providerName1
                        + " to "
                        + providerName2;
    }

    private static String moveDetailsWithResource(String resourceType, String resourceName,
                                                  String targetType, String targetName,
                                                  String providerName1, String providerName2) {
        return "Move " + resourceType + " "
                + resourceName + " of " + targetType + " " + targetName
                + " from "
                + providerName1
                + " to "
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

    private static ActionDTO.Action makeAction(long t, int tType,
                                               long s1, int s1Type,
                                               long d1, int d1Type,
                                               Long r1) {
        return genericActionStuff()
                        .setInfo(ActionInfo.newBuilder()
                            .setMove(Move.newBuilder()
                                .setTarget(ApiUtilsTest.createActionEntity(t, tType))
                                .addChanges(makeChange(s1, s1Type, d1, d1Type, r1))
                                .build())
                            .build())
                        .build();
    }

    private static ActionDTO.Action makeAction(long t, int tType,
                                               long s1, int s1Type,
                                               long d1, int d1Type,
                                               long s2, int s2Type,
                                               long d2, int d2Type) {
        return genericActionStuff()
                        .setInfo(ActionInfo.newBuilder()
                            .setMove(Move.newBuilder()
                                .setTarget(ApiUtilsTest.createActionEntity(t, tType))
                                .addChanges(makeChange(s1, s1Type, d1, d1Type, null))
                                .addChanges(makeChange(s2, s2Type, d2, d2Type, null))
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

    private static ChangeProvider makeChange(long source, int sourceType,
                                             long destination, int destType,
                                             Long resource) {
        Builder changeProviderBuilder = ChangeProvider.newBuilder()
                        .setSource(ApiUtilsTest.createActionEntity(source, sourceType))
                        .setDestination(ApiUtilsTest.createActionEntity(destination, destType));
        if (resource != null) {
            changeProviderBuilder.setResource(ApiUtilsTest.createActionEntity(resource));
        }
        return changeProviderBuilder.build();
    }
}
