package com.vmturbo.plan.orchestrator.scenario;

import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.DeleteScenarioResponse;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetScenariosOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.UpdateScenarioResponse;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc.ScenarioServiceImplBase;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.plan.orchestrator.db.tables.pojos.Scenario;

/**
 * Implements CRUD functionality for scenarios.
 */
public class ScenarioRpcService extends ScenarioServiceImplBase {
    private final Logger logger = LogManager.getLogger();
    private final ScenarioDao scenarioDao;
    private final UserSessionContext userSessionContext;
    private final GroupServiceBlockingStub groupServiceStub;
    private final ScenarioScopeAccessChecker scenarioScopeAccessChecker;

    public ScenarioRpcService(@Nonnull final ScenarioDao scenarioDao,
                              @Nonnull final IdentityInitializer identityInitializer,
                              @Nonnull final UserSessionContext userSessionContext,
                              @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub,
                              @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub) {
        this.scenarioDao = scenarioDao;
        Objects.requireNonNull(identityInitializer); // Ensure identity generator is initialized
        this.userSessionContext = userSessionContext;
        this.groupServiceStub = groupServiceBlockingStub;
        this.scenarioScopeAccessChecker = new ScenarioScopeAccessChecker(userSessionContext,
            groupServiceStub, searchServiceBlockingStub);
    }

    @Override
    public void createScenario(ScenarioInfo info, StreamObserver<PlanDTO.Scenario> responseObserver) {
        LocalDateTime curTime = LocalDateTime.now();

        // if the user is scoped, we need to check to make sure they have access to the plan scope.
        // if the plan is NOT scoped (e.g. is a "market" plan), we will scope it to the user's
        // scope groups. Otherwise, we'll make sure they have access to the plan scope.
        if (userSessionContext.isUserScoped() && !info.hasScope()) {
            // set the scenario scope to the user's scope groups
            Iterator<Group> groups = groupServiceStub.getGroups(GetGroupsRequest.newBuilder()
                    .addAllId(userSessionContext.getUserAccessScope().getScopeGroupIds())
                    .build());
            PlanScope.Builder planScopeBuilder = PlanScope.newBuilder();
            groups.forEachRemaining(group -> {
                planScopeBuilder.addScopeEntries(PlanScopeEntry.newBuilder()
                        .setScopeObjectOid(group.getId())
                        .setClassName("Group")
                        .setDisplayName(GroupProtoUtil.getGroupDisplayName(group))
                        .build());
            });
            logger.info("Setting plan scope to {} groups in user scope.",
                    planScopeBuilder.getScopeEntriesCount());
            info = info.toBuilder()
                    .setScope(planScopeBuilder)
                    .build();
        } else {
            // validate that the user has access to all of the scope entries, and verify that all
            // the scope entries exist in the system, if any of them do not exist, then the whole
            // scenario scope is invalid, and we should prevent scenario from being created
            try {
                scenarioScopeAccessChecker.checkScenarioAccessAndValidateScopes(info);
            } catch (ScenarioScopeNotFoundException e) {
                logger.error(e.getMessage());
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(
                    e.getMessage()).asException());
                return;
            } catch (UserAccessScopeException e) {
                logger.error(e.getMessage());
                responseObserver.onError(Status.PERMISSION_DENIED.withDescription(
                    e.getMessage()).asException());
                return;
            }
        }

        Scenario scenario = new Scenario(IdentityGenerator.next(), curTime, curTime, info);
        scenarioDao.createScenario(scenario);

        responseObserver.onNext(ScenarioDao.toScenarioDTO(scenario));
        responseObserver.onCompleted();
    }

    @Override
    public void updateScenario(PlanDTO.UpdateScenarioRequest request,
                               StreamObserver<PlanDTO.UpdateScenarioResponse> responseObserver) {

            Optional<PlanDTO.Scenario> scenario;
            if (request.hasNewInfo()) {
                int rowsUpdated = scenarioDao.updateScenario(request.getNewInfo(), request.getScenarioId());
                // On successful update, return the updated scenario object. No
                // need to do another DB get().
                if (rowsUpdated == 1) {
                    scenario = Optional.of(PlanDTO.Scenario.newBuilder()
                                    .setId(request.getScenarioId())
                                    .setScenarioInfo(request.getNewInfo())
                                    .build());
                } else {
                    // If > 1 row updated, something seriously wrong.
                    if (rowsUpdated > 1) {
                        logger.warn("More than one record updated for {}", request.getScenarioId());
                    }
                    scenario = Optional.empty();
                }
            } else {
                scenario = scenarioDao.getScenario(request.getScenarioId());
            }

            if (scenario.isPresent()) {
                responseObserver.onNext(UpdateScenarioResponse.newBuilder()
                        .setScenario(scenario.get())
                        .build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(
                    Status.NOT_FOUND
                        .withDescription(Long.toString(request.getScenarioId()))
                        .asException());
            }
    }

    @Override
    public void deleteScenario(PlanDTO.ScenarioId request,
                               StreamObserver<PlanDTO.DeleteScenarioResponse> responseObserver) {

        scenarioDao.deleteScenario(request.getScenarioId());
        responseObserver.onNext(DeleteScenarioResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getScenario(PlanDTO.ScenarioId request,
                            StreamObserver<PlanDTO.Scenario> responseObserver) {
        Optional<PlanDTO.Scenario> scenario =
            scenarioDao.getScenario(request.getScenarioId());
        if (scenario.isPresent()) {
            responseObserver.onNext(scenario.get());
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(Long.toString(request.getScenarioId()))
                    .asException());
        }
    }

    @Override
    public void getScenarios(GetScenariosOptions request,
                             StreamObserver<PlanDTO.Scenario> responseObserver) {

        scenarioDao.getScenarios()
            .stream()
            .map(ScenarioDao::toScenarioDTO)
            .forEach(responseObserver::onNext);

        responseObserver.onCompleted();
    }

    private PlanDTO.Scenario emptyScenarioDTO(final long scenarioId) {
        return PlanDTO.Scenario.newBuilder()
            .setId(scenarioId)
            .build();
    }
}
