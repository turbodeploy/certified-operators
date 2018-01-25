package com.vmturbo.plan.orchestrator.scenario;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.DeleteScenarioResponse;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetScenariosOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.UpdateScenarioResponse;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc.ScenarioServiceImplBase;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.plan.orchestrator.db.tables.pojos.Scenario;

/**
 * Implements CRUD functionality for scenarios.
 */
public class ScenarioRpcService extends ScenarioServiceImplBase {

    private final Logger logger = LogManager.getLogger();
    private final ScenarioDao scenarioDao;

    public ScenarioRpcService(@Nonnull final ScenarioDao scenarioDao,
                              @Nonnull final IdentityInitializer identityInitializer) {
        this.scenarioDao = scenarioDao;
        Objects.requireNonNull(identityInitializer); // Ensure identity generator is initialized
    }

    @Override
    public void createScenario(ScenarioInfo info, StreamObserver<PlanDTO.Scenario> responseObserver) {
        LocalDateTime curTime = LocalDateTime.now();

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
