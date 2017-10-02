package com.vmturbo.plan.orchestrator.scenario;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

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
import com.vmturbo.plan.orchestrator.db.tables.records.ScenarioRecord;

import static com.vmturbo.plan.orchestrator.db.tables.Scenario.SCENARIO;

/**
 * Implements CRUD functionality for scenarios.
 */
public class ScenarioRpcService extends ScenarioServiceImplBase {
    private final Logger logger = LogManager.getLogger();
    private final DSLContext dsl;

    public ScenarioRpcService(@Nonnull final DSLContext dsl,
                              @Nonnull final IdentityInitializer identityInitializer) {
        this.dsl = Objects.requireNonNull(dsl);
        Objects.requireNonNull(identityInitializer); // Ensure identity generator is initialized
    }

    @Override
    public void createScenario(ScenarioInfo info, StreamObserver<PlanDTO.Scenario> responseObserver) {
        LocalDateTime curTime = LocalDateTime.now();

        Scenario scenario = new Scenario(IdentityGenerator.next(), curTime, curTime, info);
        dsl.newRecord(SCENARIO, scenario).store();

        responseObserver.onNext(
            PlanDTO.Scenario.newBuilder()
                .setId(scenario.getId())
                .setScenarioInfo(info)
                .build());
        responseObserver.onCompleted();
    }

    private Optional<PlanDTO.Scenario> getScenario(DSLContext context, final long scenarioId) {
        Optional<ScenarioRecord> loadedScenario = Optional.ofNullable(
                context.selectFrom(SCENARIO)
                        .where(SCENARIO.ID.eq(scenarioId))
                        .fetchAny());
        return loadedScenario
                .map(record -> toScenarioDTO(record.into(Scenario.class)));
    }

    @Override
    public void updateScenario(PlanDTO.UpdateScenarioRequest request,
                               StreamObserver<PlanDTO.UpdateScenarioResponse> responseObserver) {
        dsl.transaction(configuration -> {
            if (request.hasNewInfo()) {
                DSL.using(configuration)
                        .update(SCENARIO)
                        .set(SCENARIO.SCENARIO_INFO, request.getNewInfo())
                        .where(SCENARIO.ID.eq(request.getScenarioId()))
                        .execute();
            }

            Optional<PlanDTO.Scenario> scenario =
                    getScenario(DSL.using(configuration), request.getScenarioId());
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
        });
    }

    @Override
    public void deleteScenario(PlanDTO.ScenarioId request,
                               StreamObserver<PlanDTO.DeleteScenarioResponse> responseObserver) {
        dsl.transaction(configuration -> {
            Optional<PlanDTO.Scenario> scenario =
                    getScenario(DSL.using(configuration), request.getScenarioId());
            // TODO: implement referrential integrity between PlanInstances and Scenarios
            if (scenario.isPresent()) {
                // If this throws an exception it should be a runtime
                // exeption, which will propagate out of the lambda.
                DSL.using(configuration)
                        .delete(SCENARIO)
                        .where(SCENARIO.ID.equal(request.getScenarioId()))
                        .execute();
                responseObserver.onNext(DeleteScenarioResponse.newBuilder()
                    .setScenario(scenario.get())
                    .build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(
                    Status.NOT_FOUND
                        .withDescription(Long.toString(request.getScenarioId()))
                        .asException());
            }
        });
    }

    @Override
    public void getScenario(PlanDTO.ScenarioId request,
                            StreamObserver<PlanDTO.Scenario> responseObserver) {
        Optional<PlanDTO.Scenario> scenario = getScenario(dsl, request.getScenarioId());
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
        List<Scenario> scenarios = dsl
            .selectFrom(SCENARIO)
            .fetch()
            .into(Scenario.class);

        scenarios.stream()
            .map(this::toScenarioDTO)
            .forEach(responseObserver::onNext);

        responseObserver.onCompleted();
    }

    private PlanDTO.Scenario toScenarioDTO(@Nonnull final Scenario scenario) {
        return PlanDTO.Scenario.newBuilder()
            .setId(scenario.getId())
            .setScenarioInfo(scenario.getScenarioInfo())
            .build();
    }

    private PlanDTO.Scenario emptyScenarioDTO(final long scenarioId) {
        return PlanDTO.Scenario.newBuilder()
            .setId(scenarioId)
            .build();
    }
}
