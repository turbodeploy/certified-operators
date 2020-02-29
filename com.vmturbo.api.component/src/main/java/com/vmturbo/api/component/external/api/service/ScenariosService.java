package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.validation.Errors;

import com.vmturbo.api.component.external.api.mapper.ScenarioMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.market.MarketApiDTO;
import com.vmturbo.api.dto.scenario.ScenarioApiDTO;
import com.vmturbo.api.enums.MergePolicyType;
import com.vmturbo.api.enums.PolicyType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IScenariosService;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.GetScenariosOptions;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioId;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.UpdateScenarioRequest;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.UpdateScenarioResponse;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc.ScenarioServiceBlockingStub;

/**
 * Service implementation of Scenarios
 **/
public class ScenariosService implements IScenariosService {
    private static final Logger logger = LogManager.getLogger();

    private final ScenarioServiceBlockingStub scenarioService;

    private final ScenarioMapper scenarioMapper;

    public ScenariosService(@Nonnull final ScenarioServiceBlockingStub scenarioServiceClient,
                            @Nonnull final ScenarioMapper scenarioMapper) {
        this.scenarioService = scenarioServiceClient;
        this.scenarioMapper = Objects.requireNonNull(scenarioMapper);
    }

    /**
     * Get a list of all scenarios.
     *
     * @return A list of all scenarios in the system.
     * @throws Exception
     */
    @Override
    public List<ScenarioApiDTO> getScenarios(Boolean showForAllUsers) throws Exception {
        final Iterator<Scenario> iterator =
                scenarioService.getScenarios(GetScenariosOptions.getDefaultInstance());
        final List<ScenarioApiDTO> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(scenarioMapper.toScenarioApiDTO(iterator.next()));
        }
        return result;
    }

    /**
     * This method actually gets scenarios by ID and not by Name.
     *
     * @param id The ID of the scenario to get.
     * @return The scenario with the corresponding ID.
     * @throws Exception
     */
    @Override
    public ScenarioApiDTO getScenario(Long id) throws Exception {
        try {
            final Scenario scenario = scenarioService.getScenario(ScenarioId.newBuilder()
                    .setScenarioId(id)
                    .build());
            return scenarioMapper.toScenarioApiDTO(scenario);
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                throw new UnknownObjectException(e.getStatus().getDescription());
            } else {
                throw e;
            }
        }
    }

    /**
     * Create a scenario with the given parameters.
     *
     * @param input The list of changes.
     * @return The created scenario.
     * @throws Exception
     */
    @Override
    public ScenarioApiDTO createScenario(ScenarioApiDTO input) throws Exception {
        String name = input.getDisplayName();
        Scenario scenario = scenarioService.createScenario(
            scenarioMapper.toScenarioInfo(name, input)
        );

        return scenarioMapper.toScenarioApiDTO(scenario);
    }

   /**
    * This method is unused by the UI at the moment.
    *
    * @param id id
    * @param name name
    * @param scopeList scopeList
    * @param periods periods
    * @param addHist addHist
    * @param includeRes includeRes
    * @param time time
    * @param center center
    * @param diameter diameter
    * @param hostProvision hostProvision
    * @param hostSuspension hostSuspension
    * @param dsProvision dsProvision
    * @param dsSuspension dsSuspension
    * @param resize resize
    * @param input input
    * @return ScenarioApiDTO
    * @throws Exception
    */
    @Override
    public ScenarioApiDTO configureScenario(Long id, String name, List<String> scopeList,
                                            List<Integer> periods, Boolean addHist,
                                            Boolean includeRes, String time, Float center,
                                            Float diameter, Boolean hostProvision,
                                            Boolean hostSuspension, Boolean dsProvision,
                                            Boolean dsSuspension, Boolean resize,
                                            ScenarioApiDTO input) throws Exception {
        try {
            final UpdateScenarioResponse scenarioResponse = scenarioService.updateScenario(
                    UpdateScenarioRequest.newBuilder()
                            .setScenarioId(id)
                            .setNewInfo(scenarioMapper.toScenarioInfo(name, input))
                            .build());
            return scenarioMapper.toScenarioApiDTO(scenarioResponse.getScenario());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                throw new UnknownObjectException(e.getStatus().getDescription());
            } else {
                throw e;
            }
        }
    }

    /**
     * Delete an existing scenario.
     * @throws Exception
     */
    @Override
    public Boolean deleteScenario(Long id) throws Exception {
        try {
            scenarioService.deleteScenario(ScenarioId.newBuilder()
                    .setScenarioId(id)
                    .build());
            return true;
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                throw new UnknownObjectException(e.getStatus().getDescription());
            } else {
                throw e;
            }
        }
    }

    /**
     * Unused. The UI team may delete this.
     * @throws Exception
     */
    @Override
    public ScenarioApiDTO configureEntities(final Long aLong, final String s, final Integer integer, final List<Integer> list, final String s1, final Float aFloat, final Float aFloat1, final String s2, final Boolean aBoolean) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Unused. The UI team may delete this.
     * @throws Exception
     */
    @Override
    public ScenarioApiDTO deleteEntities(Long id, String uuid, Integer projDays) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Unused. The UI team may delete this.
     * @throws Exception
     */
    @Override
    public ScenarioApiDTO configureGroups(final Long aLong, final String s, final Integer integer, final List<Integer> list, final String s1, final Float aFloat, final Float aFloat1, final String s2, final String s3, final Boolean aBoolean) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Unused. The UI team may delete this.
     * @throws Exception
     */
    @Override
    public ScenarioApiDTO addTemplate(Long id, String template, Integer number, List<Integer> projDays) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Unused. The UI team may delete this.
     * @throws Exception
     */
    @Override
    public ScenarioApiDTO addPolicy(final Long aLong, final String s, final String s1, final String s2, final List<String> list, final PolicyType policyType, final Integer integer, final MergePolicyType mergePolicyType) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Unused. The UI team may delete this.
     * @throws Exception
     */
    @Override
    public ScenarioApiDTO deletePolicy(Long id, String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }


    /**
     * This API is not well-defined. May be used at some point in the future.
     * Scenarios are loosely linked to markets and it is not obvious what markets
     * should be returned for a given scenario.
     *
     * @param id The id of the scenario whose markets should be retrieved.
     * @return The markets associated with the scenario.
     * @throws Exception
     */
    @Override
    public List<MarketApiDTO> getMarketsByScenario(Long id) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Validate ScenarioApiDTO
     * @param obj
     * @param e
     */
    @Override
    public void validateInput(Object obj, Errors e) {
        // The API will always validate any dto of type ScenarioApiDTO here
        // We want to bypass this validation for XL and not throw any errors so this doesn't interfere with other scenarios
    }

}
