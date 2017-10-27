package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.scenario.ScenarioChangeApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.scenario.ScenarioApiDTO;
import com.vmturbo.api.enums.PlanChangeType.PlanModificationState;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.DetailsCase;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;

public class ScenarioMapperTest {
    private static final String SCENARIO_NAME = "MyScenario";
    private static final long SCENARIO_ID = 0xdeadbeef;

    private RepositoryApi repositoryApi;

    private ScenarioMapper scenarioMapper;

    @Before
    public void setup() throws IOException {
        repositoryApi = Mockito.mock(RepositoryApi.class);

        // Return empty by default to keep NPE's at bay.
        when(repositoryApi.getServiceEntitiesById(any()))
            .thenReturn(Collections.emptyMap());

        scenarioMapper = new ScenarioMapper(repositoryApi);
    }

    @Test
    public void testAdditionChange() {
        ScenarioChangeApiDTO dto = new ScenarioChangeApiDTO();
        dto.setType(PlanModificationState.ADDED.name());
        dto.setProjectionDays(Collections.singletonList(2));
        dto.setDescription("addition change");
        dto.setTargets(Collections.singletonList(entity(1)));
        dto.setValue("6");

        ScenarioInfo info = ScenarioMapper.toScenarioInfo(SCENARIO_NAME, scenarioApiDTO(dto));
        assertEquals(SCENARIO_NAME, info.getName());
        assertEquals(1, info.getChangesCount());
        assertEquals("addition change", info.getChanges(0).getDescription());
        assertEquals(DetailsCase.TOPOLOGY_ADDITION, info.getChanges(0).getDetailsCase());

        TopologyAddition addition = info.getChanges(0).getTopologyAddition();
        assertEquals(6, addition.getAdditionCount());
        assertEquals(1, addition.getEntityId());
        assertEquals(Collections.singletonList(2), addition.getChangeApplicationDaysList());
    }

    @Test
    public void testRemovalChange() {
        ScenarioChangeApiDTO dto = new ScenarioChangeApiDTO();
        dto.setType(PlanModificationState.REMOVED.name());
        dto.setProjectionDays(Collections.singletonList(2));
        dto.setDescription("removal change");
        dto.setTargets(Collections.singletonList(entity(1)));

        ScenarioInfo info = ScenarioMapper.toScenarioInfo(SCENARIO_NAME, scenarioApiDTO(dto));
        assertEquals(SCENARIO_NAME, info.getName());
        assertEquals(1, info.getChangesCount());
        assertEquals("removal change", info.getChanges(0).getDescription());
        assertEquals(DetailsCase.TOPOLOGY_REMOVAL, info.getChanges(0).getDetailsCase());

        TopologyRemoval removal = info.getChanges(0).getTopologyRemoval();
        assertEquals(Collections.singletonList(2), removal.getChangeApplicationDaysList());
        assertEquals(1, removal.getEntityId());
    }

    @Test
    public void testInvalidChange() {
        ScenarioChangeApiDTO dto = new ScenarioChangeApiDTO();
        dto.setType("SCOPE");

        ScenarioInfo info = ScenarioMapper.toScenarioInfo(SCENARIO_NAME, scenarioApiDTO(dto));
        assertEquals(0, info.getChangesCount());
    }

    @Test
    public void testReplaceChange() {
        ScenarioChangeApiDTO dto = new ScenarioChangeApiDTO();
        dto.setType(PlanModificationState.REPLACED.name());
        dto.setProjectionDays(Collections.singletonList(5));
        dto.setDescription("replace change");
        dto.setTargets(Arrays.asList(entity(1), entity(2)));

        ScenarioInfo info = ScenarioMapper.toScenarioInfo(SCENARIO_NAME, scenarioApiDTO(dto));
        assertEquals(SCENARIO_NAME, info.getName());
        assertEquals(1, info.getChangesCount());
        assertEquals("replace change", info.getChanges(0).getDescription());

        assertEquals(DetailsCase.TOPOLOGY_REPLACE, info.getChanges(0).getDetailsCase());
        TopologyReplace replace = info.getChanges(0).getTopologyReplace();
        assertEquals(Collections.singletonList(5), replace.getChangeApplicationDaysList());
        assertEquals(1, replace.getAddTemplateId());
        assertEquals(2, replace.getRemoveEntityId());
    }

    @Test
    public void testMultiplesChanges() {
        ScenarioChangeApiDTO addDto = new ScenarioChangeApiDTO();
        addDto.setType(PlanModificationState.ADDED.name());
        addDto.setProjectionDays(Collections.singletonList(5));
        addDto.setDescription("add change");
        addDto.setTargets(Collections.singletonList(entity(1)));
        addDto.setValue("9");

        ScenarioChangeApiDTO removeDto = new ScenarioChangeApiDTO();
        removeDto.setType(PlanModificationState.REMOVED.name());
        removeDto.setProjectionDays(Collections.singletonList(1));
        removeDto.setDescription("remove change");
        removeDto.setTargets(Collections.singletonList(entity(2)));

        ScenarioInfo info = ScenarioMapper
            .toScenarioInfo(SCENARIO_NAME, scenarioApiDTO(addDto, removeDto));
        assertEquals(2, info.getChangesCount());

        assertEquals(DetailsCase.TOPOLOGY_ADDITION, info.getChanges(0).getDetailsCase());
        TopologyAddition addition = info.getChanges(0).getTopologyAddition();
        assertEquals(1, addition.getEntityId());

        assertEquals(DetailsCase.TOPOLOGY_REMOVAL, info.getChanges(1).getDetailsCase());
        TopologyRemoval removal = info.getChanges(1).getTopologyRemoval();
        assertEquals(2, removal.getEntityId());
    }

    @Test
    public void testToApiAdditionChange() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
            .setDescription("description")
            .setTopologyAddition(TopologyAddition.newBuilder()
                .addChangeApplicationDays(3)
                .setEntityId(1234)
                .setAdditionCount(44))
            .build());

        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);
        assertEquals(Long.toString(SCENARIO_ID), dto.getUuid());
        assertEquals(SCENARIO_NAME, dto.getDisplayName());

        // First 2 are hard-coded SCOPE and PROJECTION_COUNT.
        assertEquals(3, dto.getChanges().size());
        ScenarioChangeApiDTO changeDto = dto.getChanges().get(2);
        assertEquals(PlanModificationState.ADDED.name(), changeDto.getType());
        assertEquals(Collections.singletonList(3), changeDto.getProjectionDays());
        assertEquals("description", changeDto.getDescription());
        assertEquals("44", changeDto.getValue());
        assertEquals("1234", changeDto.getTargets().get(0).getUuid());
    }

    @Test
    public void testToApiRemovalChange() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
            .setDescription("description")
            .setTopologyRemoval(TopologyRemoval.newBuilder()
                .addChangeApplicationDays(3)
                .setEntityId(1234))
            .build());

        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);

        // First 2 are hard-coded SCOPE and PROJECTION_COUNT.
        assertEquals(3, dto.getChanges().size());
        ScenarioChangeApiDTO changeDto = dto.getChanges().get(2);
        assertEquals(PlanModificationState.REMOVED.name(), changeDto.getType());
        assertEquals("description", changeDto.getDescription());
        assertEquals("1234", changeDto.getTargets().get(0).getUuid());
        assertEquals(Collections.singletonList(3), changeDto.getProjectionDays());
    }

    @Test
    public void testToApiReplaceChange() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
            .setDescription("description")
            .setTopologyReplace(TopologyReplace.newBuilder()
                .addChangeApplicationDays(3)
                .setAddTemplateId(1234)
                .setRemoveEntityId(5678)
            ).build());

        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);

        // First 2 are hard-coded SCOPE and PROJECTION_COUNT.
        assertEquals(3, dto.getChanges().size());
        ScenarioChangeApiDTO changeDto = dto.getChanges().get(2);
        assertEquals(PlanModificationState.REPLACED.name(), changeDto.getType());
        assertEquals("description", changeDto.getDescription());
        assertEquals("1234", changeDto.getTargets().get(0).getUuid());
        assertEquals("5678", changeDto.getTargets().get(1).getUuid());
        assertEquals(Collections.singletonList(3), changeDto.getProjectionDays());
    }

    @Test
    public void testToApiEntityType() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
            .setDescription("description")
            .setTopologyAddition(TopologyAddition.newBuilder()
                .setAdditionCount(1)
                .setEntityId(1))
            .build());

        ServiceEntityApiDTO vmDto = new ServiceEntityApiDTO();
        vmDto.setClassName("VirtualMachine");
        vmDto.setDisplayName("VM #100");

        when(repositoryApi.getServiceEntitiesById(eq(Sets.newHashSet(1L))))
                .thenReturn(ImmutableMap.of(1L, Optional.of(vmDto)));

        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);
        // The first two are "special" hack changes - SCOPE and PROJECTION_PERIOD.
        assertEquals(3, dto.getChanges().size());
        final ScenarioChangeApiDTO changeDto = dto.getChanges().get(2);
        assertEquals(1, changeDto.getTargets().size());

        BaseApiDTO target = changeDto.getTargets().get(0);
        assertEquals("VirtualMachine", target.getClassName());
        assertEquals("VM #100", target.getDisplayName());
    }

    @Test
    public void testToApiEntityTypeUnknown() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
                .setDescription("description")
                .setTopologyAddition(TopologyAddition.newBuilder()
                        .setAdditionCount(1)
                        .setEntityId(1))
                .build());

        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);
        assertEquals(3, dto.getChanges().size());
        final ScenarioChangeApiDTO changeDto = dto.getChanges().get(2);
        assertEquals(1, changeDto.getTargets().size());

        BaseApiDTO target = changeDto.getTargets().get(0);
        assertEquals(UIEntityType.UNKNOWN.getValue(), target.getClassName());
        assertEquals(UIEntityType.UNKNOWN.getValue(), target.getDisplayName());
    }

    private Scenario buildScenario(ScenarioChange... changes) {
        return Scenario.newBuilder().setId(SCENARIO_ID)
            .setScenarioInfo(ScenarioInfo.newBuilder()
                .setName(SCENARIO_NAME)
                .addAllChanges(Arrays.asList(changes))
            ).build();
    }

    private BaseApiDTO entity(long entityId) {
        BaseApiDTO entity = new BaseApiDTO();

        entity.setClassName("Entity");
        entity.setDisplayName("Entity " + entityId);
        entity.setUuid(Long.toString(entityId));

        return entity;
    }

    private ScenarioApiDTO scenarioApiDTO(@Nonnull final ScenarioChangeApiDTO... changes) {
        ScenarioApiDTO dto = new ScenarioApiDTO();
        dto.setChanges(Arrays.asList(changes));

        return dto;
    }
}
