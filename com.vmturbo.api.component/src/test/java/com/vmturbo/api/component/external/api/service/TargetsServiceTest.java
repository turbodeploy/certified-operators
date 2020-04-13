package com.vmturbo.api.component.external.api.service;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.AnnotationConfigWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.WebSocketSession;

import com.vmturbo.api.NotificationDTO.Notification;
import com.vmturbo.api.TargetNotificationDTO.TargetStatusNotification.TargetStatus;
import com.vmturbo.api.component.communication.ApiComponentTargetListener;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketHandler;
import com.vmturbo.api.controller.TargetsController;
import com.vmturbo.api.dto.ErrorApiDTO;
import com.vmturbo.api.dto.target.InputFieldApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.api.handler.GlobalExceptionHandler;
import com.vmturbo.api.utils.ParamStrings;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.AccountDefEntry;
import com.vmturbo.topology.processor.api.AccountFieldValueType;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.impl.ProbeRESTApi.AccountField;

/**
 * Test the {@link TargetsService}. Mocks calls to the underlying {@link TopologyProcessor}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class TargetsServiceTest {

    private static final String TGT_NOT_FOUND = "Target not found: ";
    private static final String TGT_NOT_EDITABLE = "cannot be changed through public APIs.";
    private static final String PROBE_NOT_FOUND = "Probe not found: ";

    private static final String TARGET_DISPLAY_NAME = "target name";

    @Autowired
    private TopologyProcessor topologyProcessor;

    private final ActionsServiceMole actionsServiceBackend =
        spy(new ActionsServiceMole());

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(actionsServiceBackend);

    ActionsServiceGrpc.ActionsServiceBlockingStub actionsRpcService;

    ActionSpecMapper actionSpecMapper;

    SearchServiceBlockingStub searchServiceRpc;

    private RepositoryApi repositoryApi = mock(RepositoryApi.class);

    private MockMvc mockMvc;

    @Autowired
    private WebApplicationContext wac;
    private static final Gson GSON = new Gson();

    @Autowired
    private ApiComponentTargetListener apiComponentTargetListener;

    private Map<Long, ProbeInfo> registeredProbes;
    private Map<Long, TargetInfo> registeredTargets;
    private long idCounter;

    private static final long REALTIME_CONTEXT_ID = 7777777;
    private ApiWebsocketHandler apiWebsocketHandler;

    @Before
    public void init() throws TopologyProcessorException, CommunicationException {
        idCounter = 0;
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        actionsRpcService = ActionsServiceGrpc.newBlockingStub(grpcServer.getChannel());
        searchServiceRpc = SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());
        actionSpecMapper = Mockito.mock(ActionSpecMapper.class);
        registeredTargets = new HashMap<>();
        registeredProbes = new HashMap<>();
        when(topologyProcessor.getProbe(Mockito.anyLong()))
                        .thenAnswer(new Answer<ProbeInfo>() {

                            @Override
                            public ProbeInfo answer(InvocationOnMock invocation) throws Throwable {
                                final long id = invocation.getArgumentAt(0, long.class);
                                final ProbeInfo probeInfo = registeredProbes.get(id);
                                if (probeInfo == null) {
                                    throw new TopologyProcessorException(PROBE_NOT_FOUND + id);
                                } else {
                                    return probeInfo;
                                }
                            }
                        });
        when(topologyProcessor.getTarget(Mockito.anyLong()))
                        .thenAnswer(new Answer<TargetInfo>() {

                            @Override
                            public TargetInfo answer(InvocationOnMock invocation) throws Throwable {
                                final long id = invocation.getArgumentAt(0, long.class);
                                final TargetInfo targetInfo = registeredTargets.get(id);
                                if (targetInfo == null) {
                                    throw new TopologyProcessorException(TGT_NOT_FOUND + id);
                                } else {
                                    return targetInfo;
                                }
                            }
                        });
        when(topologyProcessor.addTarget(Mockito.anyLong(), Mockito.any(TargetData.class)))
                .thenAnswer(new Answer<Long>() {
                    @Override
                    public Long answer(InvocationOnMock invocation) throws Throwable {
                        final Long probeId = invocation.getArgumentAt(0, Long.class);
                        final TargetData data = invocation.getArgumentAt(1, TargetData.class);
                        final long targetId = idCounter++;
                        final TargetInfo target = createMockTargetInfo(probeId, targetId);
                        when(target.getAccountData()).thenReturn(data.getAccountData());
                        registeredTargets.put(targetId, target);
                        return targetId;
                    }
                });
        when(topologyProcessor.getAllProbes()).thenAnswer(new Answer<Set<ProbeInfo>>() {
            @Override
            public Set<ProbeInfo> answer(InvocationOnMock invocation) throws Throwable {
                return new HashSet<>(registeredProbes.values());
            }
        });
        when(topologyProcessor.getAllTargets()).thenAnswer(new Answer<Set<TargetInfo>>() {
            @Override
            public Set<TargetInfo> answer(InvocationOnMock invocation) throws Throwable {
                return new HashSet<>(registeredTargets.values());
            }
        });
        apiWebsocketHandler = new ApiWebsocketHandler();
    }

    private ProbeInfo createMockProbeInfo(long probeId, String type, String category,
            AccountDefEntry... entries) throws Exception {
        return createMockProbeInfo(probeId, type, category, CreationMode.STAND_ALONE, entries);
    }

    private ProbeInfo createMockProbeInfo(long probeId, String type, String category,
            CreationMode creationMode, AccountDefEntry... entries) throws Exception {
        final ProbeInfo newProbeInfo = Mockito.mock(ProbeInfo.class);
        when(newProbeInfo.getId()).thenReturn(probeId);
        when(newProbeInfo.getType()).thenReturn(type);
        when(newProbeInfo.getCategory()).thenReturn(category);
        when(newProbeInfo.getAccountDefinitions()).thenReturn(Arrays.asList(entries));
        when(newProbeInfo.getCreationMode()).thenReturn(creationMode);
        if (entries.length > 0) {
            when(newProbeInfo.getIdentifyingFields())
                            .thenReturn(Collections.singletonList(entries[0].getName()));
        } else {
            when(newProbeInfo.getIdentifyingFields()).thenReturn(Collections.emptyList());
        }
        registeredProbes.put(probeId, newProbeInfo);
        return newProbeInfo;
    }

    private TargetInfo createMockTargetInfo(long probeId, long targetId, AccountValue... accountValues)
            throws Exception {
        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        when(targetInfo.getId()).thenReturn(targetId);
        when(targetInfo.getProbeId()).thenReturn(probeId);
        when(targetInfo.getAccountData()).thenReturn(
                        new HashSet<>(Arrays.asList(accountValues)));
        when(targetInfo.getStatus()).thenReturn("Validated");
        when(targetInfo.isHidden()).thenReturn(false);
        when(targetInfo.getDisplayName()).thenReturn(TARGET_DISPLAY_NAME);
        registeredTargets.put(targetId, targetInfo);
        return targetInfo;
    }

    private TargetInfo createMockHiddenTargetInfo(long probeId, long targetId, AccountValue... accountValues)
            throws Exception {
        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        when(targetInfo.getId()).thenReturn(targetId);
        when(targetInfo.getProbeId()).thenReturn(probeId);
        when(targetInfo.getAccountData()).thenReturn(
                        new HashSet<>(Arrays.asList(accountValues)));
        when(targetInfo.getStatus()).thenReturn("Validated");
        when(targetInfo.isHidden()).thenReturn(true);
        registeredTargets.put(targetId, targetInfo);
        return targetInfo;
    }

    /**
     * Tests getting a target by id.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void getTarget() throws Exception {
        final ProbeInfo probe = createMockProbeInfo(1, "type", "category",
                        createAccountDef("field1"), createAccountDef("field2"));
        final TargetInfo target = createMockTargetInfo(probe.getId(), 3,
                        createAccountValue("field1", "value1"),
                        createAccountValue("field2", "value2"));

        final MvcResult result = mockMvc
                        .perform(get("/targets/3").accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final TargetApiDTO resp = GSON.fromJson(result.getResponse().getContentAsString(),
                        TargetApiDTO.class);
        assertEquals(target, probe, resp);
    }

    /**
     * Tests that 'displayName' is correct.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void getTargetDisplayName() throws Exception {
        final ProbeInfo probe = createMockProbeInfo(1, "type", "category",
                createAccountDef("address"));
        final TargetInfo target = createMockTargetInfo(probe.getId(), 3,
                createAccountValue("address", "targetAddress"));

        final MvcResult result = mockMvc
                        .perform(get("/targets/3").accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final TargetApiDTO resp = GSON.fromJson(result.getResponse().getContentAsString(),
                        TargetApiDTO.class);
        assertThat(resp.getDisplayName(), equalTo(TARGET_DISPLAY_NAME));
    }

    /**
     * Tests getting a target by id with invalid fields. This should be treated as internal
     * server error, as is caused by data inconsustency.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void getTargetWithInvalidField() throws Exception {
        final ProbeInfo probe = createMockProbeInfo(1, "type", "category",
                        createAccountDef("field1"), createAccountDef("field2"));
        createMockTargetInfo(probe.getId(), 3,
                        createAccountValue("field1", "value1"),
                        createAccountValue("field3", "value2"));

        final MvcResult result = mockMvc.perform(get("/targets/3").accept(MediaType
                .APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().is5xxServerError()).andReturn();
        final ErrorApiDTO resp = GSON.fromJson(result.getResponse().getContentAsString(),
                ErrorApiDTO.class);
        Assert.assertThat(resp.getMessage(),
                CoreMatchers.containsString("AccountDef Entry not found"));
    }

    /**
     * Tests getting target by id, while target is not present.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void getAbsentTarget() throws Exception {
        final ProbeInfo probe = createMockProbeInfo(1, "type", "category");
        createMockTargetInfo(probe.getId(), 3);
        final MvcResult result = mockMvc.perform(get("/targets/4").accept(MediaType
                .APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().is4xxClientError()).andReturn();
        final ErrorApiDTO resp = GSON.fromJson(result.getResponse().getContentAsString(),
                ErrorApiDTO.class);
        Assert.assertThat(resp.getMessage(), CoreMatchers.containsString(TGT_NOT_FOUND));
    }

    /**
     * Tests for retrieval of all the targets.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetAllTargets() throws Exception {
        fetchAllTargets(false);
    }

    /**
     * Tests for retrieval of all the targets with environment type filter
     * {@link com.vmturbo.api.enums.EnvironmentType#HYBRID}. All targets should
     * be fetched.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetAllTargetsWithHybridFilter() throws Exception {
        fetchAllTargets(true);
    }

    private void fetchAllTargets(boolean hybridFilterExists) throws Exception {
        final ProbeInfo probe = createMockProbeInfo(1, "type", "category");
        final Collection<TargetInfo> targets = new ArrayList<>();
        targets.add(createMockTargetInfo(probe.getId(), 2));
        targets.add(createMockTargetInfo(probe.getId(), 3));
        targets.add(createMockTargetInfo(probe.getId(), 4));
        when(targets.iterator().next().getStatus()).thenReturn("Connection refused");

        final String url = "/targets" + (hybridFilterExists ? "?environmentType=HYBRID" : "");
        final MvcResult result = mockMvc
                        .perform(get(url).accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final TargetApiDTO[] resp = GSON.fromJson(result.getResponse().getContentAsString(),
                        TargetApiDTO[].class);
        Assert.assertEquals(targets.size(), resp.length);
        final Map<Long, TargetApiDTO> map = Arrays.asList(resp).stream()
                        .collect(Collectors.toMap(tad -> Long.valueOf(tad.getUuid()), tad -> tad));
        for (TargetInfo target : targets) {
            final TargetApiDTO dto = map.get(target.getId());
            assertEquals(target, probe, dto);
        }
    }

    /**
     * Tests if the service can filter and return the cloud targets.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetAllCloudTargets() throws Exception {
        testFilterTargetsByEnvironment(true);
    }

    /**
     * Tests if the service can filter and return the onprem targets.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetAllOnPremTargets() throws Exception {
        testFilterTargetsByEnvironment(false);
    }

    private void testFilterTargetsByEnvironment(boolean cloud) throws Exception {
        final long cloudProbeId = 1L;
        final long onPremProbeId = 2L;
        final long cloudTargetId = 3L;
        final long onPremTargetId = 4L;
        createMockProbeInfo(cloudProbeId, SDKProbeType.AWS.getProbeType(), "dummy");
        createMockProbeInfo(onPremProbeId, SDKProbeType.VCENTER.getProbeType(), "dummy");
        createMockTargetInfo(cloudProbeId, cloudTargetId);
        createMockTargetInfo(onPremProbeId, onPremTargetId);

        final String url = "/targets?environment_type=" + (cloud ? "CLOUD" : "ONPREM");
        final MvcResult result = mockMvc.perform(get(url).accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                                    .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final TargetApiDTO[] resp = GSON.fromJson(result.getResponse().getContentAsString(),
                TargetApiDTO[].class);
        Assert.assertEquals(1, resp.length);
        Assert.assertEquals(Long.toString(cloud ? cloudTargetId : onPremTargetId), resp[0].getUuid());
    }

    /**
     * Tests the case where the user's environment includes a "parent" target and two derived targets.
     * Verifies that the parent's derivedTarget Dtos have been created correctly.
     *
     * @throws Exception Not expected to happen.
     */
    @Test
    public void testGetAllTargets_withDerivedTargetRelationships() throws Exception {
        final ProbeInfo probe = createMockProbeInfo(1, "type", "category");
        final TargetInfo parentTargetInfo = createMockTargetInfo(probe.getId(), 2);
        final TargetInfo childTargetInfo1 = createMockTargetInfo(probe.getId(), 3);
        final TargetInfo childTargetInfo2 = createMockTargetInfo(probe.getId(), 4);
        when(parentTargetInfo.getDerivedTargetIds()).thenReturn(Lists.newArrayList(3L, 4L));

        final MvcResult result = mockMvc
                .perform(get("/targets").accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final TargetApiDTO[] resp = GSON.fromJson(result.getResponse().getContentAsString(),
                TargetApiDTO[].class);
        final Map<Long, TargetApiDTO> allTargetDtosMap =
                Maps.uniqueIndex(Arrays.asList(resp), targetInfo -> Long.valueOf(targetInfo.getUuid()));
        final TargetApiDTO parentDto = allTargetDtosMap.get(parentTargetInfo.getId());
        final Map<Long, TargetApiDTO> derivedTargetDtosMap =
                Maps.uniqueIndex(parentDto.getDerivedTargets(), targetInfo -> Long.valueOf(targetInfo.getUuid()));

        assertEquals(childTargetInfo1, probe, derivedTargetDtosMap.get(childTargetInfo1.getId()));
        assertEquals(childTargetInfo2, probe, derivedTargetDtosMap.get(childTargetInfo2.getId()));
    }

    /**
     * Tests for retrieval of all the targets with out the filtered targets, e.g. hidden targets.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testGetAllTargetsWithoutFilteredTargets() throws Exception {
        final int hiddenTargetsCount = 1;
        final ProbeInfo probe = createMockProbeInfo(1, "type", "category");
        final Collection<TargetInfo> targets = new ArrayList<>();
        targets.add(createMockTargetInfo(probe.getId(), 2));
        targets.add(createMockTargetInfo(probe.getId(), 3));
        targets.add(createMockHiddenTargetInfo(probe.getId(), 4));
        when(targets.iterator().next().getStatus()).thenReturn("Connection refused");

        final MvcResult result = mockMvc
                        .perform(get("/targets").accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final TargetApiDTO[] resp = GSON.fromJson(result.getResponse().getContentAsString(),
                        TargetApiDTO[].class);
        Assert.assertEquals(targets.size() - hiddenTargetsCount, resp.length);
        final Map<Long, TargetApiDTO> map = Arrays.asList(resp).stream()
                .collect(Collectors.toMap(tad -> Long.valueOf(tad.getUuid()), tad -> tad));
        for (TargetInfo target : targets) {
            if (target.isHidden()) continue;
            final TargetApiDTO dto = map.get(target.getId());
            assertEquals(target, dto);
        }
    }

    /**
     * Tests for retrieval of all the targets.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testGetAllTargetsWithoutProbes() throws Exception {
        final long probeId = 1;
        final Collection<TargetInfo> targets = new ArrayList<>();
        targets.add(createMockTargetInfo(probeId, 2));
        targets.add(createMockTargetInfo(probeId, 3, createAccountValue("field2", "value2")));
        targets.add(createMockTargetInfo(probeId, 4, createAccountValue("field3", "value3"),
                        createAccountValue("field4", "value4")));

        final MvcResult result = mockMvc
                        .perform(get("/targets").accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final TargetApiDTO[] resp = GSON.fromJson(result.getResponse().getContentAsString(),
                        TargetApiDTO[].class);
        Assert.assertEquals(targets.size(), resp.length);
        final Map<Long, TargetApiDTO> map = Arrays.asList(resp).stream()
                        .collect(Collectors.toMap(tad -> Long.valueOf(tad.getUuid()), tad -> tad));
        for (TargetInfo target : targets) {
            final TargetApiDTO dto = map.get(target.getId());
            assertEquals(target, dto);
        }
    }

    /**
     * Tests target addition. This method expected not to trigger validation and discovery.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testAddTarget() throws Exception {
        final long probeId = 1;
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category", createAccountDef
                ("key"));
        final TargetApiDTO targetDto = new TargetApiDTO();
        targetDto.setType(probe.getType());
        targetDto.setInputFields(Arrays.asList(inputField("key", "value")));
        final String targetString = GSON.toJson(targetDto);
        final MvcResult result = mockMvc
                        .perform(MockMvcRequestBuilders.post("/targets")
                                        .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                                        .content(targetString)
                                        .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final ArgumentCaptor<TargetData> captor = ArgumentCaptor.forClass(TargetData.class);
        Mockito.verify(topologyProcessor).addTarget(Mockito.eq(probeId), captor.capture());
        captor.getValue().getAccountData().forEach(
                        av -> assertEquals(av, targetDto.getInputFields().iterator().next()));
        Mockito.verify(topologyProcessor, Mockito.never()).validateAllTargets();
        Mockito.verify(topologyProcessor, Mockito.never()).discoverAllTargets();

        final TargetApiDTO resp = GSON.fromJson(result.getResponse().getContentAsString(),
                        TargetApiDTO.class);
        Assert.assertEquals(Long.toString(registeredTargets.keySet().iterator().next()),
                resp.getUuid());
    }


    /**
     * Tests first target addition, and Target Listener's setValidatedFirstTarget method
     * should be invoked with argument "true".
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testAddFirstTarget() throws Exception {
        final long probeId = 1;
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category", createAccountDef
                ("key"));
        final TargetApiDTO targetDto = new TargetApiDTO();
        targetDto.setType(probe.getType());
        targetDto.setInputFields(Arrays.asList(inputField("key", "value")));
        final String targetString = GSON.toJson(targetDto);
        when(topologyProcessor.getAllTargets()).thenReturn(Collections.EMPTY_SET);
        mockMvc.perform(MockMvcRequestBuilders.post("/targets")
                        .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                        .content(targetString)
                        .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(apiComponentTargetListener).triggerBroadcastAfterNextDiscovery();
    }


    /**
     * Tests second target addition, and Target Listener's setValidatedFirstTarget
     * method should NOT be invoked.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testAddSecondTarget() throws Exception {
        final long probeId = 1;
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category", createAccountDef
                ("key"));
        final TargetApiDTO targetDto = new TargetApiDTO();
        targetDto.setType(probe.getType());
        targetDto.setInputFields(Arrays.asList(inputField("key", "value")));
        final String targetString = GSON.toJson(targetDto);
        final Set<TargetInfo> targets = ImmutableSet.of(createMockTargetInfo(probeId, 2));
        when(topologyProcessor.getAllTargets()).thenReturn(targets);
        mockMvc.perform(MockMvcRequestBuilders.post("/targets")
                        .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                        .content(targetString)
                        .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verifyZeroInteractions(apiComponentTargetListener);
    }

    /**
     * Tests target addition for non-registered probe. This method expected to return 4xx error.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testAddTargetForAbsentProbe() throws Exception {
        final TargetApiDTO targetDto = new TargetApiDTO();
        targetDto.setType("probe-type");
        targetDto.setInputFields(Arrays.asList(inputField("key", "value")));
        final String targetString = GSON.toJson(targetDto);
        final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.post("/targets")
                        .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).content(targetString)
                        .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().is4xxClientError()).andReturn();
        final ErrorApiDTO resp = GSON.fromJson(result.getResponse()
                .getContentAsString(), ErrorApiDTO.class);
        Assert.assertThat(resp.getMessage(), CoreMatchers.containsString(targetDto.getType()));
    }

    /**
     * Tests for modifying target.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testEditTarget() throws Exception {
        final long probeId = 1;
        final long targetId = 2;
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category");
        final TargetInfo targetInfo = createMockTargetInfo(probe.getId(), targetId);

        final TargetApiDTO targetDto = new TargetApiDTO();
        targetDto.setType(probe.getType());
        targetDto.setInputFields(Arrays.asList(inputField("key", "value2")));
        final String targetString = GSON.toJson(targetDto);

        final MvcResult result = mockMvc
                        .perform(MockMvcRequestBuilders.put("/targets/" + targetId)
                                        .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                                        .content(targetString)
                                        .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final ArgumentCaptor<TargetData> captor = ArgumentCaptor.forClass(TargetData.class);
        Mockito.verify(topologyProcessor).modifyTarget(Mockito.eq(targetId), captor.capture());
        final Collection<AccountValue> inputFields = captor.getValue().getAccountData();
        Assert.assertEquals(targetDto.getInputFields().size(), inputFields.size());
        final Map<String, InputFieldApiDTO> expectedFieldsMap = targetDto.getInputFields().stream()
                        .collect(Collectors.toMap(field -> field.getName(), field -> field));
        inputFields.forEach(av -> assertEquals(av, expectedFieldsMap.get(av.getName())));
    }

    /**
     * Tests the case when there is an attempt at modifying a read-only target.
     * OperationFailedException is expected to be thrown.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testEditTarget_readOnlyTarget() throws Exception {
        final long probeId = 5;
        final long targetId = 10;
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category");
        final TargetInfo targetInfo = createMockTargetInfo(probe.getId(), targetId);
        when(targetInfo.isReadOnly()).thenReturn(true);

        final TargetApiDTO targetDto = new TargetApiDTO();
        targetDto.setType(probe.getType());
        targetDto.setInputFields(Arrays.asList(inputField("key", "value2")));
        final String targetString = GSON.toJson(targetDto);

        final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.put("/targets/" + targetId)
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .content(targetString)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(MockMvcResultMatchers.status().is4xxClientError()).andReturn();
        final ErrorApiDTO resp = GSON.fromJson(result.getResponse().getContentAsString(),
                ErrorApiDTO.class);

        Assert.assertThat(resp.getMessage(), CoreMatchers.containsString(TGT_NOT_EDITABLE));
    }

    /**
     * Tests removal of existing target.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void deleteExistingTarget() throws Exception {
        final long prpbeId = 1;
        final long targetId = 2;
        final ProbeInfo probe = createMockProbeInfo(prpbeId, "type", "category");
        final TargetInfo targetInfo = createMockTargetInfo(probe.getId(), targetId);
        final MvcResult result = mockMvc
                        .perform(MockMvcRequestBuilders.delete("/targets/" + targetId).accept(
                                        MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(topologyProcessor).removeTarget(targetId);
    }

    /**
     * Tests removal trial of absent target.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void deleteAbsentTarget() throws Exception {
        final long targetId = 2;
        Mockito.doThrow(new TopologyProcessorException(TGT_NOT_FOUND)).when(topologyProcessor)
                        .removeTarget(Mockito.anyLong());
        final MvcResult result = mockMvc
                        .perform(MockMvcRequestBuilders.delete("/targets/" + targetId).accept(
                                        MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().is4xxClientError()).andReturn();
        final ErrorApiDTO resp =
                        GSON.fromJson(result.getResponse().getContentAsString(), ErrorApiDTO.class);
        Mockito.verify(topologyProcessor).removeTarget(targetId);
        Assert.assertThat(resp.getMessage(), CoreMatchers.containsString(TGT_NOT_FOUND));
    }

    /**
     * Tests validation and discovery triggering on specific targets.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testExecuteOnTarget() throws Exception {
        final ProbeInfo probeInfo = createDefaultProbeInfo();
        final TargetInfo tNothing = createMockTargetInfo(probeInfo.getId(), 2);
        final TargetInfo tValidate = createMockTargetInfo(probeInfo.getId(), 3);
        final TargetInfo tDiscovery = createMockTargetInfo(probeInfo.getId(), 4);
        final TargetInfo tBoth = createMockTargetInfo(probeInfo.getId(), 5);


        final TargetApiDTO dtoNothing = postAndReturn(Long.toString(tNothing.getId()));
        postAndReturn(tBoth.getId() + "?"
                        + ParamStrings.VALIDATE
                        + "=false&"
                        + ParamStrings.REDISCOVER
                        + "=false");
        final TargetApiDTO dtoValidate = postAndReturn(tValidate.getId() + "?"
                        + ParamStrings.VALIDATE
                        + "=true");
        final TargetApiDTO dtoDiscovery = postAndReturn(tDiscovery.getId() + "?"
                        + ParamStrings.REDISCOVER
                        + "=true");
        final TargetApiDTO dtoBoth = postAndReturn(tBoth.getId() + "?"
                        + ParamStrings.VALIDATE
                        + "=true&"
                        + ParamStrings.REDISCOVER
                        + "=true");

        Mockito.verify(topologyProcessor, Mockito.never()).validateTarget(tNothing.getId());
        Mockito.verify(topologyProcessor, Mockito.never()).discoverTarget(tNothing.getId());
        Mockito.verify(topologyProcessor).validateTarget(tValidate.getId());
        Mockito.verify(topologyProcessor, Mockito.never()).discoverTarget(tValidate.getId());
        Mockito.verify(topologyProcessor, Mockito.never()).validateTarget(tDiscovery.getId());
        Mockito.verify(topologyProcessor).discoverTarget(tDiscovery.getId());
        Mockito.verify(topologyProcessor).validateTarget(tBoth.getId());
        Mockito.verify(topologyProcessor).discoverTarget(tBoth.getId());

        assertEquals(tNothing, dtoNothing);
        assertEquals(tValidate, dtoValidate);
        assertEquals(tDiscovery, dtoDiscovery);
        assertEquals(tBoth, dtoBoth);
    }

    @Test
    public void testSynchronousValidationTimeout() throws Exception {
        final long probeId = 2;
        final long targetId = 3;

        final TopologyProcessor topologyProcessor = Mockito.mock(TopologyProcessor.class);
        final TargetsService targetsService = new TargetsService(
            topologyProcessor, Duration.ofMillis(50), Duration.ofMillis(100),
            Duration.ofMillis(50), Duration.ofMillis(100), null,
            apiComponentTargetListener,repositoryApi,actionSpecMapper,actionsRpcService,REALTIME_CONTEXT_ID, apiWebsocketHandler);

        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        when(targetInfo.getId()).thenReturn(targetId);
        when(targetInfo.getProbeId()).thenReturn(probeId);
        when(targetInfo.getStatus()).thenReturn(TargetsService.TOPOLOGY_PROCESSOR_VALIDATION_IN_PROGRESS);

        when(topologyProcessor.getTarget(targetId)).thenReturn(targetInfo);
        TargetInfo validationInfo = targetsService.validateTargetSynchronously(targetId);

        Mockito.verify(topologyProcessor, times(2)).getTarget(targetId);
        org.junit.Assert.assertEquals(
            TargetsService.TOPOLOGY_PROCESSOR_VALIDATION_IN_PROGRESS, validationInfo.getStatus());
    }

    @Test
    public void testFailedTargetValidationNotification() throws Exception {
        WebSocketSession session = mock(WebSocketSession.class);
        apiWebsocketHandler.afterConnectionEstablished(session);
        ArgumentCaptor<BinaryMessage> notificationCaptor = ArgumentCaptor.forClass(BinaryMessage.class);
        IdentityGenerator.initPrefix(0);

        final long targetId = 1;
        final TopologyProcessor topologyProcessor = Mockito.mock(TopologyProcessor.class);
        final TargetsService targetsService = new TargetsService(
            topologyProcessor, Duration.ofMillis(50), Duration.ofMillis(100),
            Duration.ofMillis(50), Duration.ofMillis(100), null, apiComponentTargetListener,
            repositoryApi, actionSpecMapper, actionsRpcService, REALTIME_CONTEXT_ID,
            apiWebsocketHandler);

        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        when(targetInfo.getId()).thenReturn(targetId);
        when(targetInfo.getStatus()).thenReturn("Validation failed.");

        when(topologyProcessor.getTarget(targetId)).thenReturn(targetInfo);
        targetsService.validateTargetSynchronously(targetId);

        verify(session).sendMessage(notificationCaptor.capture());
        final Notification notification = Notification.parseFrom(
            notificationCaptor.getValue().getPayload().array());
        Assert.assertEquals("1", notification.getTargetNotification().getTargetId());
        Assert.assertEquals("Validation failed.", notification.getTargetNotification().getStatusNotification().getDescription());
        Assert.assertEquals(TargetStatus.NOT_VALIDATED, notification.getTargetNotification().getStatusNotification().getStatus());
    }

    @Test
    public void testSynchronousDiscoveryTimeout() throws Exception {
        final long probeId = 2;
        final long targetId = 3;

        final TopologyProcessor topologyProcessor = Mockito.mock(TopologyProcessor.class);
        final TargetsService targetsService = new TargetsService(
            topologyProcessor, Duration.ofMillis(50), Duration.ofMillis(100),
            Duration.ofMillis(50), Duration.ofMillis(100), null,
            apiComponentTargetListener,repositoryApi, actionSpecMapper,actionsRpcService,REALTIME_CONTEXT_ID, apiWebsocketHandler);

        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        when(targetInfo.getId()).thenReturn(targetId);
        when(targetInfo.getProbeId()).thenReturn(probeId);
        when(targetInfo.getStatus()).thenReturn(TargetsService.TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS);

        when(topologyProcessor.getTarget(targetId)).thenReturn(targetInfo);
        TargetInfo discoveryInfo = targetsService.discoverTargetSynchronously(targetId);

        Mockito.verify(topologyProcessor, times(2)).getTarget(targetId);
        org.junit.Assert.assertEquals(
                TargetsService.TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS, discoveryInfo.getStatus());
    }

    /**
     * Tests retrieval of all the probes, registered in TopologyProcessor.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testGetProbes() throws Exception {
        final Collection<ProbeInfo> probesCollection = new ArrayList<>();
        final String field1 = "field11";
        final String field2 = "field22";
        final String field3 = "field3";
        probesCollection.add(createMockProbeInfo(1, "type1", "category1",
                        createAccountDef(field1), createAccountDef("field12")));
        probesCollection.add(createMockProbeInfo(2, "type2", "category2",
                        createAccountDef(field2), createAccountDef("field22")));
        probesCollection.add(createMockProbeInfo(3, "type3", "category4",
                        createAccountDef(field3)));
        final Map<String, ProbeInfo> probeByType = probesCollection.stream().collect(
                        Collectors.toMap(pr -> pr.getType(), pr -> pr));

        final MvcResult result = mockMvc
                        .perform(MockMvcRequestBuilders.get("/targets/specs").accept(
                                        MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final List<TargetApiDTO> resp = Arrays.asList(GSON.fromJson(result.getResponse()
                        .getContentAsString(), TargetApiDTO[].class));
        for (TargetApiDTO apiProbe : resp) {
            final ProbeInfo probeInfo = probeByType.get(apiProbe.getType());
            Assert.assertEquals(probeInfo.getIdentifyingFields(), apiProbe.getIdentifyingFields());
            Assert.assertNotNull("Unknown probe found: " + apiProbe.getType(), probeInfo);
        }
    }

    /**
     * Tests retrieval of all the probes, registered in TopologyProcessor, without the one belong to hidden
     * category (billing, storage browsing).
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testGetProbesWithoutHiddenCategory() throws Exception {
        final Set<String> hiddenProbeCategorys = new ImmutableSet.Builder<String>()
                .add(ProbeCategory.BILLING.getCategory())
                .add(ProbeCategory.STORAGE_BROWSING.getCategory())
                .build();
        final int hiddenProbesCount = 2;
        final Collection<ProbeInfo> probesCollection = new ArrayList<>();
        final String field1 = "field11";
        final String field2 = "field22";
        final String field3 = "field3";
        probesCollection.add(createMockProbeInfo(1, "type1", "category1",
                        createAccountDef(field1), createAccountDef("field12")));
        probesCollection.add(createMockProbeInfo(2, "type2", ProbeCategory.BILLING.getCategory(),
                        CreationMode.DERIVED, createAccountDef(field2), createAccountDef("field22")));
        probesCollection.add(createMockProbeInfo(3, "type3", ProbeCategory.STORAGE_BROWSING.getCategory(),
                        CreationMode.DERIVED, createAccountDef(field3)));
        final Map<String, ProbeInfo> probeByType = probesCollection.stream().collect(
                        Collectors.toMap(pr -> pr.getType(), pr -> pr));

        final MvcResult result = mockMvc
                        .perform(MockMvcRequestBuilders.get("/targets/specs").accept(
                                        MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final List<TargetApiDTO> resp = Arrays.asList(GSON.fromJson(result.getResponse()
                        .getContentAsString(), TargetApiDTO[].class));
        Assert.assertEquals(probesCollection.size() - hiddenProbesCount, resp.size());
        for (TargetApiDTO apiProbe : resp) {
            final ProbeInfo probeInfo = probeByType.get(apiProbe.getType());
            if (hiddenProbeCategorys.contains(probeInfo.getCategory())) continue;
            Assert.assertEquals(probeInfo.getIdentifyingFields(), apiProbe.getIdentifyingFields());
            Assert.assertNotNull("Unknown probe found: " + apiProbe.getType(), probeInfo);
        }
    }

    /**
     * Tests retrieval of all the probes, having wrong identifying fields property set.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testGetProbeWrongIdentifyingFields() throws Exception {
        final ProbeInfo probe =
                        createMockProbeInfo(1, "type1", "category1", createAccountDef("targetId"));
        when(probe.getIdentifyingFields())
                        .thenReturn(Collections.singletonList("non-target-id"));
        final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/targets/specs")
                        .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().is5xxServerError()).andReturn();
        final ErrorApiDTO resp = GSON.fromJson(result.getResponse().getContentAsString(),
                ErrorApiDTO.class);
        Assert.assertThat(resp.getException(),
                CoreMatchers.containsString("Fields of target " + probe.getType()));
    }

    /**
     * Tests retrieval of different specific account value types.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testProbeAccountValueTypes() throws Exception {
        final ProbeInfo probeInfo = createMockProbeInfo(1, "type1", "category1",
                        createAccountDef("fieldStr", AccountFieldValueType.STRING),
                        createAccountDef("fieldNum", AccountFieldValueType.NUMERIC),
                        createAccountDef("fieldBool", AccountFieldValueType.BOOLEAN),
                        createAccountDef("fieldGrp", AccountFieldValueType.GROUP_SCOPE));
        final MvcResult result = mockMvc
                        .perform(MockMvcRequestBuilders.get("/targets/specs")
                                        .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final List<TargetApiDTO> resp = Arrays.asList(GSON
                        .fromJson(result.getResponse().getContentAsString(), TargetApiDTO[].class));
        Assert.assertEquals(1, resp.size());
        final TargetApiDTO probe = resp.iterator().next();
        final List<InputFieldApiDTO> fields = probe.getInputFields();
        Assert.assertEquals(InputValueType.STRING, fields.get(0).getValueType());
        Assert.assertEquals(InputValueType.NUMERIC, fields.get(1).getValueType());
        Assert.assertEquals(InputValueType.BOOLEAN, fields.get(2).getValueType());
        Assert.assertEquals(InputValueType.GROUP_SCOPE, fields.get(3).getValueType());
        Assert.assertEquals(".*", fields.get(0).getVerificationRegex());

    }

    /**
     * Tests that "allowedValues" field gets populated correctly into a TargetApiDTO's InputFields.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testAllowedValues() throws Exception {
        final String key = "allowedValues";
        final List<String> allowedValuesList = Lists.newArrayList("A", "B", "C");
        final AccountField allowedValuesField =
                new AccountField(key, key + "-name", key + "-description", false, false,
                        AccountFieldValueType.LIST, null, allowedValuesList, ".*");
        final ProbeInfo probeInfo =
                createMockProbeInfo(1, "type1", "category1", allowedValuesField);
        final MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.get("/targets/specs")
                        .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final List<TargetApiDTO> resp = Arrays.asList(GSON
                .fromJson(result.getResponse().getContentAsString(), TargetApiDTO[].class));
        Assert.assertEquals(1, resp.size());
        final TargetApiDTO probe = resp.iterator().next();
        final List<InputFieldApiDTO> fields = probe.getInputFields();
        Assert.assertEquals(allowedValuesList, fields.get(0).getAllowedValues());
    }

    /**
     * Tests retrieval of all the known account value types.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testProbeAllAccountValueEnumeration() throws Exception {
        final AccountDefEntry[] accountEntries =
                        new AccountDefEntry[AccountFieldValueType.values().length];
        int i = 0;
        for (AccountFieldValueType value : AccountFieldValueType.values()) {
            accountEntries[i++] = createAccountDef("field-" + value.name(), value);
        }
        createMockProbeInfo(1, "type1", "category1", accountEntries);
        final MvcResult result = mockMvc
                        .perform(MockMvcRequestBuilders.get("/targets/specs")
                                        .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final List<TargetApiDTO> resp = Arrays.asList(GSON
                        .fromJson(result.getResponse().getContentAsString(), TargetApiDTO[].class));
        Assert.assertEquals(1, resp.size());
        final TargetApiDTO probe = resp.iterator().next();
        Assert.assertEquals(AccountFieldValueType.values().length, probe.getInputFields().size());
        final List<InputFieldApiDTO> fields = probe.getInputFields();
        for (InputFieldApiDTO field : fields) {
            Assert.assertNotNull(field.getValueType());
        }
    }

    // If no prior validation, discovery in progress should display as "Validating"
    // This is so that adding a target shows up as "Validating" in the UI.
    @Test
    public void testDiscoveryInProgressValidationStatusNoPriorValidation() throws Exception {
        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        when(targetInfo.getStatus())
            .thenReturn(TargetsService.TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS);
        when(targetInfo.getLastValidationTime()).thenReturn(null);

        org.junit.Assert.assertEquals(TargetsService.UI_VALIDATING_STATUS,
            TargetsService.mapStatusToApiDTO(targetInfo));
    }

    // Discovery in progress should be displayed as "Validating"
    @Test
    public void testDiscoveryInProgressValidationStatusWithPriorValidation() throws Exception {
        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        when(targetInfo.getStatus())
            .thenReturn(TargetsService.TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS);
        when(targetInfo.getLastValidationTime()).thenReturn(LocalDateTime.now());

        org.junit.Assert.assertEquals(TargetsService.UI_VALIDATING_STATUS,
            TargetsService.mapStatusToApiDTO(targetInfo));
    }

    // verify adding VC target without setting "isStorageBrowsingEnabled" filed,
    // it will be added with value set to "false".
    @Test
    public void testAddVCTargetWithNoStorageBrowsingFiled() throws Exception {
        final long probeId = 1;
        final String isStorageBrowsingEnabled = "isStorageBrowsingEnabled";
        final String key = "key";
        final ProbeInfo probe = createMockProbeInfo(probeId, SDKProbeType.VCENTER.getProbeType(), "category", createAccountDef
            (key), createAccountDef(isStorageBrowsingEnabled));
        final TargetApiDTO targetDto = new TargetApiDTO();
        targetDto.setType(probe.getType());
        targetDto.setInputFields(Arrays.asList(inputField(key, "value")));
        final String targetString = GSON.toJson(targetDto);
        final MvcResult result = mockMvc
            .perform(MockMvcRequestBuilders.post("/targets")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .content(targetString)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final ArgumentCaptor<TargetData> captor = ArgumentCaptor.forClass(TargetData.class);
        Mockito.verify(topologyProcessor).addTarget(Mockito.eq(probeId), captor.capture());
        final Set<AccountValue> accountDataSet = captor.getValue().getAccountData();
        assertTrue(accountDataSet.stream().anyMatch(accountValue -> accountValue.getName().equals(isStorageBrowsingEnabled)
            && accountValue.getStringValue().equals("false")));
        assertTrue(accountDataSet.stream().anyMatch(accountValue -> accountValue.getName().equals(key)));
        Mockito.verify(topologyProcessor, Mockito.never()).validateAllTargets();
        Mockito.verify(topologyProcessor, Mockito.never()).discoverAllTargets();

        final TargetApiDTO resp = GSON.fromJson(result.getResponse().getContentAsString(),
            TargetApiDTO.class);
        Assert.assertEquals(Long.toString(registeredTargets.keySet().iterator().next()),
            resp.getUuid());
    }


    // Verify use case: probeType: vCenter, isStorageBrowsingEnabled: false
    @Test
    public void testAddVCTargetWithStorageBrowsingFiledFalse() throws Exception {
        final long probeId = 1;
        final String isStorageBrowsingEnabled = "isStorageBrowsingEnabled";
        final String key = "key";
        final ProbeInfo probe = createMockProbeInfo(probeId, SDKProbeType.VCENTER.getProbeType(), "category", createAccountDef
            (key), createAccountDef(isStorageBrowsingEnabled));
        final TargetApiDTO targetDto = new TargetApiDTO();
        targetDto.setType(probe.getType());
        targetDto.setInputFields(Arrays.asList(inputField(isStorageBrowsingEnabled, "false"), inputField(key, "value")));
        final String targetString = GSON.toJson(targetDto);
        final MvcResult result = mockMvc
            .perform(MockMvcRequestBuilders.post("/targets")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .content(targetString)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final ArgumentCaptor<TargetData> captor = ArgumentCaptor.forClass(TargetData.class);
        Mockito.verify(topologyProcessor).addTarget(Mockito.eq(probeId), captor.capture());
        final Set<AccountValue> accountDataSet = captor.getValue().getAccountData();
        assertTrue(accountDataSet.stream().anyMatch(accountValue -> accountValue.getName().equals(isStorageBrowsingEnabled)
            && accountValue.getStringValue().equals("false")));
        assertTrue(accountDataSet.stream().anyMatch(accountValue -> accountValue.getName().equals(key)));
        Mockito.verify(topologyProcessor, Mockito.never()).validateAllTargets();
        Mockito.verify(topologyProcessor, Mockito.never()).discoverAllTargets();

        final TargetApiDTO resp = GSON.fromJson(result.getResponse().getContentAsString(),
            TargetApiDTO.class);
        Assert.assertEquals(Long.toString(registeredTargets.keySet().iterator().next()),
            resp.getUuid());
    }

    // Verify use case: probeType: vCenter, isStorageBrowsingEnabled: true
    @Test
    public void testAddVCTargetWithStorageBrowsingFiledTrue() throws Exception {
        final long probeId = 1;
        final String isStorageBrowsingEnabled = "isStorageBrowsingEnabled";
        final String key = "key";
        final ProbeInfo probe = createMockProbeInfo(probeId, SDKProbeType.VCENTER.getProbeType(), "category", createAccountDef
            (key), createAccountDef(isStorageBrowsingEnabled));
        final TargetApiDTO targetDto = new TargetApiDTO();
        targetDto.setType(probe.getType());
        final String value = "true";
        targetDto.setInputFields(Arrays.asList(inputField(isStorageBrowsingEnabled, value), inputField(key, "value")));
        final String targetString = GSON.toJson(targetDto);
        final MvcResult result = mockMvc
            .perform(MockMvcRequestBuilders.post("/targets")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .content(targetString)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final ArgumentCaptor<TargetData> captor = ArgumentCaptor.forClass(TargetData.class);
        Mockito.verify(topologyProcessor).addTarget(Mockito.eq(probeId), captor.capture());
        final Set<AccountValue> accountDataSet = captor.getValue().getAccountData();
        assertTrue(accountDataSet.stream().anyMatch(accountValue -> accountValue.getName().equals(isStorageBrowsingEnabled)
            && accountValue.getStringValue().equals(value)));
        assertTrue(accountDataSet.stream().anyMatch(accountValue -> accountValue.getName().equals(key)));
        Mockito.verify(topologyProcessor, Mockito.never()).validateAllTargets();
        Mockito.verify(topologyProcessor, Mockito.never()).discoverAllTargets();

        final TargetApiDTO resp = GSON.fromJson(result.getResponse().getContentAsString(),
            TargetApiDTO.class);
        Assert.assertEquals(Long.toString(registeredTargets.keySet().iterator().next()),
            resp.getUuid());
    }

    private TargetApiDTO postAndReturn(String query) throws Exception {
        final MvcResult result = mockMvc
                        .perform(MockMvcRequestBuilders.post("/targets/" + query).accept(
                                        MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final TargetApiDTO resp = GSON.fromJson(result.getResponse().getContentAsString(),
                        TargetApiDTO.class);
        return resp;
    }

    private ProbeInfo createDefaultProbeInfo() throws Exception {
        return createMockProbeInfo(1, "type1", "category1", createAccountDef("targetId"));
    }

    private InputFieldApiDTO inputField(String key, String value) {
        final InputFieldApiDTO result = new InputFieldApiDTO();
        result.setName(key);
        result.setValue(value);
        return result;
    }

    private AccountValue createAccountValue(String key, String value) {
        return new InputField(key, value, Optional.empty());
    }

    private void assertEquals(TargetInfo target, ProbeInfo probe, TargetApiDTO dto) {
        assertEquals(target, dto);
        assertEquals(probe, dto);
    }

    private void assertEquals(ProbeInfo probe, TargetApiDTO dto) {
        Assert.assertEquals(probe.getType(), dto.getType());
        Assert.assertEquals(probe.getCategory(), dto.getCategory());
        Assert.assertEquals(probe.getAccountDefinitions().size(), dto.getInputFields().size());
        final Map<String, AccountDefEntry> defEntries = probe.getAccountDefinitions().stream()
                        .collect(Collectors.toMap(de -> de.getName(), de -> de));
        dto.getInputFields().forEach(inputField -> {
            assertEquals(defEntries.get(inputField.getName()), inputField);
        });
    }

    private void assertEquals(TargetInfo target, TargetApiDTO dto) {
        Assert.assertEquals(TargetsService.mapStatusToApiDTO(target), dto.getStatus());
        // Input fields that have no value set in the TargetInfo
        // still show up in the TargetApiDTO.
        final List<InputFieldApiDTO> filledInputFields = dto.getInputFields().stream()
                .filter(inputField -> inputField.getValue() != null)
                .collect(Collectors.toList());
        Assert.assertEquals(target.getAccountData().size(), filledInputFields.size());
        final Map<String, AccountValue> accountValues = target.getAccountData().stream()
                .collect(Collectors.toMap(
                        AccountValue::getName,
                        Function.identity()));
        filledInputFields.forEach(inputField -> {
            assertEquals(accountValues.get(inputField.getName()), inputField);
        });
        Assert.assertEquals(target.isReadOnly(), dto.isReadonly());
    }

    private void assertEquals(AccountValue accountValue, InputFieldApiDTO dto) {
        Assert.assertEquals(accountValue.getName(), dto.getName());
        Assert.assertEquals(accountValue.getStringValue(), dto.getValue());
    }

    private void assertEquals(AccountDefEntry accountDef, InputFieldApiDTO dto) {
        Assert.assertEquals(accountDef.isRequired(), dto.getIsMandatory());
        Assert.assertEquals(accountDef.getDisplayName(), dto.getDisplayName());
        Assert.assertEquals(accountDef.isSecret(), dto.getIsSecret());
        Assert.assertEquals(accountDef.getDescription(), dto.getDescription());
    }

    private static AccountDefEntry createAccountDef(String key) {
        return createAccountDef(key, AccountFieldValueType.STRING);
    }

    private static AccountDefEntry createAccountDef(String key, AccountFieldValueType valueType) {
        return new AccountField(key, key + "-name", key + "-description", true, false, valueType,
                null, Collections.emptyList(), ".*");
    }

    /**
     * Spring configuration to startup all the beans, necessary for test execution.
     */
    @Configuration
    @EnableWebMvc
    public static class TestConfig extends WebMvcConfigurerAdapter {

        @Bean
        public TopologyProcessor topologyProcessor() {
            return Mockito.mock(TopologyProcessor.class);
        }

        @Bean
        public TargetsService targetsService() {
            return new TargetsService(topologyProcessor(), Duration.ofSeconds(60), Duration.ofSeconds(1),
                Duration.ofSeconds(60), Duration.ofSeconds(1), null,
                apiComponentTargetListener(), repositoryApi(),
                actionSpecMapper(),
                actionRpcService(),
                REALTIME_CONTEXT_ID,
                new ApiWebsocketHandler());
        }

        @Bean
        public RepositoryApi repositoryApi() {
            return Mockito.mock(RepositoryApi.class);
        }

        @Bean
        public ApiComponentTargetListener apiComponentTargetListener() {
            return Mockito.mock(ApiComponentTargetListener.class);
        }

        @Bean
        public ActionSpecMapper actionSpecMapper() {
            return Mockito.mock(ActionSpecMapper.class);
        }

        @Bean
        public PlanServiceMole planService() {
            return spy(PlanServiceMole.class);
        }

        @Bean
        public SettingServiceMole settingServiceMole() {
            return spy(new SettingServiceMole());
        }

        @Bean
        public GroupServiceMole groupService() {
            return spy(new GroupServiceMole());
        }

        @Bean
        public GrpcTestServer grpcTestServer() {
            try {
                final GrpcTestServer testServer = GrpcTestServer.newServer(planService(),
                    groupService(), settingServiceMole());
                testServer.start();
                return testServer;
            } catch (IOException e) {
                throw new BeanCreationException("Failed to create test channel", e);
            }
        }

        @Bean
        public ActionsServiceBlockingStub actionRpcService() {
            return ActionsServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
        }

        @Bean
        public TargetsController targetController() {
            return new TargetsController();
        }

        @Bean
        public GlobalExceptionHandler exceptionHandler() {return new GlobalExceptionHandler();}

        @Override
        public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
            GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
            msgConverter.setGson(ComponentGsonFactory.createGson());
            converters.add(msgConverter);
        }
    }
}
