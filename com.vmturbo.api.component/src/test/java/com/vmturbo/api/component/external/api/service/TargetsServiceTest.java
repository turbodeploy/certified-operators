package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.common.protobuf.utils.StringConstants.COMMUNICATION_BINDING_CHANNEL;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
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
import java.util.Objects;
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
import org.springframework.http.ResponseEntity;
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
import com.vmturbo.api.component.communication.RepositoryApi.SearchRequest;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.mapper.GroupMapper;
import com.vmturbo.api.component.external.api.mapper.PaginationMapper;
import com.vmturbo.api.component.external.api.mapper.TargetMapper;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.ServiceProviderExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketHandler;
import com.vmturbo.api.controller.TargetsController;
import com.vmturbo.api.dto.ErrorApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.target.InputFieldApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.api.handler.GlobalExceptionHandler;
import com.vmturbo.api.pagination.SearchOrderBy;
import com.vmturbo.api.serviceinterfaces.IActionsService;
import com.vmturbo.api.serviceinterfaces.IBusinessUnitsService;
import com.vmturbo.api.serviceinterfaces.IGroupsService;
import com.vmturbo.api.serviceinterfaces.IPoliciesService;
import com.vmturbo.api.serviceinterfaces.IScenariosService;
import com.vmturbo.api.serviceinterfaces.ISchedulesService;
import com.vmturbo.api.serviceinterfaces.ISettingsPoliciesService;
import com.vmturbo.api.serviceinterfaces.IUsersService;
import com.vmturbo.api.utils.ParamStrings;
import com.vmturbo.api.validators.InputDTOValidator;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.common.Pagination;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.search.Search;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.target.TargetDTO;
import com.vmturbo.common.protobuf.target.TargetDTO.SearchTargetsRequest;
import com.vmturbo.common.protobuf.target.TargetDTO.SearchTargetsResponse;
import com.vmturbo.common.protobuf.target.TargetDTOMoles.TargetsServiceMole;
import com.vmturbo.common.protobuf.target.TargetsServiceGrpc;
import com.vmturbo.common.protobuf.target.TargetsServiceGrpc.TargetsServiceBlockingStub;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.platform.sdk.common.util.Pair;
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
import com.vmturbo.topology.processor.api.dto.TargetInputFields;
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
    private static final String TGT_CANT_BE_CREATED = "cannot be created through public APIs.";
    private static final String TGT_CANT_BE_REMOVED = "cannot be removed through public APIs.";
    private static final String PROBE_NOT_FOUND = "Probe not found: ";
    private static final String PUBLIC_CLOUD_CATEGORY = "Public Cloud";

    private static final String TARGET_DISPLAY_NAME = "target name";
    private static final String APP_CONTROL = "app_control";
    private static final Duration MILLIS_100 = Duration.ofMillis(100);
    private static final Duration MILLIS_50 = Duration.ofMillis(50);

    @Autowired
    private TopologyProcessor topologyProcessor;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private MockMvc mockMvc;

    @Autowired
    private WebApplicationContext wac;
    private static final Gson GSON = new Gson();

    @Autowired
    private ApiComponentTargetListener apiComponentTargetListener;

    @Autowired
    private RepositoryApi repositoryApi;

    @Autowired
    private ActionSpecMapper actionSpecMapper;

    @Autowired
    private ActionSearchUtil actionSearchUtil;

    @Autowired
    private TargetsServiceBlockingStub targetsServiceBlockingStub;

    @Autowired
    private TargetsServiceMole targetsServiceMole;

    private Map<Long, ProbeInfo> registeredProbes;
    private Map<Long, TargetInfo> registeredTargets;
    private long idCounter;

    private static final long REALTIME_CONTEXT_ID = 7777777;

    @Autowired
    private TargetsService targetsService;

    @Autowired
    private ApiWebsocketHandler apiWebsocketHandler;

    @Before
    public void init() throws TopologyProcessorException, CommunicationException {
        idCounter = 0;
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();

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

        when(topologyProcessor.getTargets(Mockito.any()))
            .thenAnswer((Answer<List<TargetInfo>>)invocation -> {
                final List<Long> ids = (List<Long>)invocation.getArgumentAt(0, List.class);
                final List<TargetInfo> targetInfos =
                    ids.stream()
                    .map(registeredTargets::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
                return targetInfos;
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
    }

    private ProbeInfo createMockProbeInfo(long probeId, String type, String category, String uiCategory,
            AccountDefEntry... entries) throws Exception {
        return createMockProbeInfo(probeId, type, category, uiCategory, CreationMode.STAND_ALONE, entries);
    }

    private ProbeInfo createMockProbeInfo(long probeId, String type, String category, String uiCategory,
                                          CreationMode creationMode,  AccountDefEntry... entries) throws Exception {
        return createMockProbeInfo(probeId, type, category, uiCategory, creationMode, null, entries);
    }

    private ProbeInfo createMockProbeInfo(long probeId, String type, String category, String uiCategory,
            CreationMode creationMode, String license, AccountDefEntry... entries) throws Exception {
        final ProbeInfo newProbeInfo = Mockito.mock(ProbeInfo.class);
        when(newProbeInfo.getId()).thenReturn(probeId);
        when(newProbeInfo.getType()).thenReturn(type);
        when(newProbeInfo.getCategory()).thenReturn(category);
        when(newProbeInfo.getUICategory()).thenReturn(uiCategory);
        when(newProbeInfo.getAccountDefinitions()).thenReturn(Arrays.asList(entries));
        when(newProbeInfo.getCreationMode()).thenReturn(creationMode);
        when(newProbeInfo.getLicense()).thenReturn(Optional.ofNullable(license));
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
        when(targetInfo.getCommunicationBindingChannel()).thenReturn(Optional.empty());
        when(targetInfo.getStatus()).thenReturn("Validated");
        when(targetInfo.isHidden()).thenReturn(false);
        when(targetInfo.getDisplayName()).thenReturn(TARGET_DISPLAY_NAME);
        registeredTargets.put(targetId, targetInfo);
        return targetInfo;
    }
    private TargetInfo createMockTargetInfo(long probeId, long targetId,
                                            String communicationChannel,
                                            AccountValue... accountValues)
        throws Exception {
        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        when(targetInfo.getId()).thenReturn(targetId);
        when(targetInfo.getProbeId()).thenReturn(probeId);
        when(targetInfo.getAccountData()).thenReturn(
            new HashSet<>(Arrays.asList(accountValues)));
        when(targetInfo.getCommunicationBindingChannel()).thenReturn(Optional.of(communicationChannel));
        when(targetInfo.getStatus()).thenReturn("Validated");
        when(targetInfo.isHidden()).thenReturn(false);
        when(targetInfo.getDisplayName()).thenReturn(TARGET_DISPLAY_NAME);
        registeredTargets.put(targetId, targetInfo);
        return targetInfo;
    }

    private TargetInfo createMockTargetInfo(long probeId, long targetId,
            boolean isHidden, boolean isReadOnly, AccountValue... accountValues)
            throws Exception {
        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        when(targetInfo.getId()).thenReturn(targetId);
        when(targetInfo.getProbeId()).thenReturn(probeId);
        when(targetInfo.getAccountData()).thenReturn(
                new HashSet<>(Arrays.asList(accountValues)));
        when(targetInfo.getStatus()).thenReturn("Validated");
        when(targetInfo.isHidden()).thenReturn(isHidden);
        when(targetInfo.isReadOnly()).thenReturn(isReadOnly);
        when(targetInfo.getDisplayName()).thenReturn(TARGET_DISPLAY_NAME);
        registeredTargets.put(targetId, targetInfo);
        return targetInfo;
    }

    private TargetInfo createMockHiddenTargetInfo(long probeId, long targetId, AccountValue... accountValues)
            throws Exception {
        return createMockTargetInfo(probeId, targetId, true, false, accountValues);
    }

    private TargetInfo createMockReadOnlyTargetInfo(long probeId, long targetId,
            AccountValue... accountValues) throws Exception {
        return createMockTargetInfo(probeId, targetId, false, true, accountValues);
    }

    /**
     * Tests getting a target by id.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void getTarget() throws Exception {
        final ProbeInfo probe = createMockProbeInfo(1, "type", "category", "uiCategory",
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
        final ProbeInfo probe = createMockProbeInfo(1, "type", "category", "uiCategory",
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
     * Tests getting a target by id with invalid fields. These may be fields that are no
     * longer needed and should be ignored.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void getTargetWithInvalidField() throws Exception {
        final ProbeInfo probe = createMockProbeInfo(1, "type", "category", "uiCategory",
                createAccountDef("field1"), createAccountDef("field2"));
        createMockTargetInfo(probe.getId(), 3,
                        createAccountValue("field1", "value1"),
                        createAccountValue("field3", "value2"));

        final MvcResult result = mockMvc.perform(get("/targets/3").accept(MediaType
                .APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final TargetApiDTO resp = GSON.fromJson(result.getResponse().getContentAsString(),
            TargetApiDTO.class);
        Assert.assertThat(resp.getInputFields().size(), equalTo(2));
        Assert.assertThat(resp.getInputFields().get(0).getName(), not(equalTo("field3")));
        Assert.assertThat(resp.getInputFields().get(1).getName(), not(equalTo("field3")));
    }

    /**
     * Tests getting target by id, while target is not present.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void getAbsentTarget() throws Exception {
        final ProbeInfo probe = createMockProbeInfo(1, "type", "category", "uiCategory");
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
        // ARRANGE
        final ProbeInfo probe = createMockProbeInfo(1, "type", "category", "uiCategory");
        final Collection<TargetInfo> targets = new ArrayList<>();
        targets.add(createMockTargetInfo(probe.getId(), 2));
        targets.add(createMockTargetInfo(probe.getId(), 3));
        targets.add(createMockTargetInfo(probe.getId(), 4));
        when(targets.iterator().next().getStatus()).thenReturn("Connection refused");

        final ArgumentCaptor<SearchTargetsRequest> captor =
            ArgumentCaptor.forClass(SearchTargetsRequest.class);
        when(targetsServiceMole.searchTargets(captor.capture())).thenReturn(
            SearchTargetsResponse.newBuilder()
                .addTargets(2L)
                .addTargets(3L)
                .addTargets(4L)
                .setPaginationResponse(Pagination.PaginationResponse.newBuilder()
                    .setTotalRecordCount(3).build())
                .build());

        // ACT
        final String url = "/targets" + (hybridFilterExists ? "?environmentType=HYBRID" : "");
        final MvcResult result = mockMvc
                        .perform(get(url).accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final TargetApiDTO[] resp = GSON.fromJson(result.getResponse().getContentAsString(),
                        TargetApiDTO[].class);

        // ASSERT
        Assert.assertEquals(targets.size(), resp.length);
        final Map<Long, TargetApiDTO> map = Arrays.asList(resp).stream()
                        .collect(Collectors.toMap(tad -> Long.valueOf(tad.getUuid()), tad -> tad));
        for (TargetInfo target : targets) {
            final TargetApiDTO dto = map.get(target.getId());
            assertEquals(target, probe, dto);
        }

        checkHasTargetVisibilityFilter(captor.getValue());
    }

    /**
     * Tests if the service can filter and return the cloud targets.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetAllCloudTargets() throws Exception {
        testFilterTargetsByEnvironment(true, false);
    }

    /**
     * Tests if the service can filter and return the onprem targets.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetAllOnPremTargets() throws Exception {
        testFilterTargetsByEnvironment(false, false);
    }

    /**
     * Tests if the service can filter and return the cloud targets based on target category.
     *
     * @throws Exception should not happen.
     */
    @Test
    public void testGetAllCloudTargetwithCategory() throws Exception {
        testFilterTargetsByEnvironment(true, true);
    }

    private void testFilterTargetsByEnvironment(boolean cloud, boolean cloudTargetCategory) throws Exception {
        // ARRANGE
        final long cloudProbeId = 1L;
        final long onPremProbeId = 2L;
        final long cloudTargetId = 3L;
        final long onPremTargetId = 4L;

        createMockProbeInfo(cloudProbeId, SDKProbeType.AWS.getProbeType(), "dummy", PUBLIC_CLOUD_CATEGORY);
        createMockProbeInfo(onPremProbeId, SDKProbeType.VCENTER.getProbeType(), "dummy", "uiCategory");
        createMockTargetInfo(cloudProbeId, cloudTargetId);
        createMockTargetInfo(onPremProbeId, onPremTargetId);

        final ArgumentCaptor<SearchTargetsRequest> captor =
            ArgumentCaptor.forClass(SearchTargetsRequest.class);
        when(targetsServiceMole.searchTargets(captor.capture())).thenReturn(
            SearchTargetsResponse.newBuilder()
            .addTargets(cloud ? 3L : 4L)
            .setPaginationResponse(Pagination.PaginationResponse.newBuilder()
                .setTotalRecordCount(1).build())
            .build());

        // ACT
        final String url = "/targets?environment_type=" + (cloud ? "CLOUD" : "ONPREM") +
                (cloudTargetCategory ? "&target_category=" + PUBLIC_CLOUD_CATEGORY: "");
        final MvcResult result = mockMvc.perform(get(url).accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                                    .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final TargetApiDTO[] resp = GSON.fromJson(result.getResponse().getContentAsString(),
                TargetApiDTO[].class);

        // ASSERT
        Assert.assertEquals(1, resp.length);
        Assert.assertEquals(Long.toString(cloud ? cloudTargetId : onPremTargetId), resp[0].getUuid());

        // verify the search request
        SearchTargetsRequest request = captor.getValue();
        checkHasTargetVisibilityFilter(request);

        // Verify the category
        if (cloudTargetCategory) {
            Assert.assertEquals(PUBLIC_CLOUD_CATEGORY, resp[0].getCategory());
        }

        Optional<Search.PropertyFilter> probeTypeFilter = getPropertyByName(request,
            SearchableProperties.PROBE_TYPE);
        Assert.assertTrue(probeTypeFilter.isPresent());
        Assert.assertTrue(probeTypeFilter.get().hasStringFilter());
        Assert.assertEquals(probeTypeFilter.get().getStringFilter().getPositiveMatch(), cloud);
        Assert.assertThat(new HashSet<>(probeTypeFilter.get().getStringFilter().getOptionsList()),
            is(GroupMapper.CLOUD_ENVIRONMENT_PROBE_TYPES));
    }

    private void checkHasTargetVisibilityFilter(SearchTargetsRequest request) {
        Optional<Search.PropertyFilter> propertyFilter = getPropertyByName(request,
            SearchableProperties.IS_TARGET_HIDDEN);
        Assert.assertTrue(propertyFilter.isPresent());
        Assert.assertTrue(propertyFilter.get().hasStringFilter());
        Assert.assertThat(propertyFilter.get().getStringFilter().getOptionsList(),
            containsInAnyOrder("false"));
        Assert.assertTrue(propertyFilter.get().getStringFilter().getPositiveMatch());
    }

    private Optional<Search.PropertyFilter> getPropertyByName(SearchTargetsRequest request,
                                                              String propertyName) {
        return request.getPropertyFilterList()
            .stream()
            .filter(r -> propertyName.equals(r.getPropertyName()))
            .findAny();
    }

    /**
     * Tests the case where the user's environment includes a "parent" target and two derived targets.
     * Verifies that the parent's derivedTarget Dtos have been created correctly.
     *
     * @throws Exception Not expected to happen.
     */
    @Test
    public void testGetAllTargets_withDerivedTargetRelationships() throws Exception {
        final ProbeInfo probe = createMockProbeInfo(1, "type", "category", "uiCategory");
        final TargetInfo parentTargetInfo = createMockTargetInfo(probe.getId(), 2);
        final TargetInfo childTargetInfo1 = createMockTargetInfo(probe.getId(), 3);
        final TargetInfo childTargetInfo2 = createMockTargetInfo(probe.getId(), 4);

        final ArgumentCaptor<SearchTargetsRequest> captor =
            ArgumentCaptor.forClass(SearchTargetsRequest.class);
        when(targetsServiceMole.searchTargets(captor.capture())).thenReturn(
            SearchTargetsResponse.newBuilder()
                .addTargets(2)
                .setPaginationResponse(Pagination.PaginationResponse.newBuilder()
                    .setTotalRecordCount(1).build())
                .build());

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
        final ProbeInfo probe = createMockProbeInfo(1, "type", "category", "uiCategory");
        final Collection<TargetInfo> targets = new ArrayList<>();
        targets.add(createMockTargetInfo(probe.getId(), 2));
        targets.add(createMockTargetInfo(probe.getId(), 3));
        targets.add(createMockHiddenTargetInfo(probe.getId(), 4));
        when(targets.iterator().next().getStatus()).thenReturn("Connection refused");

        final ArgumentCaptor<SearchTargetsRequest> captor =
            ArgumentCaptor.forClass(SearchTargetsRequest.class);
        when(targetsServiceMole.searchTargets(captor.capture())).thenReturn(
            SearchTargetsResponse.newBuilder()
                .addTargets(2L)
                .addTargets(3L)
                .setPaginationResponse(Pagination.PaginationResponse.newBuilder()
                    .setTotalRecordCount(2).build())
                .build());

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

        checkHasTargetVisibilityFilter(captor.getValue());
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

        final ArgumentCaptor<SearchTargetsRequest> captor =
            ArgumentCaptor.forClass(SearchTargetsRequest.class);
        when(targetsServiceMole.searchTargets(captor.capture())).thenReturn(
            SearchTargetsResponse.newBuilder()
                .addTargets(2L)
                .addTargets(3L)
                .addTargets(4L)
                .setPaginationResponse(Pagination.PaginationResponse.newBuilder()
                    .setTotalRecordCount(2).build())
                .build());

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

        checkHasTargetVisibilityFilter(captor.getValue());
    }

    /**
     * Tests target addition. This method expected not to trigger validation and discovery.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testAddTarget() throws Exception {
        final long probeId = 1;
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category", "uiCategory", createAccountDef
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
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category", "uiCategory", createAccountDef
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
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category", "uiCategory", createAccountDef
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
     * Tests adding derived target (, and Target Listener's setValidatedFirstTarget method
     * should be invoked with argument "true".
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testAddDerivedTarget() throws Exception {
        final long probeId = 1;
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category", "uiCategory",
                CreationMode.DERIVED, createAccountDef("key"));
        final TargetApiDTO targetDto = new TargetApiDTO();
        targetDto.setType(probe.getType());
        targetDto.setInputFields(Arrays.asList(inputField("key", "value")));
        final String targetString = GSON.toJson(targetDto);
        when(topologyProcessor.getAllTargets()).thenReturn(Collections.EMPTY_SET);
        final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.post("/targets")
                .contentType(MediaType.APPLICATION_JSON)
                .content(targetString)
                .accept(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(MockMvcResultMatchers.status().is4xxClientError()).andReturn();
        final ErrorApiDTO resp = GSON.fromJson(result.getResponse().getContentAsString(),
                ErrorApiDTO.class);
        Assert.assertThat(resp.getMessage(), CoreMatchers.containsString(TGT_CANT_BE_CREATED));
    }

    /**
     * Tests that adding a target with a communication channel among the {@link InputFieldApiDTO}
     * results in the correct requests, with a {@link TargetData} containing the channel.
     *
     * @throws Exception on exceptions occur.
     */
    @Test
    public void testAddTargetWithCommunicationChannel() throws Exception {
        final long probeId = 1;
        final long targetId = 2;
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category", "uiCategory", createAccountDef
            ("key"));
        final String channel = "channel";
        createMockTargetInfo(probe.getId(), targetId);
        final TargetApiDTO targetDto = new TargetApiDTO();
        targetDto.setType(probe.getType());
        targetDto.setInputFields(Arrays.asList(inputField(COMMUNICATION_BINDING_CHANNEL,
            "channel")));
        final String targetString = GSON.toJson(targetDto);
        when(topologyProcessor.getAllTargets()).thenReturn(Collections.EMPTY_SET);
        final MvcResult result = mockMvc.perform(MockMvcRequestBuilders.post("/targets")
            .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
            .content(targetString)
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final ArgumentCaptor<TargetData> captor = ArgumentCaptor.forClass(TargetData.class);
        Mockito.verify(topologyProcessor).addTarget(Mockito.eq(probeId), captor.capture());
        Assert.assertEquals(channel, captor.getValue().getCommunicationBindingChannel().get());
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
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category", "uiCategory");
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
        final ArgumentCaptor<TargetInputFields> captor = ArgumentCaptor.forClass(TargetInputFields.class);
        Mockito.verify(topologyProcessor).modifyTarget(Mockito.eq(targetId), captor.capture());
        final Collection<AccountValue> inputFields = captor.getValue().getAccountData();
        Assert.assertEquals(targetDto.getInputFields().size(), inputFields.size());
        final Map<String, InputFieldApiDTO> expectedFieldsMap = targetDto.getInputFields().stream()
                        .collect(Collectors.toMap(field -> field.getName(), field -> field));
        inputFields.forEach(av -> assertEquals(av, expectedFieldsMap.get(av.getName())));
    }

    /**
     * Tests for modifying target passing a communication channel.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testEditTargetWithCommunicationChannel() throws Exception {
        final long probeId = 1;
        final long targetId = 2;
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category", "uiCategory");
        final String channel = "channel";
        final TargetApiDTO targetDtoWithChannel = new TargetApiDTO();
        final TargetInfo targetInfo = createMockTargetInfo(probe.getId(), targetId);
        targetDtoWithChannel.setType(probe.getType());
        targetDtoWithChannel.setInputFields(Arrays.asList(inputField(COMMUNICATION_BINDING_CHANNEL,
            channel)));
        final String targetString = GSON.toJson(targetDtoWithChannel);

        mockMvc
            .perform(MockMvcRequestBuilders.put("/targets/" + targetId)
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .content(targetString)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final ArgumentCaptor<TargetInputFields> captor = ArgumentCaptor.forClass(TargetInputFields.class);
        Mockito.verify(topologyProcessor).modifyTarget(Mockito.eq(targetId), captor.capture());
        Assert.assertEquals(channel, captor.getValue().getCommunicationBindingChannel().get());
    }

    /**
     * Tests for modifying target without passing a communication channel. In this case we should
     * be passing an empty communication channel
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testEditTargetWithoutCommunicationChannel() throws Exception {
        final long probeId = 1;
        final long targetId = 2;
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category", "uiCategory");
        final TargetInfo targetInfo = createMockTargetInfo(probe.getId(), targetId);
        final String channel = "channel";

        final TargetApiDTO targetDtoWithoutChannel = new TargetApiDTO();
        targetDtoWithoutChannel.setType(probe.getType());
        targetDtoWithoutChannel.setInputFields(Arrays.asList(inputField("field",
            "value")));
        final String targetString = GSON.toJson(targetDtoWithoutChannel);
        mockMvc
            .perform(MockMvcRequestBuilders.put("/targets/" + targetId)
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .content(targetString)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final ArgumentCaptor<TargetInputFields> captor = ArgumentCaptor.forClass(TargetInputFields.class);
        Mockito.verify(topologyProcessor).modifyTarget(Mockito.eq(targetId), captor.capture());
        Assert.assertFalse(captor.getValue().getCommunicationBindingChannel().isPresent());
    }

    /**
     * Tests for deleting the communication channel of a target. This is achieved by passing an
     * empty string as the value of the communication channel field
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testDeleteTargetCommunicationChannel() throws Exception {
        final long probeId = 1;
        final long targetId = 2;
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category", "uiCategory");
        final String channel = "channel";
        final TargetInfo targetInfo = createMockTargetInfo(probe.getId(), targetId);

        final TargetApiDTO targetDtoWithoutChannel = new TargetApiDTO();
        targetDtoWithoutChannel.setType(probe.getType());
        targetDtoWithoutChannel.setInputFields(Arrays.asList(inputField(COMMUNICATION_BINDING_CHANNEL,
            "")));
        final String targetString = GSON.toJson(targetDtoWithoutChannel);
        mockMvc
            .perform(MockMvcRequestBuilders.put("/targets/" + targetId)
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
                .content(targetString)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final ArgumentCaptor<TargetInputFields> captor = ArgumentCaptor.forClass(TargetInputFields.class);
        Mockito.verify(topologyProcessor).modifyTarget(Mockito.eq(targetId), captor.capture());
        Assert.assertEquals("", captor.getValue().getCommunicationBindingChannel().get());
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
        final ProbeInfo probe = createMockProbeInfo(probeId, "type", "category", "uiCategory");
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
        final ProbeInfo probe = createMockProbeInfo(prpbeId, "type", "category", "uiCategory");
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
                .getTarget(Mockito.anyLong());
        final MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.delete("/targets/" + targetId).accept(
                        MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(MockMvcResultMatchers.status().is4xxClientError()).andReturn();
        final ErrorApiDTO resp =
                GSON.fromJson(result.getResponse().getContentAsString(), ErrorApiDTO.class);
        Mockito.verify(topologyProcessor).getTarget(targetId);
        Assert.assertThat(resp.getMessage(), CoreMatchers.containsString(TGT_NOT_FOUND));
    }

    /**
     * Tests removal of a read-only target.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void deleteReadOnlyTarget() throws Exception {
        final long prpbeId = 1;
        final long targetId = 2;
        final ProbeInfo probe = createMockProbeInfo(prpbeId, "type", "category", "uiCategory");
        final TargetInfo targetInfo = createMockReadOnlyTargetInfo(probe.getId(), targetId);
        final MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.delete("/targets/" + targetId).accept(
                        MediaType.APPLICATION_JSON_VALUE))
                .andExpect(MockMvcResultMatchers.status().is4xxClientError()).andReturn();
        final ErrorApiDTO resp = GSON.fromJson(result.getResponse().getContentAsString(),
                ErrorApiDTO.class);
        Assert.assertThat(resp.getMessage(), CoreMatchers.containsString(TGT_CANT_BE_REMOVED));
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
        final TargetsService targetsService = new TargetsService(topologyProcessor, MILLIS_50,
                        MILLIS_100, MILLIS_50, MILLIS_100, Mockito.mock(LicenseCheckClient.class),
                        apiComponentTargetListener, repositoryApi, actionSpecMapper,
                        actionSearchUtil, apiWebsocketHandler,
                targetsServiceBlockingStub, new PaginationMapper(), true, 100);

        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        when(targetInfo.getId()).thenReturn(targetId);
        when(targetInfo.getProbeId()).thenReturn(probeId);
        when(targetInfo.getStatus()).thenReturn(StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_IN_PROGRESS);

        when(topologyProcessor.getTarget(targetId)).thenReturn(targetInfo);
        TargetInfo validationInfo = targetsService.validateTargetSynchronously(targetId);

        Mockito.verify(topologyProcessor, times(2)).getTarget(targetId);
        org.junit.Assert.assertEquals(
            StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_IN_PROGRESS, validationInfo.getStatus());
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
                        topologyProcessor, MILLIS_50, MILLIS_100, MILLIS_50, MILLIS_100,
                        Mockito.mock(LicenseCheckClient.class), apiComponentTargetListener,
                        repositoryApi, actionSpecMapper, actionSearchUtil,
                        apiWebsocketHandler, targetsServiceBlockingStub,
                new PaginationMapper(), true, 100);

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
                        topologyProcessor, MILLIS_50, MILLIS_100, MILLIS_50, MILLIS_100,
                        Mockito.mock(LicenseCheckClient.class), apiComponentTargetListener,
                        repositoryApi, actionSpecMapper, actionSearchUtil,
                        apiWebsocketHandler, targetsServiceBlockingStub,
                        new PaginationMapper(), true, 100);

        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        when(targetInfo.getId()).thenReturn(targetId);
        when(targetInfo.getProbeId()).thenReturn(probeId);
        when(targetInfo.getStatus()).thenReturn(StringConstants.TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS);

        when(topologyProcessor.getTarget(targetId)).thenReturn(targetInfo);
        TargetInfo discoveryInfo = targetsService.discoverTargetSynchronously(targetId);

        Mockito.verify(topologyProcessor, times(2)).getTarget(targetId);
        org.junit.Assert.assertEquals(
            StringConstants.TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS, discoveryInfo.getStatus());
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
        probesCollection.add(createMockProbeInfo(1, "type1", "category1", "uiCategory1",
                createAccountDef(field1), createAccountDef("field12")));
        probesCollection.add(createMockProbeInfo(2, "type2", "category2", "uiCategory2",
                createAccountDef(field2), createAccountDef("field22")));
        probesCollection.add(createMockProbeInfo(3, "type3", "category4", "uiCategory4",
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
        probesCollection.add(createMockProbeInfo(1, "type1", "category1", "uiCategory1",
                createAccountDef(field1), createAccountDef("field12")));
        probesCollection.add(createMockProbeInfo(2, "type2", ProbeCategory.BILLING.getCategory(),
                ProbeCategory.BILLING.getCategory(),
                CreationMode.DERIVED, createAccountDef(field2), createAccountDef("field22")));
        probesCollection.add(createMockProbeInfo(3, "type3", ProbeCategory.STORAGE_BROWSING.getCategory(),
                ProbeCategory.STORAGE_BROWSING.getCategory(),
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
                createMockProbeInfo(1, "type1", "category1", "uiCategory1", createAccountDef("targetId"));
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
                "uiCategory1",
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
                    false, AccountFieldValueType.LIST, null, allowedValuesList, ".*", null);
        final ProbeInfo probeInfo =
                createMockProbeInfo(1, "type1", "category1", "uiCategory1", allowedValuesField);
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
        Assert.assertNull(fields.get(0).getDependencyKey());
        Assert.assertNull(fields.get(0).getDependencyValue());
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
        createMockProbeInfo(1, "type1", "category1", "uiCategory1", accountEntries);
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

    /**
     * Tests that "dependencyField" is propagated from {@link ProbeInfo} to {@link TargetApiDTO}.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void testAccountValueFieldDependency() throws Exception {
        final String key = "allowedValues";
        final AccountField field =
                new AccountField(key, key + "-name", key + "-description", false, false,
                    false, AccountFieldValueType.LIST, null, null, ".*",
                        Pair.create("field", "value"));
        final ProbeInfo probeInfo =
                createMockProbeInfo(1, "type1", "category1", "uiCategory1", field);
        final MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.get("/targets/specs")
                        .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        final List<TargetApiDTO> resp = Arrays.asList(GSON
                .fromJson(result.getResponse().getContentAsString(), TargetApiDTO[].class));
        Assert.assertEquals(1, resp.size());
        final TargetApiDTO probe = resp.iterator().next();
        final List<InputFieldApiDTO> fields = probe.getInputFields();
        Assert.assertEquals("field", fields.get(0).getDependencyKey());
        Assert.assertEquals("value", fields.get(0).getDependencyValue());
    }

    // If no prior validation, discovery in progress should display as "Validating"
    // This is so that adding a target shows up as "Validating" in the UI.
    @Test
    public void testDiscoveryInProgressValidationStatusNoPriorValidation() throws Exception {
        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        when(targetInfo.getStatus())
            .thenReturn(StringConstants.TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS);
        when(targetInfo.getLastValidationTime()).thenReturn(null);

        org.junit.Assert.assertEquals(TargetMapper.UI_VALIDATING_STATUS,
            TargetMapper.mapStatusToApiDTO(targetInfo));
    }

    // Discovery in progress should be displayed as "Validating"
    @Test
    public void testDiscoveryInProgressValidationStatusWithPriorValidation() throws Exception {
        final TargetInfo targetInfo = Mockito.mock(TargetInfo.class);
        when(targetInfo.getStatus())
            .thenReturn(StringConstants.TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS);
        when(targetInfo.getLastValidationTime()).thenReturn(LocalDateTime.now());

        org.junit.Assert.assertEquals(TargetMapper.UI_VALIDATING_STATUS,
            TargetMapper.mapStatusToApiDTO(targetInfo));
    }

    // verify adding VC target without setting "isStorageBrowsingEnabled" filed,
    // it will be added with value set to "false".
    @Test
    public void testAddVCTargetWithNoStorageBrowsingFiled() throws Exception {
        final long probeId = 1;
        final String isStorageBrowsingEnabled = "isStorageBrowsingEnabled";
        final String key = "key";
        final ProbeInfo probe = createMockProbeInfo(probeId, SDKProbeType.VCENTER.getProbeType(), "category", "category", createAccountDef
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
        final ProbeInfo probe = createMockProbeInfo(probeId, SDKProbeType.VCENTER.getProbeType(), "category", "category", createAccountDef
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
        final ProbeInfo probe = createMockProbeInfo(probeId, SDKProbeType.VCENTER.getProbeType(), "category", "category", createAccountDef
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

    /**
     * Verify target management public APIs are not allowed when `allowTargetManagement` is set to false.
     *
     * @throws Exception security exception.
     */
    @Test(expected = IllegalStateException.class)
    public void testDisableTargetChangesInIntegrationMode() throws Exception {
        final TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);
        final long probeId = 2;
        final long targetId = 3;
        final long uuid = 111;
        final TargetInfo targetInfo = mock(TargetInfo.class);
        when(targetInfo.getId()).thenReturn(targetId);
        when(targetInfo.getProbeId()).thenReturn(probeId);
        when(targetInfo.getCommunicationBindingChannel()).thenReturn(Optional.empty());
        when(targetInfo.getStatus()).thenReturn(StringConstants.TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS);

        when(topologyProcessor.getTarget(targetId)).thenReturn(targetInfo);
        when(topologyProcessor.getTarget(uuid)).thenReturn(targetInfo);

        final ArgumentCaptor<SearchTargetsRequest> captor =
            ArgumentCaptor.forClass(SearchTargetsRequest.class);
        when(targetsServiceMole.searchTargets(captor.capture())).thenReturn(
            SearchTargetsResponse.newBuilder()
                .setPaginationResponse(PaginationResponse.newBuilder()
                    .setTotalRecordCount(0).build())
                .build());

        final TargetsService targetsService =
                new TargetsService(topologyProcessor, MILLIS_50, MILLIS_100, MILLIS_50,
                                   MILLIS_100, mock(LicenseCheckClient.class),
                                   apiComponentTargetListener, repositoryApi, actionSpecMapper,
                                   actionSearchUtil, apiWebsocketHandler, targetsServiceBlockingStub,
                                   new PaginationMapper(), false, 100);
        // Get is allowed
        targetsService.getTargets();
        try {
            // create is NOT allowed
            targetsService.createTarget("", Collections.emptyList());
            fail("should fail to manage target");
        } catch (IllegalStateException e) {
            // expected
        }
        try {
            // Edit is is NOT allowed
            targetsService.editTarget(String.valueOf(uuid), Collections.emptyList());
            fail("should fail to manage target");
        } catch (IllegalStateException e) {
            // expected
        }
        // Delete is NOT allowed
        targetsService.deleteTarget(String.valueOf(uuid));
    }

    /**
     * Test the case where target stats endpoint has been called with no parameter.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testTargetStatsNoParameters() throws Exception {
        // ARRANGE

        final ArgumentCaptor<TargetDTO.GetTargetsStatsRequest> captor =
            ArgumentCaptor.forClass(TargetDTO.GetTargetsStatsRequest.class);
        when(targetsServiceMole.getTargetsStats(captor.capture())).thenReturn(
            TargetDTO.GetTargetsStatsResponse.newBuilder()
                .build());

        // ACT
        mockMvc
            .perform(get("/targets/stats").accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        // ASSERT
        assertThat(captor.getValue().getGroupByCount(), is(0));
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
        return createMockProbeInfo(1, "type1", "category1", "category1", createAccountDef("targetId"));
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
        Assert.assertEquals(probe.getUICategory(), dto.getCategory());
        Assert.assertEquals(probe.getAccountDefinitions().size(), dto.getInputFields().size());
        final Map<String, AccountDefEntry> defEntries = probe.getAccountDefinitions().stream()
                        .collect(Collectors.toMap(de -> de.getName(), de -> de));
        dto.getInputFields().forEach(inputField -> {
            assertEquals(defEntries.get(inputField.getName()), inputField);
        });
    }

    private void assertEquals(TargetInfo target, TargetApiDTO dto) {
        Assert.assertEquals(TargetMapper.mapStatusToApiDTO(target), dto.getStatus());
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
        return new AccountField(key, key + "-name", key + "-description", true, false, false,
            valueType, null, Collections.emptyList(), ".*", null);
    }

    /**
     * Spring configuration to startup all the beans, necessary for test execution.
     */
    @Configuration
    @EnableWebMvc
    public static class TestConfig extends WebMvcConfigurerAdapter {

        @Bean
        public InputDTOValidator inputDTOValidator() {
            return new InputDTOValidator();
        }

        @Bean
        public IPoliciesService policyService() {
            return Mockito.mock(IPoliciesService.class);
        }

        @Bean
        public IGroupsService groupComponentService() {
            return Mockito.mock(IGroupsService.class);
        }

        @Bean
        public IBusinessUnitsService buService() {
            return Mockito.mock(IBusinessUnitsService.class);
        }

        @Bean
        public IUsersService userService() {
            return Mockito.mock(IUsersService.class);
        }

        @Bean
        public ISettingsPoliciesService settingsPolicyService() {
            return Mockito.mock(ISettingsPoliciesService.class);
        }

        @Bean
        public IScenariosService scenariosService() {
            return Mockito.mock(IScenariosService.class);
        }

        @Bean
        public ISchedulesService schedulesService() {
            return Mockito.mock(ISchedulesService.class);
        }

        @Bean
        public IActionsService actionsService() {
            return Mockito.mock(IActionsService.class);
        }

        @Bean
        public TopologyProcessor topologyProcessor() {
            return Mockito.mock(TopologyProcessor.class);
        }

        @Bean
        public ApiWebsocketHandler apiWebsocketHandler() {
            return new ApiWebsocketHandler();
        }

        @Bean
        public ActionSearchUtil actionSearchUtil() {
            return new ActionSearchUtil(actionRpcService(), actionSpecMapper(),
                Mockito.mock(PaginationMapper.class),
                Mockito.mock(SupplyChainFetcherFactory.class),
                Mockito.mock(GroupExpander.class),
                Mockito.mock(ServiceProviderExpander.class),
                REALTIME_CONTEXT_ID);
        }

        @Bean
        public TargetsService targetsService() {
            return new TargetsService(topologyProcessor(), Duration.ofSeconds(60),
                            Duration.ofSeconds(1), Duration.ofSeconds(60),
                            Duration.ofSeconds(1), Mockito.mock(LicenseCheckClient.class),
                            apiComponentTargetListener(), repositoryApi(), actionSpecMapper(),
                actionSearchUtil(),
                apiWebsocketHandler(), targetsRpcService(), new PaginationMapper(),
                true, 100);
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
        public TargetsServiceMole targetServiceMole() {
            return spy(new TargetsServiceMole());
        }

        @Bean
        public GroupServiceMole groupService() {
            return spy(new GroupServiceMole());
        }

        @Bean
        public GrpcTestServer grpcTestServer() {
            try {
                final GrpcTestServer testServer = GrpcTestServer.newServer(planService(),
                    groupService(), settingServiceMole(), targetServiceMole());
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
        public TargetsServiceBlockingStub targetsRpcService() {
            return TargetsServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
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

    /**
     * Tests backwards compatibility of getEntitiesByTargetUuid non-paginated call.
     * @throws Exception error in thread interruption or converting api dtos
     */
    @Test
    public void getEntitiesByTargetUuidWithoutPagination() throws Exception {
        //GIVEN
        String targetUuid = "123";

        SearchRequest searchRequest = Mockito.mock(SearchRequest.class);
        doReturn(searchRequest).when(repositoryApi).newSearchRequest(Mockito.any());

        List<ServiceEntityApiDTO> entities = Collections.EMPTY_LIST;
        doReturn(entities).when(searchRequest).getSEList();

        //WHEN
        ResponseEntity<List<ServiceEntityApiDTO>> response =
                this.targetsService.getEntitiesByTargetUuid(targetUuid, null, null, null, null);

        //THEN
        verify(searchRequest, times(1) ).getSEList();
        Assert.assertEquals(response.getBody(), entities);
        assertFalse(response.getHeaders().containsKey("X-Previous-Cursor"));
        assertFalse(response.getHeaders().containsKey("X-Total-Record-Count"));
        assertTrue(response.getHeaders().containsKey("X-Next-Cursor"));
    }

    /**
     * Tests getEntitiesByTargetUuid setting pagination parameters correctly.
     *
     * @throws Exception error in thread interruption or converting api dtos
     */
    @Test
    public void getEntitiesByTargetUuidWithAllPaginationArgs() throws Exception {
        //GIVEN
        String targetUuid = "123";
        final String cursor = "1";
        final Integer limit = 1;
        boolean ascending = true;
        final String searchOrderBy = SearchOrderBy.DEFAULT;


        SearchRequest searchRequestMock = Mockito.mock(SearchRequest.class);
        doReturn(searchRequestMock).when(repositoryApi).newSearchRequest(Mockito.any());
        doReturn(null).when(searchRequestMock).getPaginatedSEList(Mockito.any());

        ArgumentCaptor<Pagination.PaginationParameters> argument = ArgumentCaptor.forClass(Pagination.PaginationParameters.class);

        //WHEN
        this.targetsService.getEntitiesByTargetUuid(targetUuid, cursor, limit, ascending, searchOrderBy);

        //THEN
        verify(searchRequestMock, times(1) ).getPaginatedSEList(Mockito.any());
        verify(searchRequestMock).getPaginatedSEList(argument.capture());

        Pagination.PaginationParameters paginationParameters = argument.getValue();
        Assert.assertEquals(paginationParameters.getCursor(), cursor);
        Assert.assertEquals(paginationParameters.getLimit(), 1);
        Assert.assertEquals(paginationParameters.getAscending(), true);
        Assert.assertEquals(paginationParameters.getOrderBy().getSearch(), OrderBy.SearchOrderBy.ENTITY_NAME);
    }

    /**
     * Tests getEntitiesByTargetUuid setting pagination parameters correctly limit missing.
     *
     * @throws Exception error in thread interruption or converting api dtos
     */
    @Test
    public void getEntitiesByTargetUuidWithPaginationLimitMissing() throws Exception {
        //GIVEN
        String targetUuid = "123";
        final String cursor = "1";
        boolean ascending = true;
        final String searchOrderBy = SearchOrderBy.DEFAULT;


        SearchRequest searchRequestMock = Mockito.mock(SearchRequest.class);
        doReturn(searchRequestMock).when(repositoryApi).newSearchRequest(Mockito.any());
        doReturn(null).when(searchRequestMock).getPaginatedSEList(Mockito.any());

        ArgumentCaptor<Pagination.PaginationParameters> argument = ArgumentCaptor.forClass(Pagination.PaginationParameters.class);

        //WHEN
        this.targetsService.getEntitiesByTargetUuid(targetUuid, cursor, null, ascending, searchOrderBy);

        //THEN
        verify(searchRequestMock, times(1) ).getPaginatedSEList(Mockito.any());
        verify(searchRequestMock).getPaginatedSEList(argument.capture());

        Pagination.PaginationParameters paginationParameters = argument.getValue();
        Assert.assertEquals(paginationParameters.getCursor(), cursor);
        Assert.assertTrue(paginationParameters.hasLimit());
        Assert.assertEquals(paginationParameters.getAscending(), true);
        Assert.assertEquals(paginationParameters.getOrderBy().getSearch(), OrderBy.SearchOrderBy.ENTITY_NAME);
    }

    /**
     * Tests getEntitiesByTargetUuid setting pagination parameters correctly cursor missing.
     *
     * @throws Exception error in thread interruption or converting api dtos
     */
    @Test
    public void getEntitiesByTargetUuidWithPaginationCursorMissing() throws Exception {
        //GIVEN
        String targetUuid = "123";
        int limit = 1;
        boolean ascending = true;
        final String searchOrderBy = SearchOrderBy.DEFAULT;


        SearchRequest searchRequestMock = Mockito.mock(SearchRequest.class);
        doReturn(searchRequestMock).when(repositoryApi).newSearchRequest(Mockito.any());
        doReturn(null).when(searchRequestMock).getPaginatedSEList(Mockito.any());

        ArgumentCaptor<Pagination.PaginationParameters> argument = ArgumentCaptor.forClass(Pagination.PaginationParameters.class);

        //WHEN
        this.targetsService.getEntitiesByTargetUuid(targetUuid, null, limit, ascending, searchOrderBy);

        //THEN
        verify(searchRequestMock, times(1) ).getPaginatedSEList(Mockito.any());
        verify(searchRequestMock).getPaginatedSEList(argument.capture());

        Pagination.PaginationParameters paginationParameters = argument.getValue();
        Assert.assertEquals(paginationParameters.getLimit(), limit);
        Assert.assertFalse(paginationParameters.hasCursor());
        Assert.assertEquals(paginationParameters.getAscending(), true);
        Assert.assertEquals(paginationParameters.getOrderBy().getSearch(), OrderBy.SearchOrderBy.ENTITY_NAME);
    }

    /**
     * Tests getEntitiesByTargetUuid setting pagination default parameters orderBy, limit, ascending when cursor set correctly cursor missing.
     *
     * @throws Exception error in thread interruption or converting api dtos
     */
    @Test
    public void getEntitiesByTargetUuidWithPaginationDefaultParametersSetWhenOnlyCursorPassed() throws Exception {
        //GIVEN
        String targetUuid = "123";
        final String cursor = "";


        SearchRequest searchRequestMock = Mockito.mock(SearchRequest.class);
        doReturn(searchRequestMock).when(repositoryApi).newSearchRequest(Mockito.any());
        doReturn(null).when(searchRequestMock).getPaginatedSEList(Mockito.any());

        ArgumentCaptor<Pagination.PaginationParameters> argument = ArgumentCaptor.forClass(Pagination.PaginationParameters.class);

        //WHEN
        this.targetsService.getEntitiesByTargetUuid(targetUuid, "", null, null, null);

        //THEN
        verify(searchRequestMock, times(1) ).getPaginatedSEList(Mockito.any());
        verify(searchRequestMock).getPaginatedSEList(argument.capture());

        Pagination.PaginationParameters paginationParameters = argument.getValue();
        Assert.assertEquals(paginationParameters.getLimit(), 100);
        Assert.assertTrue(paginationParameters.hasCursor());
        Assert.assertEquals(paginationParameters.getAscending(), true);
        Assert.assertEquals(paginationParameters.getOrderBy().getSearch(), OrderBy.SearchOrderBy.ENTITY_NAME);
    }

    /**
     * Tests getEntitiesByTargetUuid supports searchOrderBy case insensitive.
     * @throws Exception error in thread interruption or converting api dtos
     */
    @Test
    public void getEntitiesByTargetUuidWithPaginationCaseInsensitiveOrderBy() throws Exception {
        //GIVEN
        String targetUuid = "123";
        int limit = 1;
        boolean ascending = true;
        final String searchOrderBy = SearchOrderBy.DEFAULT.toLowerCase();


        SearchRequest searchRequestMock = Mockito.mock(SearchRequest.class);
        doReturn(searchRequestMock).when(repositoryApi).newSearchRequest(Mockito.any());
        doReturn(null).when(searchRequestMock).getPaginatedSEList(Mockito.any());

        ArgumentCaptor<Pagination.PaginationParameters> argument = ArgumentCaptor.forClass(Pagination.PaginationParameters.class);

        //WHEN
        this.targetsService.getEntitiesByTargetUuid(targetUuid, null, limit, ascending, searchOrderBy);

        //THEN
        verify(searchRequestMock, times(1) ).getPaginatedSEList(Mockito.any());
        verify(searchRequestMock).getPaginatedSEList(argument.capture());

        Pagination.PaginationParameters paginationParameters = argument.getValue();
        Assert.assertEquals(paginationParameters.getLimit(), limit);
        Assert.assertFalse(paginationParameters.hasCursor());
        Assert.assertEquals(paginationParameters.getAscending(), true);
        Assert.assertEquals(paginationParameters.getOrderBy().getSearch(), OrderBy.SearchOrderBy.ENTITY_NAME);
    }

    /**
     * Expect thrown error when searchOrderBy value not supported.
     * @throws Exception error in thread interruption or converting api dtos
     */
    @Test(expected = Exception.class)
    public void getEntitiesByTargetUuidWithNonSupportedOrderBy() throws Exception {
        //GIVEN
        String targetUuid = "123";
        int limit = 1;
        boolean ascending = true;
        final String searchOrderBy = "olives";

        SearchRequest searchRequestMock = Mockito.mock(SearchRequest.class);
        doReturn(searchRequestMock).when(repositoryApi).newSearchRequest(Mockito.any());
        doReturn(null).when(searchRequestMock).getPaginatedSEList(Mockito.any());

        ArgumentCaptor<Pagination.PaginationParameters> argument = ArgumentCaptor.forClass(Pagination.PaginationParameters.class);

        //WHEN
        this.targetsService.getEntitiesByTargetUuid(targetUuid, null, limit, ascending, searchOrderBy);
    }
}
