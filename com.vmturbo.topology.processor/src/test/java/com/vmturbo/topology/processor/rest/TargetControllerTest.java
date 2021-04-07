package com.vmturbo.topology.processor.rest;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
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
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.util.NestedServletException;

import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowsRequest;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.FetchWorkflowsResponse;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;
import com.vmturbo.common.protobuf.workflow.WorkflowDTOMoles.WorkflowServiceMole;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc.WorkflowServiceBlockingStub;
import com.vmturbo.communication.ITransport;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.topology.processor.TestIdentityStore;
import com.vmturbo.topology.processor.actions.ActionMergeSpecsRepository;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.ITargetHealthInfo;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.dto.TargetInputFields;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.GetAllTargetsResponse;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetHealthInfo;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetInfo;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetSpec;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.operation.FailedDiscoveryTracker;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.FailedDiscoveryTracker.DiscoveryFailure;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeInfoCompatibilityChecker;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.RemoteProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore;
import com.vmturbo.topology.processor.targets.CachingTargetStore;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetDao;
import com.vmturbo.topology.processor.targets.TargetSpecAttributeExtractor;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyHandler;

/**
 * Tests for the REST interface for target management.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
// Need clean context with no probes/targets registered.
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class TargetControllerTest {

    private static final SettingPolicyServiceMole settingPolicyServiceMole =
        Mockito.spy(new SettingPolicyServiceMole());

    private static final WorkflowServiceMole workflowServiceMole =
        Mockito.spy(new WorkflowServiceMole());
    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    static class ContextConfiguration extends WebMvcConfigurerAdapter {
        @Bean
        public KeyValueStore keyValueStore() {
            return mock(KeyValueStore.class);
        }

        @Bean
        public TargetDao targetDao() {
            return Mockito.mock(TargetDao.class);
        }

        @Bean
        public IdentityProvider identityService() {
            return new IdentityProviderImpl(new MapKeyValueStore(),
                new ProbeInfoCompatibilityChecker(), 0L, mock(IdentityDatabaseStore.class), 10, 0
                , false);
        }

        @Bean
        ProbeStore probeStore() {
            return new RemoteProbeStore(keyValueStore(), identityService(), stitchingOperationStore(),  new ActionMergeSpecsRepository());
        }

        @Bean
        IdentityStore<TopologyProcessorDTO.TargetSpec> targetIdentityStore() {
            return new TestIdentityStore<>(new TargetSpecAttributeExtractor(probeStore()));
        }

        @Bean
        Scheduler schedulerMock() {
            return mock(Scheduler.class);
        }

        @Bean
        StitchingOperationStore stitchingOperationStore() {
            return mock(StitchingOperationStore.class);
        }

        @Bean
        public TargetStore targetStore() {
            GroupScopeResolver groupScopeResolver = mock(GroupScopeResolver.class);
            when(groupScopeResolver.processGroupScope(any(), any(), any()))
                .then(AdditionalAnswers.returnsSecondArg());
            return new CachingTargetStore(targetDao(), probeStore(),
                targetIdentityStore());
        }

        @Override
        public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
            GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
            msgConverter.setGson(ComponentGsonFactory.createGson());
            converters.add(msgConverter);
        }

        @Bean
        public IOperationManager operationManager() {
            final IOperationManager result = mock(IOperationManager.class);
            when(result.getLastDiscoveryForTarget(anyLong(), any(DiscoveryType.class)))
                .thenReturn(Optional.empty());
            when(result.getInProgressDiscoveryForTarget(anyLong(), any(DiscoveryType.class)))
                .thenReturn(Optional.empty());
            when(result.getLastValidationForTarget(anyLong()))
                .thenReturn(Optional.empty());
            when(result.getInProgressValidationForTarget(anyLong()))
                .thenReturn(Optional.empty());
            return result;
        }

        @Bean
        TopologyHandler topologyHandler() {
            return mock(TopologyHandler.class);
        }

        @Bean
        public GrpcTestServer grpcTestServer() {
            try {
                final GrpcTestServer testServer =
                    GrpcTestServer.newServer(settingPolicyServiceMole, workflowServiceMole);
                testServer.start();
                return testServer;
            } catch (IOException e) {
                throw new BeanCreationException("Failed to create test grpc server", e);
            }
        }

        @Bean
        public SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub() {
            return SettingPolicyServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
        }

        @Bean
        public WorkflowServiceBlockingStub workflowServiceBlockingStub() {
            return WorkflowServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
        }

        @Bean
        public TargetController targetController() {
            return new TargetController(schedulerMock(), targetStore(), probeStore(),
                operationManager(), topologyHandler(), settingPolicyServiceBlockingStub(),
                workflowServiceBlockingStub(), failedDiscoveryTracker());
        }

        /**
         * Failed Discovery Tracker.
         * @return {@link FailedDiscoveryTracker}.
         */
        @Bean
        public FailedDiscoveryTracker failedDiscoveryTracker() {
            return mock(FailedDiscoveryTracker.class);
        }
    }

    private static final Gson GSON = new Gson();

    private static MockMvc mockMvc;

    // Helper protos to construct probe infos
    // without setting all the useless required fields.
    private ProbeInfo probeInfo;
    private ProbeInfo derivedProbeInfo;
    private ProbeInfo otherProbeInfo;
    private AccountDefEntry mandatoryAccountDef;
    private AccountDefEntry optionalAccountDef;

    private ProbeStore probeStore;
    private TargetStore targetStore;
    private ITransport<MediationServerMessage, MediationClientMessage> transport;
    private IdentityProvider identityProvider;
    private IOperationManager operationManager;

    @Autowired
    private WebApplicationContext wac;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        System.setProperty("com.vmturbo.keydir", testFolder.newFolder().getAbsolutePath());
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();

        probeStore = wac.getBean(ProbeStore.class);
        targetStore = wac.getBean(TargetStore.class);
        identityProvider = wac.getBean(IdentityProvider.class);
        operationManager = wac.getBean(IOperationManager.class);
        probeInfo = createProbeInfo("test", "testCategoryStandAlone", "mandatory",
            "mandatory",   CreationMode.STAND_ALONE);
        derivedProbeInfo = createProbeInfo("testDerived", "testCategoryDerived", "mandatory",
            "mandatory",   CreationMode.DERIVED);
        otherProbeInfo = createProbeInfo("test", "testCategoryOther", "mandatory",
            "mandatory", CreationMode.OTHER);
        mandatoryAccountDef = AccountDefEntry.newBuilder()
            .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                .setName("mandatory")
                .setDisplayName("blah")
                .setDescription("BLAH"))
            .setMandatory(true)
            .build();
        optionalAccountDef = AccountDefEntry.newBuilder()
            .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                .setName("optional")
                .setDisplayName("blah")
                .setDescription("BLAH"))
            .setMandatory(false)
            .build();
        @SuppressWarnings("unchecked")
        final ITransport<MediationServerMessage, MediationClientMessage> mock =
            mock(ITransport.class);
        transport = mock;
    }

    private static ProbeInfo createProbeInfo(String probeType, String category,
                                             String identifierField, String accoutDef,
                                             CreationMode creationMode) {
        return ProbeInfo.newBuilder()
            .setProbeType(probeType)
            .setProbeCategory(category)
            .setUiProbeCategory(category)
            .addTargetIdentifierField(identifierField)
            .setCreationMode(creationMode)
            .addAccountDefinition(AccountDefEntry.newBuilder()
                .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                    .setName(accoutDef)
                    .setDescription("test").setDisplayName("test"))
                .setMandatory(true)
                .build())
            .build();
    }

    @Test
    public void addTarget() throws Exception {
        final TargetInfo result = addTestTarget("mandatory", probeInfo, HttpStatus.OK);
        Assert.assertNotNull(result.getTargetId());
    }

    @Test
    public void addTargetOther() throws Exception {
        final TargetInfo result = addTestTarget("mandatory", otherProbeInfo, HttpStatus.FORBIDDEN);
        Assert.assertNull(result.getTargetId());
    }

    private TargetInfo addTestTarget(String customDefinitionName, ProbeInfo probeInfo,
                                     HttpStatus httpStatus) throws Exception {
        final AccountDefEntry.Builder accountBuilder =
            AccountDefEntry.newBuilder(mandatoryAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName(customDefinitionName);
        ProbeInfo oneMandatory = ProbeInfo.newBuilder(probeInfo)
            .addAccountDefinition(accountBuilder).build();
        probeStore.registerNewProbe(oneMandatory, transport);
        TargetAdder adder = new TargetAdder(identityProvider.getProbeId(oneMandatory));
        adder.setAccountField("mandatory", "1");
        return adder.postAndExpect(httpStatus);
    }

    /**
     * Creates a probe with derived creation mode and checks that a derived target can be added from
     * the internal api call; it is now only blocked from the public APIs -
     * see "TargetsServiceTest.testAddDerivedTarget" in the api component.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void addDerivedTarget() throws Exception {
        final long probeId = createProbeWithOneField(derivedProbeInfo);
        final TargetAdder adder = new TargetAdder(probeId);
        final String mandatoryKey = "mandatory";
        final String stringValue = "foo";
        adder.setAccountField(mandatoryKey, stringValue);
        final TargetInfo targetInfo = adder.postAndExpect(HttpStatus.OK);
        Assert.assertNotNull(targetInfo.getTargetId());
        Assert.assertNotNull(targetInfo.getAccountData());
        Assert.assertEquals(1, targetInfo.getAccountData().size());
        Assert.assertEquals(Collections.singleton(stringValue),
            targetInfo.getAccountData().stream().filter(av -> mandatoryKey.equals(av.getName()))
                .map(AccountValue::getStringValue).collect(Collectors.toSet()));
    }

    /**
     * Tests the creation of a target with a communication channel.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void addTargetWithCommunicationChannel() throws Exception {
        Gson gson = new Gson();
        String communicationBindingChannel = "testChannel";
        ProbeInfo oneMandatory = ProbeInfo.newBuilder(probeInfo).build();
        String reqStr = gson.toJson(new TargetSpec(identityProvider.getProbeId(oneMandatory), Collections.singletonList(new InputField("mandatory",
            "mandatory", Optional.empty())), Optional.of(communicationBindingChannel)));
        probeStore.registerNewProbe(oneMandatory, transport);
        MvcResult result = mockMvc.perform(post("/target")
            .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).content(reqStr)
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(status().is(HttpStatus.OK.value()))
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
            .andReturn();
        String resultStr = result.getResponse().getContentAsString();
        TargetInfo info = gson.fromJson(resultStr, TargetInfo.class);
        Assert.assertTrue(info.getCommunicationBindingChannel().isPresent());
        Assert.assertEquals(communicationBindingChannel, info.getCommunicationBindingChannel().get());
    }

    /**
     * Test requirement that only one ServiceNow target can be add in the environment.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testRestrictionOfAddingSeveralServiceNowTargets() throws Exception {
        final ProbeInfo serviceNowProbe = createProbeInfo(SDKProbeType.SERVICENOW.getProbeType(),
            ProbeCategory.ORCHESTRATOR.getCategory(), "mandatory", "mandatory",
            CreationMode.STAND_ALONE);
        probeStore.registerNewProbe(serviceNowProbe, transport);
        final TargetAdder adder = new TargetAdder(identityProvider.getProbeId(serviceNowProbe));
        adder.setAccountField("mandatory", "1");
        final TargetInfo targetInfo = adder.postAndExpect(HttpStatus.OK);
        Assert.assertNotNull(targetInfo.getTargetId());
        // check failed to add second ServiceNow target
        final TargetInfo failedTargetInfo = adder.postAndExpect(HttpStatus.BAD_REQUEST);
        final List<String> errors = failedTargetInfo.getErrors();
        Assert.assertEquals(1, errors.size());
        Assert.assertThat(errors.get(0),
            CoreMatchers.containsString("Cannot add more than one ServiceNow target."));
    }

    @Test
    public void addTargetMissingMandatory() throws Exception {
        ProbeInfo oneMandatory = ProbeInfo.newBuilder(probeInfo).build();
        probeStore.registerNewProbe(oneMandatory, transport);
        TargetAdder adder = new TargetAdder(identityProvider.getProbeId(oneMandatory));
        TargetInfo result = adder.postAndExpect(HttpStatus.BAD_REQUEST);
        Assert.assertEquals(result.getErrors().size(), 1);
    }

    @Test
    public void addTargetMissingMultipleMandatory() throws Exception {
        final AccountDefEntry.Builder accountBuilder =
            AccountDefEntry.newBuilder(mandatoryAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName("2mandatory");
        final AccountDefEntry mandatory2 = accountBuilder.build();
        ProbeInfo twoMandatory = ProbeInfo.newBuilder(probeInfo)
            .addAccountDefinition(mandatory2).build();
        probeStore.registerNewProbe(twoMandatory, transport);
        TargetAdder adder = new TargetAdder(identityProvider.getProbeId(twoMandatory));
        TargetInfo result = adder.postAndExpect(HttpStatus.BAD_REQUEST);
        Assert.assertEquals(result.getErrors().size(), 2);
    }

    @Test
    public void addTargetMissingOptional() throws Exception {
        final AccountDefEntry.Builder accountBuilder =
            AccountDefEntry.newBuilder(optionalAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName("1optional");
        ProbeInfo optional = ProbeInfo.newBuilder(probeInfo)
            .addAccountDefinition(accountBuilder).build();
        probeStore.registerNewProbe(optional, transport);
        TargetAdder adder = new TargetAdder(identityProvider.getProbeId(optional));
        adder.setAccountField("mandatory", "abc123");
        TargetInfo result = adder.postAndExpect(HttpStatus.OK);
        Assert.assertNotNull(result.getTargetId());
    }

    @Test
    public void addTargetVerificationRegex() throws Exception {
        final AccountDefEntry.Builder accountBuilder =
            AccountDefEntry.newBuilder(mandatoryAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName("abcPrefix")
            .setVerificationRegex("abc.*");
        ProbeInfo abcPrefix = ProbeInfo.newBuilder(probeInfo)
            .addAccountDefinition(accountBuilder).build();
        probeStore.registerNewProbe(abcPrefix, transport);
        TargetAdder adder = new TargetAdder(identityProvider.getProbeId(abcPrefix));
        adder.setAccountField("abcPrefix", "abc123");
        adder.setAccountField("mandatory", "abc123");
        adder.postAndExpect(HttpStatus.OK);
    }

    @Test
    public void addTargetFailedVerification() throws Exception {
        final AccountDefEntry.Builder accountBuilder =
            AccountDefEntry.newBuilder(mandatoryAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName("abcPrefix")
            .setVerificationRegex("abc.*");
        ProbeInfo abcPrefix = ProbeInfo.newBuilder(probeInfo)
            .addAccountDefinition(accountBuilder).build();
        probeStore.registerNewProbe(abcPrefix, transport);
        TargetAdder adder = new TargetAdder(identityProvider.getProbeId(abcPrefix));
        // Invalid value, since it doesn't start with abc.
        adder.setAccountField("abcPrefix", "123");
        adder.setAccountField("mandatory", "123");
        TargetInfo result = adder.postAndExpect(HttpStatus.BAD_REQUEST);
        Assert.assertEquals(result.getErrors().size(), 1);
    }


    @Test
    public void addTargetBadJson() throws Exception {
        String ref = "{ 'boot' : 'biggy' }";
        mockMvc.perform(post("/target")
            .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).content(ref)
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(status().is(400))
            .andReturn();
    }

    @Test
    public void addTargetExtraFieldsAndMissingFields() throws Exception {
        final ProbeInfo oneMandatory = ProbeInfo.newBuilder(probeInfo).build();
        probeStore.registerNewProbe(oneMandatory, transport);

        final TargetAdder adder = new TargetAdder(identityProvider.getProbeId(oneMandatory));
        adder.setAccountField("extra", "123");

        final TargetInfo targetInfo = adder.postAndExpect(HttpStatus.BAD_REQUEST);
        Assert.assertNotNull(targetInfo.getErrors());

        // Make sure we get ALL the errors.
        Assert.assertEquals(2, targetInfo.getErrors().size());
        Assert.assertThat(targetInfo.getErrors(),
            Matchers.containsInAnyOrder("Unknown field: extra",
                "Missing mandatory field mandatory"));
    }

    @Test
    public void addTargetBadProbe() throws Exception {
        final AccountDefEntry.Builder accountBuilder =
            AccountDefEntry.newBuilder(mandatoryAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName("bad")
            .setVerificationRegex("987s6t*(&^*(&^");
        ProbeInfo bad = ProbeInfo.newBuilder(probeInfo)
            .addAccountDefinition(accountBuilder).build();
        probeStore.registerNewProbe(bad, transport);
        TargetAdder adder = new TargetAdder(identityProvider.getProbeId(bad));
        adder.setAccountField("bad", "123");

        expectedException.expect(NestedServletException.class);
        expectedException.expectMessage("Unclosed group near index 14");

        mockMvc.perform(post("/target")
            .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE)
            .content(new Gson().toJson(adder.buildSpec()))
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE));
    }

    @Test
    public void addAndGetTarget() throws Exception {
        final AccountDefEntry.Builder accountBuilder =
            AccountDefEntry.newBuilder(mandatoryAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName("mandatory");
        ProbeInfo info = ProbeInfo.newBuilder(probeInfo)
            .addAccountDefinition(accountBuilder).build();
        probeStore.registerNewProbe(info, transport);
        TargetAdder adder = new TargetAdder(identityProvider.getProbeId(info));
        adder.setAccountField("mandatory", "test",
            Collections.singletonList(Collections.singletonList("prop")));
        TargetInfo result = adder.postAndExpect(HttpStatus.OK);
        Assert.assertNotNull(result.getTargetId());
        TargetInfo resp = getTarget(result.getTargetId());

        TargetSpec retSpec = resp.getSpec();
        TargetSpec inputSpec = adder.buildSpec();

        Assert.assertNotNull(retSpec);
        assertEqualSpecs(inputSpec, retSpec);
    }

    @Test
    public void testGetNonExisting() throws Exception {
        MvcResult result = mockMvc.perform(get("/target/123")
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(status().is(HttpStatus.NOT_FOUND.value()))
            .andReturn();
        TargetInfo resp = GSON.fromJson(result.getResponse().getContentAsString(), TargetInfo.class);
        Assert.assertNotNull(resp.getErrors());
        Assert.assertEquals(ImmutableList.of("Target not found."), resp.getErrors());
    }

    @Test
    public void testGetNonNumeric() throws Exception {
        MvcResult result = mockMvc.perform(get("/target/the6")
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(status().is(HttpStatus.BAD_REQUEST.value()))
            .andReturn();
    }

    @Test
    public void testGetTargetSecretField() throws Exception {
        final AccountDefEntry.Builder accountBuilder =
            AccountDefEntry.newBuilder(mandatoryAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName("secret").setIsSecret(true);
        ProbeInfo info = ProbeInfo.newBuilder(probeInfo)
            .addAccountDefinition(accountBuilder).build();
        probeStore.registerNewProbe(info, transport);
        TargetAdder adder = new TargetAdder(identityProvider.getProbeId(info));
        adder.setAccountField("secret", "nooneknows");
        adder.setAccountField("mandatory", "nooneknows");
        TargetInfo result = adder.postAndExpect(HttpStatus.OK);

        TargetSpec retSpec = getTarget(result.getTargetId()).getSpec();
        // Expect an empty return spec, because we shouldn't
        // get secret fields.
        TargetAdder expectedAdder = new TargetAdder(identityProvider.getProbeId(info));
        expectedAdder.setAccountField("mandatory", "nooneknows");

        assertEqualSpecs(retSpec, expectedAdder.buildSpec());
    }

    @Test
    public void testGetTargetProbeNotRegistered() throws Exception {
        ProbeInfo info = ProbeInfo.newBuilder(probeInfo).build();
        probeStore.registerNewProbe(info, transport);
        TargetAdder adder = new TargetAdder(identityProvider.getProbeId(info));
        adder.setAccountField("mandatory", "nooneknows");
        TargetInfo result = adder.postAndExpect(HttpStatus.OK);

        // Now magically unregister the probeStore.
        final ProbeStore probeStore = wac.getBean(ProbeStore.class);
        probeStore.removeTransport(transport);

        String getResult = mockMvc.perform(get("/target/" + result.getTargetId())
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse().getContentAsString();

        TargetInfo resp = GSON.fromJson(getResult, TargetInfo.class);
        Assert.assertFalse(resp.getProbeConnected());
    }

    @Test
    public void testGetAll() throws Exception {
        final AccountDefEntry.Builder accountBuilder =
            AccountDefEntry.newBuilder(mandatoryAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName("mandatory");
        ProbeInfo info = ProbeInfo.newBuilder(probeInfo)
            .addAccountDefinition(accountBuilder).build();
        probeStore.registerNewProbe(info, transport);

        Map<Long, TargetSpec> expectedSpecs = new HashMap<>();
        for (int i = 0; i < 2; ++i) {
            TargetAdder adder = new TargetAdder(identityProvider.getProbeId(info));
            adder.setAccountField("mandatory", "prop" + i);
            TargetInfo result = adder.postAndExpect(HttpStatus.OK);
            Assert.assertNotNull(result.getTargetId());
            expectedSpecs.put(result.getTargetId(), adder.buildSpec());
        }

        Map<Long, TargetInfo> retTargets = getAllTargets();
        expectedSpecs.entrySet().stream().forEach(
            entry -> {
                TargetInfo retInfo = retTargets.get(entry.getKey());
                Assert.assertNotNull(retInfo);
                Assert.assertNotNull(retInfo.getSpec());
                assertEqualSpecs(entry.getValue(), retInfo.getSpec());
            }
        );
    }

    /**
     * Method tests target status retrieved during the sequence of operations success/failures.
     *
     * @throws Exception if exception occurred
     */
    @Test
    public void testGetAllDifferentStatuses() throws Exception {
        final AccountDefEntry.Builder accountBuilder =
            AccountDefEntry.newBuilder(mandatoryAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName("mandatory");
        final ProbeInfo info =
            ProbeInfo.newBuilder(probeInfo).addAccountDefinition(accountBuilder).build();
        probeStore.registerNewProbe(info, transport);

        final long probeId = identityProvider.getProbeId(info);
        final Discovery discovery = new Discovery(probeId, 0, identityProvider);
        final Validation validation = new Validation(probeId,  0, identityProvider);
        when(operationManager.getLastDiscoveryForTarget(anyLong(), any(DiscoveryType.class)))
            .thenReturn(Optional.empty());
        when(operationManager.getLastValidationForTarget(anyLong()))
            .thenReturn(Optional.empty());

        final TargetAdder adder = new TargetAdder(identityProvider.getProbeId(info));
        adder.setAccountField("mandatory", "prop");
        final TargetInfo result = adder.postAndExpect(HttpStatus.OK);
        Assert.assertNotNull(result.getTargetId());
        Assert.assertEquals(TargetController.VALIDATING, result.getStatus());
        Assert.assertNull(result.getLastValidationTime());

        when(operationManager.getLastDiscoveryForTarget(anyLong(), any(DiscoveryType.class)))
            .thenReturn(Optional.of(discovery));
        when(operationManager.getLastValidationForTarget(anyLong()))
            .thenReturn(Optional.of(validation));
        when(operationManager.getInProgressValidationForTarget(anyLong()))
            .thenReturn(Optional.of(validation));
        when(operationManager.getInProgressValidationForTarget(anyLong()))
            .thenReturn(Optional.of(validation));
        {
            final TargetInfo target2 = getTarget(result.getId());
            Assert.assertThat(target2.getStatus(), CoreMatchers.containsString(TargetController.VALIDATING));
            Assert.assertNull(target2.getLastValidationTime());

            validation.setUserInitiated(true);
            discovery.setUserInitiated(true);
            final TargetInfo target2_user = getTarget(result.getId());
            Assert.assertThat(target2_user.getStatus(), CoreMatchers.containsString("in progress"));
        }
        {
            when(operationManager.getInProgressValidationForTarget(anyLong()))
                .thenReturn(Optional.empty());
            when(operationManager.getInProgressValidationForTarget(anyLong()))
                .thenReturn(Optional.empty());
            validation.success();
            final TargetInfo target3 = getTarget(result.getId());
            Assert.assertThat(target3.getStatus(), is(StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_SUCCESS));
            Assert.assertEquals(validation.getCompletionTime(), target3.getLastValidationTime());
        }
        {
            discovery.success();
            final TargetInfo target4 = getTarget(result.getId());
            Assert.assertThat(target4.getStatus(), is(StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_SUCCESS));
            Assert.assertEquals(discovery.getCompletionTime(), target4.getLastValidationTime());
        }
        {
            final String discoveryError = "Pixies have broken out of cage";
            discovery.fail();
            discovery.addError(SDKUtil.createCriticalError(discoveryError));
            final TargetInfo target5 = getTarget(result.getId());
            Assert.assertThat(target5.getStatus(), CoreMatchers.containsString(discoveryError));
            Assert.assertEquals(discovery.getCompletionTime(), target5.getLastValidationTime());
        }
        {
            validation.success();
            final TargetInfo target = getTarget(result.getId());
            Assert.assertThat(target.getStatus(),
                CoreMatchers.containsString(StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_SUCCESS));
            Assert.assertEquals(validation.getCompletionTime(), target.getLastValidationTime());
        }
    }

    @Test
    public void testGetAllEmpty() throws Exception {
        Map<Long, TargetInfo> retTargets = getAllTargets();
        Assert.assertEquals(0, retTargets.size());
    }

    @Test
    public void testGetAllSecretField() throws Exception {
        final AccountDefEntry.Builder accountBuilder =
            AccountDefEntry.newBuilder(mandatoryAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName("secret").setIsSecret(true);
        ProbeInfo info = ProbeInfo.newBuilder(probeInfo).addTargetIdentifierField("secret")
            .addAccountDefinition(accountBuilder).build();
        probeStore.registerNewProbe(info, transport);

        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            TargetAdder adder = new TargetAdder(identityProvider.getProbeId(info));
            adder.setAccountField("secret", "nooneknows" + i);
            adder.setAccountField("mandatory", "abc123");
            TargetInfo resp = adder.postAndExpect(HttpStatus.OK);
            ids.add(resp.getTargetId());
        }

        Map<Long, TargetInfo> targets = getAllTargets();
        TargetAdder adder = new TargetAdder(identityProvider.getProbeId(info));
        adder.setAccountField("mandatory", "abc123");

        TargetSpec emptySpec = adder.buildSpec();
        ids.stream().forEach(
            targetId -> {
                Assert.assertTrue(targets.containsKey(targetId));
                assertEqualSpecs(emptySpec, targets.get(targetId).getSpec());
            }
        );
    }

    @Test
    public void testGetAllSomeProbesNotRegistered() throws Exception {
        ProbeInfo goneProbe = ProbeInfo.newBuilder(probeInfo).setProbeType("type1").build();
        @SuppressWarnings("unchecked")
        final ITransport<MediationServerMessage, MediationClientMessage> goneTransport =
            mock(ITransport.class);
        probeStore.registerNewProbe(goneProbe, goneTransport);

        ProbeInfo registeredProbe = ProbeInfo.newBuilder(probeInfo).setProbeType("type2").build();
        probeStore.registerNewProbe(registeredProbe, transport);

        final TargetAdder adder = new TargetAdder(identityProvider.getProbeId(goneProbe));
        adder.setAccountField("mandatory", "abc123");
        final Long goneId = adder.postAndExpect(HttpStatus.OK).getTargetId();

        final TargetAdder newAdder = new TargetAdder(identityProvider.getProbeId(registeredProbe));
        newAdder.setAccountField("mandatory", "abc123");
        final Long registeredId = newAdder.postAndExpect(HttpStatus.OK).getTargetId();

        // Now magically unregister one of the probes.
        ProbeStore probeStore = wac.getBean(ProbeStore.class);
        probeStore.removeTransport(goneTransport);

        MvcResult result = mockMvc.perform(get("/target")
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(status().isOk())
            .andReturn();

        String resultStr = result.getResponse().getContentAsString();
        GetAllTargetsResponse resp = GSON.fromJson(resultStr, GetAllTargetsResponse.class);
        Map<Long, TargetInfo> targets = resp.targetsById();

        Assert.assertEquals(2, targets.size());
        Assert.assertTrue(targets.containsKey(goneId));
        Assert.assertFalse(targets.get(goneId).getProbeConnected());
        Assert.assertTrue(targets.containsKey(registeredId));
        TargetAdder expectedAdder = new TargetAdder(identityProvider.getProbeId(registeredProbe));
        expectedAdder.setAccountField("mandatory", "abc123");

        assertEqualSpecs(targets.get(registeredId).getSpec(),
            expectedAdder.buildSpec());
    }

    /**
     * Creates target spec for the specified probe with a specified id, put into account values.
     *
     * @param probeId probe id
     * @param id id to put into account values.
     * @return target spec
     */
    private TopologyProcessorDTO.TargetSpec createTargetSpec(long probeId, String id) {
        final TopologyProcessorDTO.AccountValue account = TopologyProcessorDTO.AccountValue.newBuilder()
            .setKey(mandatoryAccountDef.getCustomDefinition().getName())
            .setStringValue(id).build();
        final TopologyProcessorDTO.TargetSpec spec = TopologyProcessorDTO.TargetSpec.newBuilder().setProbeId(probeId)
            .addAccountValue(account).setIsHidden(false).setReadOnly(false).build();
        return spec;
    }

    private TopologyProcessorDTO.TargetSpec createTargetSpec(long probeId, String id,
                                                             String communicationChannel) {
        final TopologyProcessorDTO.AccountValue account = TopologyProcessorDTO.AccountValue.newBuilder()
            .setKey(mandatoryAccountDef.getCustomDefinition().getName())
            .setStringValue(id).build();
        final TopologyProcessorDTO.TargetSpec spec = TopologyProcessorDTO.TargetSpec.newBuilder().setProbeId(probeId)
            .addAccountValue(account).setIsHidden(false).setReadOnly(false).setCommunicationBindingChannel(communicationChannel).build();
        return spec;
    }

    /**
     * Creates a target with one mandatory field, filled with the specified {@code id} in account
     * values.
     *
     * @param probeId probe id to register target to
     * @param id string id to be used later for verifications
     * @return target object
     * @throws Exception on exceptions occurred
     */
    private Target createTarget(long probeId, String id) throws Exception {
        return targetStore.createTarget(createTargetSpec(probeId, id));
    }

    /**
     * Creates one probe with one mandatory field and desired probe info,
     * registers it in probe store and returns its id.
     *
     * @param probeInfo of the new probe to create
     * @return the probe id
     * @throws Exception in case the registration to the probe store fails
     */
    private long createProbeWithOneField(ProbeInfo probeInfo) throws Exception {
        final ProbeInfo info = ProbeInfo.newBuilder(probeInfo)
            .addAccountDefinition(mandatoryAccountDef).build();
        probeStore.registerNewProbe(info, transport);
        final long probeId = probeStore.getProbes().keySet().iterator().next();
        return probeId;
    }

    /**
     * Tests removal of the existing target.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void removeTarget() throws Exception {
        final long probeId = createProbeWithOneField(probeInfo);
        final Target target1 = createTarget(probeId, "1");
        final Target target2 = createTarget(probeId, "2");

        Assert.assertEquals(2, targetStore.getAll().size());
        final MvcResult mvcResult =
            requestRemoveTarget(target1.getId()).andExpect(status().isOk()).andReturn();
        final TargetInfo targetDeleted = decodeResult(mvcResult, TargetInfo.class);
        Assert.assertEquals(Collections.singletonList(target2), targetStore.getAll());
        Assert.assertEquals(target1.getId(), targetDeleted.getId());
    }

    /**
     * Test case for preventing deleting orchestration target (ServiceNow).
     * Prevent deleting target if there is at least one policy with external approval settings.
     * Searching policies by workflow id, because workflow discovered by target is used as
     * setting value.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testPreventTargetRemoving() throws Exception {
        final long probeWithApprovalFeatureId = createProbeWithOneField(probeInfo);
        final Target target1 = createTarget(probeWithApprovalFeatureId, "1");

        final long workflowId = 11L;
        final String blockedPolicyName = "Blocked Policy";
        final long blockedPolicyId = 12L;
        Mockito.when(workflowServiceMole.fetchWorkflows(
            FetchWorkflowsRequest.newBuilder().addTargetId(target1.getId()).build()))
            .thenReturn(FetchWorkflowsResponse.newBuilder()
                .addWorkflows(Workflow.newBuilder().setId(workflowId).build())
                .build());
        Mockito.when(settingPolicyServiceMole.listSettingPolicies(
            ListSettingPoliciesRequest.newBuilder().addWorkflowId(workflowId).build()))
            .thenReturn(Collections.singletonList(SettingPolicy.newBuilder()
                .setInfo(SettingPolicyInfo.newBuilder().setName(blockedPolicyName).build())
                .setId(blockedPolicyId)
                .build()));

        Assert.assertEquals(1, targetStore.getAll().size());
        final MvcResult mvcResult =
            requestRemoveTarget(target1.getId()).andExpect(status().isForbidden()).andReturn();
        final TargetInfo notDeletedTarget = decodeResult(mvcResult, TargetInfo.class);
        Assert.assertEquals(Collections.singletonList(target1), targetStore.getAll());
        Assert.assertThat(notDeletedTarget.getErrors().iterator().next(), CoreMatchers.allOf(
            CoreMatchers.containsString("Cannot remove target " + target1.getDisplayName()),
            CoreMatchers.containsString(blockedPolicyName)));

        // Clean up the workflow policies so tests following this will not have undesired results
        // due to the policies introduced here.  This is because the way we use the
        // CachingTargetStore in these tests means the targetId here could be the same/re-used as
        // in some other tests.  Workflow policies created here could adversely affect the test
        // results of those tests.
        //
        Mockito.when(settingPolicyServiceMole.listSettingPolicies(
            ListSettingPoliciesRequest.newBuilder().addWorkflowId(workflowId).build()))
            .thenReturn(Collections.emptyList());
        Mockito.when(workflowServiceMole.fetchWorkflows(
            FetchWorkflowsRequest.newBuilder().addTargetId(target1.getId()).build()))
            .thenReturn(FetchWorkflowsResponse.newBuilder().build());
    }

    /**
     * Tests removal of non-existing target.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void removeNonExistingTarget() throws Exception {
        final long targetId = 1L;
        final MvcResult mvcResult =
            requestRemoveTarget(targetId).andExpect(status().isNotFound()).andReturn();
        final TargetInfo result = decodeResult(mvcResult, TargetInfo.class);
        Assert.assertEquals(1, result.getErrors().size());
        Assert.assertThat(result.getErrors().iterator().next(),
            CoreMatchers.allOf(CoreMatchers.containsString(Long.toString(targetId)),
                CoreMatchers.containsString("does not exist")));
    }

    /**
     * Creates matcher, that match source string agains target id in string representation AND the
     * specified additional substring.
     *
     * @param targetId target id to match
     * @param substring substring to match
     * @return matcher to use in assertions
     */
    private Matcher<? super String> createTargetMatcher(long targetId, String substring) {
        return CoreMatchers.allOf(CoreMatchers.containsString(Long.toString(targetId)),
            CoreMatchers.containsString(substring));
    }

    /**
     * Tests correct target modification.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void updateExistingTarget() throws Exception {
        final long probeId = createProbeWithOneField(probeInfo);
        final Target target1 = createTarget(probeId, "1");
        createTarget(probeId, "2");
        final TopologyProcessorDTO.TargetSpec newTargetSpec = createTargetSpec(probeId, "3");

        Assert.assertEquals(2, targetStore.getAll().size());
        final MvcResult mvcResult =
            requestModifyTarget(target1.getId(), new TargetSpec(newTargetSpec))
                .andExpect(status().isOk()).andReturn();
        final TargetInfo resultTarget = decodeResult(mvcResult, TargetInfo.class);

        Assert.assertEquals(2, targetStore.getAll().size());
        Assert.assertEquals(target1.getId(), (long)resultTarget.getTargetId());
        final Target newTarget = targetStore.getTarget(target1.getId()).get();
        Assert.assertEquals(newTargetSpec, newTarget.getNoSecretDto().getSpec());
    }

    /**
     * Creates a probe with derived creation mode and checks that it can be edited from the
     * internal api call; it is now only blocked from the public APIs -
     * see "TargetsServiceTest.testEditTarget_readOnlyTarget" in the api component.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void updateDerivedTarget() throws Exception {
        final long probeId = createProbeWithOneField(derivedProbeInfo);
        final Target targetBeforeOperation = createTarget(probeId, "1");
        final TopologyProcessorDTO.TargetSpec newTargetSpec = createTargetSpec(probeId, "77");
        requestModifyTarget(targetBeforeOperation.getId(), new TargetSpec(newTargetSpec))
                .andExpect(status().isOk()).andReturn();
        final Target targetAfterOperation = targetStore.getTarget(targetBeforeOperation.getId()).get();
        Assert.assertEquals(newTargetSpec, targetAfterOperation.getNoSecretDto().getSpec());
    }

    /**
     * Tests for trial to update not existing target. Expected to return NOT_FOUND error.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void updateNotExistingTarget() throws Exception {
        final long probeId = createProbeWithOneField(probeInfo);
        final TargetSpec spec = new TargetSpec(createTargetSpec(probeId, "2"));
        final long targetId = 1L;
        Assert.assertEquals(0, targetStore.getAll().size());
        final MvcResult mvcResult =
                        requestModifyTarget(targetId, spec)
                                        .andExpect(status().isNotFound()).andReturn();
        final TargetInfo resultTarget = decodeResult(mvcResult, TargetInfo.class);

        Assert.assertEquals(1, resultTarget.getErrors().size());
        Assert.assertThat(resultTarget.getErrors().iterator().next(),
                        createTargetMatcher(targetId, "does not exist "));
    }

    /**
     * Tests correct target modification with a communication channel.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void updateTargetCommunicationChannel() throws Exception {
        final long probeId = createProbeWithOneField(probeInfo);
        final String channel1 = "channel1";
        final String channel2 = "channel2";
        final Target target1 = targetStore.createTarget(createTargetSpec(probeId, "1", channel1));

        Assert.assertEquals(channel1, target1.getSpec().getCommunicationBindingChannel());

        final TopologyProcessorDTO.TargetSpec newTargetSpec = createTargetSpec(probeId, "3", channel2);

        requestModifyTarget(target1.getId(), new TargetSpec(newTargetSpec))
            .andExpect(status().isOk()).andReturn();
        final Target newTarget = targetStore.getTarget(target1.getId()).get();
        Assert.assertEquals(channel2, newTarget.getSpec().getCommunicationBindingChannel());
    }


    /**
     * Tests correct target modification with a communication channel.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void updateTargetWithoutCommunicationChannel() throws Exception {
        final long probeId = createProbeWithOneField(probeInfo);
        final String channel1 = "channel1";
        final Target target1 = targetStore.createTarget(createTargetSpec(probeId, "1", channel1));

        Assert.assertEquals(channel1, target1.getSpec().getCommunicationBindingChannel());

        requestModifyTarget(target1.getId(), new TargetSpec(createTargetSpec(probeId, "3")))
            .andExpect(status().isOk()).andReturn();

        // make sure that if we updated the target, without passing a new communication channel,
        // the target will still have the previous one
        final Target newTarget = targetStore.getTarget(target1.getId()).get();
        Assert.assertEquals(channel1, newTarget.getSpec().getCommunicationBindingChannel());
    }

    /**
     * Tests correct target communicationBindingChannel deletion.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void deleteCommunicationChannel() throws Exception {
        final long probeId = createProbeWithOneField(probeInfo);
        final String channel1 = "channel1";
        final Target target1 = targetStore.createTarget(createTargetSpec(probeId, "1", channel1));

        Assert.assertEquals(channel1, target1.getSpec().getCommunicationBindingChannel());

        final TopologyProcessorDTO.TargetSpec newTargetSpec = createTargetSpec(probeId, "3", "");

        requestModifyTarget(target1.getId(), new TargetSpec(newTargetSpec))
            .andExpect(status().isOk()).andReturn();

        final Target newTarget = targetStore.getTarget(target1.getId()).get();
        Assert.assertFalse(newTarget.getSpec().hasCommunicationBindingChannel());
    }

    /**
     * Creates a probe with derived creation mode and checks that it can be removed from the
     * internal api call; it is now only blocked from the public APIs -
     * see "TargetsServiceTest.deleteReadOnlyTarget" in the api component.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void removeDerivedTarget() throws Exception {
        final long probeId = createProbeWithOneField(derivedProbeInfo);
        final Target target = createTarget(probeId, "43");
        Assert.assertEquals(1, targetStore.getAll().size());
        //
        // Remove target calls check the workflow policies associated with the target, and the
        // way we use the CachingTargetStore in these tests means the targetId here could be the
        // same as some previous tests.  Therefore, we set up a "distinct" workflowId to avoid a
        // previously established one comes with some undesired policies that would interfere the
        // test results.
        //
        final long workflowId = 43;
        Mockito.when(workflowServiceMole.fetchWorkflows(
            FetchWorkflowsRequest.newBuilder().addTargetId(target.getId()).build()))
            .thenReturn(FetchWorkflowsResponse.newBuilder()
                .addWorkflows(Workflow.newBuilder().setId(workflowId).build())
                .build());
        requestRemoveTarget(target.getId()).andExpect(status().isOk()).andReturn();
        Assert.assertEquals(0, targetStore.getAll().size());
    }

    // Using this instead of overriding equals because tests are the only
    // place we need to compare TargetSpecs right now.
    private void assertEqualSpecs(TargetSpec expected, TargetSpec got) {
        Assert.assertEquals(expected.getProbeId(), got.getProbeId());

        // Don't want to override equals and hashcode for accountVal, so doing this
        // dumb manual unrolling.
        Map<String, InputField> expectedInputFields = expected.getInputFieldsByName();
        Map<String, InputField> gotInputFields = got.getInputFieldsByName();
        Assert.assertEquals(expectedInputFields.keySet(), gotInputFields.keySet());
        for (String name : expectedInputFields.keySet()) {
            InputField expectedField = expectedInputFields.get(name);
            InputField gotField = gotInputFields.get(name);
            Assert.assertEquals(expectedField.getValue(), gotField.getValue());
            Assert.assertEquals(expectedField.getGroupProperties(), gotField.getGroupProperties());
        }
    }

    private Map<Long, TargetInfo> getAllTargets() throws Exception {
        MvcResult result = mockMvc.perform(get("/target")
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(status().isOk())
            .andReturn();
        String resultStr = result.getResponse().getContentAsString();
        GetAllTargetsResponse resp = GSON.fromJson(resultStr, GetAllTargetsResponse.class);
        return resp.targetsById();
    }

    private TargetInfo getTarget(Long id) throws Exception {
        MvcResult result = mockMvc.perform(get("/target/" + id)
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(status().isOk())
            .andReturn();
        String resultStr = result.getResponse().getContentAsString();
        return GSON.fromJson(resultStr, TargetInfo.class);
    }

    private ResultActions requestRemoveTarget(long targetId) throws Exception {
        return mockMvc.perform(MockMvcRequestBuilders.delete("/target/" + targetId)
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE));
    }

    private ResultActions requestModifyTarget(long targetId, TargetSpec targetSpec)
        throws Exception {
        return requestModifyTarget(targetId, targetSpec.getInputFields(), targetSpec.getCommunicationBindingChannel());
    }

    private ResultActions requestModifyTarget(long targetId, List<InputField> inputFields,
                                              final Optional<String> communicationChannel)
        throws Exception {
        final String reqStr = new Gson().toJson(new TargetInputFields(inputFields, communicationChannel));
        return mockMvc.perform(MockMvcRequestBuilders.put("/target/" + targetId)
            .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).content(reqStr)
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE));
    }

    private <T> T decodeResult(final MvcResult mvcResult, Class<T> responseClass)
        throws UnsupportedEncodingException {
        final String resultStr = mvcResult.getResponse().getContentAsString();
        return GSON.fromJson(resultStr, responseClass);
    }

    @Test
    public void testSingleTargetHealthOk() throws Exception   {
        final long probeId = createProbeWithOneField(derivedProbeInfo);
        final Target target = createTarget(probeId, "43");
        final long targetId = target.getId();

        final Validation validation = new Validation(probeId,  targetId, identityProvider);
        final Discovery discovery = new Discovery(probeId, targetId, identityProvider);
        validation.success();
        discovery.success();
        when(operationManager.getLastValidationForTarget(targetId))
            .thenReturn(Optional.of(validation));
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
            .thenReturn(Optional.of(discovery));

        ITargetHealthInfo healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(Long.valueOf(targetId), healthInfo.getTargetId());
        Assert.assertEquals(ITargetHealthInfo.TargetHealthSubcategory.DISCOVERY,
                        healthInfo.getSubcategory());
        Assert.assertEquals("", healthInfo.getErrorText());
        Assert.assertNull(healthInfo.getTargetErrorType());
        Assert.assertNull(healthInfo.getTimeOfFirstFailure());
        Assert.assertEquals(0, healthInfo.getNumberOfConsecutiveFailures());
    }

    @Test
    public void testSingleTargetHealthValidationFailed() throws Exception   {
        final long probeId = createProbeWithOneField(derivedProbeInfo);
        final Target target = createTarget(probeId, "43");
        final long targetId = target.getId();

        ErrorDTO.ErrorType errorType = ErrorDTO.ErrorType.CONNECTION_TIMEOUT;
        final Validation validation = new Validation(probeId,  targetId, identityProvider);
        ErrorDTO errorDTO = ErrorDTO.newBuilder()
                        .setErrorType(errorType)
                        .setDescription("connection timed out")
                        .setSeverity(ErrorDTO.ErrorSeverity.CRITICAL)
                        .build();
        validation.addError(errorDTO);
        validation.fail();
        when(operationManager.getLastValidationForTarget(targetId))
            .thenReturn(Optional.of(validation));

        //Test with no discovery data.
        ITargetHealthInfo healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(ITargetHealthInfo.TargetHealthSubcategory.VALIDATION,
                        healthInfo.getSubcategory());
        Assert.assertEquals(errorType, healthInfo.getTargetErrorType());
        Assert.assertEquals(validation.getCompletionTime(), healthInfo.getTimeOfFirstFailure());
        Assert.assertEquals(1, healthInfo.getNumberOfConsecutiveFailures());

        //Test with present failed discovery.
        final Discovery discovery = new Discovery(probeId, targetId, identityProvider);
        discovery.addError(errorDTO);
        discovery.fail();
        DiscoveryFailure discoveryFailure = new DiscoveryFailure(discovery);
        for (int i = 0 ; i < 2 ; i++)    {
            discoveryFailure.incrementFailsCount();
        }
        Map<Long, DiscoveryFailure> targetToFailedDiscoveries = new HashMap<>();
        targetToFailedDiscoveries.put(targetId, discoveryFailure);
        final FailedDiscoveryTracker failedDiscoveriesTracker = wac.getBean(FailedDiscoveryTracker.class);
        when(failedDiscoveriesTracker.getFailedDiscoveries()).thenReturn(targetToFailedDiscoveries);
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
            .thenReturn(Optional.of(discovery));

        healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(ITargetHealthInfo.TargetHealthSubcategory.VALIDATION,
                        healthInfo.getSubcategory());
        Assert.assertEquals(errorType, healthInfo.getTargetErrorType());
        Assert.assertEquals(validation.getCompletionTime(), healthInfo.getTimeOfFirstFailure());
        Assert.assertEquals(1, healthInfo.getNumberOfConsecutiveFailures());
    }

    @Test
    public void testSingleTargetHealthDiscoveryFailed() throws Exception   {
        final long probeId = createProbeWithOneField(derivedProbeInfo);
        final Target target = createTarget(probeId, "43");
        final long targetId = target.getId();

        final Validation validation = new Validation(probeId,  targetId, identityProvider);
        validation.success();
        when(operationManager.getLastValidationForTarget(targetId))
            .thenReturn(Optional.of(validation));
        final Discovery discovery = new Discovery(probeId, targetId, identityProvider);
        discovery.fail();
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
            .thenReturn(Optional.of(discovery));

        ErrorDTO.ErrorType errorType = ErrorDTO.ErrorType.DATA_IS_MISSING;
        int numberOfFailures = 2;
        ErrorDTO errorDTO = ErrorDTO.newBuilder()
                        .setErrorType(errorType)
                        .setDescription("data is missing")
                        .setSeverity(ErrorDTO.ErrorSeverity.CRITICAL)
                        .build();
        discovery.addError(errorDTO);
        DiscoveryFailure discoveryFailure = new DiscoveryFailure(discovery);
        for (int i = 0 ; i < numberOfFailures ; i++)    {
            discoveryFailure.incrementFailsCount();
        }

        Map<Long, DiscoveryFailure> targetToFailedDiscoveries = new HashMap<>();
        targetToFailedDiscoveries.put(targetId, discoveryFailure);
        final FailedDiscoveryTracker failedDiscoveriesTracker = wac.getBean(FailedDiscoveryTracker.class);
        when(failedDiscoveriesTracker.getFailedDiscoveries()).thenReturn(targetToFailedDiscoveries);

        ITargetHealthInfo healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(ITargetHealthInfo.TargetHealthSubcategory.DISCOVERY,
                        healthInfo.getSubcategory());
        Assert.assertEquals(errorType, healthInfo.getTargetErrorType());
        Assert.assertEquals(discovery.getCompletionTime(), healthInfo.getTimeOfFirstFailure());
        Assert.assertEquals(numberOfFailures, healthInfo.getNumberOfConsecutiveFailures());
    }

    @Test
    public void testSingleTargetHealthNoGoodValidationButDiscoveryOk() throws Exception {
        final long probeId = createProbeWithOneField(derivedProbeInfo);
        final Target target = createTarget(probeId, "43");
        final long targetId = target.getId();

        final Discovery discovery = new Discovery(probeId, targetId, identityProvider);
        discovery.success();
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
            .thenReturn(Optional.of(discovery));

        //Test with no validation data.
        ITargetHealthInfo healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(ITargetHealthInfo.TargetHealthSubcategory.DISCOVERY,
                        healthInfo.getSubcategory());
        Assert.assertEquals("", healthInfo.getErrorText());
        Assert.assertNull(healthInfo.getTargetErrorType());
        Assert.assertNull(healthInfo.getTimeOfFirstFailure());
        Assert.assertEquals(0, healthInfo.getNumberOfConsecutiveFailures());

        //Now test with a failed validation but a newer discovery.
        final Validation validation = new Validation(probeId,  targetId, identityProvider);
        validation.fail();
        ErrorDTO.ErrorType errorType = ErrorDTO.ErrorType.CONNECTION_TIMEOUT;
        ErrorDTO errorDTO = ErrorDTO.newBuilder()
                        .setErrorType(errorType)
                        .setDescription("connection timed out")
                        .setSeverity(ErrorDTO.ErrorSeverity.CRITICAL)
                        .build();
        validation.addError(errorDTO);
        when(operationManager.getLastValidationForTarget(targetId))
            .thenReturn(Optional.of(validation));

        //Set time of success for discovery:
        discovery.success();

        healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(ITargetHealthInfo.TargetHealthSubcategory.DISCOVERY,
                        healthInfo.getSubcategory());
        Assert.assertEquals("", healthInfo.getErrorText());
        Assert.assertNull(healthInfo.getTargetErrorType());
        Assert.assertNull(healthInfo.getTimeOfFirstFailure());
        Assert.assertEquals(0, healthInfo.getNumberOfConsecutiveFailures());
    }

    @Test
    public void testSingleTargetHealthNoData() throws Exception {
        final long probeId = createProbeWithOneField(derivedProbeInfo);
        final Target target = createTarget(probeId, "43");
        final long targetId = target.getId();

        ITargetHealthInfo healthInfo = getTargetHealth(targetId);
        Assert.assertEquals(Long.valueOf(targetId), healthInfo.getTargetId());
        Assert.assertEquals(ITargetHealthInfo.TargetHealthSubcategory.VALIDATION,
                        healthInfo.getSubcategory());
        Assert.assertEquals("No finished validation.", healthInfo.getErrorText());
        Assert.assertNull(healthInfo.getTargetErrorType());
        Assert.assertNull(healthInfo.getTimeOfFirstFailure());
        Assert.assertEquals(0, healthInfo.getNumberOfConsecutiveFailures());
    }

    private TargetHealthInfo getTargetHealth(Long id) throws Exception {
        MvcResult result = mockMvc.perform(get("/target/" + id + "/health")
            .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
            .andExpect(status().isOk())
            .andReturn();
        String resultStr = result.getResponse().getContentAsString();
        return GSON.fromJson(resultStr, TargetHealthInfo.class);
    }

    /**
     * Helper class to interact with the REST interface to add targets.
     */
    private static class TargetAdder {
        private final long probeId;
        private final List<InputField> accountFields;

        TargetAdder(long probeId) {
            this.probeId = probeId;
            accountFields = new ArrayList<>();
        }

        void setAccountField(String name, String value) {
            accountFields.add(new InputField(name, value, Optional.empty()));
        }

        void setAccountField(String name, String value, List<List<String>> groupProperties) {
            accountFields.add(new InputField(name, value, Optional.of(groupProperties)));
        }

        TargetSpec buildSpec() {
            return new TargetSpec(probeId, accountFields, Optional.empty());
        }

        TargetInfo postAndExpect(HttpStatus expectStatus) throws Exception {
            Gson gson = new Gson();
            String reqStr = gson.toJson(buildSpec());
            MvcResult result = mockMvc.perform(post("/target")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).content(reqStr)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().is(expectStatus.value()))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();
            String resultStr = result.getResponse().getContentAsString();
            return gson.fromJson(resultStr, TargetInfo.class);
        }

    }

}
