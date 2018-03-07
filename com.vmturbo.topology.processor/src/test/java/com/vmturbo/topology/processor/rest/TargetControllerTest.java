package com.vmturbo.topology.processor.rest;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
import org.mockito.Mockito;
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

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.ITransport;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.GetAllTargetsResponse;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetInfo;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetSpec;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.RemoteProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore;
import com.vmturbo.topology.processor.targets.KVBackedTargetStore;
import com.vmturbo.topology.processor.targets.Target;
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

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    static class ContextConfiguration extends WebMvcConfigurerAdapter {
        @Bean
        public KeyValueStore keyValueStore() {
            return Mockito.mock(KeyValueStore.class);
        }

        @Bean
        public IdentityProvider identityService() {
            return new IdentityProviderImpl(
                    new IdentityService(
                            new IdentityServiceInMemoryUnderlyingStore(
                                    Mockito.mock(IdentityDatabaseStore.class)),
                            new HeuristicsMatcher()),
                    new MapKeyValueStore(), 0L);
        }

        @Bean
        ProbeStore probeStore() {
            return new RemoteProbeStore(keyValueStore(), identityService(), stitchingOperationStore());
        }

        @Bean
        Scheduler schedulerMock() {
            return Mockito.mock(Scheduler.class);
        }

        @Bean
        StitchingOperationStore stitchingOperationStore() {
            return Mockito.mock(StitchingOperationStore.class);
        }

        @Bean
        public TargetStore targetStore() {
            IdentityProvider mockIdSvc = Mockito.mock(IdentityProvider.class);
            Mockito.when(mockIdSvc.getTargetId(Mockito.anyObject())).thenAnswer( c -> IdentityGenerator.next());
            return new KVBackedTargetStore(keyValueStore(), mockIdSvc, probeStore());
        }

        @Override
        public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
            GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
            msgConverter.setGson(ComponentGsonFactory.createGson());
            converters.add(msgConverter);
        }

        @Bean
        public IOperationManager operationManager() {
            final IOperationManager result = Mockito.mock(IOperationManager.class);
            Mockito.when(result.getLastDiscoveryForTarget(Mockito.anyLong()))
                    .thenReturn(Optional.empty());
            Mockito.when(result.getLastValidationForTarget(Mockito.anyLong()))
                    .thenReturn(Optional.empty());
            return result;
        }

        @Bean
        TopologyHandler topologyHandler() {
            return Mockito.mock(TopologyHandler.class);
        }

        @Bean
        public TargetController targetController() {
            return new TargetController(schedulerMock(), targetStore(), probeStore(),
                            operationManager(), topologyHandler());
        }
    }

    private static final Gson GSON = new Gson();

    private static MockMvc mockMvc;

    // Helper protos to construct probe infos
    // without setting all the useless required fields.
    private ProbeInfo probeInfo;
    private AccountDefEntry mandatoryAccountDef;
    private AccountDefEntry optionalAccountDef;

    private ProbeStore probeStore;
    private TargetStore targetStore;
    private ITransport<MediationServerMessage, MediationClientMessage> transport;
    private IdentityProvider identityProvider;
    private KeyValueStore mockKvStore;
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
        mockKvStore = wac.getBean(KeyValueStore.class);
        operationManager = wac.getBean(IOperationManager.class);
        probeInfo = ProbeInfo.newBuilder()
                .setProbeType("test")
                .setProbeCategory("testCat")
                .addTargetIdentifierField("mandatory")
                .build();
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
                        Mockito.mock(ITransport.class);
        transport = mock;
    }

    @Test
    public void addTarget() throws Exception {
        final AccountDefEntry.Builder accountBuilder =
                        AccountDefEntry.newBuilder(mandatoryAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName("mandatory");
        ProbeInfo oneMandatory = ProbeInfo.newBuilder(probeInfo)
                .addAccountDefinition(accountBuilder).build();
        probeStore.registerNewProbe(oneMandatory, transport);
        TargetAdder adder = new TargetAdder(identityProvider.getProbeId(oneMandatory));
        adder.setAccountField("mandatory", "1");
        TargetInfo result = adder.postAndExpect(HttpStatus.OK);
        Assert.assertNotNull(result.getTargetId());
    }

    @Test
    public void addTargetMissingMandatory() throws Exception {
        final AccountDefEntry.Builder accountBuilder =
                        AccountDefEntry.newBuilder(mandatoryAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName("mandatory");
        ProbeInfo oneMandatory = ProbeInfo.newBuilder(probeInfo)
                .addAccountDefinition(accountBuilder).build();
        probeStore.registerNewProbe(oneMandatory, transport);
        TargetAdder adder = new TargetAdder(identityProvider.getProbeId(oneMandatory));
        TargetInfo result = adder.postAndExpect(HttpStatus.BAD_REQUEST);
        Assert.assertEquals(result.getErrors().size(), 1);
    }

    @Test
    public void addTargetMissingMultipleMandatory() throws Exception {
        final AccountDefEntry.Builder accountBuilder =
                        AccountDefEntry.newBuilder(mandatoryAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName("1mandatory");
        final AccountDefEntry mandatory1 = accountBuilder.build();
        accountBuilder.getCustomDefinitionBuilder().setName("2mandatory");
        final AccountDefEntry mandatory2 = accountBuilder.build();
        ProbeInfo twoMandatory = ProbeInfo.newBuilder(probeInfo)
                .addAccountDefinition(mandatory1)
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
        final AccountDefEntry.Builder accountBuilder =
                        AccountDefEntry.newBuilder(mandatoryAccountDef);
        accountBuilder.getCustomDefinitionBuilder().setName("mandatory");
        final ProbeInfo oneMandatory = ProbeInfo.newBuilder(probeInfo)
                .addAccountDefinition(accountBuilder).build();
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
        TargetInfo result = adder.postAndExpect(HttpStatus.OK);

        TargetSpec retSpec = getTarget(result.getTargetId()).getSpec();
        // Expect an empty return spec, because we shouldn't
        // get secret fields.
        TargetSpec expectedSpec = new TargetAdder(identityProvider.getProbeId(info)).buildSpec();

        assertEqualSpecs(retSpec, expectedSpec);
    }

    @Test
    public void testGetTargetProbeNotRegistered() throws Exception {
        ProbeInfo info = ProbeInfo.newBuilder(probeInfo).build();
        probeStore.registerNewProbe(info, transport);
        TargetInfo result = new TargetAdder(identityProvider.getProbeId(info)).postAndExpect(HttpStatus.OK);

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
        final Discovery discovery = new Discovery(0, probeId, identityProvider);
        final Validation validation = new Validation(0, probeId, identityProvider);
        Mockito.when(operationManager.getLastDiscoveryForTarget(Mockito.anyLong()))
                .thenReturn(Optional.empty());
        Mockito.when(operationManager.getLastValidationForTarget(Mockito.anyLong()))
                .thenReturn(Optional.empty());

        final TargetAdder adder = new TargetAdder(identityProvider.getProbeId(info));
        adder.setAccountField("mandatory", "prop");
        final TargetInfo result = adder.postAndExpect(HttpStatus.OK);
        Assert.assertNotNull(result.getTargetId());
        Assert.assertEquals("<status unknown>", result.getStatus());
        Assert.assertNull(result.getLastValidationTime());

        Mockito.when(operationManager.getLastDiscoveryForTarget(Mockito.anyLong()))
                .thenReturn(Optional.of(discovery));
        Mockito.when(operationManager.getLastValidationForTarget(Mockito.anyLong()))
                .thenReturn(Optional.of(validation));
        {
            final TargetInfo target2 = getTarget(result.getId());
            Assert.assertThat(target2.getStatus(), CoreMatchers.containsString("in progress"));
            Assert.assertNull(target2.getLastValidationTime());
        }
        {
            validation.success();
            final TargetInfo target3 = getTarget(result.getId());
            Assert.assertThat(target3.getStatus(), CoreMatchers.containsString("in progress"));
            Assert.assertEquals(validation.getCompletionTime(), target3.getLastValidationTime());
        }
        {
            discovery.success();
            final TargetInfo target4 = getTarget(result.getId());
            Assert.assertThat(target4.getStatus(),
                    CoreMatchers.containsString(TargetController.VALIDATED));
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
                    CoreMatchers.containsString(TargetController.VALIDATED));
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
        ProbeInfo info = ProbeInfo.newBuilder(probeInfo)
                .addAccountDefinition(accountBuilder).build();
        probeStore.registerNewProbe(info, transport);

        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            TargetAdder adder = new TargetAdder(identityProvider.getProbeId(info));
            adder.setAccountField("secret", "nooneknows" + i);
            TargetInfo resp = adder.postAndExpect(HttpStatus.OK);
            ids.add(resp.getTargetId());
        }

        Map<Long, TargetInfo> targets = getAllTargets();
        TargetSpec emptySpec = new TargetAdder(identityProvider.getProbeId(info)).buildSpec();
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
                        (ITransport<MediationServerMessage, MediationClientMessage>)Mockito
                                        .mock(ITransport.class);
        probeStore.registerNewProbe(goneProbe, goneTransport);

        ProbeInfo registeredProbe = ProbeInfo.newBuilder(probeInfo).setProbeType("type2").build();
        probeStore.registerNewProbe(registeredProbe, transport);

        final Long goneId = new TargetAdder(identityProvider.getProbeId(goneProbe)).postAndExpect(HttpStatus.OK).getTargetId();
        final Long registeredId = new TargetAdder(identityProvider.getProbeId(registeredProbe)).postAndExpect(HttpStatus.OK).getTargetId();

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
        assertEqualSpecs(targets.get(registeredId).getSpec(),
                new TargetAdder(identityProvider.getProbeId(registeredProbe)).buildSpec());
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
                        .addAccountValue(account).build();
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
     * Creates one probe with one mandatory field, registeres it in probe store and returns its id.
     *
     * @return id of the probe
     * @throws Exception on exceptions occurred.
     */
    private long createProbeWithOneField() throws Exception {
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
        final long probeId = createProbeWithOneField();
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
        final long probeId = createProbeWithOneField();
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
     * Tests for trial to update not existing target. Expected to return NOT_FOUND error.
     *
     * @throws Exception on exceptions occur
     */
    @Test
    public void updateNotExistingTarget() throws Exception {
        final long probeId = createProbeWithOneField();
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
        final String reqStr = new Gson().toJson(targetSpec.getInputFields());
        return mockMvc.perform(MockMvcRequestBuilders.put("/target/" + targetId)
                        .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).content(reqStr)
                        .accept(MediaType.APPLICATION_JSON_UTF8_VALUE));
    }

    private <T> T decodeResult(final MvcResult mvcResult, Class<T> responseClass)
                    throws UnsupportedEncodingException {
        final String resultStr = mvcResult.getResponse().getContentAsString();
        return GSON.fromJson(resultStr, responseClass);
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
            return new TargetSpec(probeId, accountFields);
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
