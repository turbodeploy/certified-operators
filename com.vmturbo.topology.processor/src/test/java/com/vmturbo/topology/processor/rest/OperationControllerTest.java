package com.vmturbo.topology.processor.rest;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
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
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.vmturbo.communication.ITransport;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.ValidationRequest;
import com.vmturbo.topology.processor.TestProbeStore;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.api.impl.OperationRESTApi.DiscoverAllResponse;
import com.vmturbo.topology.processor.api.impl.OperationRESTApi.OperationDto;
import com.vmturbo.topology.processor.api.impl.OperationRESTApi.OperationResponse;
import com.vmturbo.topology.processor.api.impl.OperationRESTApi.ValidateAllResponse;
import com.vmturbo.topology.processor.api.impl.TargetRESTApi.TargetSpec;
import com.vmturbo.topology.processor.communication.RemoteMediation;
import com.vmturbo.topology.processor.communication.RemoteMediationServer;
import com.vmturbo.topology.processor.controllable.EntityActionDao;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;
import com.vmturbo.topology.processor.operation.Operation;
import com.vmturbo.topology.processor.operation.OperationListener;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.operation.OperationMessageHandler;
import com.vmturbo.topology.processor.operation.OperationTestUtilities;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileUploader;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.targets.KVBackedTargetStore;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyHandler;

/**
 * Testing the Operation REST API.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
// Need clean context with no probes/targets registered.
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class OperationControllerTest {
    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    static class ContextConfiguration  extends WebMvcConfigurerAdapter {
        @Bean
        IdentityProvider identityProvider() {
            return new IdentityProviderImpl(
                    new IdentityService(
                        new IdentityServiceInMemoryUnderlyingStore(
                                Mockito.mock(IdentityDatabaseStore.class)),
                        new HeuristicsMatcher()),
                    new MapKeyValueStore(), 0L);
        }

        @Bean
        TestProbeStore probeStore() {
            return new TestProbeStore(identityProvider());
        }

        @Bean
        TargetStore targetStore() {
            return new KVBackedTargetStore(new MapKeyValueStore(), identityProvider(),
                            probeStore());
        }

        /**
         * Mock the remote mediation server to avoid sending actual websocket requests
         * in non-feature tests.
         *
         * @return A mock {@link RemoteMediationServer}
         */
        @Bean
        RemoteMediation mockRemoteMediation() {
            return Mockito.mock(RemoteMediation.class);
        }

        @Bean
        Scheduler scheduler() {
            return Mockito.mock(Scheduler.class);
        }

        @Bean
        TopologyHandler topologyHandler() {
            return Mockito.mock(TopologyHandler.class);
        }

        @Bean
        EntityStore entityRepository() {
            return Mockito.mock(EntityStore.class);
        }

        @Bean
        DiscoveredGroupUploader groupRecorder() {
            return Mockito.mock(DiscoveredGroupUploader.class);
        }

        @Bean
        DiscoveredTemplateDeploymentProfileUploader discoveredTemplatesUploader() {
            return Mockito.mock(DiscoveredTemplateDeploymentProfileUploader.class);
        }

        @Bean
        EntityActionDao controllableDao() {
            return Mockito.mock(EntityActionDao.class);
        }

        @SuppressWarnings("unchecked")
        OperationListener operationListener() {
            return Mockito.mock(OperationListener.class);
        }

        @Bean
        OperationManager operationManager() {
            return new OperationManager(identityProvider(),
                targetStore(),
                probeStore(),
                mockRemoteMediation(),
                operationListener(),
                entityRepository(),
                groupRecorder(), discoveredTemplatesUploader(), controllableDao(),
                10, 10, 10
            );
        }

        @Bean
        public OperationController operationController() {
            return new OperationController(operationManager(), scheduler(), targetStore());
        }

        @Override
        public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
            GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
            msgConverter.setGson(ComponentGsonFactory.createGson());
            converters.add(msgConverter);
        }
    }

    private final Gson gson = new Gson();

    private long probeId;
    private long targetId;

    private MockMvc mockMvc;
    private OperationManager operationManager;
    private RemoteMediation mockRemoteMediation;
    private Scheduler mockScheduler;
    private IdentityProvider identityProvider;
    private ProbeStore probeStore;
    private TargetStore targetStore;

    @SuppressWarnings("unchecked")
    private final ITransport<MediationServerMessage, MediationClientMessage> transport =
            (ITransport<MediationServerMessage, MediationClientMessage>)Mockito.mock(ITransport.class);

    @Autowired
    private WebApplicationContext wac;

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        System.setProperty("com.vmturbo.keydir", testFolder.newFolder().getAbsolutePath());
        // Initialize identity generator so that targets can get IDs.
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();

        identityProvider = wac.getBean(IdentityProvider.class);
        probeStore = wac.getBean(TestProbeStore.class);
        operationManager = wac.getBean(OperationManager.class);
        mockScheduler = wac.getBean(Scheduler.class);
        mockRemoteMediation = wac.getBean(RemoteMediation.class);
        targetStore = wac.getBean(TargetStore.class);

        probeId = addProbe("category", "type");
        targetId = addTarget(probeId);

        Mockito.when(mockRemoteMediation.getMessageHandlerExpirationClock())
                        .thenReturn(Clock.systemUTC());
    }

    /**
     * Test that triggering a discovery via the REST API works.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testDiscoverSuccess() throws Exception {
        final OperationResponse response = postDiscovery(targetId, HttpStatus.OK);

        Mockito.verify(mockRemoteMediation).sendDiscoveryRequest(
                Mockito.eq(probeId),
                Matchers.any(DiscoveryRequest.class),
                Matchers.any(OperationMessageHandler.class));

        final Discovery discovery = operationManager.getInProgressDiscovery(response.operation.getId()).get();

        Assert.assertNotNull(discovery);
        Assert.assertEquals(discovery.getId(), response.operation.getId());
        Assert.assertEquals(OperationController.toEpochMillis(discovery.getStartTime()),
                        response.operation.getStartTime());
        Assert.assertEquals(Status.IN_PROGRESS, response.operation.getStatus());
        Assert.assertEquals(targetId, (long)response.targetId);
    }

    /**
     *  Test that getting an ongoing discovery by ID works.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetOngoingDiscoveryById() throws Exception {
        final OperationResponse response = postDiscovery(targetId, HttpStatus.OK);
        final OperationResponse operation = getDiscoveryById(response.operation.getId());
        Assert.assertTrue(response.isSuccess());
        Assert.assertTrue(operation.isSuccess());
        assertOperationsEq(response.operation, operation.operation);
    }

    /**
     * Test that two calls to kick off a discovery return the same discovery.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testDiscoverReturnsSameWhenInProgress() throws Exception {
        final OperationResponse firstResponse = postDiscovery(targetId, HttpStatus.OK);
        final OperationResponse secondResponse = postDiscovery(targetId, HttpStatus.OK);

        Assert.assertEquals(firstResponse.operation.getId(), secondResponse.operation.getId());
    }

    /**
     * Test that getting a discovery by target ID works.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetDiscoveryByTarget() throws Exception {
        final OperationResponse firstResponse = postDiscovery(targetId, HttpStatus.OK);
        final OperationResponse byTargetResponse = getDiscoveryByTargetId(targetId);

        assertOperationsEq(firstResponse.operation, byTargetResponse.operation);

        // Simulate the validation completing on the server.
        final Discovery actualDiscovery = operationManager.getLastDiscoveryForTarget(targetId).get();
        operationManager.notifyDiscoveryResult(actualDiscovery,
                com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse.getDefaultInstance());
        OperationTestUtilities.waitForDiscovery(operationManager, firstResponse.operation);

        final OperationResponse completeResponse = getDiscoveryByTargetId(targetId);
        Assert.assertEquals(Status.SUCCESS, completeResponse.operation.getStatus());
    }

    /**
     * Test that an exception during triggering a discovery fails the REST request.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testDiscoveryException() throws Exception {
        Mockito.doThrow(TargetNotFoundException.class)
                .when(mockRemoteMediation)
                .sendDiscoveryRequest(
                        Matchers.anyLong(),
                        Matchers.any(DiscoveryRequest.class),
                        Matchers.any(OperationMessageHandler.class)
                );

        final OperationResponse response = postDiscovery(targetId, HttpStatus.NOT_FOUND);
        Assert.assertNull(response.operation);
    }

    /**
     * Test triggering a discovery when the underlying probe is missing.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testDiscoverProbeNotFound() throws Exception {
        final OperationResponse response = postDiscovery(9999, HttpStatus.NOT_FOUND);
        Assert.assertNull(response.operation);
        Assert.assertEquals(
                "Unable to initiate discovery (Target with id 9999 does not exist in the store.)",
                response.error);
    }

    @Test
    public void testDiscoveryResetsSchedule() throws Exception {
        postDiscovery(targetId, HttpStatus.OK);
        Mockito.verify(mockScheduler).resetDiscoverySchedule(targetId);
    }

    @Test
    public void testDiscoveryScheduleNotResetWhenAlreadyInProgress() throws Exception {
        postDiscovery(targetId, HttpStatus.OK);
        Mockito.verify(mockScheduler, Mockito.times(1)).resetDiscoverySchedule(targetId);

        postDiscovery(targetId, HttpStatus.OK);
        Mockito.verify(mockScheduler, Mockito.times(1)).resetDiscoverySchedule(targetId);
    }

    /**
     * Test that discovering all targets works.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testDiscoverAll() throws Exception {
        final long secondTargetId = addTarget(addProbe("second-category", "second-type"));
        final long thirdTargetId = addTarget(addProbe("third-category", "third-type"));

        final Map<Long, OperationResponse> discoveries = postDiscoverAll();

        Assert.assertEquals(3, discoveries.size());
        Assert.assertTrue(discoveries.get(targetId).isSuccess());
        Assert.assertTrue(discoveries.get(secondTargetId).isSuccess());
        Assert.assertTrue(discoveries.get(thirdTargetId).isSuccess());
    }

    @Test
    public void testDiscoverAllResetsSchedule() throws Exception {
        final long secondTargetId = addTarget(addProbe("second-category", "second-type"));
        final long thirdTargetId = addTarget(addProbe("third-category", "third-type"));

        postDiscoverAll();

        Mockito.verify(mockScheduler).resetDiscoverySchedule(targetId);
        Mockito.verify(mockScheduler).resetDiscoverySchedule(secondTargetId);
        Mockito.verify(mockScheduler).resetDiscoverySchedule(thirdTargetId);
    }

    /**
     * Test getting all ongoing discoveries.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetOngoingDiscoveries() throws Exception {
        final OperationResponse response = postDiscovery(targetId, HttpStatus.OK);
        // Also request a validation, which shouldn't appear in the results
        postValidation(targetId, HttpStatus.OK);

        final List<Discovery> ongoingDiscoveries = getAllDiscoveries();

        Assert.assertEquals(1, ongoingDiscoveries.size());
        final Discovery discovery = ongoingDiscoveries.get(0);

        assertOperationsEq(response.operation, discovery);
    }

    /**
     * Discovery of the first target should fail, but discovery of the second target should succeed.
     * @throws Exception When there is an exception.
     */
    @Test
    public void testDiscoverAllWithException() throws Exception {
        Mockito.doThrow(TargetNotFoundException.class)
                .when(mockRemoteMediation)
                .sendDiscoveryRequest(
                        Matchers.eq(probeId),
                        Matchers.any(DiscoveryRequest.class),
                        Matchers.any(OperationMessageHandler.class)
                );

        final long secondProbeId = addProbe("second-category", "second-type");
        final long secondTargetId = addTarget(secondProbeId);
        Mockito.doNothing().when(mockRemoteMediation)
                .sendDiscoveryRequest(
                        Matchers.eq(secondProbeId),
                        Matchers.any(DiscoveryRequest.class),
                        Matchers.any(OperationMessageHandler.class)
                );

        final Map<Long, OperationResponse> discoveries = postDiscoverAll();
        Assert.assertEquals(2, discoveries.size());
        Assert.assertTrue(discoveries.get(secondTargetId).isSuccess());
        Assert.assertEquals("Unable to initiate discovery (null)", discoveries.get(targetId).error);
    }

    /**
     * Test that triggering a validation via the REST API works.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testValidationSuccess() throws Exception {
        final OperationResponse response = postValidation(targetId, HttpStatus.OK);

        Mockito.verify(mockRemoteMediation).sendValidationRequest(
                Mockito.eq(probeId),
                Matchers.any(ValidationRequest.class),
                Matchers.any(OperationMessageHandler.class));

        Assert.assertNotNull(response.operation);
        final Operation validation = operationManager.getInProgressValidation(response.operation.getId()).get();

        Assert.assertNotNull(validation);
        assertOperationsEq(response.operation, validation);

    }

    /**
     * Test that multiple triggers of a validation via. the REST API return the same ID.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testValidationReturnsSameWhenInProgress() throws Exception {
        final OperationResponse firstResponse = postValidation(targetId, HttpStatus.OK);
        final OperationResponse secondResponse = postValidation(targetId, HttpStatus.OK);

        assertOperationsEq(firstResponse.operation, secondResponse.operation);
    }

    /**
     * Test that getting a validation by target works.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetValidationByTarget() throws Exception {
        final OperationResponse firstResponse = postValidation(targetId, HttpStatus.OK);
        final OperationResponse byTargetResponse = getValidationByTargetId(targetId);

        assertOperationsEq(firstResponse.operation, byTargetResponse.operation);

        // Simulate the validation completing on the server.
        final Validation actualValidation = operationManager.getLastValidationForTarget(targetId).get();
        operationManager.notifyValidationResult(actualValidation,
                com.vmturbo.platform.common.dto.Discovery.ValidationResponse.getDefaultInstance());
        OperationTestUtilities.waitForValidation(operationManager, firstResponse.operation);

        final OperationResponse completeResponse = getValidationByTargetId(targetId);
        Assert.assertEquals(Status.SUCCESS, completeResponse.operation.getStatus());
        Assert.assertEquals(targetId, (long)completeResponse.targetId);
    }

    /**
     * Test that failure to start a validation returns the appropriate code.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testValidationException() throws Exception {
        Mockito.doThrow(TargetNotFoundException.class)
                .when(mockRemoteMediation)
                .sendValidationRequest(
                        Matchers.anyLong(),
                        Matchers.any(ValidationRequest.class),
                        Matchers.any(OperationMessageHandler.class)
                );

        final OperationResponse response = postValidation(targetId, HttpStatus.NOT_FOUND);
        Assert.assertNull(response.operation);
    }

    /**
     * Test that triggering a validation fails when the probe is invalid.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testValidationProbeNotFound() throws Exception {
        final OperationResponse response = postValidation(9999, HttpStatus.NOT_FOUND);
        Assert.assertNull(response.operation);
        Assert.assertEquals(
                "Unable to initiate validation (Target with id 9999 does not exist in the store.)",
                response.error);
    }

    /**
     * Test that triggering validations on all targets works.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testValidateAll() throws Exception {
        final long secondTargetId = addTarget(addProbe("second-category", "second-type"));
        final long thirdTargetId = addTarget(addProbe("third-category", "third-type"));

        final Map<Long, OperationResponse> validations = postValidateAll();
        Assert.assertEquals(3, validations.size());
        Assert.assertTrue(validations.get(targetId).isSuccess());
        Assert.assertTrue(validations.get(secondTargetId).isSuccess());
        Assert.assertTrue(validations.get(thirdTargetId).isSuccess());
    }

    /**
     * Test that getting an ongoing validation by ID works.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetOngoingValidationById() throws Exception {
        final OperationResponse response = postValidation(targetId, HttpStatus.OK);
        final OperationResponse operation = getValidationById(response.operation.getId());
        Assert.assertTrue(response.isSuccess());
        Assert.assertTrue(operation.isSuccess());
        assertOperationsEq(response.operation, operation.operation);
    }

    /**
     * Test that getting all ongoing validations works.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetOngoingValidations() throws Exception {
        final OperationResponse response = postValidation(targetId, HttpStatus.OK);
        // Also request a discovery, which shouldn't appear in the results
        postDiscovery(targetId, HttpStatus.OK);
        final List<Validation> ongoingValidations = getAllValidations();

        Assert.assertEquals(1, ongoingValidations.size());
        final Validation validation = ongoingValidations.get(0);

        assertOperationsEq(response.operation, validation);
    }

    private long addProbe(@Nonnull final String probeCategory,
                          @Nonnull final String probeType) throws Exception {
        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeCategory(probeCategory)
                .setProbeType(probeType)
                .addTargetIdentifierField("name")
                .build();
        probeStore.registerNewProbe(probeInfo, transport);
        return identityProvider.getProbeId(probeInfo);
    }

    private long addTarget(final long probeId) throws Exception {
        TargetSpec target = new TargetSpec(probeId, Collections.emptyList());
        return targetStore.createTarget(target.toDto()).getId();
    }

    private void assertOperationsEq(@Nonnull final OperationDto op1,
                    @Nonnull final Operation op2) {
        assertOperationsEq(op1, op2.toDto());
    }

    private void assertOperationsEq(@Nonnull final OperationDto op1,
                                    @Nonnull final OperationDto op2) {
        Assert.assertEquals(op1.getClass(), op2.getClass());
        Assert.assertEquals(op1.getId(), op2.getId());
        Assert.assertEquals(op1.getStatus(), op2.getStatus());
        Assert.assertEquals(op1.getErrors(), op2.getErrors());
        Assert.assertEquals(op1.getStartTime(), op2.getStartTime());
        Assert.assertEquals(op1.getCompletionTime(), op2.getCompletionTime());
    }

    @Nonnull
    private OperationResponse postDiscovery(final long targetId,
                                            @Nonnull final HttpStatus expectStatus) throws Exception {
        final String response = postAndExpect("/target/" + Long.toString(targetId) + "/discovery",
                expectStatus);
        return gson.fromJson(response, OperationResponse.class);
    }

    @Nonnull
    private OperationResponse postValidation(final long targetId,
                                              @Nonnull final HttpStatus expectStatus) throws Exception {
        final String response = postAndExpect("/target/" + Long.toString(targetId) + "/validation",
                expectStatus);
        return gson.fromJson(response, OperationResponse.class);
    }

    @Nonnull
    private Map<Long, OperationResponse> postDiscoverAll() throws Exception {
        final String response = postAndExpect("/target/discovery", HttpStatus.OK);
        return gson.fromJson(response, DiscoverAllResponse.class).getResponseMap();
    }

    @Nonnull
    private Map<Long, OperationResponse> postValidateAll() throws Exception {
        final String response = postAndExpect("/target/validation", HttpStatus.OK);
        return gson.fromJson(response, ValidateAllResponse.class).getResponseMap();
    }

    @Nonnull
    private OperationResponse getDiscoveryById(final long operationId) throws Exception {
        final String response = getResult("/target/discovery/" + Long.toString(operationId));
        return gson.fromJson(response, OperationResponse.class);
    }

    @Nonnull
    private OperationResponse getValidationById(final long operationId) throws Exception {
        final String response = getResult("/target/validation/" + Long.toString(operationId));
        return gson.fromJson(response, OperationResponse.class);
    }

    @Nonnull
    private OperationResponse getDiscoveryByTargetId(final long targetId) throws Exception {
        final String response = getResult("/target/" + Long.toString(targetId) + "/discovery");
        return gson.fromJson(response, OperationResponse.class);
    }

    @Nonnull
    private OperationResponse getValidationByTargetId(final long targetId) throws Exception {
        final String response = getResult("/target/" + Long.toString(targetId) + "/validation");
        return gson.fromJson(response, OperationResponse.class);
    }

    @Nonnull
    private List<Discovery> getAllDiscoveries() throws Exception {
        final String response = getResult("/target/discovery");
        return gson.fromJson(response, new TypeToken<List<Discovery>>(){}.getType());
    }

    @Nonnull
    private List<Validation> getAllValidations() throws Exception {
        final String response = getResult("/target/validation");
        return gson.fromJson(response, new TypeToken<List<Validation>>(){}.getType());
    }

    @Nonnull
    private String postAndExpect(@Nonnull final String path,
                                 @Nonnull final HttpStatus expectStatus) throws Exception {
        final MvcResult result = mockMvc.perform(post(path)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().is(expectStatus.value()))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();
        return result.getResponse().getContentAsString();
    }

    @Nonnull
    private String getResult(@Nonnull final String path) throws Exception {
        final MvcResult result = mockMvc.perform(get(path)
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andReturn();
        return result.getResponse().getContentAsString();
    }
}
