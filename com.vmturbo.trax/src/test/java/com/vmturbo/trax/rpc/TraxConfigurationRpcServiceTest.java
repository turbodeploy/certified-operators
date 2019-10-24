package com.vmturbo.trax.rpc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import io.grpc.Status.Code;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.trax.Trax.ClearAllConfigurationsRequest;
import com.vmturbo.common.protobuf.trax.Trax.ClearAllConfigurationsResponse;
import com.vmturbo.common.protobuf.trax.Trax.ClearConfigurationsRequest;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimit;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimit.TrackingTimeLimit;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimit.TrackingUseLimit;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder.TrackingUseLimitRemainder;
import com.vmturbo.common.protobuf.trax.Trax.TraxConfigurationItem;
import com.vmturbo.common.protobuf.trax.Trax.TraxConfigurationListingRequest;
import com.vmturbo.common.protobuf.trax.Trax.TraxConfigurationRequest;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration.Verbosity;
import com.vmturbo.common.protobuf.trax.TraxConfigurationServiceGrpc;
import com.vmturbo.common.protobuf.trax.TraxConfigurationServiceGrpc.TraxConfigurationServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxConfiguration;
import com.vmturbo.trax.TraxConfiguration.TopicSettings;
import com.vmturbo.trax.TraxConfiguration.TraxContext;
import com.vmturbo.trax.TraxConfiguration.TraxNoLimit;
import com.vmturbo.trax.TraxConfiguration.TraxTimeLimit;
import com.vmturbo.trax.TraxConfiguration.TraxUseLimit;

/**
 * Tests for TraxConfigurationRpcService.
 */
public class TraxConfigurationRpcServiceTest {

    private TraxConfigurationRpcService traxConfigurationBackend =
        new TraxConfigurationRpcService();

    /**
     * The server.
     */
    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(traxConfigurationBackend);

    /**
     * The expected exception.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private TraxConfigurationServiceBlockingStub traxConfigStub;

    /**
     * setup.
     *
     * @throws Exception if something goes wrong.
     */
    @Before
    public void setup() throws Exception {
        TraxConfiguration.clearAllConfiguration();

        MockitoAnnotations.initMocks(this);
        traxConfigStub = TraxConfigurationServiceGrpc.newBlockingStub(server.getChannel());
    }

    /**
     * tearDown.
     */
    @After
    public void tearDown() {
        TraxConfiguration.clearAllConfiguration();
    }

    /**
     * testSetConfiguration.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testSetConfiguration() throws Exception {
        traxConfigStub.setConfiguration(TraxConfigurationRequest.newBuilder()
                .addTopicNames("foo")
                .setTopicConfiguration(TraxTopicConfiguration.newBuilder()
                        .setVerbosity(Verbosity.DEBUG)
                        .setMaxDecimalPlaces(3)
                        .build()
                ).setTrackingLimit(TrackingLimit.newBuilder()
                        .setTimeLimit(TrackingTimeLimit.newBuilder()
                            .setTimeLimitMinutes(90))
                ).build()
        );

        final TopicSettings config = TraxConfiguration.getTrackingTopics().values().iterator().next();
        assertEquals(Verbosity.DEBUG, config.getVerbosity());
        assertEquals(3, config.getMaxDecimalPlaces());
        assertEquals("foo", config.getTopicNames().iterator().next());

        assertTrue(config.getLimit() instanceof TraxTimeLimit);
        final TraxTimeLimit timeLimit = (TraxTimeLimit)config.getLimit();
        assertFalse(timeLimit.isExhausted());
        assertEquals(90, timeLimit.getOriginalTimeLimitMinutes());
    }

    /**
     * testSetConfigurationNoTopic.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testSetConfigurationNoTopic() throws Exception {
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INTERNAL)
            .descriptionContains("At least one topic name is required"));

        traxConfigStub.setConfiguration(
            TraxConfigurationRequest.newBuilder()
                .setTopicConfiguration(TraxTopicConfiguration.newBuilder()
                        .setVerbosity(Verbosity.DEBUG)
                        .setMaxDecimalPlaces(3)
                        .build()
                ).setTrackingLimit(TrackingLimit.newBuilder()
                    .setTimeLimit(TrackingTimeLimit.newBuilder()
                        .setTimeLimitMinutes(90))
            ).build()
        );
    }

    /**
     * testSetConfigurationEmptyTopic.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testSetConfigurationEmptyTopic() throws Exception {
        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INTERNAL)
            .descriptionContains("Empty topic names are not permitted"));

        traxConfigStub.setConfiguration(
            TraxConfigurationRequest.newBuilder()
                .addTopicNames("foo")
                .addTopicNames("")
                .addTopicNames("bar")
                .setTopicConfiguration(TraxTopicConfiguration.newBuilder()
                        .setVerbosity(Verbosity.DEBUG)
                        .setMaxDecimalPlaces(3)
                        .build()
                ).setTrackingLimit(TrackingLimit.newBuilder()
                    .setTimeLimit(TrackingTimeLimit.newBuilder()
                        .setTimeLimitMinutes(90))
            ).build()
        );
    }

    /**
     * testListTopicConfigurations.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testListTopicConfigurations() throws Exception {
        TraxConfiguration.configureTopics(new TopicSettings(TraxTopicConfiguration.newBuilder()
            .setVerbosity(Verbosity.DEBUG)
            .setMaxDecimalPlaces(22)
            .build(), TraxNoLimit.getInstance(), Collections.singleton("foo")));
        TraxConfiguration.configureTopics("bar", Verbosity.TRACE);

        final HashMap<String, TraxConfigurationItem> beforeItems = fetchConfigurationItems();

        assertThat(beforeItems.get("foo").getTopicConfiguration(), is(TraxTopicConfiguration.newBuilder()
            .setVerbosity(TraxTopicConfiguration.Verbosity.DEBUG)
            .setMaxDecimalPlaces(22)
            .build()));

        assertThat(beforeItems.get("bar").getTopicConfiguration(), is(TraxTopicConfiguration.newBuilder()
            .setVerbosity(TraxTopicConfiguration.Verbosity.TRACE)
            .build()));
    }

    /**
     * testListConfigurationRemainder.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testListConfigurationRemainder() throws Exception {
        TraxConfiguration.configureTopics(new TopicSettings(TraxTopicConfiguration.newBuilder()
            .setVerbosity(Verbosity.DEBUG).build(), new TraxUseLimit(4), Collections.singleton("foo")));
        TraxConfiguration.configureTopics(new TopicSettings(TraxTopicConfiguration.newBuilder()
            .setVerbosity(Verbosity.TRACE).build(), TraxNoLimit.getInstance(), Collections.singleton("bar")));

        try (TraxContext context = Trax.track("foo")) {
            // don't actually do anything, but set up a use
        }

        final HashMap<String, TraxConfigurationItem> beforeItems = fetchConfigurationItems();

        assertThat(beforeItems.get("foo").getLimitRemainder(), is(TrackingLimitRemainder.newBuilder()
            .setUseLimit(TrackingUseLimitRemainder.newBuilder()
                .setOriginalUseLimit(4)
                .setRemainingUseLimit(3))
            .build()));
    }

    /**
     * testOverridingTopicConfiguration.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testOverridingTopicConfiguration() throws Exception {
        traxConfigStub.setConfiguration(TraxConfigurationRequest.newBuilder()
                .addTopicNames("foo")
                .addTopicNames("bar")
                .setTopicConfiguration(TraxTopicConfiguration.newBuilder()
                        .setVerbosity(Verbosity.DEBUG)
                        .setMaxDecimalPlaces(3)
                        .build()
                ).setTrackingLimit(TrackingLimit.newBuilder()
                        .setTimeLimit(TrackingTimeLimit.newBuilder()
                            .setTimeLimitMinutes(90))
                ).build()
        );

        final HashMap<String, TraxConfigurationItem> beforeItems = fetchConfigurationItems();
        assertEquals(beforeItems.get("foo"), beforeItems.get("bar"));

        traxConfigStub.setConfiguration(TraxConfigurationRequest.newBuilder()
                .addTopicNames("bar")
                .setTopicConfiguration(TraxTopicConfiguration.newBuilder()
                        .setVerbosity(Verbosity.TRACE)
                        .setMaxDecimalPlaces(6)
                        .build()
                ).setTrackingLimit(TrackingLimit.newBuilder()
                        .setUseLimit(TrackingUseLimit.newBuilder()
                            .setMaxCalculationsToTrack(50))
                ).build()
        );

        final HashMap<String, TraxConfigurationItem> afterItems = fetchConfigurationItems();
        assertNotEquals(afterItems.get("foo"), beforeItems.get("bar"));
    }

    /**
     * testClearSpecificConfigurations.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testClearSpecificConfigurations() throws Exception {
        TraxConfiguration.configureTopics(new TopicSettings(TraxTopicConfiguration.newBuilder()
            .setVerbosity(Verbosity.DEBUG)
            .setMaxDecimalPlaces(22)
            .build(), TraxNoLimit.getInstance(), Arrays.asList("foo", "bar")));
        TraxConfiguration.configureTopics("baz", Verbosity.TRACE);

        assertThat(TraxConfiguration.getTrackingTopics().keySet(), containsInAnyOrder("foo", "bar", "baz"));

        assertThat(traxConfigStub.clearConfigurations(ClearConfigurationsRequest.newBuilder()
            .addTopicNames("foo")
            .addTopicNames("baz")
            .build())
            .getClearedTopicNamesList(), containsInAnyOrder("foo", "baz"));
        assertThat(traxConfigStub.clearConfigurations(ClearConfigurationsRequest.newBuilder()
            .addTopicNames("foo")
            .addTopicNames("baz")
            .build())
            .getClearedTopicNamesList(), is(empty()));

        assertThat(fetchConfigurationItems().keySet(), contains("bar"));
        assertThat(traxConfigStub.clearConfigurations(ClearConfigurationsRequest.newBuilder()
            .addTopicNames("bar")
            .build())
            .getClearedTopicNamesList(), contains("bar"));
    }

    /**
     * testClearAllConfigurations.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testClearAllConfigurations() throws Exception {
        TraxConfiguration.configureTopics(new TopicSettings(TraxTopicConfiguration.newBuilder()
            .setVerbosity(Verbosity.DEBUG)
            .setMaxDecimalPlaces(22)
            .build(), TraxNoLimit.getInstance(), Collections.singleton("foo")));
        TraxConfiguration.configureTopics("bar", Verbosity.TRACE);

        assertEquals(2, TraxConfiguration.getTrackingTopics().size());
        final ClearAllConfigurationsResponse response = traxConfigStub.clearAllConfigurations(
            ClearAllConfigurationsRequest.getDefaultInstance());
        assertEquals(2, response.getConfigurationClearCount());

        assertEquals(0, TraxConfiguration.getTrackingTopics().size());
        assertEquals(0, fetchConfigurationItems().size());
    }

    private HashMap<String, TraxConfigurationItem> fetchConfigurationItems() {
        final Iterable<TraxConfigurationItem> listing = () -> traxConfigStub
            .listConfigurations(TraxConfigurationListingRequest.getDefaultInstance());
        final HashMap<String, TraxConfigurationItem> items = new HashMap<>();
        for (TraxConfigurationItem item : listing) {
            item.getTopicNamesList().forEach(name -> items.put(name, item));
        }

        return items;
    }
}