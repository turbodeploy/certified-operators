package com.vmturbo.components.common.health;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.time.temporal.ChronoUnit;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentIdentifier;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentInfo;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStarting;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStopping;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.UriInfo;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.notification.ComponentStatusNotificationSender;
import com.vmturbo.components.common.utils.BuildProperties;

/**
 * Unit tests for the {@link ComponentStatusNotifier}.
 */
public class ComponentStatusNotifierTest {

    private ComponentStatusNotificationSender notificationSender = mock(ComponentStatusNotificationSender.class);

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private static final String COMPONENT_TYPE = "foo";
    private static final String INSTANCE_ID = "foo-instance-1";
    private static final String INSTANCE_IP = "10.01.01.10";
    private static final String INSTANCE_ROUTE = "some-route";
    private static final int PORT = 819;

    /**
     * Test sending a starting notification.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testNotifyStart() throws Exception {

        final ComponentStatusNotifier notifier = new ComponentStatusNotifier(notificationSender,
            true, COMPONENT_TYPE, INSTANCE_ID, INSTANCE_IP, INSTANCE_ROUTE, PORT, clock);
        notifier.notifyComponentStartup();

        final long startMillis = clock.millis();
        clock.addTime(1, ChronoUnit.MINUTES);

        ArgumentCaptor<ComponentStarting> startingCaptor = ArgumentCaptor.forClass(ComponentStarting.class);
        verify(notificationSender).sendStartingNotification(startingCaptor.capture());

        final ComponentStarting componentStarting = startingCaptor.getValue();
        assertThat(componentStarting, is(ComponentStarting.newBuilder()
            .setStartTimestamp(startMillis)
            .setComponentInfo(ComponentInfo.newBuilder()
                .setId(ComponentIdentifier.newBuilder()
                    .setComponentType(COMPONENT_TYPE)
                    .setInstanceId(INSTANCE_ID)
                    .setJvmId(startMillis))
                .setUriInfo(UriInfo.newBuilder()
                    .setIpAddress(INSTANCE_IP)
                    .setRoute(INSTANCE_ROUTE)
                    .setPort(PORT)))
            .setBuildProperties(BuildProperties.get().toProto())
            .build()));

    }

    /**
     * Test that when registration is disabled we don't send out startup notification.
     */
    @Test
    public void testNotifyStartRegistrationDisabled() {
        final ComponentStatusNotifier notifier = new ComponentStatusNotifier(notificationSender,
            false, COMPONENT_TYPE, INSTANCE_ID, INSTANCE_IP, INSTANCE_ROUTE, PORT, clock);
        notifier.notifyComponentStartup();

        verifyZeroInteractions(notificationSender);
    }

    /**
     * Test sending a shutdown notification.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testNotifyShutdown() throws Exception {
        final ComponentStatusNotifier notifier = new ComponentStatusNotifier(notificationSender,
            true, COMPONENT_TYPE, INSTANCE_ID, INSTANCE_IP, INSTANCE_ROUTE, PORT, clock);

        final long startMillis = clock.millis();
        clock.addTime(1, ChronoUnit.MINUTES);

        notifier.notifyComponentShutdown();

        final ArgumentCaptor<ComponentStopping> stoppingCaptor = ArgumentCaptor.forClass(ComponentStopping.class);
        verify(notificationSender).sendStoppingNotification(stoppingCaptor.capture());
        final ComponentStopping componentStopping = stoppingCaptor.getValue();
        assertThat(componentStopping, is(ComponentStopping.newBuilder()
            .setStartTimestamp(startMillis)
            .setComponentId(ComponentIdentifier.newBuilder()
                .setComponentType(COMPONENT_TYPE)
                .setInstanceId(INSTANCE_ID)
                .setJvmId(startMillis))
            .build()));
    }

    /**
     * Test that when registration is disabled we don't send out shutdown notification.
     */
    @Test
    public void testNotifyShutdownRegistrationDisabled() {
        final ComponentStatusNotifier notifier = new ComponentStatusNotifier(notificationSender,
            false, COMPONENT_TYPE, INSTANCE_ID, INSTANCE_IP, INSTANCE_ROUTE, PORT, clock);
        notifier.notifyComponentShutdown();

        verifyZeroInteractions(notificationSender);
    }
}