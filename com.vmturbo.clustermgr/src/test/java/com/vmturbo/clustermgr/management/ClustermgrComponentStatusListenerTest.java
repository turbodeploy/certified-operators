package com.vmturbo.clustermgr.management;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.vmturbo.clustermgr.management.ComponentRegistry.RegistryUpdateException;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentIdentifier;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentInfo;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStarting;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStatusNotification;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStopping;

/**
 * Unit tests for {@link ClustermgrComponentStatusListener}.
 */
public class ClustermgrComponentStatusListenerTest {

    private ComponentRegistry registry = mock(ComponentRegistry.class);

    private ClustermgrComponentStatusListener statusListener =
        new ClustermgrComponentStatusListener(registry);

    private ComponentInfo componentInfo = ComponentInfo.newBuilder()
        .setId(ComponentIdentifier.newBuilder()
            .setInstanceId("foo"))
        .build();

    /**
     * Test that registration messages get forwarded to the registry.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRegistration() throws Exception {
        final ComponentStarting starting = ComponentStarting.newBuilder()
            .setComponentInfo(componentInfo)
            .build();
        statusListener.onComponentNotification(ComponentStatusNotification.newBuilder()
            .setStartup(starting)
            .build());

        verify(registry).registerComponent(starting);
    }

    /**
     * Test the case where the registry throws exceptions.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRegistrationException() throws Exception {
        final ComponentStarting starting = ComponentStarting.newBuilder()
            .setComponentInfo(componentInfo)
            .build();
        when(registry.registerComponent(starting)).thenThrow(new RegistryUpdateException("foo"));

        statusListener.onComponentNotification(ComponentStatusNotification.newBuilder()
            .setStartup(starting)
            .build());

        verify(registry).registerComponent(starting);
    }

    /**
     * Test that deregistration messages get forwarded to the registry.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDeregistration() throws Exception {
        ComponentInfo componentInfo = ComponentInfo.newBuilder()
            .setId(ComponentIdentifier.newBuilder()
                .setInstanceId("foo"))
            .build();
        statusListener.onComponentNotification(ComponentStatusNotification.newBuilder()
            .setShutdown(ComponentStopping.newBuilder()
                .setComponentId(componentInfo.getId()))
            .build());

        verify(registry).deregisterComponent(componentInfo.getId());
    }

    /**
     * Test the case where the deregistration throws exceptions.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDeregistrationException() throws Exception {
        when(registry.deregisterComponent(componentInfo.getId())).thenThrow(new RegistryUpdateException("foo"));

        statusListener.onComponentNotification(ComponentStatusNotification.newBuilder()
            .setShutdown(ComponentStopping.newBuilder()
                .setComponentId(componentInfo.getId()))
            .build());

        verify(registry).deregisterComponent(componentInfo.getId());
    }

}