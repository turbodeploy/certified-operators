package com.vmturbo.components.common.health;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewCheck;
import com.ecwid.consul.v1.agent.model.NewService;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.ComponentController;

/**
 * Test the handler for providing service registration calls to Consul.
 */
public class ConsulHealthcheckRegistrationTest {

    // parameters to the ConsulHealthcheckRegistration constructor
    private final ConsulClient consulClient = Mockito.mock(ConsulClient.class);
    private final String componentType = "component_type";
    private final String instanceId = "component_instance_id";
    private final String instanceIp = "1.2.3.4";
    private final String instanceRoute = "";
    private final Integer serverPort = 8080;
    private final int maxRetrySecs = 60;
    private final MutableFixedClock clock = new MutableFixedClock(1_000_000);

    // the ConsulHealthcheckRegistration under test
    private ConsulHealthcheckRegistration consulHealthcheckRegistration;


    private String expectedRandomizedId(final String instanceId) {
        return instanceId + "-" + clock.millis();
    }

    /**
     * Test the register service call when registration is enabled.
     */
    @Test
    public void testRegisterConsulServiceEnabled() {
        // arrange
        consulHealthcheckRegistration = new ConsulHealthcheckRegistration(consulClient,
            true, componentType, instanceId, instanceIp, instanceRoute, serverPort,
                maxRetrySecs, maxRetrySecs, "", clock);
        NewService expectedNewService = new NewService();
        expectedNewService.setName(componentType);
        expectedNewService.setId(expectedRandomizedId(instanceId));
        expectedNewService.setAddress(instanceIp);
        expectedNewService.setPort(serverPort);
        expectedNewService.setTags(Collections.singletonList(ConsulHealthcheckRegistration.COMPONENT_TAG));
        NewCheck expectedNewCheck = new NewCheck();
        expectedNewCheck.setId("service:" + expectedRandomizedId(instanceId));
        expectedNewCheck.setName("Service '" + componentType + "' check");
        expectedNewCheck.setHttp("http://" + instanceIp + ':' + serverPort +
            ComponentController.HEALTH_PATH);
        expectedNewCheck.setServiceId(expectedRandomizedId(instanceId));
        expectedNewCheck.setInterval("60s");
        expectedNewCheck.setDeregisterCriticalServiceAfter("60m");
        Response<Void> dummyResponse = new Response<>(null, new Long(1), true, new Long(0));
        Mockito.when(consulClient.agentServiceRegister(Mockito.any())).thenReturn(dummyResponse);
        Mockito.when(consulClient.agentCheckRegister(Mockito.any())).thenReturn(dummyResponse);

        // act
        consulHealthcheckRegistration.registerService();

        // assert
        ArgumentCaptor<NewService> serviceArgCaptor = ArgumentCaptor.forClass(NewService.class);
        verify(consulClient).agentServiceRegister(serviceArgCaptor.capture());
        assertEquals(expectedNewService.toString(), serviceArgCaptor.getValue().toString());

        ArgumentCaptor<NewCheck> healthcheckArgCaptor = ArgumentCaptor.forClass(NewCheck.class);
        verify(consulClient).agentCheckRegister(healthcheckArgCaptor.capture());
        assertEquals(expectedNewCheck.toString(), healthcheckArgCaptor.getValue().toString());
        verifyNoMoreInteractions(consulClient);
    }

    /**
     * Test the register service call when registration is enabled and instanceIP is blank.
     * The IP of the local machine should be used.
     *
     * @throws UnknownHostException - if the cannot find an IP for localhost - should not happen
     */
    @Test
    public void testRegisterConsulServiceEnabledNoInstanceIp() throws UnknownHostException {
        // arrange
        consulHealthcheckRegistration = new ConsulHealthcheckRegistration(consulClient,
            true, componentType, instanceId, null, instanceRoute,
                serverPort, maxRetrySecs, maxRetrySecs, "", clock);
        String localhostIp = InetAddress.getLocalHost().getHostAddress();
        NewService expectedNewService = new NewService();
        expectedNewService.setName(componentType);
        expectedNewService.setId(expectedRandomizedId(instanceId));
        expectedNewService.setTags(Collections.singletonList(ConsulHealthcheckRegistration.COMPONENT_TAG));
        expectedNewService.setAddress(localhostIp);
        expectedNewService.setPort(serverPort);
        NewCheck expectedNewCheck = new NewCheck();
        expectedNewCheck.setId("service:" + expectedRandomizedId(instanceId));
        expectedNewCheck.setName("Service '" + componentType + "' check");
        expectedNewCheck.setHttp("http://" + localhostIp + ':' + serverPort +
            ComponentController.HEALTH_PATH);
        expectedNewCheck.setServiceId(expectedRandomizedId(instanceId));
        expectedNewCheck.setInterval("60s");
        expectedNewCheck.setDeregisterCriticalServiceAfter("60m");
        Response<Void> dummyResponse = new Response<>(null, new Long(1), true, new Long(0));
        Mockito.when(consulClient.agentServiceRegister(Mockito.any())).thenReturn(dummyResponse);
        Mockito.when(consulClient.agentCheckRegister(Mockito.any())).thenReturn(dummyResponse);

        // act
        consulHealthcheckRegistration.registerService();

        // assert
        ArgumentCaptor<NewService> serviceArgCaptor = ArgumentCaptor.forClass(NewService.class);
        verify(consulClient).agentServiceRegister(serviceArgCaptor.capture());
        assertEquals(expectedNewService.toString(), serviceArgCaptor.getValue().toString());

        ArgumentCaptor<NewCheck> healthcheckArgCaptor = ArgumentCaptor.forClass(NewCheck.class);
        verify(consulClient).agentCheckRegister(healthcheckArgCaptor.capture());
        assertEquals(expectedNewCheck.toString(), healthcheckArgCaptor.getValue().toString());
        verifyNoMoreInteractions(consulClient);
    }

    /**
     * Test the register service call when registration is not enabled.
     * Should not call Consul registration at all.
     */
    @Test
    public void testRegisterConsulServiceDisabled() {
        // arrange
        consulHealthcheckRegistration = new ConsulHealthcheckRegistration(consulClient,
            false, componentType, instanceId, instanceIp, instanceRoute,
                serverPort, 0, 0, "", clock);
        // act
        consulHealthcheckRegistration.registerService();
        // assert
        verifyNoMoreInteractions(consulClient);
    }

    /**
     * Test the de-register service call when registration is enabled.
     */
    @Test
    public void deregisterServiceEnabled() {
        // arrange
        consulHealthcheckRegistration = new ConsulHealthcheckRegistration(consulClient,
            true, componentType, instanceId, instanceIp, instanceRoute,
                serverPort, maxRetrySecs, maxRetrySecs, "", clock);
        // act
        consulHealthcheckRegistration.deregisterService();
        // assert
        verify(consulClient).agentServiceDeregister(eq(expectedRandomizedId(instanceId)));
        verifyNoMoreInteractions(consulClient);
    }

    /**
     * Test the register service call when registration is not enabled.
     * No consul service calls should be made.
     */
    @Test
    public void deregisterServiceDisabled() {
        // arrange
        consulHealthcheckRegistration = new ConsulHealthcheckRegistration(consulClient,
            false, componentType, instanceId, instanceIp, instanceRoute,
                serverPort, maxRetrySecs, maxRetrySecs, "", clock);
        // act
        consulHealthcheckRegistration.deregisterService();
        // assert
        verifyNoMoreInteractions(consulClient);
    }

    /**
     * Test that encoding/decoding the instance route to a Consul tag works as expected.
     */
    @Test
    public void testRouteEncodingRoundTrip() {
        final String route = "/foo/bar";
        assertThat(ConsulHealthcheckRegistration.decodeInstanceRoute(
            ConsulHealthcheckRegistration.encodeInstanceRoute(route)).get(), is(route));
    }
}