package com.vmturbo.components.common.health;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.agent.model.NewCheck;
import com.ecwid.consul.v1.agent.model.NewService;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

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
    private final Integer serverPort = 8080;

    // the ConsulHealthcheckRegistration under test
    private ConsulHealthcheckRegistration consulHealthcheckRegistration;


    /**
     * Test the register service call when registration is enabled.
     */
    @Test
    public void testRegisterConsulServiceEnabled() {
        // arrange
        consulHealthcheckRegistration = new ConsulHealthcheckRegistration(consulClient,
            true, componentType, instanceId, instanceIp, serverPort);
        NewService expectedNewService = new NewService();
        expectedNewService.setName(componentType);
        expectedNewService.setId(instanceId);
        expectedNewService.setAddress(instanceIp);
        expectedNewService.setPort(serverPort);
        NewCheck expectedNewCheck = new NewCheck();
        expectedNewCheck.setId("service:" + instanceId);
        expectedNewCheck.setName("Service '" + componentType + "' check");
        expectedNewCheck.setHttp("http://" + instanceIp + ':' + serverPort +
            ComponentController.HEALTH_PATH);
        expectedNewCheck.setServiceId(instanceId);
        expectedNewCheck.setInterval("60s");
        expectedNewCheck.setDeregisterCriticalServiceAfter("60m");

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
            true, componentType, instanceId, null, serverPort);
        String localhostIp = InetAddress.getLocalHost().getHostAddress();
        NewService expectedNewService = new NewService();
        expectedNewService.setName(componentType);
        expectedNewService.setId(instanceId);
        expectedNewService.setAddress(localhostIp);
        expectedNewService.setPort(serverPort);
        NewCheck expectedNewCheck = new NewCheck();
        expectedNewCheck.setId("service:" + instanceId);
        expectedNewCheck.setName("Service '" + componentType + "' check");
        expectedNewCheck.setHttp("http://" + localhostIp + ':' + serverPort +
            ComponentController.HEALTH_PATH);
        expectedNewCheck.setServiceId(instanceId);
        expectedNewCheck.setInterval("60s");
        expectedNewCheck.setDeregisterCriticalServiceAfter("60m");

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
            false, componentType, instanceId, instanceIp, serverPort);
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
            true, componentType, instanceId, instanceIp, serverPort);
        // act
        consulHealthcheckRegistration.deregisterService();
        // assert
        verify(consulClient).agentServiceDeregister(eq(instanceId));
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
            false, componentType, instanceId, instanceIp, serverPort);
        // act
        consulHealthcheckRegistration.deregisterService();
        // assert
        verifyNoMoreInteractions(consulClient);
    }
}