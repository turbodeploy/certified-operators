package com.vmturbo.components.test.utilities.component;

import static org.junit.Assert.assertEquals;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Vector;

import org.junit.Ignore;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

//@RunWith(PowerMockRunner.class)
//@PrepareForTest({ ComponentUtils.class })
//// Prevent linkage error:
//// http://stackoverflow.com/questions/16520699/mockito-powermock-linkageerror-while-mocking-system-class
//@PowerMockIgnore("javax.management.*")
@Ignore("This unit test fails on Java11. Need to be fixed later")
public class ComponentUtilsTest {

    @Test
    public void testGetOsHostRouteForMac() throws Exception {
        assertEquals(ComponentUtils.DOCKER_FOR_MAC_HOST_ROUTE,
            ComponentUtils.getOsHostRoute(ComponentUtils.MAC_OSX_OS_NAME));
    }

    @Test
    public void testGetOsHostRouteForOthers() throws Exception {
        assertEquals(ComponentUtils.DOCKER0_HOST_ROUTE,
            ComponentUtils.getOsHostRoute("foo"));
    }

    @Test
    public void testGetLocalIpDoesNotSelectLoopback() throws Exception {
        NetworkInterface iface = PowerMockito.mock(NetworkInterface.class);
        PowerMockito.when(iface.isLoopback()).thenReturn(true);

        Enumeration<NetworkInterface> interfaces = new Vector<>(Collections.singletonList(iface)).elements();

        assertEquals(Optional.empty(), ComponentUtils.getLocalIp(interfaces));
    }

    @Test
    public void testGetLocalIpDoesNotSelectGateway() throws Exception {
        NetworkInterface iface = PowerMockito.mock(NetworkInterface.class);
        PowerMockito.when(iface.isLoopback()).thenReturn(false);

        InterfaceAddress gateway1 = mockAddress(ComponentUtils.GATEWAY_IPS.get(0));
        InterfaceAddress gateway2 = mockAddress(ComponentUtils.GATEWAY_IPS.get(1));
        PowerMockito.when(iface.getInterfaceAddresses()).thenReturn(
            Arrays.asList(gateway1, gateway2));

        Enumeration<NetworkInterface> interfaces = new Vector<>(Collections.singletonList(iface)).elements();
        assertEquals(Optional.empty(), ComponentUtils.getLocalIp(interfaces));
    }

    @Test
    public void testGetLocalIpSelectsLocalIp() throws Exception {
        NetworkInterface iface = PowerMockito.mock(NetworkInterface.class);
        PowerMockito.when(iface.isLoopback()).thenReturn(false);

        InterfaceAddress gateway = mockAddress(ComponentUtils.GATEWAY_IPS.get(0));
        InterfaceAddress local = mockAddress("10.10.201.87");
        PowerMockito.when(iface.getInterfaceAddresses()).thenReturn(
            Arrays.asList(gateway, local));

        Enumeration<NetworkInterface> interfaces = new Vector<>(Collections.singletonList(iface)).elements();
        assertEquals(Optional.of("10.10.201.87"), ComponentUtils.getLocalIp(interfaces));
    }

    @Test
    public void testGetLocalIpSkipsIpv6() throws Exception {
        NetworkInterface iface = PowerMockito.mock(NetworkInterface.class);
        PowerMockito.when(iface.isLoopback()).thenReturn(false);

        final InetAddress v6Addr = PowerMockito.mock(Inet6Address.class);
        InterfaceAddress v6Iface = mockAddress(v6Addr);
        PowerMockito.when(iface.getInterfaceAddresses()).thenReturn(
            Arrays.asList(v6Iface));

        Enumeration<NetworkInterface> interfaces = new Vector<>(Collections.singletonList(iface)).elements();
        assertEquals(Optional.empty(), ComponentUtils.getLocalIp(interfaces));
    }

    private static InterfaceAddress mockAddress(String addressIp) {
        final InetAddress inetAddr = PowerMockito.mock(Inet4Address.class);
        PowerMockito.when(inetAddr.getHostAddress()).thenReturn(addressIp);

        return mockAddress(inetAddr);
    }

    private static InterfaceAddress mockAddress(InetAddress inetAddr) {
        InterfaceAddress ifaceAddr = PowerMockito.mock(InterfaceAddress.class);
        PowerMockito.when(ifaceAddr.getAddress()).thenReturn(inetAddr);
        return ifaceAddr;
    }
}