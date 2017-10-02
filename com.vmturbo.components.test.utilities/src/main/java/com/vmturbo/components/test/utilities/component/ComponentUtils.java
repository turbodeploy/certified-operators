package com.vmturbo.components.test.utilities.component;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;

/**
 * Various utilities related to component configuration and inter-component communication.
 */
public class ComponentUtils {

    private static final Logger logger = LogManager.getLogger();

    private ComponentUtils() {}

    /**
     * (roman, March 16 2017) This route was added to support contacting the host in
     * docker for Docker for Mac, as well as (presumably) other versions of docker.
     * As long as it works, it's a better alternative than programmatically getting
     * the local IP of the host on the network because:
     *    - It works even if the host is offline.
     *    - It's less error prone (no need to handle various configuration options).
     *
     * The obvious disadvantage is that it's an undocumented feature, and may go
     * away during docker upgrades.
     *
     * See: https://github.com/docker/docker/issues/22753#issuecomment-282725489
     *
     * Note that this address does NOT work on Linux.
     */
    public static final String DOCKER_FOR_MAC_HOST_ROUTE = "192.168.65.1";

    /**
     * This route is the "docker0" interface on Linux. For more details see
     * https://docs.docker.com/engine/userguide/networking/#default-networks.
     *
     * Not sure if "docker0" is guaranteed to always resolve to this IP, but it
     * has in all testing so far.
     *
     * See issue here https://github.com/docker/docker/issues/17305 for
     * further documentation on this address.
     */
    public static final String DOCKER0_HOST_ROUTE = "172.17.0.1";

    /**
     * This is the port for the http server in each component.
     * Defined in the bootstrap.yml file of the component.
     * It's (current) policy for the http server port to be the same
     * for all components.
     */
    public static final int GLOBAL_HTTP_PORT = 8080;

    /**
     * This is the port for the gRPC server in each component.
     * Defined in the bootstrap.yml file of the component.
     * It's (current) policy for the gRPC server port to be the same
     * for all components.
     */
    public static final int GLOBAL_GRPC_PORT = 9001;

    /**
     * This is the hard-coded context ID for the realtime topology in use
     * throughout the system.
     */
    public static final long REALTIME_TOPOLOGY_CONTEXT = 777777;

    /**
     * IP addresses for well-known XL "gateway"s.
     */
    public static final List<String> GATEWAY_IPS = Arrays.asList(
        "10.10.10.1", // Dev/Prod gateway
        "10.10.11.2"  // Test gateway
    );

    public static final String MAC_OSX_OS_NAME = "Mac OS X";

    /**
     * Get an IPv4 IP address of the host machine that a container can use to reach the host.
     *
     * @return An IPv4 IP address of the host.
     */
    public static String getDockerHostRoute() {
        final String osName = System.getProperty("os.name");
        final String hostRoute;
        if (osName.equals(MAC_OSX_OS_NAME) &&  StringUtils.isEmpty(System.getenv("DOCKER_HOST"))) {
            // when running test on mac, docker-host is mac the getLocalIp() is unreliable
            hostRoute = getOsHostRoute(osName);
        } else {
            // otherwise, try the localIp() first
            hostRoute = getLocalIp().orElse(getOsHostRoute(osName));
        }

        logger.info("Selected docker host route: {}", hostRoute);
        return hostRoute;
    }

    @VisibleForTesting
    static String getOsHostRoute(@Nonnull final String osName) {
        if (osName.equals(MAC_OSX_OS_NAME)) {
            return DOCKER_FOR_MAC_HOST_ROUTE;
        } else {
            return DOCKER0_HOST_ROUTE;
        }
    }

    /**
     * Get the IPv4 IP address of the host on the local network.
     *
     * This is useful because we can pass this IP address to a container to allow it
     * to communicate with a service running on the host. Selects the first usable IP from the list
     * of all network interfaces.
     *
     * @return The IPv4 IP address of the host on the local network. An empty optional if not found.
     */
    public static Optional<String> getLocalIp() {
        try {
            return getLocalIp(NetworkInterface.getNetworkInterfaces());
        } catch (SocketException e) {
            logger.warn("Unable to obtain local IP: ", e);
        }

        return Optional.empty();
    }

    /**
     * Get the IPv4 IP address of the host on the local network.
     *
     * This is useful because we can pass this IP address to a container to allow it
     * to communicate with a service running on the host. Selects the first usable IP from the list
     * of all network interfaces.
     *
     * "docker0"; // [OSX fail] [Linux fail]
     * "default"; // [OSX fail] [Linux fail]
     * "10.10.10.1"; // [OSX fail] [Linux fail] -- gateway address for docker network used for dev/prod environments
     * "10.10.11.2"; // [OSX fail] [Linux fail] -- gateway address for docker network used for test environments
     * "172.17.0.1"; // [OSX fail] [Linux pass]
     * "192.168.65.1"; // [OSX pass] [Linux fail]
     * "{actual network address}"; // [OSX pass] [Linux pass] -- only works if machine is on the network
     *
     * @return The IPv4 IP address of the host on the local network. An empty optional if not found.
     * @throws SocketException If there is an issue retrieving the addresses.
     */
    public static Optional<String> getLocalIp(@Nonnull final Enumeration<NetworkInterface> interfaces)
        throws SocketException {
        while(interfaces.hasMoreElements()) {

            final NetworkInterface cur = interfaces.nextElement();
            if (cur.isLoopback()) {
                continue;
            }
            for (final InterfaceAddress addr : cur.getInterfaceAddresses()) {
                final InetAddress inetAddress = addr.getAddress();
                if (isReachableFromContainer(inetAddress)) {
                    return Optional.of(inetAddress.getHostAddress());
                }
            }
        }

        return Optional.empty();
    }

    /**
     * Check if an address is reachable from a container. Only accepts IPv4 addresses that are not
     * gateway addresses for docker networks.
     *
     * @param addr The address to check.
     * @return True if the address is reachable from a container, false if not.
     */
    private static boolean isReachableFromContainer(@Nonnull final InetAddress addr) {
        if (!(addr instanceof Inet4Address)) {
            return false;
        }

        return !GATEWAY_IPS.contains(addr.getHostAddress());
    }

    /**
     * Return the {@link TopologyType} associated with a particular topology context ID.
     * In a multi-topology-processor deployment there is no singular mapping from context ID
     * to {@link TopologyType}, but for performance test purposes this mapping is accurate.
     *
     * @param topologyContextId The context ID.
     * @return The associated {@link TopologyType}.
     */
    public static TopologyType topologyType(final long topologyContextId) {
        return topologyContextId == REALTIME_TOPOLOGY_CONTEXT ?
                TopologyType.REALTIME : TopologyType.PLAN;
    }
}
