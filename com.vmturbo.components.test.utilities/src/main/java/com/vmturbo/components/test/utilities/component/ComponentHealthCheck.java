package com.vmturbo.components.test.utilities.component;

import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.Nonnull;

import org.apache.http.client.utils.URIBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

/**
 * An implementation of a {@link ServiceHealthCheck} specific for components.
 *
 * Component readiness is decided by pinging the health endpoint for a component
 * until that endpoint reports success or a timeout occurs.
 */
public class ComponentHealthCheck extends ServiceHealthCheck {

    public static final String HEALTH_CHECK_PATH = "/api/v2/health";

    /**
     * Check if a component is healthy.
     *
     * @param container The container hosting the component whose health should be checked.
     * @return True if the component reports itself as healthy, false if the components
     *         reports unhealthy status or it cannot be reached.
     * @throws ContainerUnreadyException If the container is no longer running.
     * @throws InterruptedException If the thread got interrupted while executing the command.
     */
    @Override
    public SuccessOrFailure isHealthy(@Nonnull final Container container)
            throws ContainerUnreadyException, InterruptedException {
        checkContainerStatus(container);

        DockerPort dockerPort = container.port(ComponentUtils.GLOBAL_HTTP_PORT);
        try {
            URI uri = new URIBuilder().setScheme("http")
                    .setHost(dockerPort.getIp())
                    .setPort(dockerPort.getExternalPort())
                    .setPath(HEALTH_CHECK_PATH)
                    .build();
            return isHealthy(uri, new RestTemplate());
        } catch (URISyntaxException e) {
            return SuccessOrFailure.failure("Got unexpected syntax exception: " + e.getMessage());
        } catch (RuntimeException e) {
            return SuccessOrFailure.failure("Got runtime exception: " + e.getMessage());
        }
    }

    @VisibleForTesting
    SuccessOrFailure isHealthy(@Nonnull final URI uri, @Nonnull final RestTemplate restTemplate) {
        ResponseEntity<String> response = restTemplate.getForEntity(uri, String.class);
        if (response.getStatusCode().is2xxSuccessful()) {
            return SuccessOrFailure.success();
        } else {
            return SuccessOrFailure.failure("Did not get successful response.");
        }
    }
}
