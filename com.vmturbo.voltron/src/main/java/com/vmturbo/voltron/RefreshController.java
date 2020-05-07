package com.vmturbo.voltron;

import java.util.Map;
import java.util.Optional;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

/**
 * Controller to allow stopping/refreshing Voltron sub-contexts.
 */
@Api(value = "/voltron")
@RequestMapping(value = "/voltron")
@RestController
public class RefreshController {

    private Map<Component, AnnotationConfigWebApplicationContext> components;

    /**
     * Refresh a sub-context. This can be useful during development if you changed something
     * (small) in the Spring configuration and want to see the change reflected in Voltron.
     * This can also be used to simulate a component restart.
     *
     * @param contextName The name of the context.
     * @return A string response (to indicate success or failure).
     */
    @RequestMapping(value = "/refresh/{contextName}",
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ApiOperation(value = "Refresh a particular sub-context.")
    public String refreshComponentContext(@PathVariable("contextName") String contextName) {
        return Optional.ofNullable(components)
                .flatMap(c -> Optional.ofNullable(c.get(contextName)))
                .map(context -> {
                    context.refresh();
                    return "Successfully refreshed context " + context.getNamespace() + "\n";
                })
                .orElse("Unable to find context for " + contextName + "\n");
    }

    /**
     * Stop a sub-context.
     *
     * @param contextName The name of the context.
     * @return A string response (to indicate success or failure).
     */
    @RequestMapping(value = "/stop/{contextName}",
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ApiOperation(value = "Stop a particular sub-context. Simulates shutting down a component.")
    public String stopComponentContext(@PathVariable("contextName") String contextName) {
        return Optional.ofNullable(components)
                .flatMap(c -> Optional.ofNullable(c.get(contextName)))
                .map(context -> {
                    context.stop();
                    return "Successfully closed context " + context.getNamespace() + "\n";
                })
                .orElse("Unable to find context for " + contextName + "\n");
    }

    /**
     * Start a sub-context. Note - you can only start a context that's been stopped with "stop".
     * This doesn't add a new component that wasn't part of the Voltron configuration.
     *
     * @param contextName The name of the context.
     * @return A string response (to indicate success or failure).
     */
    @RequestMapping(value = "/start/{contextName}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ApiOperation(value = "Start a particular sub-context. Simulates starting up a component shut down with the /stop.")
    public String startComponentContext(@PathVariable("contextName") String contextName) {
        return Optional.ofNullable(components)
                .flatMap(c -> Optional.ofNullable(c.get(contextName)))
                .map(context -> {
                    context.start();
                    return "Successfully started context " + context.getNamespace() + "\n";
                })
                .orElse("Unable to find context for " + contextName + "\n");
    }

    /**
     * Injected after Spring initialization.
     *
     * @param components The component and their contexts.
     */
    public void setComponentContexts(Map<Component, AnnotationConfigWebApplicationContext> components) {
        this.components = components;
    }
}
