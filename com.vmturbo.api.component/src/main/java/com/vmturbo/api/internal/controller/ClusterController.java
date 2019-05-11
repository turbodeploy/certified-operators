package com.vmturbo.api.internal.controller;

import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.SwaggerDefinition;
import io.swagger.annotations.Tag;

import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;
import com.vmturbo.api.serviceinterfaces.IClusterService;

/**
 * Controller for XL Cluster Management Options.
 * <p>
 * Deals with the overall Cluster Configuration, Component Types, Component Instances, Component
 * Instance Lifecycle,
 * and Diagnostics.
 **/
@Controller
@RequestMapping(path = "/cluster",
    produces = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE})
@Api(value = "Cluster")
@SwaggerDefinition(tags = {@Tag(name = "Cluster", description = "Methods for managing the XL Cluster")})
public class ClusterController {

    @Autowired
    private IClusterService clusterService;

    /**
     * Is the XL Cluster Management Function available?
     *
     * @return a boolean 'true' iff the XL Cluster Management function is available.
     */
    @RequestMapping(path = "/isXLEnabled",
        method = RequestMethod.GET)
    @ResponseBody
    public boolean isXLEnabled() {
        return clusterService.isXLEnabled();
    }

    /**
     * Retrieves the Telemetry initialized flag.
     *
     * @return The Telemetry initialized flag.
     */
    @RequestMapping(path = "/proactive/initialized",
        method = RequestMethod.GET)
    @ResponseBody
    public Boolean getProactiveSupportInitialized() {
        return clusterService.isTelemetryInitialized();
    }

    /**
     * Retrieves the Telemetry enabled flag.
     *
     * @return The Telemetry enabled flag.
     */
    @RequestMapping(path = "/proactive/enabled",
        produces = {MediaType.APPLICATION_JSON_VALUE},
        method = RequestMethod.GET)
    @ResponseBody
    public Boolean getProactiveSupportEnabled() {
        return clusterService.isTelemetryEnabled();
    }

    /**
     * Sets the Telemetry parameters.
     * curl -H "Content-Type:application/json" -H "Accept: application/json" -X PUT
     * localhost:8889/api/v2/proactive/enabled -d 'false'
     *
     * @param proactiveEnabled The Telemetry enabled flag.
     */
    @RequestMapping(path = "/proactive/enabled",
        consumes = {MediaType.APPLICATION_JSON_VALUE},
        produces = {MediaType.APPLICATION_JSON_VALUE},
        method = RequestMethod.PUT)
    @ResponseBody
    public void setProactiveSupportEnabled(@ApiParam(
        value = "Proactive support enable flag",
        required = true) @RequestParam(value = "proactiveEnabled", required = true)
                                               Boolean proactiveEnabled) {
        clusterService.setTelemetryEnabled(proactiveEnabled);
    }

    /**
     * Get a dump of the current Cluster Configuration
     *
     * @return a {@link ClusterConfigurationDTO} object containing known components,
     * components associated with each node, and property key/value maps for each component.
     */
    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public ClusterConfigurationDTO getClusterConfiguration() {
        return clusterService.getClusterConfiguration();
    }

    /**
     * Replace the current Cluster Configuration with a new one.
     *
     * @param newConfiguration configuration of the cluster
     * @return the new Cluster configuration, read back from the key/value store.
     */
    @RequestMapping(
        consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE},
        method = RequestMethod.PUT)
    @ResponseBody
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    @SuppressWarnings("unused")
    public ClusterConfigurationDTO setClusterConfiguration(
        @RequestBody ClusterConfigurationDTO newConfiguration) {
        return clusterService.setClusterConfiguration(newConfiguration);
    }

    /**
     * Get the map of default (property name -> value) for a VMTurbo component type.
     *
     * @return the map of default property-name/value pairs for the given component type
     */
    @RequestMapping(path = "/components/{componentType}/defaults",
        method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public ComponentPropertiesDTO getDefaultPropertiesForComponentType(
        @PathVariable String componentType) {
        return clusterService.getDefaultPropertiesForComponentType(componentType);
    }

    /**
     * Return the node that a given Component Instance will run on.
     *
     * @return the node name on which this Component Instance should be run.
     */
    @RequestMapping(path = "/components/{componentType}/instances/{instanceId}/node",
        method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public String getNodeForInstance(@PathVariable String componentType,
                                     @PathVariable String instanceId) {
        return clusterService.getNodeForComponentInstance(componentType, instanceId);
    }

    /**
     * Set the name of the Ops Manager Cluster Node on which the given VMT Component Instance
     * should run.
     * <p>
     * Component Instance IDs must be unique, and must be legal path components in a URL - no
     * whitespace, '/', etc.
     * <p>
     * The Node ID may be either a hostname, IP address, or empty ("") to reset to running on the
     * "default" node
     * in the cluster.
     *
     * @return the name of the Cluster Node on which this component will run.
     */
    @RequestMapping(path = "/components/{componentType}/instances/{instanceId}/node",
        consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE},
        method = RequestMethod.PUT)
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    @ResponseBody
    @SuppressWarnings("unused")
    public String setNodeForComponentInstance(@PathVariable String componentType,
                                              @PathVariable String instanceId,
                                              @RequestBody String nodeName) {
        return clusterService.setNodeForComponentInstance(componentType, instanceId, nodeName);
    }

    /**
     * Get the map of (property name -> value) for a VMTurbo component instance.
     *
     * @return the map of property-name/value pairs for the given component instance
     */
    @RequestMapping(path = "/components/{componentType}/instances/{instanceId}/properties",
        method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public ComponentPropertiesDTO getPropertiesForComponent(@PathVariable String componentType,
                                                            @PathVariable String instanceId) {
        return clusterService.getComponentInstanceProperties(componentType, instanceId);
    }

    /**
     * Replace the map of (property name -> value) for a VMTurbo component instance.
     *
     * @return the map of property-name/value pairs for the given component instance
     */
    @RequestMapping(path = "/components/{componentType}/instances/{instanceId}/properties",
        consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE},
        method = RequestMethod.PUT)
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    @ResponseBody
    @SuppressWarnings("unused")
    public ComponentPropertiesDTO putPropertiesForComponent(@PathVariable String componentType,
                                                            @PathVariable String instanceId,
                                                            @RequestBody
                                                                ComponentPropertiesDTO
                                                                updatedProperties) {
        return clusterService
            .putComponentInstanceProperties(componentType, instanceId, updatedProperties);
    }

    /**
     * Get the value of a default configuration property for a VMTurbo component type.
     *
     * @return the value of the named property for the given component type
     */
    @RequestMapping(path = "/components/{componentType}/defaults/{propertyName}",
        method = RequestMethod.GET
    )
    @ResponseBody
    @SuppressWarnings("unused")
    public String getPropertyForComponentType(
        @PathVariable String componentType,
        @PathVariable String propertyName) {
        return clusterService.getComponentTypeProperty(componentType, propertyName);
    }

    /**
     * Get the value of a configuration property for a VMTurbo component instance.
     *
     * @return the value of the named property for the given component instance
     */
    @RequestMapping(
        path = "/components/{componentType}/instances/{instanceId}/properties/{propertyName}",
        method = RequestMethod.GET
    )
    @ResponseBody
    @SuppressWarnings("unused")
    public String getPropertyForComponent(
        @PathVariable String componentType,
        @PathVariable String instanceId,
        @PathVariable String propertyName) {
        return clusterService.getComponentInstanceProperty(componentType, instanceId, propertyName);
    }

    /**
     * Set the value of a configuration property for a VMTurbo component instance.
     *
     * @return the new value of the named property for the given component instance
     */
    @RequestMapping(
        path = "/components/{componentType}/instances/{instanceId}/properties/{propertyName}",
        consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE},
        method = RequestMethod.PUT)
    @PreAuthorize("hasRole('ADMINISTRATOR')")
    @ResponseBody
    @SuppressWarnings("unused")
    public String setPropertyForComponent(
        @PathVariable String componentType,
        @PathVariable String instanceId,
        @PathVariable String propertyName,
        @RequestBody String propertyValue) {
        return clusterService
            .setPropertyForComponentInstance(componentType, instanceId, propertyName,
                propertyValue);
    }

    /**
     * Get the set of components known to VMTurbo
     *
     * @return the set of all component names known to VMTurbo.
     */
    @RequestMapping(path = "/components",
        method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public Set<String> getComponents() {
        return clusterService.getKnownComponents();
    }

    /**
     * Get processing state from all running VmtComponents
     */
    @RequestMapping(path = "/state")
    @ResponseBody
    @SuppressWarnings("unused")
    public Map<String, String> getComponentsState() {
        return clusterService.getComponentsState();
    }

    /**
     * Get diagnostics from all running VmtComponents
     */
    @RequestMapping(path = "/diagnostics",
        produces = {"application/zip"})
    @ResponseBody
    @SuppressWarnings("unused")
    public void getDiagnostics(OutputStream responseOutput) {
        clusterService.collectComponentDiagnostics(responseOutput);
    }

    /**
     * Health-check API used by Consul to determine whether a component is alive. A "200" response is adequate
     * to indicate liveness; the content of the response is ignored.
     *
     * @return anything, in this case "true"; it is the "ok" HTTP response code that matters.
     */
    @RequestMapping(path = "/health",
        method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public String getHealth() {
        return "true";
    }
}
