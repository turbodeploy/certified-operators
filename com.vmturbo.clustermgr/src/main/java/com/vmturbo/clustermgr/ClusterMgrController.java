package com.vmturbo.clustermgr;

import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.orbitz.consul.model.health.HealthCheck;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

/**
 * REST endpoint for ClusterMgr, exposing APIs for component status, component configuration, node configuration.
 *
 * REST urls:
 *
 * GET or PUT the entire cluster configuration
 * / <- -> {@link ClusterConfiguration}
 *
 * GET the set of component types registered with VMTurbo
 * /components
 *
 * GET the properties of a given component instance
 * /components/{component_type}/instances/{instance_id}/properties -> {property_name -> value_string map}
 *
 * PUT a property of a given component instance
 * /components/{component_type}/instances/{instance_id}/{property_name} <- {property_value_string}
 *
 * GET (TODO: or PUT) the execution node for the given instanceId
 * /components/{component_type}/instances/{instance_id}/node = {node to run instance on}
 *
 * TODO:
 * GET the default property value map for the given component type
 * /components/{component_type}/defaults -> {property_name -> value_string map}
 *
 *
 * /state
 *
 * GET the aggregate diagnostic .zip file for the entire cluster
 * /diagnostics
 *
 * GET the health of the
 * /health
 *
 **/
@Component
@RestController
@Api("ClusterMgrController")
@RequestMapping(path = "/api/v2",
        produces = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE})

public class ClusterMgrController {
    private final ClusterMgrService clusterMgrService;

    ClusterMgrController(@Nonnull final ClusterMgrService clusterMgrService) {
        this.clusterMgrService = Objects.requireNonNull(clusterMgrService);
    }

    /**
     * Get a dump of the current Cluster Configuration
     *
     * @return a {@link ClusterConfiguration} object containing known components,
     * components associated with each node, and property key/value maps for each component.
     */
    @ApiOperation("Get the full cluster configuration")
    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public ClusterConfiguration getClusterConfiguration() {
        return clusterMgrService.getClusterConfiguration();
    }

    /**
     * Retrieves the Telemetry initialized flag. This flag is used in Classic to tell whether
     * or not this is an upgrade from a prior version in which Telemetry was not installed.
     *
     * In XL the only setup required is that the Key/Value store has been initialized during
     * clustermgr startup.
     *
     * @return The Telemetry initialized flag.
     */
    @ApiOperation("Return whether Proactive Support is initialized")
    @RequestMapping(path = "/proactive/initialized",
                    produces = {MediaType.APPLICATION_JSON_VALUE},
                    method = RequestMethod.GET)
    @ResponseBody
    public Boolean getProactiveSupportInitialized() {
        return clusterMgrService.isClusterKvStoreInitialized();
    }

    /**
     * Retrieves the Telemetry enabled flag.
     *
     * @return The Telemetry enabled flag.
     */
    @ApiOperation("Return whether Proactive Support is enabled")
    @RequestMapping(path = "/proactive/enabled",
                    produces = {MediaType.APPLICATION_JSON_VALUE},
                    method = RequestMethod.GET)
    @ResponseBody
    public Boolean getProactiveSupportEnabled() {
        return clusterMgrService.isTelemetryEnabled();
    }

    /**
     * Sets the Telemetry parameters.
     * curl -H "Content-Type:application/json" -H "Accept: application/json" -X PUT
     * localhost:8889/api/v2/proactive/enabled -d 'false'
     *
     * @param enabled The Telemetry enabled flag.
     */
    @ApiOperation("Set the Proactive Support parameters")
    @RequestMapping(path = "/proactive/enabled",
                    consumes = {MediaType.APPLICATION_JSON_VALUE},
                    method = RequestMethod.PUT)
    public void setProactiveSupportEnabled(@RequestBody Boolean enabled) {
        clusterMgrService.setTelemetryEnabled(enabled);
    }

    /**
     * Replace the current Cluster Configuration with a new one.
     *
     * @return the new Cluster configuration, read back from the key/value store.
     */
    @ApiOperation("Replace the current Cluster Configuration with a new one.")
    @RequestMapping(
            consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE},
            method = RequestMethod.PUT)
    @ResponseBody
    @SuppressWarnings("unused")
    public ClusterConfiguration setClusterConfiguration(
            @RequestBody ClusterConfiguration newConfiguration) {
        return clusterMgrService.setClusterConfiguration(newConfiguration);
    }

    /**
     * Get the map of default (property name -> value) for a VMTurbo component type.
     *
     * @return the map of default property-name/value pairs for the given component type
     */
    @ApiOperation("Get the map of default (property name -> value) for a component type.")
    @RequestMapping(path = "components/{componentType}/defaults",
            method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public ComponentProperties getDefaultPropertiesForComponentType(@PathVariable("componentType") String componentType) {
        return clusterMgrService.getDefaultPropertiesForComponentType(componentType);
    }

    /**
     * Replace the default {@link ComponentProperties}, (property name -> value), for a VMTurbo component type.
     * Any previous {@link ComponentProperties} will be removed.
     *
     * @param componentType the component type
     * @param newProperties the new default {@link ComponentProperties} to apply for this component type
     * @return the map of default property-name/value pairs for the given component type
     */
    @ApiOperation("Replace the default ComponentProperties, (property name -> value), for a component type.")
    @RequestMapping(path = "components/{componentType}/defaults",
            consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE},
            method = RequestMethod.PUT)
    @ResponseBody
    @SuppressWarnings("unused")
    public ComponentProperties putDefaultPropertiesForComponentType(
            @PathVariable("componentType") String componentType,
            @RequestBody ComponentProperties newProperties) {
        return clusterMgrService.putDefaultPropertiesForComponentType(componentType, newProperties);
    }

    /**
     * Return the Set of component instances for a given component type that are defined.
     *
     * The instance ids are returned regardless of the execution state of each instance. In other words,
     * the instance may not be running, currently, but the ID will still be included.
     *
     * @return the Set of instance id's for components of this type.
     */
    @ApiOperation("Return the Set of component instances for a given component type that are defined.")
    @RequestMapping(path = "components/{componentType}/instances",
            method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public Set<String> getInstancesForType(@PathVariable("componentType") String componentType) {
        return clusterMgrService.getComponentInstanceIds(componentType);
    }

    /**
     * Return the node that a given Component Instance will run on.
     *
     * @return the node name on which this Component Instance should be run.
     */
    @ApiOperation("Return the node that a given Component Instance will run on.")
    @RequestMapping(path = "components/{componentType}/instances/{instanceId}/node",
            method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public String getNodeForInstance(@PathVariable("componentType") String componentType,
                                     @PathVariable("instanceId") String instanceId) {
        return clusterMgrService.getNodeForComponentInstance(componentType, instanceId);
    }

    /**
     * Set the name of the Ops Manager Cluster Node on which the given VMT Component Instance should run.
     * <p>
     * Component Instance IDs must be unique, and must be legal path components in a URL - no whitespace, '/', etc.
     * <p>
     * The Node ID may be either a hostname, IP address, or empty ("") to reset to running on the "default" node
     * in the cluster.
     *
     * @return the name of the Cluster Node on which this component will run.
     */
    @ApiOperation("Set the name of the Ops Manager Cluster Node on which the given VMT Component Instance should run.")
    @RequestMapping(path = "components/{componentType}/instances/{instanceId}/node",
            consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE},
            method = RequestMethod.PUT)
    @ResponseBody
    @SuppressWarnings("unused")
    public String setNodeForComponentInstance(@PathVariable("componentType") String componentType,
                                              @PathVariable("instanceId") String instanceId,
                                              @RequestBody String nodeName) {
        return clusterMgrService.setNodeForComponentInstance(componentType, instanceId, nodeName);
    }

    /**
     * Get the map of (property name -> value) for a VMTurbo component instance.
     *
     * @return the map of property-name/value pairs for the given component instance
     */
    @ApiOperation("Get the map of (property name -> value) for a component instance.")
    @RequestMapping(path = "components/{componentType}/instances/{instanceId}/properties",
            method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public ComponentProperties getPropertiesForComponent(@PathVariable("componentType") String componentType,
                                                         @PathVariable("instanceId") String instanceId) {
        return clusterMgrService.getComponentInstanceProperties(componentType, instanceId);
    }

    /**
     * Replace the map of (property name -> value) for a VMTurbo component instance.
     *
     * @return the map of property-name/value pairs for the given component instance
     */
    @ApiOperation("Replace the map of (property name -> value) for a component instance.")
    @RequestMapping(path = "components/{componentType}/instances/{instanceId}/properties",
            consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE},
            method = RequestMethod.PUT)
    @ResponseBody
    @SuppressWarnings("unused")
    public ComponentProperties putPropertiesForComponent(@PathVariable("componentType") String componentType,
                                                         @PathVariable("instanceId") String instanceId,
                                                         @RequestBody ComponentProperties updatedProperties) {
        return clusterMgrService.putComponentInstanceProperties(componentType, instanceId, updatedProperties);
    }

    /**
     * Get the value of a default configuration property for a VMTurbo component type.
     *
     * @return the value of the named property for the given component type
     */
    @ApiOperation("Get the value of a default configuration property for a VMTurbo component type.")
    @RequestMapping(path = "components/{componentType}/defaults/{propertyName}",
            method = RequestMethod.GET
    )
    @ResponseBody
    @SuppressWarnings("unused")
    public String getPropertyForComponentType(
            @PathVariable("componentType") String componentType,
            @PathVariable("propertyName") String propertyName) {
        return clusterMgrService.getComponentTypeProperty(componentType, propertyName);
    }

    /**
     * Set the value of a default configuration property for a VMTurbo component type.
     *
     * @return the new value of the named property for the given component type
     */
    @ApiOperation("Set the value of a default configuration property for a VMTurbo component type.")
    @RequestMapping(path = "components/{componentType}/defaults/{propertyName}",
            consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE},
            method = RequestMethod.PUT)
    @ResponseBody
    @SuppressWarnings("unused")
    public String setPropertyForComponentType(
            @PathVariable("componentType") String componentType,
            @PathVariable("propertyName") String propertyName,
            @RequestBody String propertyValue) {
        return clusterMgrService.setPropertyForComponentType(componentType, propertyName, propertyValue);
    }

    /**
     * Get the value of a configuration property for a VMTurbo component instance.
     *
     * @return the value of the named property for the given component instance
     */
    @ApiOperation("Get the value of a configuration property for a VMTurbo component instance.")
    @RequestMapping(path = "components/{componentType}/instances/{instanceId}/properties/{propertyName}",
            method = RequestMethod.GET
    )
    @ResponseBody
    @SuppressWarnings("unused")
    public String getPropertyForComponent(
            @PathVariable("componentType") String componentType,
            @PathVariable("instanceId") String instanceId,
            @PathVariable("propertyName") String propertyName) {
        return clusterMgrService.getComponentInstanceProperty(componentType, instanceId, propertyName);
    }

    /**
     * Set the value of a configuration property for a VMTurbo component instance.
     *
     * @return the new value of the named property for the given component instance
     */
    @ApiOperation("Set the value of a configuration property for a VMTurbo component instance.")
    @RequestMapping(path = "components/{componentType}/instances/{instanceId}/properties/{propertyName}",
            consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE},
            method = RequestMethod.PUT)
    @ResponseBody
    @SuppressWarnings("unused")
    public String setPropertyForComponent(
            @PathVariable("componentType") String componentType,
            @PathVariable("instanceId") String instanceId,
            @PathVariable("propertyName") String propertyName,
            @RequestBody String propertyValue) {
        return clusterMgrService.setPropertyForComponentInstance(componentType, instanceId, propertyName, propertyValue);
    }

    /**
     * Get the set of components known to VMTurbo
     *
     * @return the set of all component names known to VMTurbo.
     */
    @ApiOperation("Get the set of components known to VMTurbo")
    @RequestMapping(path = "/components",
            method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public Set<String> getComponents() {
        return clusterMgrService.getKnownComponents();
    }

    /**
     * Get processing state from all running VmtComponents
     */
    @ApiOperation("Get processing state from all running VmtComponents")
    @RequestMapping(path="/state")
    @ResponseBody
    @SuppressWarnings("unused")
    public Map<String, String> getComponentsState() {
        return clusterMgrService.getComponentsState().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().name() ));
    }

    /**
     * Get service health from all running VmtComponents
     */
    @ApiOperation("Get service health from all running VmtComponents")
    @RequestMapping(path="/cluster/health")
    @ResponseBody
    @SuppressWarnings("unused")
    public Map<String, HealthCheck> getClusterHealth() {
        return clusterMgrService.getAllComponentsHealth();
    }

    /**
     * Get diagnostics from all running VmtComponents
     */
    @ApiOperation("Get diagnostics from all running VmtComponents")
    @RequestMapping(path="/diagnostics",
            produces={"application/zip"})
    @ResponseBody
    @SuppressWarnings("unused")
    public void getDiagnostics(OutputStream responseOutput) {
        clusterMgrService.collectComponentDiagnostics(responseOutput);
    }

    /**
     * Health-check API used by Consul to determine whether a component is alive. A "200" response is adequate
     * to indicate liveness; the content of the response is ignored.
     *
     * @return anything, in this case "true"; it is the "ok" HTTP response code that matters.
     */
    @ApiOperation("Health-check API used by Consul to determine whether a component is alive.")
    @RequestMapping(path = "/health",
            method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public ResponseEntity<String> getHealth() {
        return clusterMgrService.isClusterKvStoreInitialized() ?
                ResponseEntity.ok("true") :
                ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("false");
    }

}
