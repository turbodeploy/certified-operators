package com.vmturbo.clustermgr;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.orbitz.consul.model.health.HealthCheck;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;

import org.springframework.beans.factory.annotation.Autowired;
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
import org.springframework.web.client.HttpServerErrorException;

import com.vmturbo.clustermgr.api.ClusterConfiguration;
import com.vmturbo.clustermgr.api.ComponentProperties;
import com.vmturbo.clustermgr.api.HttpProxyConfig;
import com.vmturbo.components.common.health.CompositeHealthMonitor;

/**
 * REST endpoint for ClusterMgr, exposing APIs for component status, component configuration.
 *
 * <p>REST urls:
 *
 * <p>GET or PUT the entire cluster configuration
 * / <- -> {@link ClusterConfiguration}
 *
 * <p>GET the set of component types registered with VMTurbo
 * /components
 *
 * <p>GET the properties of a given component instance
 * /components/{component_type}/instances/{instance_id}/properties ->
 *       {property_name -> value_string map}
 *
 * <p>PUT a property of a given component instance
 * /components/{component_type}/instances/{instance_id}/{property_name} <- {property_value_string}
 *
 * <p>Fetch the runtime state of all the components
 * /state
 *
 * <p>GET the aggregate diagnostic .zip file for the entire cluster
 * /diagnostics
 *
 * <p>GET the health of the
 * /health
 *
 **/
@Component
@RestController
@Api("ClusterMgrController")
@RequestMapping(path = "",
        produces = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE})

public class ClusterMgrController {

    @Autowired
    private CompositeHealthMonitor healthMonitor;

    // see HttpResponse.TOO_MANY_REQUESTS
    private static final int HTTP_RESPONSE_TOO_MANY_REQUESTS = 429;

    private final ClusterMgrService clusterMgrService;

    ClusterMgrController(@Nonnull final ClusterMgrService clusterMgrService) {
        this.clusterMgrService = Objects.requireNonNull(clusterMgrService);
    }

    /**
     * Get a dump of the current Cluster Configuration.
     *
     * @return a {@link ClusterConfiguration} object containing known components, and
     * property key/value maps for each component.
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
     * <p>In XL the only setup required is that the Key/Value store has been initialized during
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
     * localhost:8889/proactive/enabled -d 'false'
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
     * @param newConfiguration the replacement Cluster Configuration, including component types
     *                         and configuration properties
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
     * @param componentType type of the component to query
     * @return the map of default property-name/value pairs for the given component type
     */
    @ApiOperation("Get the map of default (property name -> value) for a component type.")
    @RequestMapping(path = "components/{componentType}/defaults",
            method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public ComponentProperties getDefaultPropertiesForComponentType(
        @PathVariable("componentType") String componentType) {
        return clusterMgrService.getDefaultPropertiesForComponentType(componentType);
    }

    /**
     * Replace the map of default (property name -> value) for a VMTurbo component type.
     *
     * @param componentType the name of the component type to update
     * @param updatedProperties the new properties to store as the default values
     * @return the updated map of default property-name/value pairs for the given component type
     */
    @ApiOperation("Replace the map of default (property name -> value) for a component type.")
    @RequestMapping(path = "components/{componentType}/defaults",
            consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE},
            method = RequestMethod.PUT)
    @ResponseBody
    @SuppressWarnings("unused")
    public ComponentProperties putDefaultPropertiesForType(
        @PathVariable("componentType") String componentType,
        @RequestBody ComponentProperties updatedProperties) {
        return clusterMgrService.putDefaultPropertiesForComponentType(componentType,
            updatedProperties);
    }

    /**
     * Get the map of local (property name -> value) for a VMTurbo component type.
     *p
     * @param componentType the name of the component type to fetch local properties for
     * @return the map of local property-name/value pairs for the given component type
     */
    @ApiOperation("Get the map of current (property name -> value) for a component type.")
    @RequestMapping(path = "components/{componentType}/localProperties",
        method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public ComponentProperties getLocalPropertiesForComponentType(
        @PathVariable("componentType") String componentType) {
        return clusterMgrService.getLocalPropertiesForComponentType(componentType);
    }

    /**
     * Replace the map of local (property name -> value) for a VMTurbo component type.
     *
     * @param componentType the name of the component type to update
     * @param updatedProperties the new properties to store as the current values
     * @return the updated map of default property-name/value pairs for the given component type
     */
    @ApiOperation("Replace the map of default (property name -> value) for a component type.")
    @RequestMapping(path = "components/{componentType}/localProperties",
            consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE},
            method = RequestMethod.PUT)
    @ResponseBody
    @SuppressWarnings("unused")
    public ComponentProperties putLocalPropertiesForComponentType(
        @PathVariable("componentType") String componentType,
        @RequestBody(required = false) ComponentProperties updatedProperties) {
        return clusterMgrService.putLocalPropertiesForComponentType(componentType, updatedProperties);
    }


    /**
     * Return the Set of component instances for a given component type that are defined.
     *
     * <p>The instance ids are returned regardless of the execution state of each instance. In other words,
     * the instance may not be running, currently, but the ID will still be included.
     *
     * @param componentType type of component to be updated
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
     * Get the effective map of (property name -> value) for a VMTurbo component instance.
     * The effective properties include the defaults for the component type (lowest priority)
     * merged with the current values for the component type, and the instance values for this
     * particular instance.
     *
     * @param componentType the type of component to fetch from
     * @param instanceId the instance id for the specific component instance to fetch from
     * @return the map of property-name/value pairs for the given component instance
     */
    @ApiOperation("Get the effective combined map of (property name -> value) for a component instance.")
    @RequestMapping(path = "components/{componentType}/instances/{instanceId}/effectiveProperties",
            method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public ComponentProperties getEffectivePropertiesForComponent(
        @PathVariable("componentType") String componentType,
        @PathVariable("instanceId") String instanceId) {
        return clusterMgrService.getEffectiveInstanceProperties(componentType, instanceId);
    }

    /**
     * Get the map of (property name -> value) for a VMTurbo component instance. If some properties
     * are absent, they are substituted with defaults for this component type.
     *
     * @param componentType type of component to query
     * @param instanceId specific instance id to fetch configuration properties from
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
     * @param componentType type of component to update
     * @param instanceId specific instance id for which configuration properties are to be replaced
     * @param updatedProperties the new configuration properties to store
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
     * @param componentType the type of the component to be queried
     * @param propertyName the name of the default configuration property to be fetched
     * @return the value of the named property for the given component type
     */
    @ApiOperation("Get the value of a default configuration property for a VMTurbo component type.")
    @RequestMapping(path = "components/{componentType}/defaults/{propertyName}",
            method = RequestMethod.GET
    )
    @ResponseBody
    @SuppressWarnings("unused")
    public String getComponentDefaultProperty(
            @PathVariable("componentType") String componentType,
            @PathVariable("propertyName") String propertyName) {
        return clusterMgrService.getDefaultComponentProperty(componentType, propertyName);
    }

    /**
     * Get the value of a local configuration property for a VMTurbo component type.
     *
     * @param componentType the type of component to fetch from
     * @param propertyName the name of the local property to fetch for the given component
     * @return the value of the named property for the given component type
     */
    @ApiOperation("Get the value of a local configuration property for a VMTurbo component type.")
    @RequestMapping(path = "components/{componentType}/localProperties/{propertyName}",
            method = RequestMethod.GET
    )
    @ResponseBody
    @SuppressWarnings("unused")
    public String getComponentLocalProperty(
            @PathVariable("componentType") String componentType,
            @PathVariable("propertyName") String propertyName) {
        return clusterMgrService.getLocalComponentProperty(componentType, propertyName);
    }

    /**
     * Get the value of a configuration property for a VMTurbo component instance.
     *
     * @param componentType type of component to query
     * @param instanceId id of specific instance to query
     * @param propertyName name of the configuration property to fetch
     * @return the value of the named property for the given component instance
     */
    @ApiOperation("Get the value of a configuration property for a VMTurbo component instance.")
    @RequestMapping(path = "components/{componentType}/instances/{instanceId}/properties/{propertyName}",
            method = RequestMethod.GET
    )
    @ResponseBody
    @SuppressWarnings("unused")
    public String getComponentInstanceProperty(
            @PathVariable("componentType") String componentType,
            @PathVariable("instanceId") String instanceId,
            @PathVariable("propertyName") String propertyName) {
        return clusterMgrService.getComponentInstanceProperty(componentType, instanceId, propertyName);
    }

    /**
     * Set the local value of a configuration property for a VMTurbo component type.
     *
     * @param componentType the type of component to update
     * @param propertyName the name of the local property to store for the given component
     * @param propertyValue the value to store as local property
     * @return the new value of the named property for the given component type
     */
    @ApiOperation("Set the local value of a configuration property for a VMTurbo component type.")
    @RequestMapping(path = "components/{componentType}/localProperties/{propertyName}",
            consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE},
            method = RequestMethod.PUT)
    @ResponseBody
    @SuppressWarnings("unused")
    public String setLocalPropertyForComponent(
            @PathVariable("componentType") String componentType,
            @PathVariable("propertyName") String propertyName,
            @RequestBody String propertyValue) {
        return clusterMgrService.putComponentLocalProperty(componentType, propertyName,
            propertyValue);
    }

    /**
     * Delete the local value of a configuration property for a VMTurbo component type.
     *
     * @param componentType the type of component to update
     * @param propertyName the name of the local property to delete for the given component
     * @return the default value of the named property for the given component type
     */
    @ApiOperation("Set the current value of a configuration property for a VMTurbo component type.")
    @RequestMapping(path = "components/{componentType}/localProperties/{propertyName}",
            method = RequestMethod.DELETE)
    @ResponseBody
    @SuppressWarnings("unused")
    public String deleteLocalPropertyForComponent(
            @PathVariable("componentType") String componentType,
            @PathVariable("propertyName") String propertyName) {
        return clusterMgrService.deleteComponentLocalProperty(componentType, propertyName);
    }

    /**
     * Set the value of a configuration property for a VMTurbo component instance.
     *
     * @param componentType type of component to update
     * @param instanceId id of the component instance to update
     * @param propertyName the name of the configuration property to change
     * @param propertyValue the new value to assign to the configuration property
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
     * Get the set of components known to VMTurbo.
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
     * Get processing state from all running VmtComponents.
     *
     * @return a map from component type -> {@link ComponentState} for that type
     */
    @ApiOperation("Get processing state from all running VmtComponents")
    @RequestMapping(path = "/state",
        method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public Map<String, String> getComponentsState() {
        return clusterMgrService.getComponentsState().entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().name()));
    }

    /**
     * Get service health from all running VmtComponents.
     *
     * @return a map from component type -> {@link HealthCheck}
     */
    @ApiOperation("Get service health from all running VmtComponents")
    @RequestMapping(path = "/cluster/health",
        method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public Map<String, HealthCheck> getClusterHealth() {
        return clusterMgrService.getAllComponentsHealth();
    }

    /**
     * Get diagnostics from all running VmtComponents. The diagnostic output file is in the form
     * of a .zip file containing nested .zip files, one for each component instance.
     *
     * @param responseOutput the output stream the diagnostic zip files should be appended to
     */
    @ApiOperation("Get diagnostics from all running VmtComponents")
    @RequestMapping(path = "/diagnostics",
        method = RequestMethod.GET,
        produces = {"application/zip"})
    @ApiResponse(code = HTTP_RESPONSE_TOO_MANY_REQUESTS,
        message = "Diagnostics operation already in progress; only one allowed at a time.")
    @ResponseBody
    @SuppressWarnings("unused")
    public void getDiagnostics(OutputStream responseOutput) {
        try {
            clusterMgrService.collectComponentDiagnostics(responseOutput);
        } catch (IOException e) {
            throw new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    /**
     * Health-check API used by Consul/K8s to determine whether a component is alive. Currently
     * check connection to ClusterKvStore and DB.
     *
     * @return health status.
     */
    @ApiOperation("Health-check API used by Consul to determine whether a component is alive.")
    @RequestMapping(path = "/health", method = RequestMethod.GET)
    @ResponseBody
    @SuppressWarnings("unused")
    public ResponseEntity<CompositeHealthMonitor.CompositeHealthStatus> getHealth() {
        final CompositeHealthMonitor.CompositeHealthStatus status = healthMonitor.getHealthStatus();
        return clusterMgrService.isClusterKvStoreInitialized() && status.isHealthy()
                ? ResponseEntity.ok(status) : ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(status);
    }


    /**
     * Export XL diagnostics to upload.vmturbo.com.
     *
     * @param httpProxyConfig value object to hold http proxy information
     * @return true if diagnostics is uploaded successfully; false otherwise
     */
    @ApiOperation("Export XL diagnostics to upload.vmturbo.com.")
    @RequestMapping(
        path = "/diagnostics",
        consumes = {MediaType.APPLICATION_JSON_UTF8_VALUE, MediaType.TEXT_PLAIN_VALUE},
        method = RequestMethod.POST)
    @ApiResponse(code = HTTP_RESPONSE_TOO_MANY_REQUESTS,
        message = "Diagnostics operation already in progress; only one allowed at a time.")
    @ResponseBody
    @SuppressWarnings("unused")
    public Boolean exportDiagnotics(
        @RequestBody HttpProxyConfig httpProxyConfig) {
        return clusterMgrService.exportDiagnostics(httpProxyConfig);
    }

}
