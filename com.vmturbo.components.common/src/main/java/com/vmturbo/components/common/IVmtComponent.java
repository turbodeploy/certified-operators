package com.vmturbo.components.common;

import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.health.CompositeHealthMonitor;

/**
 * The interface spec for a VMTurbo Component. This includes:
 * <dl>
 *     <dt>name</dt>
 *     <dd>a unique name for the component</dd>
 *     <dt>execution status</dt>
 *     <dd>represents the current {@link ExecutionStatus} in the lifecycle of an {@link IVmtComponent}</dd>
 *     <dt>configuration properties</dt>
 *     <dd>a {@link Properties} object for dyanmic control of the component's operation</dd>
 *     <dt>diagnostics</dt>
 *     <dd>the ability to dump diagnostic information, e.g. log files, onto a compressed {@link ZipOutputStream}</dd>
 * </dl>
 * TODO: implement separate componentId, which is unique, and name, which is user-facing.
 */
public interface IVmtComponent {

    /**
     * an ID for this component,  guaranteed to be unique.
     *
     * @return the unique ID for this component
     */
    String getComponentName();

    /**
     * Retrieve the current status of the component. See {@link ExecutionStatus} for the list of states and legal
     * transitions.
     *
     * @return the current {@link ExecutionStatus} of the component, e.g. {@code ExecutionStatus.RUNNING}
     */
    ExecutionStatus getComponentStatus();

    /**
     * Initiate a transition to the {@code ExecutionStatus.STARTING} state.
     *
     * <p>Only valid in the {@code ExecutionStatus.NEW} state.
     *
     * @param applicationContext The constructed context of the component.
     */
    void startComponent(@Nonnull ApplicationContext applicationContext);

    /**
     * Initiate a transition to the {@code ExecutionStatus.STOPPING} state.
     *
     * <p>Valid in any {@code ExecutionStatus.NEW} state except {@code ExecutionStatus.STOPPED} or {@code ExecutionStatus.FAILED}.
     */
    void stopComponent();

    /**
     * Initiate a transition to the {@code ExecutionStatus.PAUSED} state.
     *
     * <p>Only valid in the {@code ExecutionStatus.RUNNING} state.
     */
    void pauseComponent();

    /**
     * Initiate a transition to the {@code ExecutionStatus.RUNNING} state.
     *
     * <p>Only valid in the {@code ExecutionStatus.PAUSED} state.
     */
    void resumeComponent();

    /**
     * Initiate a transition to the {@code ExecutionStatus.FAILED} state.
     *
     * <p>This transition may occur in  state.
     */
    void failedComponent();

    /**
     * Get the component health status provider
     *
     * @return the component health status provider
     */
    CompositeHealthMonitor getHealthMonitor();

    /**
     * Update the current configuration for this component. The updated hierarchy of configuration sources, maintained
     * by Spring, is passed in.
     *
     * <p>This environment is based on configuration sources from Spring Boot "Externalized Configuration":
     * <ul>
     * <li>http://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html
     * </ul>
     * with the addition of the Spring-Cloud-Configuration sources:
     * <ul>
     *      <li>http://cloud.spring.io/spring-cloud-consul/spring-cloud-consul.html ;
     *      see the heading "Distributed Configuration with Consul"</li>
     * </ul>
     * and is triggered by a Spring Cloud Environment Changed event:
     * <ul>
     *      <li>http://projects.spring.io/spring-cloud/spring-cloud.html#_refresh_scope</li>
     * </ul>
     * This method is typically meant to be overridden by an individual subclass, which may choose to apply the changed
     * configuration dynamically, ignore the changes, or restart itself which would automatically apply the changes.
     *
     * @param configurationProperties an {@link Environment} object that provides configuration property values for the
     *                                {@link IVmtComponent}
     * @param changedPropertyKeys a set of configuration property keys whose values have changed.
     */
    void configurationPropertiesChanged(@Nonnull final Environment configurationProperties,
                                        @Nonnull final Set<String> changedPropertyKeys);

    /**
     * Provide a diagnostic snapshot of the information regarding this component. This will typically include log files,
     * performance and memory information, and may include Operating System information as well as a snapshot of the
     * Java application itself. Each aspect of the diagnostics should be written into a  {@link java.util.zip.ZipEntry}.
     * Thus the resulting .zip file will easy to "unzip" and analyze for the receiving client.
     *
     * @param diagnosticZip a {@link ZipOutputStream} to which the individual diagnostic information
     *                      aspects should be written
     * @exception DiagnosticsException if there is an error creating the collection or writing to
     * to the stream     */
    void dumpDiags(@Nonnull ZipOutputStream diagnosticZip) throws DiagnosticsException;
}
