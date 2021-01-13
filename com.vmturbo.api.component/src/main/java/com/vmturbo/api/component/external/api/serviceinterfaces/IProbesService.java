package com.vmturbo.api.component.external.api.serviceinterfaces;

import java.nio.file.AccessDeniedException;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.probe.ProbePropertyNameValuePairApiDTO;
import com.vmturbo.api.dto.probe.ProbeApiDTO;
import com.vmturbo.api.dto.probe.ProbePropertyApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;

public interface IProbesService {
    /**
     * Get information for a specific probe.
     *
     * @param probeId string representation of the probe oid.
     * @return information on the probe.
     * @throws Exception operation failed.
     */
    @Nonnull
    ProbeApiDTO getProbe(@Nonnull String probeId) throws Exception;

    /**
     * Get a list of all probes.
     *
     * @return a list of information about each probe.
     * @throws Exception operation failed.
     */
    @Nonnull
    List<TargetApiDTO> getProbes() throws Exception;

    /**
     * Get a list of all probe properties.
     *
     * @return a list of all probe properties.
     * @throws UnknownObjectException should not happen.
     * @throws OperationFailedException should not happen.
     * @throws UnauthorizedObjectException user not authenticated.
     * @throws AccessDeniedException user not authorized.
     * @throws InterruptedException the operation was interrupted.
     */
    @Nonnull
    List<ProbePropertyApiDTO> getAllProbeProperties() throws Exception;

    /**
     * Get all probe properties specific to a probe.
     *
     * @param probeId string representation of the probe id.
     * @return a list of all probe properties specific to the probe.
     * @throws UnknownObjectException probe does not exist.
     * @throws OperationFailedException bad input.
     * @throws UnauthorizedObjectException user not authenticated.
     * @throws AccessDeniedException user not authorized.
     * @throws InterruptedException the operation was interrupted.
     */
    @Nonnull
    List<ProbePropertyNameValuePairApiDTO> getAllProbeSpecificProbeProperties(@Nonnull String probeId)
        throws Exception;

    /**
     * Get a probe property specific to a probe.
     *
     * @param probeId string representation of the probe id.
     * @param propertyName the name of the property.
     * @return the value of the property (the string is empty if the probe property does not exist).
     * @throws UnknownObjectException probe does not exist.
     * @throws OperationFailedException bad input.
     * @throws UnauthorizedObjectException user not authenticated.
     * @throws AccessDeniedException user not authorized.
     * @throws InterruptedException the operation was interrupted.
     */
    @Nonnull
    String getProbeSpecificProbeProperty(@Nonnull String probeId, @Nonnull String propertyName)
        throws Exception;

    /**
     * Get all probe properties specific to a target.
     *
     * @param probeId string representation of the probe id that discovers the target.
     * @param targetId string representation of the target id.
     * @return a list of all probe properties specific to the target.
     * @throws UnknownObjectException probe or target does not exist.
     * @throws OperationFailedException bad input.
     * @throws UnauthorizedObjectException user not authenticated.
     * @throws AccessDeniedException user not authorized.
     * @throws InterruptedException the operation was interrupted.
     */
    @Nonnull
    List<ProbePropertyNameValuePairApiDTO> getAllTargetSpecificProbeProperties(
        @Nonnull String probeId,
        @Nonnull String targetId)
        throws Exception;

    /**
     * Get a probe property specific to a target.
     *
     * @param probeId string representation of the probe id that discovers the target.
     * @param targetId string representation of the target id.
     * @param propertyName the name of the property.
     * @return the value of the property (the string is empty if the probe property does not exist).
     * @throws UnknownObjectException probe or target does not exist.
     * @throws OperationFailedException bad input.
     * @throws UnauthorizedObjectException user not authenticated.
     * @throws AccessDeniedException user not authorized.
     * @throws InterruptedException the operation was interrupted.
     */
    @Nonnull
    String getTargetSpecificProbeProperty(
        @Nonnull String probeId,
        @Nonnull String targetId,
        @Nonnull String propertyName)
        throws Exception;

    /**
     * Update all probe properties associated with a probe.
     *
     * @param probeId string representation of the probe id.
     * @param newProbeProperties the probe properties that are going to replace the existing ones.
     * @throws UnknownObjectException probe does not exist.
     * @throws OperationFailedException bad input.
     * @throws UnauthorizedObjectException user not authenticated.
     * @throws AccessDeniedException user not authorized.
     * @throws InterruptedException the operation was interrupted.
     */
    void putAllProbeSpecificProperties(
            @Nonnull String probeId,
            @Nonnull List<ProbePropertyNameValuePairApiDTO> newProbeProperties)
        throws Exception;

    /**
     * Update one probe-specific probe property.  The property need not exist.
     *
     * @param probeId string representation of the probe id.
     * @param name the name of the property.
     * @param value the new value of the property.
     * @throws UnknownObjectException probe does not exist.
     * @throws OperationFailedException bad input.
     * @throws UnauthorizedObjectException user not authenticated.
     * @throws AccessDeniedException user not authorized.
     * @throws InterruptedException the operation was interrupted.
     */
    void putProbeSpecificProperty(@Nonnull String probeId, @Nonnull String name, @Nonnull String value)
        throws Exception;

    /**
     * Update all probe properties associated with a target.
     *
     * @param probeId string representation of the probe id that discovers the target.
     * @param targetId string representation of the target id.
     * @param newProbeProperties the probe properties that are going to replace the existing ones.
     * @throws UnknownObjectException probe or target does not exist.
     * @throws OperationFailedException bad input.
     * @throws UnauthorizedObjectException user not authenticated.
     * @throws AccessDeniedException user not authorized.
     * @throws InterruptedException the operation was interrupted.
     */
    void putAllTargetSpecificProperties(
        @Nonnull String probeId,
        @Nonnull String targetId,
        @Nonnull List<ProbePropertyNameValuePairApiDTO> newProbeProperties)
        throws Exception;

    /**
     * Update one target-specific probe property.  The property need not exist.
     *
     * @param probeId string representation of the probe id that discovers the target.
     * @param targetId string representation of the target id.
     * @param name the name of the property.
     * @param value the new value of the property.
     * @throws UnknownObjectException probe or target does not exist.
     * @throws OperationFailedException bad input.
     * @throws UnauthorizedObjectException user not authenticated.
     * @throws AccessDeniedException user not authorized.
     * @throws InterruptedException the operation was interrupted.
     */
    void putTargetSpecificProperty(
        @Nonnull String probeId,
        @Nonnull String targetId,
        @Nonnull String name,
        @Nonnull String value)
        throws Exception;

    /**
     * Delete one probe-specific probe property.
     *
     * @param probeId string representation of the probe id.
     * @param name the name of the property.
     * @throws UnknownObjectException probe does not exist.
     * @throws OperationFailedException bad input.
     * @throws UnauthorizedObjectException user not authenticated.
     * @throws AccessDeniedException user not authorized.
     * @throws InterruptedException the operation was interrupted.
     */
    void deleteProbeSpecificProperty(@Nonnull String probeId, @Nonnull String name) throws Exception;

    /**
     * Delete one target-specific probe property.
     *
     * @param probeId string representation of the probe id that discovers the target.
     * @param targetId string representation of the target id.
     * @param name the name of the property.
     * @throws UnknownObjectException probe or target does not exist.
     * @throws OperationFailedException bad input.
     * @throws UnauthorizedObjectException user not authenticated.
     * @throws AccessDeniedException user not authorized.
     * @throws InterruptedException the operation was interrupted.
     */
    void deleteTargetSpecificProperty(
        @Nonnull String probeId,
        @Nonnull String targetId,
        @Nonnull String name)
        throws Exception;

    static boolean validProbePropertyName(@Nonnull String name) {
        return !Objects.requireNonNull(name).isEmpty() && name.matches("[a-zA-Z0-9\\.\\_]*");
    }
}
