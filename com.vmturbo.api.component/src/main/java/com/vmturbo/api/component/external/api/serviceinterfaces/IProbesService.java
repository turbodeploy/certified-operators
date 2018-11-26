package com.vmturbo.api.component.external.api.serviceinterfaces;

import java.util.List;

import com.vmturbo.api.dto.probe.ProbePropertyNameValuePairApiDTO;
import com.vmturbo.api.dto.probe.ProbeApiDTO;
import com.vmturbo.api.dto.probe.ProbePropertyApiDTO;

public interface IProbesService {
    /**
     * Get information for a specific probe.
     *
     * @param probeId string representation of the probe oid.
     * @return information on the probe.
     * @throws Exception operation failed.
     */
    ProbeApiDTO getProbe(String probeId) throws Exception;

    /**
     * Get a list of all probe properties.
     *
     * @return a list of all probe properties.
     * @throws Exception operation failed.
     */
    List<ProbePropertyApiDTO> getAllProbeProperties() throws Exception;

    /**
     *  Get all probe properties specific to a probe.
     *
     *  @param probeId string representation of the probe id.
     *  @return a list of all probe properties specific to the probe.
     *  @throws Exception operation failed.
     */
    List<ProbePropertyNameValuePairApiDTO> getAllProbeSpecificProbeProperties(String probeId) throws Exception;

    /**
     * Get a probe property specific to a probe.
     *
     * @param probeId string representation of the probe id.
     * @param propertyName the name of the property.
     * @return a list of at most one probe property description.
     * @throws Exception operation failed.
     */
    String getProbeSpecificProbeProperty(String probeId, String propertyName)
        throws Exception;

    /**
     *  Get all probe properties specific to a target.
     *
     *  @param probeId string representation of the probe id that discovers the target.
     *  @param targetId string representation of the target id.
     *  @return a list of all probe properties specific to the target.
     *  @throws Exception operation failed.
     */
    List<ProbePropertyNameValuePairApiDTO> getAllTargetSpecificProbeProperties(String probeId, String targetId)
        throws Exception;

    /**
     * Get a probe property specific to a target.
     *
     * @param probeId string representation of the probe id that discovers the target.
     * @param targetId string representation of the target id.
     * @param propertyName the name of the property.
     * @return a list of at most one probe property description.
     * @throws Exception operation failed.
     */
    String getTargetSpecificProbeProperty(
        String probeId, String targetId, String propertyName) throws Exception;

    /**
     * Update all probe properties associated with a probe.
     *
     * @param probeId string representation of the probe id.
     * @param newProbeProperties the probe properties that are going to replace the existing ones.
     * @throws Exception operation failed.
     */
    void putAllProbeSpecificProperties(String probeId, List<ProbePropertyNameValuePairApiDTO> newProbeProperties)
        throws Exception;

    /**
     * Update one probe-specific probe property.  The property need not exist.
     *
     * @param probeId string representation of the probe id.
     * @param name the name of the property.
     * @param value the new value of the property.
     * @throws Exception operation failed.
     */
    void putProbeSpecificProperty(String probeId, String name, String value) throws Exception;

    /**
     * Update all probe properties associated with a target.
     *
     * @param probeId string representation of the probe id that discovers the target.
     * @param targetId string representation of the target id.
     * @param newProbeProperties the probe properties that are going to replace the existing ones.
     * @throws Exception operation failed.
     */
    void putAllTargetSpecificProperties(
        String probeId, String targetId, List<ProbePropertyNameValuePairApiDTO> newProbeProperties) throws Exception;

    /**
     * Update one target-specific probe property.  The property need not exist.
     *
     * @param probeId string representation of the probe id that discovers the target.
     * @param targetId string representation of the target id.
     * @param name the name of the property.
     * @param value the new value of the property.
     * @throws Exception operation failed.
     */
    void putTargetSpecificProperty(String probeId, String targetId, String name, String value)
        throws Exception;

    /**
     * Delete one probe-specific probe property.
     *
     * @param probeId string representation of the probe id.
     * @param name the name of the property.
     * @throws Exception operation failed.
     */
    void deleteProbeSpecificProperty(String probeId, String name) throws Exception;

    /**
     * Delete one target-specific probe property.
     *
     * @param probeId string representation of the probe id that discovers the target.
     * @param targetId string representation of the target id.
     * @param name the name of the property.
     * @throws Exception operation failed.
     */
    void deleteTargetSpecificProperty(String probeId, String targetId, String name) throws Exception;
}
