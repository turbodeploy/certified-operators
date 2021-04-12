package com.vmturbo.api.component.external.api.mapper;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.api.serviceinterfaces.IProbesService;
import com.vmturbo.api.dto.probe.ProbePropertyNameValuePairApiDTO;
import com.vmturbo.api.dto.probe.ProbeApiDTO;
import com.vmturbo.api.dto.probe.ProbePropertyApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbeInfoResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.ProbePropertyNameValuePair;
import com.vmturbo.common.protobuf.probe.ProbeDTO.ProbeOrTarget;
import com.vmturbo.common.protobuf.probe.ProbeDTO.ProbePropertyInfo;

/**
 * Conversions between different representations of probes and probe properties.
 */
public class ProbeMapper {
    private ProbeMapper() {
    }

    /**
     * Convert a {@link ProbePropertyInfo} protobuf representation of one probe property
     * into a {@link ProbePropertyApiDTO} object that can be used in the REST API.
     *
     * @param probePropertyInfo protobuf representation of one probe property.
     * @return REST API model representation of the probe property.
     */
    @Nonnull
    public static ProbePropertyApiDTO convertToProbePropertyApiDTO(
            @Nonnull ProbePropertyInfo probePropertyInfo) {
        final ProbePropertyApiDTO result = new ProbePropertyApiDTO();
        final ProbeOrTarget table = probePropertyInfo.getProbePropertyTable();
        result.setProbeId(table.getProbeId());
        if (table.hasTargetId()) {
            result.setTargetId(table.getTargetId());
        }
        result.setName(probePropertyInfo.getProbePropertyNameAndValue().getName());
        result.setValue(probePropertyInfo.getProbePropertyNameAndValue().getValue());
        return result;
    }

    /**
     * Convert a {@link ProbePropertyNameValuePair} protobuf representation of a name/value pair into a
     * {@link ProbePropertyNameValuePairApiDTO} object that can be used in the REST API.
     *
     * @param nameValuePair protobuf representation of the name/value pair.
     * @return REST API model representation of the name/value pair.
     */
    @Nonnull
    public static ProbePropertyNameValuePairApiDTO convertToNameValueApiDTO(
            @Nonnull ProbePropertyNameValuePair nameValuePair) {
        final ProbePropertyNameValuePairApiDTO result = new ProbePropertyNameValuePairApiDTO();
        if (nameValuePair.hasName()) {
            result.setName(nameValuePair.getName());
        }
        if (nameValuePair.hasValue()) {
            result.setValue(nameValuePair.getValue());
        }
        return result;
    }

    /**
     * Convert a {@link ProbePropertyNameValuePairApiDTO} object coming from the REST API into a
     * {@link ProbePropertyNameValuePair} protobuf representation.  The input object is coming from a list of
     * probe properties in a REST request.<p/>
     *
     * This conversion may be one element of a set of multiple conversions.  If so, the name of the pair
     * should be unique among all these conversions.
     *
     * @param nameValuePair the input object.
     * @param observedNames the set of names already observed in other conversions.
     * @return the protobuf message.
     * @throws OperationFailedException if one of the above-mentioned integrity conditions is not met.
     */
    @Nonnull
    public static ProbePropertyNameValuePair convertToNameValuePair(
            @Nonnull ProbePropertyNameValuePairApiDTO nameValuePair,
            @Nonnull Set<String> observedNames) throws OperationFailedException {
        // check validity of the input
        if (nameValuePair.getName() == null ||
            !IProbesService.validProbePropertyName(nameValuePair.getName())) {
            throw new OperationFailedException(
                "Invalid property name: " + nameValuePair.getName());
        }
        if (nameValuePair.getValue() == null || nameValuePair.getValue().isEmpty()) {
            throw new OperationFailedException("Missing probe property value");
        }

        // check if this name has been observed already
        if (!observedNames.add(nameValuePair.getName())) {
            throw new OperationFailedException("Duplicate probe property name: " + nameValuePair.getName());
        }

        return
            ProbePropertyNameValuePair.newBuilder()
                .setName(nameValuePair.getName())
                .setValue(nameValuePair.getValue())
                .build();
    }

    /**
     * Convert a {@link GetProbeInfoResponse} protobuf message into a {@link ProbeApiDTO} object.
     *
     * @param response the protobuf message.
     * @return the {@link ProbeApiDTO} object.
     */
    @Nonnull
    public static ProbeApiDTO getProbeApiDTO(@Nonnull GetProbeInfoResponse response) {
        final ProbeApiDTO result = new ProbeApiDTO();
        result.setId(response.getOid());
        result.setCategory(response.getCategory());
        result.setType(response.getType());
        return result;
    }
}
