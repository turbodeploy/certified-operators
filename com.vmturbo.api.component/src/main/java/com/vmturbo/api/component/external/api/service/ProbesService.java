package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.ProbeMapper;
import com.vmturbo.api.component.external.api.serviceinterfaces.IProbesService;
import com.vmturbo.api.dto.probe.ProbeApiDTO;
import com.vmturbo.api.dto.probe.ProbePropertyApiDTO;
import com.vmturbo.api.dto.probe.ProbePropertyNameValuePairApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.probe.ProbeDTO.DeleteProbePropertyRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetAllProbePropertiesRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbeInfoRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbeInfoResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbePropertyValueRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetProbePropertyValueResponse;
import com.vmturbo.common.protobuf.probe.ProbeDTO.GetTableOfProbePropertiesRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.ProbeOrTarget;
import com.vmturbo.common.protobuf.probe.ProbeDTO.ProbePropertyInfo;
import com.vmturbo.common.protobuf.probe.ProbeDTO.ProbePropertyNameValuePair;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateOneProbePropertyRequest;
import com.vmturbo.common.protobuf.probe.ProbeDTO.UpdateProbePropertyTableRequest;
import com.vmturbo.common.protobuf.probe.ProbeRpcServiceGrpc.ProbeRpcServiceBlockingStub;

/**
 * Implementation of the probe service.
 */
public class ProbesService implements IProbesService {

    private final ProbeRpcServiceBlockingStub probeRpcService;

    /**
     * Probe service constructor.
     *
     * @param probeRpcService RPC connection to Topology Processor.
     */
    public ProbesService(@Nonnull ProbeRpcServiceBlockingStub probeRpcService) {
        this.probeRpcService = Objects.requireNonNull(probeRpcService);
    }

    @Override
    @Nonnull
    public ProbeApiDTO getProbe(@Nonnull String probeId) throws Exception {
        final long oid;
        try {
            oid = Long.valueOf(probeId);
        } catch (NumberFormatException e) {
            throw new OperationFailedException("Illegal probe id: " + probeId);
        }
        final GetProbeInfoResponse response;
        response = probeRpcService.getProbeInfo(GetProbeInfoRequest.newBuilder().setOid(oid).build());
        return ProbeMapper.getProbeApiDTO(response);
    }

    @Override
    @Nonnull
    public List<ProbePropertyApiDTO> getAllProbeProperties()
            throws Exception {
        try {
            return
                probeRpcService
                    .getAllProbeProperties(GetAllProbePropertiesRequest.newBuilder().build())
                    .getProbePropertiesList().stream()
                    .map(ProbeMapper::convertToProbePropertyApiDTO)
                    .collect(Collectors.toList());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    @Override
    @Nonnull
    public List<ProbePropertyNameValuePairApiDTO> getAllProbeSpecificProbeProperties(@Nonnull String probeId)
            throws Exception {
        try {
            return
                probeRpcService
                    .getTableOfProbeProperties(makeGetTableOfPropertiesRequest(probeId))
                    .getProbePropertiesList().stream()
                    .map(ProbeMapper::convertToNameValueApiDTO)
                    .collect(Collectors.toList());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    @Override
    @Nonnull
    public String getProbeSpecificProbeProperty(
            @Nonnull String probeId,
            @Nonnull String propertyName)
            throws Exception {
        if (!IProbesService.validProbePropertyName(propertyName)) {
            throw new OperationFailedException(
                "During reading of probe property specific to probe " + probeId +
                ": invalid property name: " + propertyName);
        }
        final GetProbePropertyValueResponse response;
        try {
            response =
                probeRpcService.getProbePropertyValue(
                    GetProbePropertyValueRequest.newBuilder()
                        .setProbePropertyTable(makeProbePropertyTable(probeId))
                        .setName(propertyName)
                        .build());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
        return response.hasValue() ? response.getValue() : "";
    }

    @Override
    @Nonnull
    public List<ProbePropertyNameValuePairApiDTO> getAllTargetSpecificProbeProperties(
            @Nonnull String probeId, @Nonnull String targetId)
            throws Exception {
        try {
            return
                probeRpcService.getTableOfProbeProperties(makeGetTableOfPropertiesRequest(probeId, targetId))
                    .getProbePropertiesList().stream()
                    .map(ProbeMapper::convertToNameValueApiDTO)
                    .collect(Collectors.toList());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    @Override
    @Nonnull
    public String getTargetSpecificProbeProperty(
            @Nonnull String probeId,
            @Nonnull String targetId,
            @Nonnull String propertyName)
            throws Exception {
        if (!IProbesService.validProbePropertyName(propertyName)) {
            throw new OperationFailedException(
                "During reading of probe property specific to probe " + probeId + " and target " +
                targetId + ": invalid property name: " + propertyName);
        }
        final GetProbePropertyValueResponse response;
        try {
            response = probeRpcService.getProbePropertyValue(
                GetProbePropertyValueRequest.newBuilder()
                    .setProbePropertyTable(makeProbePropertyTable(probeId, targetId))
                    .setName(propertyName)
                    .build());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
        return response.hasValue() ? response.getValue() : "";
    }

    @Override
    public void putAllProbeSpecificProperties(
            @Nonnull String probeId,
            @Nonnull List<ProbePropertyNameValuePairApiDTO> newProbeProperties)
            throws Exception {
        final Set<String> observed = new HashSet<>();
        final List<ProbePropertyNameValuePair> newProbePropertiesConverted = new ArrayList<>();
        for (ProbePropertyNameValuePairApiDTO p : newProbeProperties) {
            newProbePropertiesConverted.add(ProbeMapper.convertToNameValuePair(p, observed));
        }
        try {
            probeRpcService.updateProbePropertyTable(
                UpdateProbePropertyTableRequest.newBuilder()
                    .setProbePropertyTable(makeProbePropertyTable(probeId))
                    .addAllNewProbeProperties(newProbePropertiesConverted)
                    .build());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    @Override
    public void putProbeSpecificProperty(
            @Nonnull String probeId,
            @Nonnull String name,
            @Nonnull String value)
            throws Exception {
        if (!IProbesService.validProbePropertyName(name)) {
            throw new OperationFailedException(
                "During updating of probe property specific to probe " + probeId +
                ": invalid property name: " + name);
        }
        try {
            probeRpcService.updateOneProbeProperty(
                UpdateOneProbePropertyRequest.newBuilder()
                    .setNewProbeProperty(makeProbePropertyInfo(probeId, name, value))
                    .build());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    @Override
    public void putAllTargetSpecificProperties(
            @Nonnull String probeId,
            @Nonnull String targetId,
            @Nonnull List<ProbePropertyNameValuePairApiDTO> newProbeProperties)
            throws Exception {
        final Set<String> observed = new HashSet<>();
        final List<ProbePropertyNameValuePair> newProbePropertiesConverted = new ArrayList<>();
        for (ProbePropertyNameValuePairApiDTO p : newProbeProperties) {
            newProbePropertiesConverted.add(ProbeMapper.convertToNameValuePair(p, observed));
        }
        try {
            probeRpcService.updateProbePropertyTable(
                UpdateProbePropertyTableRequest.newBuilder()
                    .setProbePropertyTable(makeProbePropertyTable(probeId, targetId))
                    .addAllNewProbeProperties(newProbePropertiesConverted)
                    .build());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    @Override
    public void putTargetSpecificProperty(
            @Nonnull String probeId,
            @Nonnull String targetId,
            @Nonnull String name,
            @Nonnull String value)
            throws Exception {
        if (!IProbesService.validProbePropertyName(name)) {
            throw new OperationFailedException(
                "During updating of probe property specific to probe " + probeId +
                "and target " + targetId + ": invalid property name: " + name);
        }
        try {
            probeRpcService.updateOneProbeProperty(
                UpdateOneProbePropertyRequest.newBuilder()
                    .setNewProbeProperty(makeProbePropertyInfo(probeId, targetId, name, value))
                    .build());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    @Override
    public void deleteProbeSpecificProperty(@Nonnull String probeId, @Nonnull String name)
            throws Exception {
        if (!IProbesService.validProbePropertyName(name)) {
            throw new OperationFailedException(
                "During deletion of probe property specific to probe " + probeId +
                ": invalid property name: " + name);
        }
        try {
            probeRpcService.deleteProbeProperty(
                DeleteProbePropertyRequest.newBuilder()
                    .setProbePropertyTable(makeProbePropertyTable(probeId))
                    .setName(name)
                    .build());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    @Override
    public void deleteTargetSpecificProperty(
            @Nonnull String probeId,
            @Nonnull String targetId,
            @Nonnull String name)
            throws Exception {
        if (!IProbesService.validProbePropertyName(name)) {
            throw new OperationFailedException(
                "During deletion of probe property specific to probe " + probeId +
                ": invalid property name: " + name);
        }
        try {
            probeRpcService.deleteProbeProperty(
                DeleteProbePropertyRequest.newBuilder()
                    .setProbePropertyTable(makeProbePropertyTable(probeId, targetId))
                    .setName(name)
                    .build());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    @Nonnull
    private ProbeOrTarget makeProbePropertyTable(@Nonnull String probeId) throws OperationFailedException {
        final long oid;
        try {
            oid = Long.valueOf(probeId);
        } catch (NumberFormatException e) {
            throw new OperationFailedException("Illegal probe id: " + probeId);
        }
        return ProbeOrTarget.newBuilder().setProbeId(oid).build();
    }

    @Nonnull
    private ProbeOrTarget makeProbePropertyTable(@Nonnull String probeId, @Nonnull String targetId)
            throws OperationFailedException {
        final long probeOid;
        try {
            probeOid = Long.valueOf(probeId);
        } catch (NumberFormatException e) {
            throw new OperationFailedException("Illegal probe id: " + probeId);
        }
        final long targetOid;
        try {
            targetOid = Long.valueOf(targetId);
        } catch (NumberFormatException e) {
            throw new OperationFailedException("Illegal target id: " + targetId);
        }
        return ProbeOrTarget.newBuilder().setProbeId(probeOid).setTargetId(targetOid).build();
    }

    @Nonnull
    private GetTableOfProbePropertiesRequest makeGetTableOfPropertiesRequest(@Nonnull String probeId)
            throws OperationFailedException {
        return
            GetTableOfProbePropertiesRequest.newBuilder()
                .setProbePropertyTable(makeProbePropertyTable(probeId))
                .build();
    }

    @Nonnull
    private GetTableOfProbePropertiesRequest makeGetTableOfPropertiesRequest(
            @Nonnull String probeId, @Nonnull String targetId) throws OperationFailedException {
        return
            GetTableOfProbePropertiesRequest.newBuilder()
                .setProbePropertyTable(makeProbePropertyTable(probeId, targetId))
                .build();
    }

    @Nonnull
    private ProbePropertyNameValuePair makeNameValuePair(
            @Nonnull String name, @Nonnull String value) throws OperationFailedException {
        if (name.isEmpty()) {
            throw new OperationFailedException("Missing probe property name");
        }
        if (value.isEmpty()) {
            throw new OperationFailedException("Missing probe property value");
        }
        return ProbePropertyNameValuePair.newBuilder().setName(name).setValue(value).build();
    }

    @Nonnull
    private ProbePropertyInfo makeProbePropertyInfo(
            @Nonnull String probeId,
            @Nonnull String name,
            @Nonnull String value) throws OperationFailedException {
        return
            ProbePropertyInfo.newBuilder()
                .setProbePropertyTable(makeProbePropertyTable(probeId))
                .setProbePropertyNameAndValue(makeNameValuePair(name, value))
                .build();
    }

    @Nonnull
    private ProbePropertyInfo makeProbePropertyInfo(
            @Nonnull String probeId,
            @Nonnull String targetId,
            @Nonnull String name,
            @Nonnull String value) throws OperationFailedException {
        return
            ProbePropertyInfo.newBuilder()
                .setProbePropertyTable(makeProbePropertyTable(probeId, targetId))
                .setProbePropertyNameAndValue(makeNameValuePair(name, value))
                .build();
    }

}
