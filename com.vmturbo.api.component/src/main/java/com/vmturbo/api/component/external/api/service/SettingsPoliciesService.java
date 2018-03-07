package com.vmturbo.api.component.external.api.service;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;

import io.grpc.Channel;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.ISettingsPoliciesService;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.DeleteSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyResponse;

/**
 * Implement SettingsPolicies API services.
 *
 * This class is a placeholder, for now.
 **/

@Component
public class SettingsPoliciesService implements ISettingsPoliciesService {

    private final Logger logger = LogManager.getLogger();

    private final SettingPolicyServiceBlockingStub settingPolicyService;

    private final SettingsMapper settingsMapper;

    public SettingsPoliciesService(@Nonnull final SettingsMapper settingsMapper,
                                   @Nonnull final Channel groupChannel) {
        this.settingsMapper = Objects.requireNonNull(settingsMapper);
        this.settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(groupChannel);
    }

    /**
     * Get the list of defined setting policies.
     *
     * @param onlyDefaults Show only the defaults.
     * @param entityTypes Filter the list by entity type.
     * @return The list of {@link SettingsPolicyApiDTO}, one for each setting policy that exists
     *         in the system.
     * @throws Exception If something goes wrong.
     */
    @Override
    public List<SettingsPolicyApiDTO> getSettingsPolicies(boolean onlyDefaults,
                                                          List<String> entityTypes) throws Exception {
        final Set<Integer> acceptableEntityTypes = entityTypes == null || entityTypes.isEmpty() ?
                Collections.emptySet() :
                entityTypes.stream()
                    .map(ServiceEntityMapper::fromUIEntityType)
                    .collect(Collectors.toSet());


        final ListSettingPoliciesRequest.Builder reqBuilder = ListSettingPoliciesRequest.newBuilder();
        if (onlyDefaults) {
            reqBuilder.setTypeFilter(Type.DEFAULT);
        }

        final List<SettingPolicy> settingPolicies = new LinkedList<>();
        settingPolicyService.listSettingPolicies(reqBuilder.build())
            .forEachRemaining(policy -> {
                // We use an empty acceptable set to indicate everything is accepted.
                if (acceptableEntityTypes.isEmpty() ||
                        acceptableEntityTypes.contains(policy.getInfo().getEntityType())) {
                    settingPolicies.add(policy);
                }
            });

        return settingsMapper.convertSettingPolicies(settingPolicies);
    }

    @Override
    public SettingsPolicyApiDTO getSettingsPolicyByUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public SettingsPolicyApiDTO createSettingsPolicy(SettingsPolicyApiDTO settingPolicy) throws Exception {

        final SettingPolicyInfo policyInfo = settingsMapper.convertNewInputPolicy(settingPolicy);

        final CreateSettingPolicyResponse response;
        try {
            response = settingPolicyService.createSettingPolicy(
                    CreateSettingPolicyRequest.newBuilder()
                            .setSettingPolicyInfo(policyInfo)
                            .build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.ALREADY_EXISTS)) {
                throw new OperationFailedException(e.getStatus().getDescription());
            } else if (e.getStatus().getCode().equals(Code.INVALID_ARGUMENT)) {
                throw new InvalidOperationException(e.getStatus().getDescription());
            } else {
                throw e;
            }
        }

        return settingsMapper.convertSettingPolicy(response.getSettingPolicy());
    }

    /**
     * edit a settings policy
     * PUT /settingspolicies/{uuid}
     *
     * @param uuid          policy setting uuid
     * @param setDefault    flag if policy setting needs to be reset to default
     *                      Currently unsupported.
     * @param settingPolicy setting policy to be used to edito
     * @return updated SettingsPolicyApiDTO
     * @throws Exception
     */
    @Override
    public SettingsPolicyApiDTO editSettingsPolicy(String uuid,
                                                   boolean setDefault,
                                                   SettingsPolicyApiDTO settingPolicy)
            throws Exception {
        final long id = Long.valueOf(uuid);
        final SettingPolicyInfo policyInfo =
                settingsMapper.convertEditedInputPolicy(id, settingPolicy);

        final UpdateSettingPolicyResponse response;
        try {
            response = settingPolicyService.updateSettingPolicy(
                UpdateSettingPolicyRequest.newBuilder()
                    .setId(id)
                    .setNewInfo(policyInfo)
                    .build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.ALREADY_EXISTS)) {
                throw new OperationFailedException(e.getStatus().getDescription());
            } else if (e.getStatus().getCode().equals(Code.INVALID_ARGUMENT)) {
                throw new InvalidOperationException(e.getStatus().getDescription());
            } else if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                throw new UnknownObjectException(e.getStatus().getDescription());
            } else {
                throw e;
            }
        }

        return settingsMapper.convertSettingPolicy(response.getSettingPolicy());
    }

    @Override
    public boolean deleteSettingsPolicy(String uuid) throws Exception {
        final long id = Long.valueOf(uuid);
        try {
            settingPolicyService.deleteSettingPolicy(
                DeleteSettingPolicyRequest.newBuilder()
                    .setId(id)
                    .build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Code.INVALID_ARGUMENT)) {
                throw new InvalidOperationException(e.getStatus().getDescription());
            } else if (e.getStatus().getCode().equals(Code.NOT_FOUND)) {
                throw new UnknownObjectException(e.getStatus().getDescription());
            } else {
                throw e;
            }
        }
        return true;
    }

    @Override
    public void validateInput(Object obj, Errors e) {
        // We do the validation in the group component as part of saving the policy.
    }
}
