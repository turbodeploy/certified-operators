package com.vmturbo.api.component.external.api.service;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;

import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.ISettingsPoliciesService;
import com.vmturbo.auth.api.auditing.AuditAction;
import com.vmturbo.auth.api.auditing.AuditLog;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.DeleteSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.ResetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.ResetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;


/**
 * Implement SettingsPolicies API services.
 *
 * This class is a placeholder, for now.
 **/

@Component
public class SettingsPoliciesService implements ISettingsPoliciesService {

    private final Logger logger = LogManager.getLogger();

    private final SettingPolicyServiceBlockingStub settingPolicyService;

    private final SettingServiceBlockingStub settingService;

    private final SettingsMapper settingsMapper;


    public SettingsPoliciesService(@Nonnull final SettingServiceBlockingStub settingServiceBlockingStub, @Nonnull final SettingsMapper settingsMapper,
                                   @Nonnull final SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub) {
        this.settingsMapper = Objects.requireNonNull(settingsMapper);
        this.settingPolicyService = settingPolicyServiceBlockingStub;
        this.settingService = settingServiceBlockingStub;

    }

    /**
     * Get the list of defined setting policies.
     *
     * @param onlyDefaults Show only the defaults.
     * @param entityTypes Filter the list by entity type.
     * @return The list of {@link SettingsPolicyApiDTO}, one for each setting policy that exists
     *         in the system.
     */
    @Override
    public List<SettingsPolicyApiDTO> getSettingsPolicies(boolean onlyDefaults,
                                                          List<String> entityTypes) {
        final Set<Integer> acceptableEntityTypes = entityTypes == null || entityTypes.isEmpty()
                ? Collections.emptySet()
                : entityTypes.stream()
                    .map(ApiEntityType::fromString)
                    .map(ApiEntityType::typeNumber)
                    .collect(Collectors.toSet());
        return getSettingsPolicies(onlyDefaults, acceptableEntityTypes, Collections.emptySet());
    }

    /**
     * Get the list of defined setting policies.
     *
     * @param onlyDefaults Show only the defaults.
     * @param acceptableEntityTypes Filter the list by entity type.
     * @param managersToInclude the set of managers to include in the response, if
     *                          managersToInclude is empty, return all managers
     * @return The list of {@link SettingsPolicyApiDTO}, one for each setting policy that exists
     *         in the system.
     */
    public List<SettingsPolicyApiDTO> getSettingsPolicies(boolean onlyDefaults,
                                                          @Nonnull Set<Integer> acceptableEntityTypes,
                                                          @Nonnull Set<String> managersToInclude) {
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

        // Inject settings to make it visible in UI if no entity type was provided.
        if (acceptableEntityTypes.isEmpty()) {
            settingPolicies.add(createGlobalSettingPolicy());
        }

        return settingsMapper.convertSettingPolicies(settingPolicies, managersToInclude);
    }

    /**
     * "Create Global Action mode" settings policy to make it visible in UI.
     * Since it is a policy that does not have any entity associated with it we
     * force its creation for UI visibility.
     * @return setting policy to inject
     */
    @VisibleForTesting
    SettingPolicy createGlobalSettingPolicy() {
       return SettingPolicy.newBuilder()
            .setSettingPolicyType(Type.DEFAULT)
            .setId(SettingsMapper.GLOBAL_SETTING_POLICY_ID)
            .setInfo(SettingPolicyInfo.newBuilder()
                .setName(SettingsMapper.GLOBAL_SETTING_POLICY_NAME)
                .setEnabled(true))
            .build();
    }

    @Override
    public SettingsPolicyApiDTO getSettingsPolicyByUuid(String uuid) throws Exception {

        final GetSettingPolicyResponse response;
        try {
            response = settingPolicyService.getSettingPolicy(
                    GetSettingPolicyRequest.newBuilder()
                            .setId(Long.valueOf(uuid))
                            .build());
        } catch (StatusRuntimeException e) {
            throw ExceptionMapper.translateStatusException(e);
        }

        return settingsMapper.convertSettingPolicy(response.getSettingPolicy());
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
            final String details = String.format("Created policy %s", settingPolicy.getDisplayName());
            AuditLog.newEntry(AuditAction.CREATE_POLICY,
                details, true)
                .targetName(settingPolicy.getDisplayName())
                .audit();
        } catch (StatusRuntimeException e) {
            final String details = String.format("Failed to create policy %s", settingPolicy.getDisplayName());
            AuditLog.newEntry(AuditAction.CREATE_POLICY,
                details, false)
                .targetName(settingPolicy.getDisplayName())
                .audit();
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
     * @param setDefault    flag if policy setting needs to be reset to default.
     * @param settingPolicy setting policy to be used to edito
     * @return updated SettingsPolicyApiDTO
     * @throws Exception
     */
    @Override
    public SettingsPolicyApiDTO editSettingsPolicy(String uuid,
                                                   boolean setDefault,
                                                   SettingsPolicyApiDTO settingPolicy)
            throws Exception {
        if (uuid.equals(String.valueOf(SettingsMapper.GLOBAL_SETTING_POLICY_ID))) {
            return editGlobalSettingPolicy(setDefault, settingPolicy);
        }
        final long id = Long.valueOf(uuid);
        final SettingPolicy editedPolicy;
        try {
            if (setDefault) {
                final ResetSettingPolicyResponse response =
                    settingPolicyService.resetSettingPolicy(ResetSettingPolicyRequest.newBuilder()
                        .setSettingPolicyId(id)
                        .build());
                editedPolicy = response.getSettingPolicy();

                if (SettingsMapper.isVmEntityType(response.getSettingPolicy().getInfo().getEntityType())) {
                    String rateOfResizeSettingName = GlobalSettingSpecs.RateOfResize.getSettingName();
                    float defaultValue = GlobalSettingSpecs.RateOfResize.createSettingSpec().getNumericSettingValueType().getDefault();
                    settingService.updateGlobalSetting(UpdateGlobalSettingRequest.newBuilder()
                        .addSetting(Setting.newBuilder().setSettingSpecName(rateOfResizeSettingName)
                            .setNumericSettingValue(SettingDTOUtil.createNumericSettingValue(defaultValue)))
                            .build());
                }

            } else {
                final SettingPolicyInfo policyInfo =
                        settingsMapper.convertEditedInputPolicy(id, settingPolicy);

                    final UpdateSettingPolicyResponse response = settingPolicyService.updateSettingPolicy(
                                    UpdateSettingPolicyRequest.newBuilder()
                                            .setId(id)
                                            .setNewInfo(policyInfo)
                                            .build());
                    editedPolicy = response.getSettingPolicy();
            }
            final String details = String.format("Changed policy %s", settingPolicy.getDisplayName());
            AuditLog.newEntry(AuditAction.CHANGE_POLICY,
                details, true)
                .targetName(settingPolicy.getDisplayName())
                .audit();
        } catch (StatusRuntimeException e) {
            final String details = String.format("Failed to change policy %s", settingPolicy.getDisplayName());
            AuditLog.newEntry(AuditAction.CHANGE_POLICY,
                details, false)
                .targetName(settingPolicy.getDisplayName())
                .audit();
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

        return settingsMapper.convertSettingPolicy(editedPolicy);
    }

    private SettingsPolicyApiDTO editGlobalSettingPolicy(boolean setDefault, SettingsPolicyApiDTO settingPolicy) {
        try {
            settingsMapper.updateGlobalSettingPolicy(setDefault, settingPolicy);
            final String details = String.format("Changed policy %s", settingPolicy.getDisplayName());
            AuditLog.newEntry(AuditAction.CHANGE_POLICY,
                details, true)
                .targetName(settingPolicy.getDisplayName())
                .audit();
            return settingsMapper.convertSettingPolicy(createGlobalSettingPolicy());
        } catch (RuntimeException e) {
            final String details = String.format("Failed to change policy %s", settingPolicy.getDisplayName());
            AuditLog.newEntry(AuditAction.CHANGE_POLICY,
                details, false)
                .targetName(settingPolicy.getDisplayName())
                .audit();
            throw e;
        }
    }

    @Override
    public boolean deleteSettingsPolicy(String uuid) throws Exception {
        final long id = Long.valueOf(uuid);
        try {
            settingPolicyService.deleteSettingPolicy(
                DeleteSettingPolicyRequest.newBuilder()
                    .setId(id)
                    .build());
            final String details = String.format("Deleted policy with uuid: %s", uuid);
            AuditLog.newEntry(AuditAction.DELETE_POLICY,
                details, true)
                .targetName(uuid)
                .audit();
        } catch (StatusRuntimeException e) {
            final String details = String.format("Failed to delete policy with uuid: %s", uuid);
            AuditLog.newEntry(AuditAction.DELETE_POLICY,
                details, false)
                .targetName(uuid)
                .audit();
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
