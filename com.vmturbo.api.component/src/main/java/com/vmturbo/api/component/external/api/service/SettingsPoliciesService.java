package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;

import io.grpc.Channel;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsPolicyApiDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.serviceinterfaces.ISettingsPoliciesService;
import com.vmturbo.common.protobuf.SettingDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;

/**
 * Implement SettingsPolicies API services.
 *
 * This class is a placeholder, for now.
 **/

@Component
public class SettingsPoliciesService implements ISettingsPoliciesService {

    private final Logger logger = LogManager.getLogger();

    private final SettingServiceBlockingStub settingService;

    private final SettingPolicyServiceBlockingStub spService;

    private final GroupServiceBlockingStub groupService;

    private final SettingsMapper settingsMapper;

    public SettingsPoliciesService(@Nonnull final SettingsMapper settingsMapper,
                                   @Nonnull final Channel groupChannel) {
        this.settingsMapper = Objects.requireNonNull(settingsMapper);
        this.spService = SettingPolicyServiceGrpc.newBlockingStub(groupChannel);
        this.groupService = GroupServiceGrpc.newBlockingStub(groupChannel);
        this.settingService = SettingServiceGrpc.newBlockingStub(groupChannel);
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
        spService.listSettingPolicies(reqBuilder.build())
            .forEachRemaining(policy -> {
                // We use an empty acceptable set to indicate everything is accepted.
                if (acceptableEntityTypes.isEmpty() ||
                        acceptableEntityTypes.contains(policy.getInfo().getEntityType())) {
                    settingPolicies.add(policy);
                }
            });

        final Set<Long> involvedGroups = SettingDTOUtil.getInvolvedGroups(settingPolicies);
        final Map<Long, String> groupNames = new HashMap<>();
        if (!involvedGroups.isEmpty()) {
            groupService.getGroups(GetGroupsRequest.newBuilder()
                    .addAllId(involvedGroups)
                    .build())
                    .forEachRemaining(group -> groupNames.put(group.getId(),
                            group.getInfo().getName()));
        }

        return settingPolicies.stream()
            .map(settingPolicy -> settingsMapper.convertSettingsPolicy(settingPolicy, groupNames))
            .collect(Collectors.toList());
    }

    @Override
    public SettingsPolicyApiDTO getSettingsPolicyByUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public SettingsPolicyApiDTO createSettingsPolicy(SettingsPolicyApiDTO settingPolicy) throws Exception {
        final Set<String> involvedSettings = settingPolicy.getSettingsManagers().stream()
                .flatMap(settingMgr -> settingMgr.getSettings().stream())
                .map(SettingApiDTO::getUuid)
                .collect(Collectors.toSet());

        final Map<String, SettingSpec> specsByName = new HashMap<>();
        settingService.searchSettingSpecs(SearchSettingSpecsRequest.newBuilder()
                .addAllSettingSpecName(involvedSettings)
                .build())
            .forEachRemaining(spec -> specsByName.put(spec.getName(), spec));

        // Technically this shouldn't happen because we only return settings we support
        // from the SettingsService.
        if (!specsByName.keySet().equals(involvedSettings)) {
            throw new InvalidOperationException("Attempted to create a settings policy with " +
                    "invalid specs: " + Sets.difference(involvedSettings, specsByName.keySet()));
        }

        final SettingPolicyInfo policyInfo =
                settingsMapper.convertInputPolicy(settingPolicy, specsByName);

        final CreateSettingPolicyResponse response;
        try {
            response = spService.createSettingPolicy(
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

        final SettingPolicy newSettingPolicy = response.getSettingPolicy();

        final Set<Long> involvedGroups = SettingDTOUtil.getInvolvedGroups(newSettingPolicy);
        final Map<Long, String> groupNames = new HashMap<>();
        if (!involvedGroups.isEmpty()) {
            groupService.getGroups(GetGroupsRequest.newBuilder()
                    .addAllId(involvedGroups)
                    .build())
                    .forEachRemaining(group -> groupNames.put(group.getId(),
                            group.getInfo().getName()));
        }

        return settingsMapper.convertSettingsPolicy(response.getSettingPolicy(), groupNames);
    }

    @Override
    public SettingsPolicyApiDTO editSettingsPolicy(String uuid, SettingsPolicyApiDTO settingPolicy) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public boolean deleteSettingsPolicy(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void validateInput(Object obj, Errors e) {
        // We do the validation in the group component as part of saving the policy.
    }
}
