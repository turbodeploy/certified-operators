package com.vmturbo.topology.processor.group.settings;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.topology.processor.group.ResolvedGroup;

/**
 * A hook by which pipeline stages may modify settings policies. Migration plans for example use
 * this to identify certain discovered template exclusion policies and expand them to cover
 * migrating VMs.
 */
public interface SettingPolicyEditor {
    /**
     * Applies edits to list of settings policies.
     *
     * @param settingPolicies A list of setting policies which may be edited.
     * @param groups the list of all groups references by the settings policies.
     *               The editor may modify this map, eg to add a newly referenced group.
     * @return The new list of settings policies.
     */
    @Nonnull
    List<SettingPolicy> applyEdits(@Nonnull List<SettingPolicy> settingPolicies,
                                   @Nonnull Map<Long, ResolvedGroup> groups);
}
