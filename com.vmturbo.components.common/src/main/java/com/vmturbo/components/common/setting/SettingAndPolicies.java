package com.vmturbo.components.common.setting;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;

/**
 * Contains setting with associated list of policies ids.
 * Setting can has several associated policies if setting was created by merging setting values
 * from settings associated with different policies.
 */
public class SettingAndPolicies {

    private final Setting setting;
    private final Collection<Long> policiesIds;
    private final Collection<Long> defaultPoliciesIds;

    /**
     * Constructor of {@link SettingAndPolicies}.
     *
     * @param setting setting
     * @param policiesIds collection of policies associated with setting
     * @param defaultPoliciesIds collection of default policies associated with setting
     */
    public SettingAndPolicies(@Nonnull Setting setting, @Nonnull Collection<Long> policiesIds,
            @Nonnull Collection<Long> defaultPoliciesIds) {
        this.setting = setting;
        this.policiesIds = policiesIds;
        this.defaultPoliciesIds = defaultPoliciesIds;
    }

    /**
     * Return {@link Setting}.
     *
     * @return protobuf setting.
     */
    @Nonnull
    public Setting getSetting() {
        return setting;
    }

    /**
     * Return policies associated with setting {@link SettingAndPolicies#setting}.
     *
     * @return policies associated with setting.
     */
    @Nonnull
    public Collection<Long> getPoliciesIds() {
        return policiesIds;
    }

    /**
     * Return default policies associated with setting {@link SettingAndPolicies#setting}.
     *
     * @return default policies associated with setting.
     */
    @Nonnull
    public Collection<Long> getDefaultPoliciesIds() {
        return defaultPoliciesIds;
    }
}
