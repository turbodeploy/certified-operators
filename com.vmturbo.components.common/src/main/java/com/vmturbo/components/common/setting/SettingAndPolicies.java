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

    private Setting setting;
    private Collection<Long> policiesIds;

    /**
     * Constructor of {@link SettingAndPolicies}.
     *
     * @param setting setting
     * @param policiesIds collection of policies associated with setting
     */
    public SettingAndPolicies(@Nonnull Setting setting, @Nonnull Collection<Long> policiesIds) {
        this.setting = setting;
        this.policiesIds = policiesIds;
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
}
