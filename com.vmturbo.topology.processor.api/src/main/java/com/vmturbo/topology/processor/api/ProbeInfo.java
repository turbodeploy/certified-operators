package com.vmturbo.topology.processor.api;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;

/**
 * Represents probe information.
 */
public interface ProbeInfo {

    /**
     * Returns id of the probe.
     *
     * @return id of the probe
     */
    long getId();

    /**
     * Returns probe type.
     *
     * @return probe type
     */
    @Nonnull
    String getType();

    /**
     * Returns probe category.
     *
     * @return probe category
     */
    @Nonnull
    String getCategory();

    /**
     * Returns probe UI category.
     *
     * @return probe ui category
     */
    @Nonnull
    String getUICategory();

    /**
     * Returns probe license.
     *
     * @return probe license
     */
    Optional<String> getLicense();

    /**
     * The creation mode of the probe.
     *
     * @return true if the probe is hidden in the UI.
     */
    @Nonnull
    CreationMode getCreationMode();

    /**
     * Returns account definitions list for this probe.
     *
     * @return account definitions
     */
    @Nonnull
    List<AccountDefEntry> getAccountDefinitions();

    /**
     * List of fields, that can identify the target. All the field names, specified in the list must
     * be present in {@link #getAccountDefinitions()}. For example, if the probe is uniquely
     * identified by field named "nameOrAddress", this list should be filled with only one element -
     * "nameOrAddress", if probe is identified by "address" and "port", this list should be
     * {"address", "port"}
     *
     * @return list of identifying fields
     */
    List<String> getIdentifyingFields();
}
