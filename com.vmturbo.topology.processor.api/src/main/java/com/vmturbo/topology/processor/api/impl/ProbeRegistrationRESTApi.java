package com.vmturbo.topology.processor.api.impl;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.base.Strings;

import io.swagger.annotations.ApiModelProperty;

import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.api.ProbeRegistrationInfo;

/**
 * REST API classes (DTO part) for probe registrations.
 */
public class ProbeRegistrationRESTApi {
    /**
     * User-visible parts of the {@link ProbeRegistrationInfo} proto-generated class, extracted
     * mostly for Swagger integration for the REST API.
     */
    public static final class ProbeRegistrationDescription implements ProbeRegistrationInfo {

        @ApiModelProperty(value = "The ID of the probe registration.", required = true)
        private final long id;

        @ApiModelProperty(value = "The display name of the probe registration.", required = true)
        private final String displayName;

        @ApiModelProperty(value = "The probe type id associated with this probe registration.", required = true)
        private final long probeId;

        @ApiModelProperty(value = "The communication binding channel of the probe registration.")
        private final String communicationBindingChannel;

        @ApiModelProperty(value = "The version of the probe registration.", required = true)
        private final String version;

        @ApiModelProperty(value = "The registered time of the probe.", required = true)
        private final long registeredTime;

        /**
         * Constructor for empty ProbeRegistrationDescription which will be created in
         * deserialization, or the probe registration cannot be found in the probe store.
         */
        public ProbeRegistrationDescription() {
            this.id = -1;
            this.displayName = "";
            this.probeId = -1;
            this.communicationBindingChannel = null;
            this.version = "";
            this.registeredTime = 0;
        }

        /**
         * Constructs a {@link ProbeRegistrationDescription} given the list of inputs.
         *
         * @param id the id of the probe registration
         * @param probeId the id of the probe type
         * @param probeInfo the info about this probe
         * @param containerInfo the info about the mediation container
         */
        public ProbeRegistrationDescription(final long id, final long probeId,
                @Nonnull final ProbeInfo probeInfo, @Nonnull ContainerInfo containerInfo) {
            this.id = id;
            this.probeId = probeId;
            this.communicationBindingChannel = Objects.requireNonNull(containerInfo).getCommunicationBindingChannel();
            this.version = Objects.requireNonNull(probeInfo).getVersion();
            this.registeredTime = System.currentTimeMillis();
            if (Strings.isNullOrEmpty(probeInfo.getDisplayName())) {
                if (Strings.isNullOrEmpty(communicationBindingChannel)) {
                    this.displayName = probeInfo.getProbeType() + " Probe " + id;
                } else {
                    this.displayName = probeInfo.getProbeType() + " Probe " + communicationBindingChannel;
                }
            } else {
                this.displayName = probeInfo.getDisplayName();
            }
        }

        @Override
        public long getId() {
            return id;
        }

        @Nonnull
        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public long getProbeId() {
            return probeId;
        }

        @Nonnull
        @Override
        public Optional<String> getCommunicationBindingChannel() {
            return Optional.ofNullable(communicationBindingChannel);
        }

        @Nonnull
        @Override
        public String getVersion() {
            return version;
        }

        @Override
        public long getRegisteredTime() {
            return registeredTime;
        }
    }

    /**
     * Response object for the GET call to /probe/registration.
     */
    public static final class GetAllProbeRegistrations {
        @ApiModelProperty(value = "List of all probe registrations.", required = true)
        private final Collection<ProbeRegistrationDescription> probeRegistrations;

        /**
         * Constructs a {@link GetAllProbeRegistrations} with a null list of probe registrations.
         */
        protected GetAllProbeRegistrations() {
            probeRegistrations = null;
        }

        /**
         * Constructs a {@link GetAllProbeRegistrations} given the input list of probe registrations.
         *
         * @param probeRegistrations the input list of probe registrations.
         */
        public GetAllProbeRegistrations(@Nonnull final Collection<ProbeRegistrationDescription> probeRegistrations) {
            this.probeRegistrations = Objects.requireNonNull(probeRegistrations, "probeRegistrations field is absent");
        }

        /**
         * Returns the list of probe registrations.
         *
         * @return the list of probe registrations.
         */
        public Collection<ProbeRegistrationDescription> getProbeRegistrations() {
            return Objects.requireNonNull(probeRegistrations, "probeRegistrations field is absent");
        }
    }

}
