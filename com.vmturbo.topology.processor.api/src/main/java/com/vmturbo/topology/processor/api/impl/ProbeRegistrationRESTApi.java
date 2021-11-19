package com.vmturbo.topology.processor.api.impl;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.swagger.annotations.ApiModelProperty;

import com.vmturbo.topology.processor.api.ProbeRegistrationInfo;

import common.HealthCheck.HealthState;

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

        @ApiModelProperty(value = "The health state of the probe registration.", required = true)
        private final HealthState healthState;

        @ApiModelProperty(value = "The status of the probe registration.", required = false)
        private final String status;

        /**
         * Constructs a {@link ProbeRegistrationDescription} given the list of inputs.
         *
         * @param id the id of the probe registration
         * @param probeId the id of the probe type
         * @param communicationBindingChannel the communication binding channel of the probe registration
         * @param version the version of the probe
         * @param registeredTime the registered time of the probe
         * @param displayName the display name of the probe registration
         * @param healthState the health state of the probe registration
         * @param status the status of the probe registration
         */
        public ProbeRegistrationDescription(final long id, final long probeId,
                final String communicationBindingChannel, final String version,
                final long registeredTime, final String displayName, final HealthState healthState,
                final String status) {
            this.id = id;
            this.probeId = probeId;
            this.communicationBindingChannel = communicationBindingChannel;
            this.version = version;
            this.registeredTime = registeredTime;
            this.displayName = displayName;
            this.healthState = healthState;
            this.status = status;
        }

        /**
         * Constructor for empty ProbeRegistrationDescription which will be created in
         * deserialization, or the probe registration cannot be found in the probe store.
         */
        public ProbeRegistrationDescription() {
            this(-1, -1, null, "", 0, "", HealthState.NORMAL, "");
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

        @Nonnull
        @Override
        public HealthState getHealthState() {
            return healthState;
        }

        @Nullable
        @Override
        public String getStatus() {
            return status;
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
