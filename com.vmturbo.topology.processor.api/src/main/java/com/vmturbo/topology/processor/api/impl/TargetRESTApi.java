package com.vmturbo.topology.processor.api.impl;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorException;
import com.vmturbo.topology.processor.api.dto.InputField;
import com.vmturbo.topology.processor.api.dto.TargetInputFields;

import io.swagger.annotations.ApiModelProperty;

/**
 * Common class for Java objects representing request and response objects
 * for the TargetController REST API calls.
 * <p/>
 * These objects are meant to be serializable by Gson. Include
 * an empty constructor for all objects and subobjects that sets
 * all fields to null (so that unset fields do not appear in the resulting JSON).
 * This is not strictly necessary, but recommended to avoid unsafe operations.
 * See: http://stackoverflow.com/questions/18645050/is-default-no-args-constructor-mandatory-for-gson
 */
public class TargetRESTApi {
    /**
     * Response to a GET to the target/ route.
     */
    public static class GetAllTargetsResponse {
        private final List<TargetInfo> targets;

        protected GetAllTargetsResponse() {
            targets = null;
        }

        public GetAllTargetsResponse(final List<TargetInfo> targets) {
            this.targets = targets;
        }

        @ApiModelProperty(value = "Return the list of per-target information.")
        public List<TargetInfo> getTargets() {
            return ImmutableList.copyOf(targets);
        }

        @ApiModelProperty(hidden = true)
        public Map<Long, TargetInfo> targetsById() {
            return targets.stream()
                    .filter(element -> element.getSpec() != null)
                    .collect(Collectors.toMap(
                            TargetInfo::getTargetId,
                            Function.identity()));
        }
    }

    /**
     * Java representation of the JSON object to
     * send when retrieving target info.
     */
    public static class TargetInfo implements com.vmturbo.topology.processor.api.TargetInfo {
        /**
         * The OID of the target. Using Long instead of a primitive type
         * so that it's nullable.
         */
        private final Long targetId;
        private final String displayName;
        private final TargetSpec spec;
        private final Boolean probeConnected;
        private final List<String> errors;
        private final String status;
        private final LocalDateTime lastValidationTime;

        protected TargetInfo() {
            targetId = null;
            displayName = null;
            spec = null;
            probeConnected = null;
            errors = null;
            status = null;
            lastValidationTime = null;
        }

        public TargetInfo(final Long id, String displayName, final List<String> errors, final TargetSpec spec,
                        final Boolean probeConnected, String status, LocalDateTime lastValidationTime) {
            this.targetId = id;
            this.displayName = displayName;
            this.spec = spec;
            this.probeConnected = probeConnected;
            this.errors = errors;
            this.status = status;
            this.lastValidationTime = lastValidationTime;
        }

        @ApiModelProperty(value = "If non-null, the id of the target. If null, errors should be non-null.")
        public Long getTargetId() {
            return targetId;
        }

        @ApiModelProperty(value = "The display name of the target.")
        public String getDisplayName() {
            return displayName;
        }

        @ApiModelProperty(value = "If non-null, the spec for this target. If null, errors should be non-null.")
        public TargetSpec getSpec() {
            return spec;
        }

        @ApiModelProperty(value = "If non-null, whether the probe this target is attached to is connected. If null, errors should be non-null.")
        public boolean getProbeConnected() {
            return probeConnected;
        }

        @ApiModelProperty(value = "If non-empty, the error(s) encountered during the operation. If empty, id and spec should be non-null.")
        public List<String> getErrors() {
            return errors;
        }

        @Override
        public long getId() {
            return targetId;
        }

        @Override
        public long getProbeId() {
            return spec.getProbeId();
        }

        @Override
        public Set<AccountValue> getAccountData() {
            return spec.getAccountData();
        }

        @Override
        public LocalDateTime getLastValidationTime() {
            return lastValidationTime;
        }

        @Override
        public String getStatus() {
            return status;
        }

        @Override
        public boolean isHidden() {
            return spec.getIsHidden();
        }
        @Override
        public boolean isReadOnly() {
            return spec.getReadOnly();
        }

        @Override
        public List<Long> getDerivedTargetIds() {
            return spec.getDerivedTargetIds();
        }
    }

    /**
     * Java representation of the JSON object
     * to send when adding a target.
     *
     * <p>The specification for a target.
     * Provides all the necessary information to
     * create a target and perform operations.
     */
    public static class TargetSpec extends TargetInputFields {

        @ApiModelProperty(value = "Probe to which the target belongs.")
        private final Long probeId;
        @ApiModelProperty(value = "Is the target hidden from users.")
        private final boolean isHidden;
        @ApiModelProperty(value = "Whether the target cannot be changed through public APIs")
        private final boolean readOnly;
        @ApiModelProperty(value = "The derived target IDs associated with this target")
        private final List<Long> derivedTargetIds;

        protected TargetSpec() {
            probeId = null;
            isHidden = false;
            readOnly = false;
            derivedTargetIds = Lists.newArrayList();
        }

        public TargetSpec(@Nonnull final Long probeId,
                        @Nonnull final List<InputField> accountFields) {
            super(accountFields);
            this.probeId = Objects.requireNonNull(probeId);
            this.isHidden = false;
            this.readOnly = false;
            this.derivedTargetIds = Lists.newArrayList();
        }

        public TargetSpec(@Nonnull final TopologyProcessorDTO.TargetSpec targetSpec) {
            super(targetSpec.getAccountValueList().stream()
                            .map(accountValue -> new InputField(accountValue))
                            .collect(Collectors.toList()));
            this.probeId = targetSpec.getProbeId();
            this.isHidden = targetSpec.getIsHidden();
            this.readOnly = targetSpec.getReadOnly();
            this.derivedTargetIds = targetSpec.getDerivedTargetIdsList();
        }

        public Long getProbeId() {
            return probeId;
        }

        public boolean getIsHidden() {
            return isHidden;
        }

        public boolean getReadOnly() {
            return readOnly;
        }

        public List<Long> getDerivedTargetIds() {
            return derivedTargetIds;
        }

        /**
         * creates target spec DTO object.
         *
         * @return DTO representation of target spec
         * @throws TopologyProcessorException if required fields are not when parsed from JSON.
         */
        public TopologyProcessorDTO.TargetSpec toDto() throws TopologyProcessorException {
            if (probeId == null || getInputFields() == null) {
                throw new TopologyProcessorException("Missing JSON fields.");
            }

            return TopologyProcessorDTO.TargetSpec.newBuilder().setProbeId(probeId)
                            .addAllAccountValue(getInputFields().stream()
                                            .map(InputField::toAccountValue)
                                            .collect(Collectors.toList()))
                            .build();
        }

        public Map<String, InputField> getInputFieldsByName() {
            return getInputFields().stream().collect(Collectors
                            .toMap(inputField -> inputField.getName(), Function.identity()));
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("Target probe:").append(probeId);
            sb.append(super.toString());
            return sb.toString();
        }
    }
}
