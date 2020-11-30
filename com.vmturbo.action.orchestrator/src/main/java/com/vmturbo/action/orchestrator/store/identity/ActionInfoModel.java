package com.vmturbo.action.orchestrator.store.identity;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.xml.bind.DatatypeConverter;

import org.apache.commons.codec.digest.DigestUtils;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;

/**
 * Identity model for {@link com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo}.
 * This class is used in {@link IdentityServiceImpl} in order to determine that the market
 * recommendations are logically the same. If two models are equal, it means that they reflect
 * two actions that are logically the same.
 */
@Immutable
public class ActionInfoModel {
    private final ActionTypeCase actionType;
    private final long targetId;
    private final String details;
    private final Set<String> additionalDetails;
    private final byte[] actionHash;
    private final String actionHashHex;

    /**
     * Constructs action info model from the parts.
     *
     * @param actionType action type
     * @param targetId action target entity OID
     * @param details action details (varied by action type).
     * @param additionalDetails additional details, to be used as a set. If this parameter
     *         is {@code null} this means that there is no additional details expected for this
     *         action type. If value is an empty set this means that additional details could
     *         occur for such an actions but this specific action does not have any.
     */
    public ActionInfoModel(@Nonnull ActionTypeCase actionType, long targetId,
            @Nullable String details, @Nullable Set<String> additionalDetails) {
        this.actionType = Objects.requireNonNull(actionType);
        this.targetId = targetId;
        this.details = details;
        this.additionalDetails = additionalDetails;
        actionHash = createHash();
        actionHashHex = DatatypeConverter.printHexBinary(actionHash);
    }

    /**
     * Creates SHA1 hash based on the properties of this action.
     *
     * @return the SHA1 hash.
     */
    private byte[] createHash() {
        // If you change the implementation of this method, you should also migrate the hashes in
        // the DB, otherwise the oid of action will change.
        StringBuilder sb = new StringBuilder();
        sb.append(getActionType().getNumber());
        sb.append(" ");
        sb.append(getTargetId());
        sb.append(" ");
        sb.append(getDetails().orElse(""));
        sb.append(" ");
        sb.append(
            getAdditionalDetails().map(s -> s.stream()
                .sorted()
                .collect(Collectors.joining(" ")))
                .orElse(""));

        return DigestUtils.sha1(sb.toString());
    }

    @Nonnull
    public ActionTypeCase getActionType() {
        return actionType;
    }

    public long getTargetId() {
        return targetId;
    }

    @Nonnull
    public Optional<String> getDetails() {
        return Optional.ofNullable(details);
    }

    @Nonnull
    public Optional<Set<String>> getAdditionalDetails() {
        return Optional.ofNullable(additionalDetails);
    }

    /**
     * Gets the SHA1 hash of this object.
     *
     * @return the SHA1 hash of this object.
     */
    @Nonnull
    public byte[] getActionHash() {
        return actionHash;
    }

    /**
     * Returns the hex representation of action hash.
     *
     * @return the hex representation of action hash.
     */
    @Nonnull
    public String getActionHexHash() {
        return actionHashHex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionInfoModel)) {
            return false;
        }
        ActionInfoModel that = (ActionInfoModel)o;
        return Objects.equals(actionHashHex, that.actionHashHex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actionHashHex);
    }

    @Override
    public String toString() {
        return actionType + ":" + targetId;
    }
}
