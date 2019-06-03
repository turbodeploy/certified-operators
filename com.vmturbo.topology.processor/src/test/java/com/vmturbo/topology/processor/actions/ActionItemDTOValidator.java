package com.vmturbo.topology.processor.actions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.protobuf.Descriptors.FieldDescriptor;

import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Validates that the right fields (and only the right fields) are set
 * in the {@link ActionItemDTO} based on the action type.
 * <p>
 * (May 18, 2017)
 * Normally, and in the long term, this validation should not be done here. The probes
 * should send information to the Topology Processor w.r.t. the format they expect
 * {@link ActionItemDTO}s to arrive in, and the tests can then validate the DTO's the
 * {@link ActionExecutionRpcService} produces against that format.
 * <p>
 * HOWEVER, since there is currently no such feature in the SDK, we use this class to validate
 * DTO's so that the logic for what's "right" lives in one place.
 */
public class ActionItemDTOValidator {

    private ActionItemDTOValidator() { }

    public static void validateRequest(@Nonnull final List<ActionItemDTO> actionItems) throws ValidationError {
        for (ActionItemDTO actionItem : actionItems) {
            validateRequest(actionItem);
        }
    }

    public static void validateRequest(@Nonnull final ActionItemDTO actionItem)
            throws ValidationError {
        switch (actionItem.getActionType()) {
            case MOVE:
            case CHANGE:
                validateMove(actionItem);
                break;
            case RIGHT_SIZE:
            case RESIZE:
                validateResize(actionItem);
                break;
            case START:
            case ADD_PROVIDER:
                validateStart(actionItem);
                break;
            case SUSPEND:
            case DELETE:
                validateSuspend(actionItem);
                break;
            default:
                throw new ValidationError(Collections.singleton(
                        "Unsupported action type: " + actionItem.getActionType()));
        }
    }

    private static void validateStart(@Nonnull final ActionItemDTO start)
            throws ValidationError {
        final ValidationErrors errors = new ValidationErrors();

        if (start.getTargetSE().getEntityType().equals(EntityType.VIRTUAL_MACHINE)) {
            errors.exactOptionalFields(start, "hostedBySE");
        }

        errors.throwIfError();
    }

    private static void validateSuspend(@Nonnull final ActionItemDTO suspend)
            throws ValidationError {
        final ValidationErrors errors = new ValidationErrors();

        if (suspend.getTargetSE().getEntityType().equals(EntityType.VIRTUAL_MACHINE)) {
            errors.exactOptionalFields(suspend, "hostedBySE");
        }

        errors.throwIfError();
    }

    private static void validateResize(@Nonnull final ActionItemDTO resizeItem)
            throws ValidationError {
        final ValidationErrors errors = new ValidationErrors();

        if (resizeItem.getTargetSE().getEntityType() == EntityType.VIRTUAL_MACHINE) {
            errors.exactOptionalFields(resizeItem,
                "currentComm", "newComm", "commodityAttribute", "hostedBySE");
        } else {
            errors.exactOptionalFields(resizeItem,
                "currentComm", "newComm", "commodityAttribute");
        }

        // TODO (roman, May 16 2017): Should validate that the (entity type, commodity type) tuple
        // makes sense - e.g. doesn't make sense to resize vCPU for physical machines, etc.

        errors.throwIfError();
    }

    private static void validateMove(@Nonnull final ActionItemDTO moveItem)
            throws ValidationError {
        final ValidationErrors errors = new ValidationErrors();
        if (moveItem.getTargetSE().getEntityType() == EntityType.VIRTUAL_MACHINE) {
            errors.exactOptionalFields(moveItem, "currentSE", "newSE", "hostedBySE");
        } else {
            errors.exactOptionalFields(moveItem, "currentSE", "newSE");
        }

        // CHANGE is a storage move.
        if (moveItem.getActionType().equals(ActionType.CHANGE)) {
            errors.checkEntitiesType(EntityType.STORAGE, moveItem.getCurrentSE(), moveItem.getNewSE());
        } else if (moveItem.getActionType().equals(ActionType.MOVE)) {
            if (moveItem.getTargetSE().getEntityType().equals(EntityType.VIRTUAL_MACHINE)) {
                errors.checkEntitiesType(EntityType.PHYSICAL_MACHINE,
                    moveItem.getCurrentSE(), moveItem.getNewSE());
            } else if (moveItem.getTargetSE().getEntityType().equals(EntityType.STORAGE)) {
                errors.checkEntitiesType(EntityType.DISK_ARRAY,
                    moveItem.getCurrentSE(), moveItem.getNewSE());
            } else {
                errors.addError("Unable to handle MOVE targetSE type: "
                    + moveItem.getTargetSE().getEntityType());
            }
        }
        errors.throwIfError();
    }

    /**
     * Helper class to aggregate all errors encountered during validation.
     */
    public static class ValidationErrors {
        private Set<String> errors = new HashSet<>();

        public void throwIfError() throws ValidationError {
            if (!errors.isEmpty()) {
                throw new ValidationError(errors);
            }
        }

        private void checkEntitiesType(@Nonnull final EntityType expectedType,
                                       @Nonnull final EntityDTO... entities) {
            Arrays.stream(entities)
                    .filter(entity -> !entity.getEntityType().equals(expectedType))
                    .map(entity -> "Entity " + entity.getId() + " has unexpected type: " +
                            entity.getEntityType() + ". Expected: " + expectedType)
                    .forEach(errors::add);
        }

        public void exactOptionalFields(@Nonnull final ActionItemDTO action,
                                        String... expectedOptionalFields) {
            final Set<String> expected = new HashSet<>();
            Collections.addAll(expected, expectedOptionalFields);

            final Set<String> setOptionalFields = action.getAllFields().keySet().stream()
                    .filter(descriptor -> !descriptor.isRequired())
                    .map(FieldDescriptor::getName)
                    .collect(Collectors.toSet());

            if (!setOptionalFields.containsAll(expected)) {
                final Set<String> expectedCopy = new HashSet<>(expected);
                expectedCopy.removeAll(setOptionalFields);
                expectedCopy.forEach(fieldName -> errors.add(fieldName + " not set."));
            }

            setOptionalFields.removeAll(expected);
            setOptionalFields.forEach(fieldName -> errors.add(fieldName + " set, but not expected."));
        }

        public void addError(@Nonnull final String errorMessage) {
            errors.add(errorMessage);
        }
    }

    /**
     * Error validating {@link ActionItemDTO}.
     */
    public static class ValidationError extends Exception {

        public ValidationError(@Nonnull final Set<String> errors) {
            super("Errors encountered: " + String.join(", ", errors));
        }
    }
}
