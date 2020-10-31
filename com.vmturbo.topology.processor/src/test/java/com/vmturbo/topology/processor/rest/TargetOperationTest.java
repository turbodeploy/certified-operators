package com.vmturbo.topology.processor.rest;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;

public class TargetOperationTest {

    /**
     * Testing the mapping between Target creation mode to its invalid operations.
     */
    public static final Map<TargetOperation, Set<CreationMode>> testTargetOperationToCreationMode =
            Arrays.stream(TargetOperation.values()).collect(Collectors.toMap(
                    Function.identity(), TargetOperation::getInvalidCreationModes));

    /**
     * Validating that the real creationModeToRestrictedTargetOperations structure.
     */
    @Test
    public void invalidTargetOperationsTest() {
        Set<TargetOperation> allTargetOperations = Sets.newHashSet(TargetOperation.ADD, TargetOperation.DISCOVER,
            TargetOperation.VALIDATE, TargetOperation.REMOVE, TargetOperation.UPDATE);
        allTargetOperations.stream().forEach(targetOperation ->
            Assert.assertTrue(targetOperation.isValidTargetOperation(CreationMode.STAND_ALONE) ==
                    !testTargetOperationToCreationMode.get(targetOperation).contains(CreationMode.STAND_ALONE))
        );
        allTargetOperations.stream().forEach(targetOperation ->
            Assert.assertTrue(targetOperation.isValidTargetOperation(CreationMode.DERIVED) ==
                !testTargetOperationToCreationMode.get(targetOperation).contains(CreationMode.DERIVED))
        );
        allTargetOperations.stream().forEach(targetOperation ->
            Assert.assertTrue(targetOperation.isValidTargetOperation(CreationMode.ANY) ==
                !testTargetOperationToCreationMode.get(targetOperation).contains(CreationMode.ANY))
        );
        allTargetOperations.stream().forEach(targetOperation ->
            Assert.assertTrue(targetOperation.isValidTargetOperation(CreationMode.OTHER) ==
                !testTargetOperationToCreationMode.get(targetOperation).contains(CreationMode.OTHER))
        );
    }
}
