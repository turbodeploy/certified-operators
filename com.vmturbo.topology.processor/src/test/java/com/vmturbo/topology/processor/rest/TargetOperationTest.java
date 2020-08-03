package com.vmturbo.topology.processor.rest;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;

public class TargetOperationTest {

    /**
     * Testing the mapping between Target creation mode to its invalid operations.
     */
    public static final Map<TargetOperation, Set<CreationMode>> testTargetOperationToCreationMode =
        new ImmutableMap.Builder<TargetOperation, Set<CreationMode>>()
            .put(TargetOperation.ADD, Sets.newHashSet(CreationMode.DERIVED, CreationMode.OTHER))
            .put(TargetOperation.DISCOVER, Collections.emptySet())
            .put(TargetOperation.VALIDATE, Collections.emptySet())
            .put(TargetOperation.REMOVE, Sets.newHashSet(CreationMode.DERIVED, CreationMode.OTHER))
            .put(TargetOperation.UPDATE, Sets.newHashSet(CreationMode.DERIVED, CreationMode.OTHER))
            .build();

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
