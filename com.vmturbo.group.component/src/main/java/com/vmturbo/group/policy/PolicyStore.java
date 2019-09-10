package com.vmturbo.group.policy;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.gson.reflect.TypeToken;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredPolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutablePolicyUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.PolicyNotFoundException;
import com.vmturbo.group.common.TargetCollectionUpdate.TargetPolicyUpdate;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.tables.pojos.Policy;
import com.vmturbo.group.db.tables.pojos.PolicyGroup;
import com.vmturbo.group.db.tables.records.PolicyRecord;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.proactivesupport.DataMetricCounter;

/**
 * The {@link PolicyStore} class is used for CRUD operations on policies, to abstract away the
 * persistence details from the rest of the component.
 */
public class PolicyStore implements Diagnosable {

    private static final String GET_LABEL = "get";

    private static final String CREATE_LABEL = "create";

    private static final String UPDATE_LABEL = "update";

    private static final String DELETE_LABEL = "delete";

    private static final DataMetricCounter POLICY_STORE_ERROR_COUNT = DataMetricCounter.builder()
            .withName("policy_store_error_count")
            .withHelp("Number of errors encountered in operating the policy store.")
            .withLabelNames("operation")
            .build()
            .register();

    private static final DataMetricCounter POLICY_STORE_DUPLICATE_NAME_COUNT = DataMetricCounter.builder()
            .withName("policy_store_duplicate_name_count")
            .withHelp("Number of duplicate name attempts in operating the policy store.")
            .build()
            .register();

    private final static Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;

    private final IdentityProvider identityProvider;

    private final DiscoveredPoliciesMapperFactory discoveredPoliciesMapperFactory;

    public PolicyStore(@Nonnull final DSLContext dslContext,
                       @Nonnull final DiscoveredPoliciesMapperFactory mapperFactory,
                       @Nonnull final IdentityProvider identityProvider) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.discoveredPoliciesMapperFactory = Objects.requireNonNull(mapperFactory);
    }

    /**
     * Update the set of policies discovered by a particular target.
     * The new set of policies will completely replace the old, even if the new set is empty.
     *
     * <p>See {@link TargetPolicyUpdate} for details on the update behavior.
     *
     * @param context The context to use to do the updates.
     * @param targetId The ID of the target that discovered the policies.
     * @param policyInfos The new set of {@link DiscoveredPolicyInfo}s.
     * @param groupOids A mapping from group display names to group OIDs. We need this mapping
     *                  because discovered policies reference groups by display name.
     * @throws DataAccessException If there is an error interacting with the database.
     */
    public void updateTargetPolicies(@Nonnull final DSLContext context,
                 final long targetId,
                 @Nonnull final List<DiscoveredPolicyInfo> policyInfos,
                 @Nonnull final Map<String, Long> groupOids) throws DataAccessException {
        logger.info("Updating policies discovered by {}. Got {} policies.",
            targetId, policyInfos.size());

        final DiscoveredPoliciesMapper mapper = discoveredPoliciesMapperFactory.newMapper(groupOids);
        final List<PolicyInfo> discoveredPolicies = policyInfos.stream()
                        .map(mapper::inputPolicy)
                        .filter(Optional::isPresent).map(Optional::get)
                        .collect(Collectors.toList());
        final TargetPolicyUpdate update = new TargetPolicyUpdate(targetId, identityProvider,
                discoveredPolicies, getDiscoveredByTarget(context, targetId));
        update.apply((policy) -> internalCreate(context, policy),
                (policy) -> internalUpdate(context, policy),
                (policy) -> deleteDiscoveredPolicy(context, policy, true));
        logger.info("Finished updating discovered groups.");
    }

    /**
     * Get the {@link PolicyDTO.Policy} object associated with an ID.
     *
     * @param id The ID.
     * @return An {@link Optional} containing the policy, or an empty optional if the
     *         ID is not found.
     */
    @Nonnull
    public Optional<PolicyDTO.Policy> get(final long id) {
        try {
            return internalGet(dslContext, id);
        } catch (Exception e) {
            POLICY_STORE_ERROR_COUNT.labels(GET_LABEL).increment();
            throw e;
        }
    }

    /**
     * Get all {@link PolicyDTO.Policy} objects in the store.
     *
     * @return A collection of {@link PolicyDTO.Policy}.
     */
    @Nonnull
    public Collection<PolicyDTO.Policy> getAll() {
        try {
            return dslContext.selectFrom(Tables.POLICY)
                    .fetch()
                    .into(Policy.class)
                    .stream()
                    .map(this::toPolicyProto)
                    .collect(Collectors.toList());
        } catch (DataAccessException e) {
            POLICY_STORE_ERROR_COUNT.labels(GET_LABEL).increment();
            throw e;
        }
    }

    /**
     * Create a new user policy.
     *
     * @param policyInfo The customizable properties of the policy.
     * @return The {@link PolicyDTO.Policy} describing the newly created policy.
     * @throws DuplicateNameException If a policy with the same name already exists.
     */
    @Nonnull
    public PolicyDTO.Policy newUserPolicy(@Nonnull final PolicyInfo policyInfo)
            throws DuplicateNameException {
        final long id = identityProvider.next();
        try {
            return dslContext.transactionResult(configuration ->
                internalCreate(DSL.using(configuration),
                    PolicyDTO.Policy.newBuilder()
                            .setId(id)
                            .setPolicyInfo(policyInfo)
                            .build()));
        } catch (DataAccessException e) {
            POLICY_STORE_ERROR_COUNT.labels(CREATE_LABEL).increment();
            if (e.getCause() instanceof DuplicateNameException) {
                throw (DuplicateNameException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Edit an existing policy.
     *
     * Right now both discovered and user-created policies can be edited freely.
     * TODO (roman, June 2018): We should only allow enabling/disabling discovered policies
     * instead of actually editing them. Any other edits will be written over in the next
     * broadcast anyway.
     *
     * @param id The id of the policy to edit.
     * @param policyInfo The {@link PolicyDTO.PolicyInfo} describing the updated
     *                   customizable properties of the policy.
     * @return The new {@link PolicyDTO.Policy} describing the newly edited policies.
     * @throws PolicyNotFoundException If the policy described by the ID doesn't exist.
     * @throws DuplicateNameException If a policy with the same name already exists.
     */
    @Nonnull
    public PolicyDTO.Policy editPolicy(final long id,
                                       @Nonnull final PolicyInfo policyInfo)
            throws PolicyNotFoundException, DuplicateNameException {
        try {
            final PolicyDTO.Policy existingPolicy =
                    get(id).orElseThrow(() -> new PolicyNotFoundException(id));
            final PolicyDTO.Policy updatedPolicy = existingPolicy.toBuilder()
                    .setPolicyInfo(policyInfo)
                    .build();
            return internalUpdate(dslContext, updatedPolicy);
        } catch (Exception e) {
            POLICY_STORE_ERROR_COUNT.labels(UPDATE_LABEL).increment();
            throw e;
        }
    }

    /**
     * Delete the policies associated with a particular group.
     *
     * @param context The transaction context to use to do the deletion. We need this so that
     *                the policy deletions happen within the same transaction as the original
     *                group deletion.
     * @param groupId The ID of the group being deleted.
     * @throws PolicyDeleteException If there is an error deleting the policies.
     */
    public void deletePoliciesForGroup(@Nonnull final DSLContext context,
                                       final long groupId) throws PolicyDeleteException {
        try {
            // Delete the placement policies associated with the group.
            final Set<Long> associatedPolicies = new HashSet<>(
                    context.select(Tables.POLICY_GROUP.POLICY_ID)
                            .from(Tables.POLICY_GROUP)
                            .where(Tables.POLICY_GROUP.GROUP_ID.eq(groupId))
                            .fetch()
                            .getValues(Tables.POLICY_GROUP.POLICY_ID));
            for (final Long policyId : associatedPolicies) {
                try {
                    internalDelete(context, policyId, false);
                } catch (ImmutablePolicyUpdateException e) {
                    logger.error("Policy {} could not be deleted because it's immutable", policyId);
                    throw new PolicyDeleteException(policyId, groupId, e);
                } catch (PolicyNotFoundException e) {
                    // In this case we want to continue deleting associated policies - this isn't
                    // a fatal error.
                    logger.warn("Policy {} not found even though it was associated with group {}!",
                            policyId, groupId);
                } catch (DataAccessException e) {
                    logger.error("Related policy " + policyId + " could not be deleted.", e);
                    throw new PolicyDeleteException(policyId, groupId, e);
                }
            }
        } catch (Exception e) {
            POLICY_STORE_ERROR_COUNT.labels(DELETE_LABEL).increment();
            throw e;
        }
    }

    /**
     * Delete a policy created by {@link PolicyStore#newUserPolicy(PolicyInfo)}.
     *
     * Only user policies can be deleted - attempting to delete discovered policies will throw
     * an {@link ImmutablePolicyUpdateException}.
     *
     * @param id The ID of the policy to delete.
     * @return The {@link PolicyDTO.Policy} describing the deleted policy.
     * @throws ImmutablePolicyUpdateException If the ID describes a discovered policy.
     * @throws PolicyNotFoundException If no policy with that ID exists.
     */
    @Nonnull
    public PolicyDTO.Policy deleteUserPolicy(final long id)
            throws ImmutablePolicyUpdateException, PolicyNotFoundException {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext transactionDsl = DSL.using(configuration);
                return internalDelete(transactionDsl, id, false);
            });
        } catch (DataAccessException e) {
            POLICY_STORE_ERROR_COUNT.labels(DELETE_LABEL).increment();
            if (e.getCause() instanceof ImmutablePolicyUpdateException) {
                throw (ImmutablePolicyUpdateException)e.getCause();
            } else if (e.getCause() instanceof PolicyNotFoundException) {
                throw (PolicyNotFoundException)e.getCause();
            } else {
                throw e;
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<String> collectDiags() throws DiagnosticsException {
        final Collection<PolicyDTO.Policy> policies = getAll();
        logger.info("Collected diags for {} policies.", policies.size());

        return Collections.singletonList(ComponentGsonFactory
            .createGsonNoPrettyPrint().toJson(policies));
    }

    /**
     * {@inheritDoc}
     * Restore policies to the {@link PolicyStore} from the collected diags.
     *
     * @param collectedDiags The diags collected from a previous call to
     *      {@link Diagnosable#collectDiags()}. Must be in the same order.
     * @throws DiagnosticsException If there is a problem writing the policies
     *         to the store.
     */
    @Override
    public void restoreDiags(@Nonnull List<String> collectedDiags) throws DiagnosticsException {
        // Replace all existing groups with the ones in the collected diags.
        Collection<PolicyDTO.Policy> policies = ComponentGsonFactory.createGsonNoPrettyPrint()
            .fromJson(collectedDiags.get(0), new TypeToken<Collection<PolicyDTO.Policy>>(){}.getType());
        logger.info("Attempting to restore {} policies from diagnostics.", policies.size());

        try {
            dslContext.transaction(configuration -> {
                final DSLContext transactionContext = DSL.using(configuration);
                final int rowsAffected = transactionContext.deleteFrom(Tables.POLICY).execute();
                logger.info("Deleted {} existing policies.", rowsAffected);
                for (final PolicyDTO.Policy policy : policies) {
                    try {
                        internalCreate(transactionContext, policy);
                    } catch (DataAccessException e) {
                        // Log the exception, but continue attempting to restore other policies.
                        logger.error("Failed to restore policy " + policy.getPolicyInfo().getName()
                                + "!", e);
                    }
                }
            });
            logger.info("Finished restoring policies from diagnostics.");
        } catch (DataAccessException e) {
            throw new DiagnosticsException(e);
        }
    }

    @Nonnull
    private Collection<PolicyDTO.Policy> getDiscoveredByTarget(@Nonnull final DSLContext context,
                                                               final long targetId) {
        return context.selectFrom(Tables.POLICY)
                .where(Tables.POLICY.DISCOVERED_BY_ID.eq(targetId))
                .fetch()
                .into(Policy.class)
                .stream()
                .map(this::toPolicyProto)
                .collect(Collectors.toList());
    }

    private Optional<PolicyDTO.Policy> internalGet(@Nonnull final DSLContext context,
                                                   final long id) {
        return context.selectFrom(Tables.POLICY)
                .where(Tables.POLICY.ID.eq(id))
                .fetch()
                .into(Policy.class)
                .stream()
                .findFirst()
                .map(this::toPolicyProto);
    }

    /**
     * Store a policy in the underlying database.
     * This method should be executed inside a transaction.
     *
     * @param context The transaction context. This should NOT be the root DSLContext.
     * @param policyProto The protobuf representation of the policy.
     * @return The protobuf representation of the policy.
     * @throws DataAccessException If there is an issue connecting to the database.
     * @throws DuplicateNameException If a policy with the same name already exists in the database.
     */
    @Nonnull
    private PolicyDTO.Policy internalCreate(@Nonnull final DSLContext context,
                                            @Nonnull final PolicyDTO.Policy policyProto)
            throws DataAccessException, DuplicateNameException {
        checkForDuplicates(context, policyProto.getId(), policyProto.getPolicyInfo().getName());

        final Policy policy = new Policy(policyProto.getId(),
                policyProto.getPolicyInfo().getName(),
                policyProto.getPolicyInfo().getEnabled(),
                policyProto.hasTargetId() ? policyProto.getTargetId() : null,
                policyProto.getPolicyInfo());

        final int returnCode = context.newRecord(Tables.POLICY, policy).store();
        if (returnCode == 0) {
            // This should never happen, because we're creating a new record, and store() should
            // always execute.
            throw new IllegalStateException("Failed to insert record.");
        }

        // Create the associations between the policy and the groups it relates to.
        insertReferencedGroups(context, policy.getId(), GroupProtoUtil.getPolicyGroupIds(policyProto));

        return toPolicyProto(policy);
    }

    @Nonnull
    private PolicyDTO.Policy internalUpdate(@Nonnull final DSLContext context,
                                            @Nonnull final PolicyDTO.Policy newPolicyProto)
            throws PolicyNotFoundException, DataAccessException, DuplicateNameException {
        final PolicyRecord existingRecord =
                context.fetchOne(Tables.POLICY, Tables.POLICY.ID.eq(newPolicyProto.getId()));
        if (existingRecord == null) {
            throw new PolicyNotFoundException(newPolicyProto.getId());
        }

        final PolicyDTO.Policy existingPolicyProto = toPolicyProto(existingRecord.into(Policy.class));

        checkForDuplicates(context, newPolicyProto.getId(), newPolicyProto.getPolicyInfo().getName());

        existingRecord.setName(newPolicyProto.getPolicyInfo().getName());
        existingRecord.setEnabled(newPolicyProto.getPolicyInfo().getEnabled());
        if (newPolicyProto.hasTargetId()) {
            existingRecord.setDiscoveredById(newPolicyProto.getTargetId());
        }
        existingRecord.setPolicyData(newPolicyProto.getPolicyInfo());

        final int modifiedRecords = existingRecord.update();
        if (modifiedRecords == 0) {
            // This should never happen, because we overwrote fields in the record,
            // and update() should always execute an UPDATE statement if some fields
            // got overwritten.
            throw new IllegalStateException("Failed to update record.");
        }

        final Set<Long> curReferencedGroups = GroupProtoUtil.getPolicyGroupIds(existingPolicyProto);
        final Set<Long> newReferencedGroups = GroupProtoUtil.getPolicyGroupIds(newPolicyProto);
        if (!newReferencedGroups.equals(curReferencedGroups)) {
            // Re-create the associations between the policy and the groups it relates to.
            final int deletedRows = context.deleteFrom(Tables.POLICY_GROUP)
                    .where(Tables.POLICY_GROUP.POLICY_ID.eq(newPolicyProto.getId()))
                    .execute();
            logger.info("Deleted {} existing policy-group associations. Re-creating associations...", deletedRows);

            insertReferencedGroups(context, newPolicyProto.getId(), newReferencedGroups);
        }

        return toPolicyProto(existingRecord.into(Policy.class));
    }

    private void insertReferencedGroups(@Nonnull final DSLContext context,
                                        final long policyId,
                                        @Nonnull final Set<Long> referencedGroups) {
        referencedGroups.forEach(groupId -> {
            final PolicyGroup policyGroup = new PolicyGroup();
            policyGroup.setGroupId(groupId);
            policyGroup.setPolicyId(policyId);
            final int retCode = context.newRecord(Tables.POLICY_GROUP, policyGroup).store();
            if (retCode == 0) {
                // This should never happen, because we're creating a new record, and store() should
                // always execute.
                // However, don't throw an exception so we can continue to insert the other records.
                logger.error("Failed to insert policy-group record for group {} and policy {}!",
                        groupId, policyId);
            }
        });
    }

    @Nonnull
    private PolicyDTO.Policy deleteDiscoveredPolicy(@Nonnull final DSLContext context,
                                            final PolicyDTO.Policy policy,
                                            final boolean allowDiscoveredPolicyDelete)
            throws PolicyNotFoundException, ImmutablePolicyUpdateException {
        return internalDelete(context, policy.getId(), allowDiscoveredPolicyDelete);
    }

    @Nonnull
    private PolicyDTO.Policy internalDelete(@Nonnull final DSLContext context,
                                            final long id,
                                            final boolean allowDiscoveredPolicyDelete)
            throws PolicyNotFoundException, ImmutablePolicyUpdateException {
        final PolicyRecord existingRecord =
                context.fetchOne(Tables.POLICY, Tables.POLICY.ID.eq(id));
        if (existingRecord == null) {
            throw new PolicyNotFoundException(id);
        }
        if (existingRecord.getDiscoveredById() != null && !allowDiscoveredPolicyDelete) {
            throw new ImmutablePolicyUpdateException(existingRecord.getName());
        }
        // The entry from the POLICY_GROUP table should be deleted automatically
        // because of the foreign key constraint.
        existingRecord.delete();

        return toPolicyProto(existingRecord.into(Policy.class));
    }

    /**
     * Check if there is a duplicate group.
     * A duplicate group has the same name as the groupInfo but a different id.
     *
     * @param context The {@link DSLContext} for the current transaction.
     * @param id The id of the new group.
     * @param name The name for the new group.
     * @throws DuplicateNameException If there is a group with the same name but a different ID.
     */
    private void checkForDuplicates(@Nonnull final DSLContext context,
                            final long id,
                            @Nonnull final String name)
            throws DuplicateNameException {
        final List<Long> sameNameDiffId = context.select(Tables.POLICY.ID)
                .from(Tables.POLICY)
                .where(Tables.POLICY.NAME.eq(name))
                .and(Tables.POLICY.ID.ne(id))
                .fetch()
                .getValues(Tables.POLICY.ID);
        if (!sameNameDiffId.isEmpty()) {
            if (sameNameDiffId.size() > 1) {
                // This shouldn't happen, because there is a constraint on the name.
                logger.error("Multiple policies ({}) exist with the name {}. " +
                                "This should never happen because the name column is unique.",
                        sameNameDiffId, name);
            }
            // TODO - add metric here
            throw new DuplicateNameException(sameNameDiffId.get(0), name);
        }
    }

    @Nonnull
    private PolicyDTO.Policy toPolicyProto(@Nonnull final Policy policy) {
        final PolicyDTO.Policy.Builder policyBuilder = PolicyDTO.Policy.newBuilder()
                .setId(policy.getId())
                .setPolicyInfo(policy.getPolicyData());
        if (policy.getDiscoveredById() != null) {
            policyBuilder.setTargetId(policy.getDiscoveredById());
        }

        // Overwrite the saved data with the column values, even though they should be the same.
        policyBuilder.getPolicyInfoBuilder().setName(policy.getName());
        policyBuilder.getPolicyInfoBuilder().setEnabled(policy.getEnabled());

        return policyBuilder.build();
    }

    /**
     * A custom exception for policy delete failure.
     */
    public static class PolicyDeleteException extends Exception {
        public PolicyDeleteException(List<Long> ids) {
            super("Failed to delete policies " + ids);
        }

        public PolicyDeleteException(final long id, final long groupId, final Throwable cause) {
            super("Could not delete policy " + id + " associated with group " + groupId, cause);
        }
    }

}
