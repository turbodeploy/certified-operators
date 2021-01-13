package com.vmturbo.group.policy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Insert;
import org.jooq.InsertOnDuplicateStep;
import org.jooq.Param;
import org.jooq.Record;
import org.jooq.Record4;
import org.jooq.TableRecord;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.group.DiscoveredObjectVersionIdentity;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutablePolicyUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.PolicyNotFoundException;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.tables.pojos.Policy;
import com.vmturbo.group.db.tables.records.PolicyGroupRecord;
import com.vmturbo.group.db.tables.records.PolicyRecord;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.proactivesupport.DataMetricCounter;

/**
 * The {@link PolicyStore} class is used for CRUD operations on policies, to abstract away the
 * persistence details from the rest of the component.
 */
public class PolicyStore implements DiagsRestorable<DSLContext>, IPlacementPolicyStore {

    /**
     * The file name for the policies dump collected from the {@link PolicyStore}.
     * It's a string file, so the "diags" extension is required for compatibility
     * with {@link DiagsZipReader}.
     */
    private static final String POLICIES_DUMP_FILE = "policies_dump";
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

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;

    private final IdentityProvider identityProvider;

    private final PolicyValidator policyValidator;

    /**
     * Constructs placement policy store.
     *
     * @param dslContext DB connection context to use
     * @param identityProvider identity provider to use
     * @param policyValidator policy validator
     */
    public PolicyStore(@Nonnull final DSLContext dslContext,
                       @Nonnull final IdentityProvider identityProvider,
                       @Nonnull final PolicyValidator policyValidator) {
        // TODO get rid of identity provider and policy validator here. Store should only store
        // the data (operate with the DB)
        this.dslContext = Objects.requireNonNull(dslContext);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.policyValidator = Objects.requireNonNull(policyValidator);
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
            List<PolicyDTO.Policy> policies = internalGet(dslContext, ImmutableSet.of(id));
            if (!policies.isEmpty()) {
                return Optional.of(policies.get(0));
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            POLICY_STORE_ERROR_COUNT.labels(GET_LABEL).increment();
            throw e;
        }
    }

    /**
     * Get the {@link PolicyDTO.Policy} objects associated with IDs.
     *
     * @param ids The policy ids.
     * @return An {@link Optional} containing the policy, or an empty optional if the
     *         ID is not found.
     */
    @Nonnull
    public List<PolicyDTO.Policy> getPolicies(final Collection<Long> ids) {
        try {
            return internalGet(dslContext, ids);
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
     * @throws InvalidPolicyException If the policy is invalid.
     */
    @Nonnull
    public PolicyDTO.Policy newUserPolicy(@Nonnull final PolicyInfo policyInfo)
        throws DuplicateNameException, InvalidPolicyException {
        final long id = identityProvider.next();
        try {

            return dslContext.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                final PolicyDTO.Policy policyProto = PolicyDTO.Policy.newBuilder()
                        .setId(id)
                        .setPolicyInfo(policyInfo)
                        .build();
                policyValidator.validatePolicy(context, policyProto);
                return internalCreate(context, policyProto, false);
            });
        } catch (DataAccessException e) {
            POLICY_STORE_ERROR_COUNT.labels(CREATE_LABEL).increment();
            if (e.getCause() instanceof DuplicateNameException) {
                throw e.getCause(DuplicateNameException.class);
            } else if (e.getCause() instanceof InvalidPolicyException) {
                throw e.getCause(InvalidPolicyException.class);
            } else {
                throw e;
            }
        }
    }

    /**
     * Edit an existing policy.
     *
     * <p>Right now both discovered and user-created policies can be edited freely.
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
     * @throws InvalidPolicyException If the new policy info is not valid.
     */
    @Nonnull
    public PolicyDTO.Policy editPolicy(final long id,
                                       @Nonnull final PolicyInfo policyInfo)
            throws PolicyNotFoundException, DuplicateNameException, InvalidPolicyException {
        try {
            final PolicyDTO.Policy existingPolicy =
                    get(id).orElseThrow(() -> new PolicyNotFoundException(id));
            final PolicyDTO.Policy updatedPolicy = existingPolicy.toBuilder()
                    .setPolicyInfo(policyInfo)
                    .build();
            policyValidator.validatePolicy(dslContext, updatedPolicy);
            return internalUpdate(dslContext, updatedPolicy);
        } catch (Exception e) {
            POLICY_STORE_ERROR_COUNT.labels(UPDATE_LABEL).increment();
            throw e;
        }
    }

    /**
     * This method will attempt to remove all placement policies that are associated with a groups
     * being deleted. The main problem is that there is no direct foreign key from policy table
     * to grouping table. Instead an intermediate table policy_group is used. This method is
     * using the table (policy_group) to remove all the placement policies associated with the
     * group being deleted.
     *
     * @param groupIds ids of the group deleted.
     */
    @Override
    public void deletePoliciesForGroupBeingRemoved(final Collection<Long> groupIds) {
        try {
            final String userPolicies = dslContext.select(Tables.POLICY.ID, Tables.POLICY.NAME)
                    .from(Tables.POLICY)
                    .join(Tables.POLICY_GROUP)
                    .on(Tables.POLICY.ID.eq(Tables.POLICY_GROUP.POLICY_ID))
                    .where(Tables.POLICY.DISCOVERED_BY_ID.isNull())
                    .and(Tables.POLICY_GROUP.GROUP_ID.in(groupIds))
                    .fetch()
                    .stream()
                    .map(record -> record.value1() + "(" + record.value2() + ")")
                    .collect(Collectors.joining(","));
            if (!userPolicies.isEmpty()) {
                logger.info(
                        "Removing the following user-defined placement policies because of removal"
                                + " of groups {} they are create on top of: [{}]", groupIds,
                        userPolicies);
            }
            final int rowsDeleted = dslContext.deleteFrom(Tables.POLICY)
                    .where(Tables.POLICY.ID.in(dslContext.select(Tables.POLICY_GROUP.POLICY_ID)
                            .from(Tables.POLICY_GROUP)
                            .where(Tables.POLICY_GROUP.GROUP_ID.in(groupIds))))
                    .execute();
            logger.debug("Removed {} placement policies associated with groups {}", rowsDeleted,
                    groupIds);
        } catch (RuntimeException e) {
            POLICY_STORE_ERROR_COUNT.labels(DELETE_LABEL).increment();
            throw e;
        }
    }

    /**
     * Delete a policy created by {@link PolicyStore#newUserPolicy(PolicyInfo)}.
     *
     * <p>Only user policies can be deleted - attempting to delete discovered policies will throw
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
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        final Collection<PolicyDTO.Policy> policies = getAll();
        logger.info("Collected diags for {} policies.", policies.size());
        appender.appendString(ComponentGsonFactory.createGsonNoPrettyPrint().toJson(policies));
    }

    @Override
    public void restoreDiags(@Nonnull List<String> collectedDiags, @Nonnull DSLContext context) throws DiagnosticsException {
        // Replace all existing groups with the ones in the collected diags.
        Collection<PolicyDTO.Policy> policies = getPoliciesFromJsonString(collectedDiags);
        try {
            replacePolicies(policies, context);
        } catch (DataAccessException ex) {
            throw new DiagnosticsException(ex);
        }
    }

    private void replacePolicies(@Nonnull Collection<PolicyDTO.Policy> policies,
                                 @Nonnull DSLContext context) {
        logger.info("Attempting to restore {} policies from diagnostics.", policies.size());
        final int rowsAffected = context.deleteFrom(Tables.POLICY).execute();
        logger.info("Deleted {} existing policies.", rowsAffected);
        for (final PolicyDTO.Policy policy : policies) {
            try {
                internalCreate(context, policy, true);
            } catch (DuplicateNameException | RuntimeException e) {
                // Log the exception, but continue attempting to restore other policies.
                logger.error("Failed to restore policy " + policy.getPolicyInfo().getName()
                    + "!", e);
            }
        }
        logger.info("Finished restoring policies from diagnostics.");
    }

    @Nonnull
    private Collection<PolicyDTO.Policy> getPoliciesFromJsonString(@Nonnull List<String> collectedDiags) {
        if (CollectionUtils.isEmpty(collectedDiags)) {
            return Collections.emptyList();
        }
        return ComponentGsonFactory.createGsonNoPrettyPrint()
            .fromJson(collectedDiags.get(0), new TypeToken<Collection<PolicyDTO.Policy>>(){}.getType());
    }

    @Nonnull
    @Override
    public String getFileName() {
        return POLICIES_DUMP_FILE;
    }

    private List<PolicyDTO.Policy> internalGet(@Nonnull final DSLContext context,
                                                   final Collection<Long> ids) {
        return context.selectFrom(Tables.POLICY)
                .where(Tables.POLICY.ID.in(ids))
                .fetch()
                .into(Policy.class)
                .stream()
                .map(this::toPolicyProto)
                .collect(Collectors.toList());
    }

    /**
     * Store a policy in the underlying database.
     * This method should be executed inside a transaction.
     *
     * <p>Policy is not validated in this method. It is vital to be able importing customers
     * topology as-is using {@link #restoreDiags(List,DSLContext)}.
     *
     * @param context The transaction context. This should NOT be the root DSLContext.
     * @param policyProto The protobuf representation of the policy.
     * @param ignoreFailures ignore the insert failures.
     * @return The protobuf representation of the policy.
     * @throws DataAccessException If there is an issue connecting to the database.
     * @throws DuplicateNameException If a policy with the same name already exists in the database.
     */
    @Nonnull
    private PolicyDTO.Policy internalCreate(@Nonnull final DSLContext context,
                                            @Nonnull final PolicyDTO.Policy policyProto,
                                            boolean ignoreFailures)
            throws DataAccessException, DuplicateNameException {
        checkForDuplicates(context, policyProto.getId(), policyProto.getPolicyInfo().getName());
        createNewPolicies(context, Collections.singleton(policyProto), ignoreFailures);
        return policyProto;
    }

    /**
     * Updates the existing policy.
     *
     * <p>Policy is not validated in this method. It is vital to be able importing customers
     * topology as-is using {@link #restoreDiags(List,DSLContext)}.
     *
     * @param context transactional context to use
     * @param newPolicyProto new version of policy proto
     * @return the version actually written to the db (existing one merged with a new one)
     * @throws PolicyNotFoundException if policy to update is not found in the DB
     * @throws DataAccessException in case of SQL errors
     * @throws DuplicateNameException if policy name duplication detected
     */
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
        existingRecord.setDisplayName(newPolicyProto.getPolicyInfo().getDisplayName());
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
            dslContext.batchInsert(
                    createReferencedGroups(Collections.singleton(newPolicyProto)))
                    .execute();
        }

        return toPolicyProto(existingRecord.into(Policy.class));
    }

    private List<PolicyGroupRecord> createReferencedGroups(
            @Nonnull Collection<PolicyDTO.Policy> policies) {
        final List<PolicyGroupRecord> result = new ArrayList<>();
        for (PolicyDTO.Policy policy: policies) {
            final long policyId = policy.getId();
            final Set<Long> groupIds = GroupProtoUtil.getPolicyGroupIds(policy);
            for (long groupId: groupIds) {
                final PolicyGroupRecord record = new PolicyGroupRecord(policyId, groupId);
                result.add(record);
            }
        }
        return result;
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
        policyBuilder.getPolicyInfoBuilder().setDisplayName(policy.getDisplayName());
        policyBuilder.getPolicyInfoBuilder().setEnabled(policy.getEnabled());

        return policyBuilder.build();
    }

    /**
     * Check if there is a duplicate group.
     * A duplicate group has the same name as the group definition but a different id.
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
                logger.error("Multiple policies ({}) exist with the name {}. "
                        + "This should never happen because the name column is unique.",
                    sameNameDiffId, name);
            }
            POLICY_STORE_DUPLICATE_NAME_COUNT.increment();
            throw new DuplicateNameException(sameNameDiffId.get(0), name);
        }
    }

    /**
     * Returns discovered policies stored in this policy store.
     *
     * @return map of policy name -> policy Id grouped by target id
     */
    @Override
    @Nonnull
    public Map<Long, Map<String, DiscoveredObjectVersionIdentity>> getDiscoveredPolicies() {
        final Collection<Record4<Long, String, Long, byte[]>> records = dslContext
                .select(Tables.POLICY.DISCOVERED_BY_ID, Tables.POLICY.NAME,
                        Tables.POLICY.ID, Tables.POLICY.HASH)
                .from(Tables.POLICY)
                .where(Tables.POLICY.DISCOVERED_BY_ID.isNotNull())
                .fetch();
        final Map<Long, Map<String, DiscoveredObjectVersionIdentity>> result = new HashMap<>();
        for (Record4<Long, String, Long, byte[]> record : records) {
            final long targetId = record.value1();
            final String policyName = record.value2();
            final long policyOid = record.value3();
            final byte[] hash = record.value4();
            final DiscoveredObjectVersionIdentity identity = new DiscoveredObjectVersionIdentity(policyOid, hash);
            result.computeIfAbsent(targetId, key -> new HashMap<>()).put(policyName, identity);
        }
        return result;
    }

    /**
     * Deletes the specified policies in the store.
     *
     * @param policiesToDelete policies OIDs to delete.
     * @return number of removed policies.
     */
    @Override
    public int deletePolicies(
            @Nonnull Collection<Long> policiesToDelete) {
        return dslContext.deleteFrom(Tables.POLICY)
                .where(Tables.POLICY.ID.in(policiesToDelete))
                .execute();
    }

    @Override
    public int createPolicies(@Nonnull Collection<PolicyDTO.Policy> policies) {
        return createNewPolicies(dslContext, policies, false);
    }

    /**
     * Creates new policies in DAO.
     *
     * @param context transactional DB context
     * @param policies policies to create
     * @param ignoreFailures if the insert failures should be ignored.
     * @return number of policies created
     */
    private int createNewPolicies(@Nonnull DSLContext context,
            @Nonnull Collection<PolicyDTO.Policy> policies,
            boolean ignoreFailures) {
        final List<PolicyRecord> policyRecords = new ArrayList<>(policies.size());
        for (PolicyDTO.Policy policyProto : policies) {
            final byte[] hash = PlacementPolicyHash.hash(policyProto);
            final PolicyRecord policy = new PolicyRecord(policyProto.getId(),
                    policyProto.getPolicyInfo().getName(), policyProto.getPolicyInfo().getEnabled(),
                    policyProto.hasTargetId() ? policyProto.getTargetId() : null,
                    policyProto.getPolicyInfo(), hash, policyProto.getPolicyInfo().getDisplayName());
            policyRecords.add(policy);
        }

        // if we are ignoring failure add the ignore to inserts
        List<Insert<PolicyRecord>> policyInserts = policyRecords.stream().map(
            r -> recordToBatchQuery(context, r, ignoreFailures)).collect(Collectors.toList());

        final int[] rowsAffected = context.batch(policyInserts).execute();
        int policyCount = logFailures(policyInserts, rowsAffected);

        List<PolicyGroupRecord> referenceRecords = createReferencedGroups(policies);
        // if we are ignoring failure add the ignore to inserts
        List<Insert<PolicyGroupRecord>> referenceInsert = referenceRecords.stream().map(
            r -> recordToBatchQuery(context, r, ignoreFailures)).collect(Collectors.toList());

        final int[] rowsAffectedGroupInserts = context.batch(referenceInsert).execute();
        logFailures(referenceInsert, rowsAffectedGroupInserts);
        return policyCount;
    }



    /**
     * Converts the table record to an insert statement including the ignore if indicated by input.
     *
     * @param context the transaction context.
     * @param record the record being inserted.
     * @param ignoreFailure if the insert should include ignore.
     * @param <R> the type of record.
     * @return the insert statement.
     */
    private static <R extends TableRecord<R>> Insert<R> recordToBatchQuery(DSLContext context,
                                               TableRecord<R> record, boolean ignoreFailure) {
        InsertOnDuplicateStep<R> ret = context.insertInto(record.getTable()).set(record);
        return addIgnoreIfNeeded(ret, ignoreFailure);
    }

    /**
     * Add ignore to insert statement if indicated by input.
     *
     * @param insert the insert statement.
     * @param ignoreFailure if we should add ignore to statement.
     * @param <R> the type of record for insert statement.
     * @return the result insert.
     */
    private static <R extends TableRecord<R>> Insert<R> addIgnoreIfNeeded(
            InsertOnDuplicateStep<R> insert, boolean ignoreFailure) {
        if (ignoreFailure) {
            // onDuplicateKeyIgnore is translated to INSERT IGNORE in MySql. INSERT IGNORE will
            // ignore the foreign key constraint.  Other DBMSes might not behave like this.
            return insert.onDuplicateKeyIgnore();
        } else {
            return insert;
        }
    }

    private <T extends Record> int logFailures(List<Insert<T>> inserts,
                                               int[] rowsAffected) {
        int successfulInserts = 0;
        for (int idx = 0; idx < rowsAffected.length; idx++) {
            // all the queries are insert so if no row was changes there is something wrong.
            if (rowsAffected[idx] == 0) {
                logger.info("There was an error occurred while inserting policies. "
                        + "The insert with statement \"{}\" and params \"{}\" did not execute "
                        + "successfully",
                    inserts.get(idx).getSQL(),
                    inserts.get(idx).getParams().values().stream().map(Param::toString)
                        .collect(Collectors.joining(",")),
                    rowsAffected[idx]);
            } else {
                successfulInserts++;
            }
        }
        return successfulInserts;
    }
}
