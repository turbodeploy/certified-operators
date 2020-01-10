package com.vmturbo.group.setting;

import static com.vmturbo.group.db.Tables.GLOBAL_SETTINGS;
import static com.vmturbo.group.db.Tables.SETTING_POLICY;
import static com.vmturbo.group.db.Tables.SETTING_POLICY_SCHEDULE;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;

import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutableSettingPolicyUpdateException;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.common.ItemNotFoundException.SettingPolicyNotFoundException;
import com.vmturbo.group.common.TargetCollectionUpdate.TargetPolicyUpdate;
import com.vmturbo.group.common.TargetCollectionUpdate.TargetSettingPolicyUpdate;
import com.vmturbo.group.db.enums.SettingPolicyPolicyType;
import com.vmturbo.group.db.tables.pojos.SettingPolicy;
import com.vmturbo.group.db.tables.pojos.SettingPolicySchedule;
import com.vmturbo.group.db.tables.records.SettingPolicyRecord;
import com.vmturbo.group.db.tables.records.SettingPolicyScheduleRecord;
import com.vmturbo.group.identity.IdentityProvider;

/**
 * The {@link SettingStore} class is used to store settings-related objects, and retrieve them
 * in an efficient way.
 */
public class SettingStore implements Diagnosable {

    private final Logger logger = LogManager.getLogger();

    /**
     * A DSLContext with which to interact with an underlying persistent datastore.
     */
    private final DSLContext dslContext;

    private final SettingSpecStore settingSpecStore;

    private final IdentityProvider identityProvider;

    private final SettingPolicyValidator settingPolicyValidator;

    private final GroupToSettingPolicyIndex groupToSettingPolicyIndex;

    private final SettingsUpdatesSender settingsUpdatesSender;
    /**
     * Create a new SettingStore.
     *
     * @param settingSpecStore The source, providing the {@link SettingSpec} definitions.
     * @param dslContext A context with which to interact with the underlying datastore.
     * @param identityProvider The identity provider used to assign OIDs.
     * @param settingPolicyValidator settingPolicyValidator.
     * @param settingsUpdatesSender broadcaster for settings updates.
     *
     */
    public SettingStore(@Nonnull final SettingSpecStore settingSpecStore,
            @Nonnull final DSLContext dslContext,
            @Nonnull final IdentityProvider identityProvider,
            @Nonnull final SettingPolicyValidator settingPolicyValidator,
            @Nonnull final SettingsUpdatesSender settingsUpdatesSender) {
        this.settingSpecStore = Objects.requireNonNull(settingSpecStore);
        this.dslContext = Objects.requireNonNull(dslContext);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.settingPolicyValidator = Objects.requireNonNull(settingPolicyValidator);
        this.settingsUpdatesSender = settingsUpdatesSender;
        // This makes a blocking call to the DB during object initialization.
        // Since the amount of setting policies will be small, this shouldn't
        // be a problem. If this assumption changes, this loading may have to be
        // made async.
        this.groupToSettingPolicyIndex =
                new GroupToSettingPolicyIndex(
                       this.getSettingPolicies(
                                SettingPolicyFilter.newBuilder().build()));
    }

    /**
     * This is the internal version of {@link SettingStore#createUserSettingPolicy(SettingPolicyInfo)}
     * which allows the specification of the setting policy's type. External classes should NOT
     * use this method.
     *
     * @param settingPolicyInfo See {@link SettingStore#createUserSettingPolicy(SettingPolicyInfo)}.
     * @param type The type of the policy to create.
     * @return See {@link SettingStore#createUserSettingPolicy(SettingPolicyInfo)}.
     * @throws InvalidItemException See {@link SettingStore#createUserSettingPolicy(SettingPolicyInfo)}.
     * @throws DuplicateNameException See {@link SettingStore#createUserSettingPolicy(SettingPolicyInfo)}.
     * @throws DataAccessException See {@link SettingStore#createUserSettingPolicy(SettingPolicyInfo)}.
     */
    @Nonnull
    private SettingProto.SettingPolicy internalCreateSettingPolicy(
            @Nonnull final SettingPolicyInfo settingPolicyInfo,
            @Nonnull final SettingProto.SettingPolicy.Type type)
            throws InvalidItemException, DuplicateNameException, DataAccessException {
        // Validate before doing anything.
        settingPolicyValidator.validateSettingPolicy(settingPolicyInfo, type);

        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                // Explicitly search for an existing policy with the same name, so that we
                // know when to throw a DuplicateNameException as opposed to a generic
                // DataIntegrityException.
                final Record1<Long> existingId = context.select(SETTING_POLICY.ID)
                        .from(SETTING_POLICY)
                        .where(SETTING_POLICY.NAME.eq(settingPolicyInfo.getName()))
                        .fetchOne();
                if (existingId != null) {
                    throw new DuplicateNameException(existingId.value1(),
                            settingPolicyInfo.getName());
                }

                final long settingPolicyId = identityProvider.next();
                final SettingPolicy jooqSettingPolicy = new SettingPolicy(
                    settingPolicyId,
                    settingPolicyInfo.getName(),
                    settingPolicyInfo.getEntityType(),
                    settingPolicyInfo,
                    SettingPolicyTypeConverter.typeToDb(type),
                    settingPolicyInfo.getTargetId());
                context.newRecord(SETTING_POLICY, jooqSettingPolicy).store();
                if (settingPolicyInfo.hasScheduleId()) {
                    // OM-52825 will deal with SettingPolicyInfo refactoring
                    assignSchedule(context, settingPolicyId,
                        settingPolicyInfo.getScheduleId());
                }
                SettingProto.SettingPolicy newSettingPolicy = toSettingPolicy(jooqSettingPolicy);
                groupToSettingPolicyIndex.add(newSettingPolicy);
                return newSettingPolicy;

            });
        } catch (DataAccessException e) {
            // Jooq will rethrow a DuplicateNameException thrown in the transactionResult call
            // wrapped in a DataAccessException. Check to see if that's why the transaction failed.
            if (e.getCause() instanceof DuplicateNameException) {
                throw (DuplicateNameException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Persist a new user {@link SettingPolicy} in the {@link SettingStore} based on the info in the
     * {@link SettingPolicyInfo}.
     * Note that setting policies must have unique names.
     *
     * @param settingPolicyInfo The info to be applied to the SettingPolicy.
     * @return A SettingPolicy whose info matches the input {@link SettingPolicyInfo}.
     * An ID will be assigned to this policy.
     * @throws InvalidItemException If the input setting policy is not valid.
     * @throws DuplicateNameException If there is already a setting policy with the same name as
     * the input setting policy.
     * @throws DataAccessException If there is another problem connecting to the database.
     */
    @Nonnull
    public SettingProto.SettingPolicy createUserSettingPolicy(
            @Nonnull final SettingPolicyInfo settingPolicyInfo)
            throws InvalidItemException, DuplicateNameException {
        return internalCreateSettingPolicy(settingPolicyInfo, Type.USER);
    }

    /**
     * Persist a new default {@link SettingPolicy} in the {@link SettingStore} based on the info in the
     * {@link SettingPolicyInfo}.
     * Note that setting policies must have unique names.
     *
     * @param settingPolicyInfo The info to be applied to the SettingPolicy.
     * @return A SettingPolicy whose info matches the input {@link SettingPolicyInfo}.
     * An ID will be assigned to this policy.
     * @throws InvalidItemException If the input setting policy is not valid.
     * @throws DuplicateNameException If there is already a setting policy with the same name as
     * the input setting policy.
     * @throws DataAccessException If there is another problem connecting to the database.
     */
    @Nonnull
    public SettingProto.SettingPolicy createDefaultSettingPolicy(
            @Nonnull final SettingPolicyInfo settingPolicyInfo)
            throws InvalidItemException, DuplicateNameException {
        return internalCreateSettingPolicy(settingPolicyInfo, Type.DEFAULT);
    }

    /**
     * Persist a new discovered {@link SettingPolicy} in the {@link SettingStore} based on the info in the
     * {@link SettingPolicyInfo}.
     * Note that setting policies must have unique names.
     *
     * @param settingPolicyInfo The info to be applied to the SettingPolicy.
     * @return A SettingPolicy whose info matches the input {@link SettingPolicyInfo}.
     * An ID will be assigned to this policy.
     * @throws InvalidItemException If the input setting policy is not valid.
     * @throws DuplicateNameException If there is already a setting policy with the same name as
     * the input setting policy.
     * @throws DataAccessException If there is another problem connecting to the database.
     */
    @Nonnull
    public SettingProto.SettingPolicy createDiscoveredSettingPolicy(
            @Nonnull final SettingPolicyInfo settingPolicyInfo)
            throws InvalidItemException, DuplicateNameException {
        return internalCreateSettingPolicy(settingPolicyInfo, Type.DISCOVERED);
    }

    /**
     * Update an existing setting policy in the {@link SettingStore}, overwriting the
     * existing {@link SettingPolicyInfo} with a new one.
     *
     * @param id The ID of the policy to update.
     * @param newInfo The new {@link SettingPolicyInfo}. This will completely replace the old
     * info, and must pass validation.
     * @return The updated {@link SettingProto.SettingPolicy}.
     * @throws SettingPolicyNotFoundException If the policy to update doesn't exist.
     * @throws InvalidItemException If the update attempt would violate constraints on
     * the setting policy.
     * @throws DuplicateNameException If there is already a setting policy with the same name as
     * the new info (other than the policy to update).
     * @throws DataAccessException If there is an error interacting with the database.
     */
    @Nonnull
    public SettingProto.SettingPolicy updateSettingPolicy(final long id,
            @Nonnull final SettingPolicyInfo newInfo)
            throws SettingPolicyNotFoundException, InvalidItemException, DuplicateNameException,
            DataAccessException {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return updateSettingPolicy(context, id, newInfo);
            });
        } catch (DataAccessException e) {
            // Jooq will rethrow exceptions thrown in the transactionResult call
            // wrapped in a DataAccessException. Check to see if that's why the transaction failed.
            if (e.getCause() instanceof DuplicateNameException) {
                throw (DuplicateNameException)e.getCause();
            } else if (e.getCause() instanceof SettingPolicyNotFoundException) {
                throw (SettingPolicyNotFoundException)e.getCause();
            } else if (e.getCause() instanceof InvalidItemException) {
                throw (InvalidItemException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    @Nonnull
    private SettingProto.SettingPolicy updateSettingPolicy(@Nonnull DSLContext context,
            final long id, @Nonnull final SettingPolicyInfo newInfo)
            throws SettingPolicyNotFoundException, InvalidItemException, DuplicateNameException,
            DataAccessException {
        final SettingProto.SettingPolicy existingPolicy = getSettingPolicies(context,
                SettingPolicyFilter.newBuilder().withId(id).build()).findFirst()
                .orElseThrow(() -> new SettingPolicyNotFoundException(id));

        final SettingProto.SettingPolicy.Type type = existingPolicy.getSettingPolicyType();

        // Additional update-only validation for default policies
        // to ensure certain fields are not changed.
        if (type.equals(Type.DEFAULT)) {
            // For default setting policies we don't allow changes to names
            // or entity types.
            if (newInfo.getEntityType() != existingPolicy.getInfo().getEntityType()) {
                throw new InvalidItemException("Illegal attempt to change the " +
                        " entity type of a default setting policy.");
            }
            if (!newInfo.getName().equals(existingPolicy.getInfo().getName())) {
                throw new InvalidItemException(
                        "Illegal attempt to change the name of a default setting policy " +
                                existingPolicy.getInfo().getName());
            }
            Set<String> newSettingNames = newInfo.getSettingsList()
                    .stream()
                    .map(Setting::getSettingSpecName)
                    .collect(Collectors.toSet());

            final SettingPolicyInfo defaultSettingPolicy = DefaultSettingPolicyCreator
                    .defaultSettingPoliciesFromSpecs(settingSpecStore.getAllSettingSpecs())
                    .get(existingPolicy.getInfo().getEntityType());
            if (defaultSettingPolicy == null) {
                logger.error("Cannot get default info for policy {}, entity type {}",
                        newInfo.getName(), existingPolicy.getInfo().getEntityType());
            }
            final Map<String, Setting> defaultSettings = defaultSettingPolicy == null
                    ? Collections.emptyMap()
                    : defaultSettingPolicy.getSettingsList().stream()
                    .collect(Collectors.toMap(Setting::getSettingSpecName, Functions.identity()));
            List<Setting> settingsToAdd = new ArrayList<>();
            for (Setting existingSetting : existingPolicy.getInfo().getSettingsList()) {
                final String existingSettingName = existingSetting.getSettingSpecName();
                if (!newSettingNames.contains(existingSettingName)) {
                    /*
                     * SLA is created behind the scenes, not exposed in the UI.
                     * TODO If this changes this will have to be revisited.
                     */
                    if (existingSettingName.contains("slaCapacity")) {
                        settingsToAdd.add(existingSetting);
                        continue;
                    }

                    final Setting defaultSetting = defaultSettings.get(existingSettingName);
                    if (defaultSetting == null) {
                        logger.error("Cannot get default value for setting {} in policy {}",
                                existingSettingName, newInfo.getName());
                    } else if (SettingDTOUtil.areValuesEqual(existingSetting, defaultSetting)) {
                        // Prevent removing setting if its existing value matches default
                        settingsToAdd.add(existingSetting);
                        continue;
                    }

                    /*
                     * TODO (Marco, August 05 2018) OM-48940
                     * ActionWorkflow and ActionScript settings are missing in the ui and
                     * in the payload of the request due to OM-48950.
                     * This workaround will be removed when OM-48950 will be fixed.
                     */
                    if (!existingSettingName.contains("ActionWorkflow") &&
                            !existingSettingName.contains("ActionScript")) {
                        throw new InvalidItemException(
                                "Illegal attempt to remove a default setting " +
                                        existingSettingName);
                    }
                }
            }

            if (!settingsToAdd.isEmpty()) {
                // add the default entities that are missing
                settingsToAdd.addAll(newInfo.getSettingsList());

                SettingPolicyInfo newNewInfo = SettingPolicyInfo.newBuilder(newInfo)
                    .clearSettings()
                    .addAllSettings(settingsToAdd)
                    .build();

                return internalUpdateSettingPolicy(context, existingPolicy.toBuilder()
                                .setInfo(newNewInfo)
                                .build());
            }
        }

        if (type.equals(Type.DISCOVERED)) {
            throw new InvalidItemException(
                    "Illegal attempt to modify a discovered setting policy " + id);
        }

        return internalUpdateSettingPolicy(context,
                existingPolicy.toBuilder().setInfo(newInfo).build());
    }

    /**
     * Reset a default setting policy to the factory defaults.
     *
     * @param id The ID of the setting policy.
     * @return The {@link SettingProto.SettingPolicy} representing the updated setting policy.
     * @throws SettingPolicyNotFoundException If the setting policy does not exist.
     * @throws IllegalArgumentException If the setting policy ID refers to an invalid policy (i.e.
     *         a non-default policy).
     */
    public SettingProto.SettingPolicy resetSettingPolicy(final long id)
            throws SettingPolicyNotFoundException, IllegalArgumentException {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);

                final SettingProto.SettingPolicy existingPolicy = getSettingPolicies(context,
                    SettingPolicyFilter.newBuilder()
                        .withId(id)
                        .build()).findFirst().orElseThrow(() -> new SettingPolicyNotFoundException(id));
                final SettingProto.SettingPolicy.Type type = existingPolicy.getSettingPolicyType();

                if (!type.equals(Type.DEFAULT)) {
                    throw new IllegalArgumentException("Cannot reset setting policy " + id +
                            ". Type is " + type + ". Must be " + Type.DEFAULT);
                }

                final Map<Integer, SettingPolicyInfo> defaultSettingPolicies =
                        DefaultSettingPolicyCreator.defaultSettingPoliciesFromSpecs(
                                settingSpecStore.getAllSettingSpecs());
                final SettingPolicyInfo newPolicyInfo =
                        defaultSettingPolicies.get(existingPolicy.getInfo().getEntityType());
                if (newPolicyInfo == null) {
                    // As of right now (April 6 2018) this could happen if we remove all settings
                    // for an entity type during a migration. In this case, don't update anything.
                    logger.error("Attempting to reset a default setting policy {} failed because " +
                        "there are no setting specs for entity type {} in the setting spec store.",
                        id, existingPolicy.getInfo().getEntityType());
                    return existingPolicy;
                }

                return internalUpdateSettingPolicy(context, existingPolicy.toBuilder()
                    .setInfo(newPolicyInfo)
                    .build());
            });
        } catch (DataAccessException e) {
            // Jooq will rethrow exceptions thrown in the transactionResult call
            // wrapped in a DataAccessException. Check to see if that's why the transaction failed.
            if (e.getCause() instanceof SettingPolicyNotFoundException) {
                throw (SettingPolicyNotFoundException) e.getCause();
            } else if (e.getCause() instanceof InvalidItemException) {
                // This shouldn't happen, because default setting policies created from the
                // setting specs should always be valid. Must be some programming error!
                throw new IllegalStateException("DefaultSettingPolicyCreator produced an " +
                        "invalid setting policy! Error: " + e.getMessage());
            } else if (e.getCause() instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Delete a setting policy.
     *
     * @param id The ID of the setting policy to delete.
     * @return The deleted {@link SettingProto.SettingPolicy}.
     * @throws SettingPolicyNotFoundException If the setting policy does not exist.
     * @throws ImmutableSettingPolicyUpdateException If the setting policy is a DEFAULT policy or it is a user update
     *                                       attempting to delete a DISCOVERED policy.
     */
    @Nonnull
    public SettingProto.SettingPolicy deleteUserSettingPolicy(final long id)
            throws SettingPolicyNotFoundException, ImmutableSettingPolicyUpdateException {
        return internalDeleteSettingPolicy(dslContext, id, true);
    }

    @Nonnull
    private SettingProto.SettingPolicy deleteSettingPolicy(@Nonnull final DSLContext context,
                                                               final SettingProto.SettingPolicy settingPolicy,
                                                               boolean initializedByUser)
            throws SettingPolicyNotFoundException, ImmutableSettingPolicyUpdateException {
        return internalDeleteSettingPolicy(dslContext, settingPolicy.getId(), initializedByUser);
    }

    @Nonnull
    private SettingProto.SettingPolicy internalDeleteSettingPolicy(@Nonnull final DSLContext context,
                                                           final long settingPolicyId,
                                                           boolean initializedByUser)
            throws SettingPolicyNotFoundException, ImmutableSettingPolicyUpdateException {
        final SettingPolicyRecord record =
                context.fetchOne(SETTING_POLICY, SETTING_POLICY.ID.eq(settingPolicyId));
        if (record == null) {
            throw new SettingPolicyNotFoundException(settingPolicyId);
        }

        if (record.getPolicyType().equals(SettingPolicyPolicyType.default_)) {
            throw new ImmutableSettingPolicyUpdateException(
                    "Cannot delete default setting policy: " + record.getName());
        } else if (record.getPolicyType().equals(SettingPolicyPolicyType.discovered)
            && initializedByUser) {
            throw new ImmutableSettingPolicyUpdateException(
                    "User cannot delete discovered setting policy: " + record.getName());
        }

        final int modifiedRecords = record.delete();
        if (modifiedRecords == 0) {
            // This should never happen, because the record definitely exists if we
            // got to this point.
            throw new IllegalStateException("Failed to delete record.");
        }

        // also delete any schedule mapping
        context.delete(SETTING_POLICY_SCHEDULE)
            .where(SETTING_POLICY_SCHEDULE.SETTING_POLICY_ID.eq(settingPolicyId))
            .execute();

        SettingProto.SettingPolicy deletedPolicy = toSettingPolicy(record);
        groupToSettingPolicyIndex.remove(deletedPolicy);
        return deletedPolicy;
    }

    private Stream<SettingProto.SettingPolicy> getSettingPolicies(
            @Nonnull final DSLContext context,
            @Nonnull final SettingPolicyFilter filter) throws DataAccessException {
        final Stream<SettingProto.SettingPolicy> settingPolicies =  context.selectFrom(SETTING_POLICY)
            .where(filter.getConditions())
            .fetch()
            .into(SettingPolicy.class)
            .stream()
            .map(this::toSettingPolicy);
        return addScheduleToSettingPolicy(context, settingPolicies);
    }

    /**
     * Fetch all settings policies that use specified schedule.
     *
     * @param scheduleId ID of the {@link com.vmturbo.group.db.tables.pojos.Schedule} setting
     * policies are being fetched for
     * @return A stream of {@link SettingPolicy} objects that use the schedule.
     * @throws DataAccessException If there is an exception executing the query.
     */
    @Nonnull
    public Stream<SettingProto.SettingPolicy> getSettingPoliciesUsingSchedule(final long scheduleId)
        throws DataAccessException {
        return dslContext.transactionResult(configuration -> {
            final DSLContext context = DSL.using(configuration);
            return context.select(SETTING_POLICY.fields())
                .from(SETTING_POLICY
                    .join(SETTING_POLICY_SCHEDULE)
                    .on(SETTING_POLICY_SCHEDULE.SETTING_POLICY_ID.eq(SETTING_POLICY.ID)))
                .where(SETTING_POLICY_SCHEDULE.SCHEDULE_ID.eq(scheduleId))
                .fetch()
                .into(SettingPolicy.class)
                .stream()
                .map(sPolicyRecord -> toSettingPolicyWithSchedule(sPolicyRecord, scheduleId));
        });
    }

    /**
     * Get setting policies matching a filter.
     *
     * @param filter The {@link SettingPolicyFilter}.
     * @return A stream of {@link SettingPolicy} objects that match the filter.
     * @throws DataAccessException If there is an error connecting to the database.
     */
    public Stream<SettingProto.SettingPolicy> getSettingPolicies(
            @Nonnull final SettingPolicyFilter filter) throws DataAccessException {
        return getSettingPolicies(dslContext, filter);
    }

    /**
     * Get a setting policy by its unique OID.
     *
     * @param oid The OID (object id) of the setting policy to retrieve.
     * @return The {@link SettingProto.SettingPolicy} associated with the name, or an empty policy.
     */
    public Optional<SettingProto.SettingPolicy> getSettingPolicy(final long oid) {
        return getSettingPolicies(SettingPolicyFilter.newBuilder().withId(oid).build()).findFirst();
    }

    /**
     * Update the set of {@link SettingPolicy}s discovered by a particular target.
     * The new set of setting policies will completely replace the old, even if the new set is empty.
     *
     * <p>See {@link TargetPolicyUpdate} for details on the update behavior.
     *
     * @param context The context to use to do the updates.
     * @param targetId The ID of the target that discovered the setting policies.
     * @param settingPolicyInfos The new set of {@link DiscoveredSettingPolicyInfo}s.
     * @param groupOids a mapping of group name to group oid.
     * @throws DataAccessException If there is an error interacting with the database.
     */
    public void updateTargetSettingPolicies(@Nonnull final DSLContext context,
                final long targetId,
                @Nonnull final List<DiscoveredSettingPolicyInfo> settingPolicyInfos,
                @Nonnull final Map<String, Long> groupOids) throws DataAccessException {
        logger.info("Updating setting policies discovered by {}. Got {} setting policies.",
            targetId, settingPolicyInfos.size());

        final DiscoveredSettingPoliciesMapper mapper = new DiscoveredSettingPoliciesMapper(groupOids);
        final List<SettingPolicyInfo> discoveredSettingPolicies = settingPolicyInfos.stream()
            .map(info -> mapper.mapToSettingPolicyInfo(info, targetId))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
        final TargetSettingPolicyUpdate update = new TargetSettingPolicyUpdate(targetId, identityProvider,
                discoveredSettingPolicies,
                getSettingPoliciesDiscoveredByTarget(context, targetId));
        update.apply((settingPolicy) -> storeDiscoveredSettingPolicy(context, settingPolicy),
                (settingPolicy) -> internalUpdateSettingPolicy(context, settingPolicy),
                (settingPolicy) -> deleteSettingPolicy(context, settingPolicy, false));
        logger.info("Finished updating discovered groups.");
    }

    @Nonnull
    private SettingProto.SettingPolicy internalUpdateSettingPolicy(
                @Nonnull final DSLContext context,
                @Nonnull final SettingProto.SettingPolicy policy)
            throws SettingPolicyNotFoundException, DuplicateNameException, InvalidItemException {
        final SettingPolicyRecord record =
                context.fetchOne(SETTING_POLICY, SETTING_POLICY.ID.eq(policy.getId()));
        if (record == null) {
            throw new SettingPolicyNotFoundException(policy.getId());
        }

        // Validate the setting policy.
        // This should throw an exception if it's invalid.
        settingPolicyValidator.validateSettingPolicy(policy.getInfo(), policy.getSettingPolicyType());


        // Explicitly search for an existing policy with the same name that's NOT
        // the policy being edited. We do this because we want to know
        // know when to throw a DuplicateNameException as opposed to a generic
        // DataIntegrityException.
        final Record1<Long> existingId = context.select(SETTING_POLICY.ID)
                .from(SETTING_POLICY)
                .where(SETTING_POLICY.NAME.eq(policy.getInfo().getName()))
                .and(SETTING_POLICY.ID.ne(policy.getId()))
                .fetchOne();
        if (existingId != null) {
            throw new DuplicateNameException(existingId.value1(), policy.getInfo().getName());
        }

        record.setId(policy.getId());
        record.setName(policy.getInfo().getName());
        record.setEntityType(policy.getInfo().getEntityType());
        record.setSettingPolicyData(policy.getInfo());
        record.setTargetId(policy.getInfo().getTargetId());
        record.setPolicyType(SettingPolicyTypeConverter.typeToDb(policy.getSettingPolicyType()));

        final int modifiedRecords = record.update();
        if (modifiedRecords == 0) {
            // This should never happen, because we overwrote fields in the record,
            // and update() should always execute an UPDATE statement if some fields
            // got overwritten.
            throw new IllegalStateException("Failed to update record.");
        }
        if (policy.getInfo().hasScheduleId()) {
            // OM-52825 will deal with SettingPolicyInfo refactoring
            assignSchedule(context, policy.getId(), policy.getInfo().getScheduleId());
        } else {
            // delete assigned schedule if any
            context.delete(SETTING_POLICY_SCHEDULE)
                .where(SETTING_POLICY_SCHEDULE.SETTING_POLICY_ID.eq(policy.getId()))
                .execute();
        }
        SettingProto.SettingPolicy updatedPolicy = toSettingPolicy(record);
        groupToSettingPolicyIndex.update(updatedPolicy);
        return updatedPolicy;
    }

    @VisibleForTesting
    @Nonnull
    Collection<SettingProto.SettingPolicy> getSettingPoliciesDiscoveredByTarget(
                @Nonnull final DSLContext dslContext, final long targetId)
            throws DataAccessException {
        return getSettingPolicies(dslContext,
                SettingPolicyFilter.newBuilder()
                    .withTargetId(targetId)
                    .build())
            .collect(Collectors.toList());
    }

    private void storeDiscoveredSettingPolicy(@Nonnull final DSLContext context,
                                              @Nonnull final SettingProto.SettingPolicy settingPolicy)
            throws DataAccessException, InvalidItemException {
        try {
            final Type type = settingPolicy.getSettingPolicyType();
            Preconditions.checkArgument(type == Type.DISCOVERED);

            // Validate before doing anything.
            final SettingPolicyInfo settingPolicyInfo = settingPolicy.getInfo();
            settingPolicyValidator.validateSettingPolicy(settingPolicyInfo, type);

            final SettingPolicy jooqSettingPolicy = new SettingPolicy(
                settingPolicy.getId(),
                settingPolicyInfo.getName(),
                settingPolicyInfo.getEntityType(),
                settingPolicyInfo,
                SettingPolicyTypeConverter.typeToDb(type),
                settingPolicyInfo.getTargetId());
            context.newRecord(SETTING_POLICY, jooqSettingPolicy).store();
            groupToSettingPolicyIndex.update(settingPolicy);
        } catch (DataAccessException e) {
            throw new DataAccessException("Unable to store discovered setting policy " + settingPolicy, e);
        }
    }

    /**
     * Return the SettingPolicies associated with the Groups.
     *
     * @param groupIds The set of group ids.
     * @return Setting Policies associated with the group.
     */
    public Map<Long, List<SettingProto.SettingPolicy>> getSettingPoliciesForGroups(Set<Long> groupIds) {
        Map<Long, Set<Long>> groupIdToSettingPolicyIds =
                groupToSettingPolicyIndex.getSettingPolicyIdsForGroup(groupIds);

        SettingPolicyFilter.Builder filter = SettingPolicyFilter.newBuilder();
        groupIdToSettingPolicyIds.values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet())
                .forEach(id -> filter.withId(id));

        Map<Long, SettingProto.SettingPolicy> settingPolicyIdToSettingPolicy =
                getSettingPolicies(filter.build())
                        .collect(Collectors.toMap(SettingProto.SettingPolicy::getId,
                                Function.identity()));

        return groupIds.stream().collect(Collectors.toMap(Function.identity(),
            groupId -> groupIdToSettingPolicyIds.get(groupId)
                    .stream()
                    .map(id -> settingPolicyIdToSettingPolicy.get(id))
                    .collect(Collectors.toList())));

    }

    /**
     * Insert the settings into the Global Settings table in a single batch.
     * If key already exists, it is skipper. Only new records are persisted in
     * the DB.
     *
     * @param settings List of settings to be inserted into the database
     * @throws DataAccessException
     * @throws InvalidProtocolBufferException
     * @throws SQLTransientException
     *
     */
    @Retryable(value = {SQLTransientException.class},
        maxAttempts = 3, backoff = @Backoff(delay = 2000))
    private void insertGlobalSettingsInternal(@Nonnull final List<Setting> settings)
            throws SQLTransientException, DataAccessException, InvalidProtocolBufferException {

        if (settings.isEmpty()) {
            return;
        }

        try {
            // insert only those settings which don't exist in the database
            Map<String, Setting> inputSettings =
                settings
                    .stream()
                    .collect(Collectors.toMap(Setting::getSettingSpecName, Function.identity()));
            Map<String, Setting> existingSettings =
                getAllGlobalSettings()
                    .stream()
                    .collect(Collectors.toMap(Setting::getSettingSpecName, Function.identity()));

            List<Setting> newSettings = new ArrayList<>();
            for (Setting s : inputSettings.values()) {
                if (!existingSettings.containsKey(s.getSettingSpecName())) {
                    newSettings.add(s);
                }
            }

            if (newSettings.isEmpty()) {
                logger.info("No new global settings to add to the database");
                return;
            }
            BatchBindStep batch = dslContext.batch(
                //have to provide dummy values for jooq
                dslContext.insertInto(GLOBAL_SETTINGS, GLOBAL_SETTINGS.NAME, GLOBAL_SETTINGS.SETTING_DATA)
                                .values(newSettings.get(0).getSettingSpecName(), newSettings.get(0).toByteArray()));
            for (Setting setting : newSettings) {
                batch.bind(setting.getSettingSpecName(), setting.toByteArray());
            }
            batch.execute();
        } catch (DataAccessException e) {
            if ((e.getCause() instanceof SQLException) &&
                    (((SQLException)e.getCause()).getCause() instanceof  SQLTransientException)) {
                // throw SQLTransientException so that it can be retried
                throw new SQLTransientException(((SQLException)e.getCause()).getCause());
            } else {
                throw e;
            }
        }
    }

    public void insertGlobalSettings(@Nonnull final List<Setting> settings)
        throws DataAccessException, InvalidProtocolBufferException {

        try {
            insertGlobalSettingsInternal(settings);
        } catch (SQLTransientException e) {
            throw new DataAccessException("Failed to insert settings into DB", e.getCause());
        }
    }

    @Retryable(value = {SQLTransientException.class},
        maxAttempts = 3, backoff = @Backoff(delay = 2000))
    private void updateGlobalSettingInternal(@Nonnull final Setting setting)
            throws SQLTransientException, DataAccessException {
        try {
            dslContext.update(GLOBAL_SETTINGS)
                        .set(GLOBAL_SETTINGS.SETTING_DATA, setting.toByteArray())
                        .where(GLOBAL_SETTINGS.NAME.eq(setting.getSettingSpecName()))
                        .execute();
            try {
                settingsUpdatesSender.notifySettingsUpdated(setting);
            } catch (InterruptedException e) {
                logger.error("Interrupted exception while broadcasting notification for setting {}",
                        setting.getSettingSpecName(), e);
            } catch (CommunicationException e) {
                logger.error("CommunicationException exception while broadcasting notification for" +
                                " setting {}", setting.getSettingSpecName(), e);
            }
        } catch (DataAccessException e) {
            if ((e.getCause() instanceof SQLException) &&
                    (((SQLException)e.getCause()).getCause() instanceof  SQLTransientException)) {
                // throw SQLTransientException so that it can be retried
                throw new SQLTransientException(((SQLException)e.getCause()).getCause());
            } else {
                throw e;
            }
        }
    }

    public void updateGlobalSetting(@Nonnull final Setting setting)
            throws DataAccessException {

        try {
            updateGlobalSettingInternal(setting);
        } catch (SQLTransientException e) {
            throw new DataAccessException("Failed to update setting: "
                + setting.getSettingSpecName(), e.getCause());
        }
    }

    public Optional<Setting> getGlobalSetting(@Nonnull final String settingName)
        throws DataAccessException, InvalidProtocolBufferException {

            Record1<byte[]> result = dslContext.select(GLOBAL_SETTINGS.SETTING_DATA)
                            .from(GLOBAL_SETTINGS)
                            .where(GLOBAL_SETTINGS.NAME.eq(settingName)).fetchOne();

        if (result != null) {
            return Optional.of(Setting.parseFrom(result.value1()));
        }

        return Optional.empty();
    }

    public List<Setting> getAllGlobalSettings()
        throws DataAccessException, InvalidProtocolBufferException {

        List<byte[]> result =  dslContext.select().from(GLOBAL_SETTINGS)
                                    .fetch().getValues(GLOBAL_SETTINGS.SETTING_DATA);
        List<Setting> settings = new ArrayList<>();

        for (byte[] settingBytes : result) {
                settings.add(Setting.parseFrom(settingBytes));
        }

        return settings;
    }

    /**
     * Get a setting policy by its unique name.
     *
     * @param name The name of the setting policy to retrieve.
     * @return The {@link SettingProto.SettingPolicy} associated with the name, or an empty
     * policy.
     */
    public Optional<SettingProto.SettingPolicy> getSettingPolicy(@Nonnull final String name) {
        return getSettingPolicies(
                SettingPolicyFilter.newBuilder().withName(name).build()).findFirst();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Stream<String> collectDiagsStream() throws DiagnosticsException {
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();

        try {
            return Stream.of(
                gson.toJson(getAllGlobalSettings()),
                gson.toJson(getSettingPolicies(SettingPolicyFilter.newBuilder().build())
                    .collect(Collectors.toList())));
        } catch (DataAccessException | InvalidProtocolBufferException e) {
            throw new DiagnosticsException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreDiags(@Nonnull List<String> collectedDiags) throws DiagnosticsException {
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        final List<String> errors = new ArrayList<>();

        if (collectedDiags.size() != 2) {
            throw new DiagnosticsException("Wrong number of diagnostics lines: "
                + collectedDiags.size() + ". Expected: 2.");
        }

        try {
            final List<Setting> globalSettingsToRestore =
                gson.fromJson(collectedDiags.get(0), new TypeToken<List<Setting>>() { }.getType());
            logger.info("Attempting to restore {} global settings.", globalSettingsToRestore.size());

            // Attempt to restore global settings.
            deleteAllGlobalSettings();
            insertGlobalSettings(globalSettingsToRestore);


        } catch (DataAccessException | InvalidProtocolBufferException e) {
            errors.add("Failed to restore global settings: " + e.getMessage() + ": " +
                ExceptionUtils.getStackTrace(e));
        }

        try {
            // Attempt to restore setting policies.
            final List<SettingProto.SettingPolicy> settingPoliciesToRestore =
                gson.fromJson(collectedDiags.get(1),
                    new TypeToken<List<SettingProto.SettingPolicy>>() { }.getType());
            logger.info("Attempting to restore {} setting policies.", settingPoliciesToRestore.size());

            // Attempt to restore setting policies.
            deleteAllSettingPolicies();
            insertAllSettingPolicies(settingPoliciesToRestore);

        } catch (DataAccessException | SQLTransientException e) {
            errors.add("Failed to restore setting policies: " + e.getMessage() + ": " +
                ExceptionUtils.getStackTrace(e));
        }

        if (!errors.isEmpty()) {
            throw new DiagnosticsException(errors);
        }
    }

    /**
     * Delete all global settings.
     */
    private void deleteAllGlobalSettings() {
        dslContext.truncate(GLOBAL_SETTINGS).execute();
    }

    /**
     * Delete all setting policies.
     */
    private void deleteAllSettingPolicies() {
        // Calling delete instead of truncate so that ON DELETE triggers in child table
        // can be fired
        dslContext.transactionResult(configuration -> {
            final DSLContext context = DSL.using(configuration);
            return context.deleteFrom(SETTING_POLICY)
                .execute();
        });
        groupToSettingPolicyIndex.clear();
    }

    /**
     * Insert all the setting policies in the list into the database.
     * For internal use only when restoring setting policies from diagnostics.
     *
     * @param settingPolicies The setting policies to insert.
     */
    private void insertAllSettingPolicies(@Nonnull final List<SettingProto.SettingPolicy> settingPolicies)
        throws SQLTransientException, DataAccessException{
        // It should be unusual to have a a very high number of setting policies, so
        // creating them iteratively should be ok. If there is a performance issue here,
        // consider batching or performing dump/restore via the SQL dump/restore feature.
        for (SettingProto.SettingPolicy settingPolicy : settingPolicies) {
            final SettingPolicyInfo settingPolicyInfo = settingPolicy.getInfo();

            final SettingPolicy jooqSettingPolicy = new SettingPolicy(
                settingPolicy.getId(),
                settingPolicyInfo.getName(),
                settingPolicyInfo.getEntityType(),
                settingPolicyInfo,
                SettingPolicyTypeConverter.typeToDb(settingPolicy.getSettingPolicyType()),
                settingPolicyInfo.getTargetId());
            dslContext.newRecord(SETTING_POLICY, jooqSettingPolicy).store();
        }

        groupToSettingPolicyIndex.update(settingPolicies);
    }

    /**
     * Handle the event of a group being deleted. When a user-created group is removed, we'll remove
     * references to the group being removed from all user-created {@link SettingPolicy} instances.
     *
     * @param deletedGroupId the group that was removed.
     * @param context Jooq context to use for transactional purposes
     * @return the number of setting policies affected by the change.
     */
    public int onGroupDeleted(@Nonnull DSLContext context, long deletedGroupId) {
        // get the set of setting policies that included the specified group id.
        final Map<Long, List<SettingProto.SettingPolicy>> policiesForGroup =
                getSettingPoliciesForGroups(Collections.singleton(deletedGroupId));

        if (policiesForGroup.isEmpty()) {
            // no policies need updating.
            return 0;
        }

        List<SettingProto.SettingPolicy> policies = policiesForGroup.get(deletedGroupId);
        int policiesUpdated = 0;
        for (final SettingProto.SettingPolicy policy : policies) {
            if (! (policy.hasInfo() && policy.getInfo().hasScope())) {
                continue;
            }

            // only handle user policies. discovered policies will be updated via the upload
            // mechanism, and default policies aren't scoped anyways.
            if (policy.getSettingPolicyType() != Type.USER) {
                continue;
            }

            // remove any references to the group in this policy.
            SettingProto.SettingPolicyInfo.Builder policyInfoBuilder = policy.getInfo().toBuilder();
            List<Long> originalGroups = policyInfoBuilder.getScopeBuilder().getGroupsList();
            List<Long> modifiedGroups = originalGroups.stream()
                    .filter(groupId -> ! groupId.equals(deletedGroupId))
                    .collect(Collectors.toList());
            boolean wasUpdated = originalGroups.size() != modifiedGroups.size();

            if (wasUpdated) {
                policiesUpdated += 1;
                // update the policy
                try {
                    policyInfoBuilder.getScopeBuilder().clearGroups()
                            .addAllGroups(modifiedGroups);
                    updateSettingPolicy(context, policy.getId(), policyInfoBuilder.build());
                } catch (SettingPolicyNotFoundException | DuplicateNameException | InvalidItemException e) {
                    // not a huge deal -- log a warning and move on
                    logger.warn("Attempted to update policy id {}, but received: ",
                            policy.getId(), e.getMessage());
                }
            }
        }

        if (policiesUpdated > 0) {
            logger.info("Removed references to group {} from {} SettingPolicies.",
                    deletedGroupId, policiesUpdated);
            return policiesUpdated;
        }
        return 0;
    }

    /**
     * Convert a {@link SettingPolicy} retrieved from the database into a {@link SettingPolicy}.
     *
     * @param jooqSettingPolicy The setting policy retrieved from the database via jooq.
     * @return An equivalent {@link SettingPolicy}.
     */
    @Nonnull
    private SettingProto.SettingPolicy toSettingPolicy(
            @Nonnull final SettingPolicy jooqSettingPolicy) {
        return SettingProto.SettingPolicy.newBuilder()
                .setId(jooqSettingPolicy.getId())
                .setSettingPolicyType(
                        SettingPolicyTypeConverter.typeFromDb(jooqSettingPolicy.getPolicyType()))
                .setInfo(jooqSettingPolicy.getSettingPolicyData())
                .build();
    }

    /**
     * Assign the specified schedule to the setting policy.
     *
     * @param context Jooq context to use for transactional purposes
     * @param settingPolicyId {@link SettingPolicy} id
     * @param scheduleId {@link com.vmturbo.group.db.tables.pojos.Schedule} id
     * @return Number of modifies records
     */
    private int assignSchedule(@Nonnull final DSLContext context,
                               final long settingPolicyId, final long scheduleId) {
        Result<SettingPolicyScheduleRecord> settingPolicySchedules =
            context.selectFrom(SETTING_POLICY_SCHEDULE)
            .where(SETTING_POLICY_SCHEDULE.SETTING_POLICY_ID.eq(settingPolicyId))
            .fetch();
        // For now we support at most one schedule per setting policy
        if (settingPolicySchedules.size() > 1) {
            logger.error("Found {} schedules for setting policy id {}, expected at most one schedule",
            () -> settingPolicySchedules.size(), () -> settingPolicyId);
            throw new IllegalStateException("More than one schedule found for setting policy");
        }
        if (settingPolicySchedules.isEmpty()) {
            final SettingPolicySchedule settingPolicySchedule = new SettingPolicySchedule(
                settingPolicyId, scheduleId);
            return context.newRecord(SETTING_POLICY_SCHEDULE, settingPolicySchedule).store();
        } else {
            SettingPolicyScheduleRecord existingRecord = settingPolicySchedules.get(0);
            if (scheduleId != existingRecord.getScheduleId()) {
                int numUpdated = context.update(SETTING_POLICY_SCHEDULE)
                    .set(SETTING_POLICY_SCHEDULE.SCHEDULE_ID, scheduleId)
                    .where(SETTING_POLICY_SCHEDULE.SETTING_POLICY_ID.eq(settingPolicyId))
                    .execute();
                if (numUpdated == 0) {
                    throw new IllegalStateException("Failed to update setting_policy_schedule record" +
                        " for setting policy ID " + settingPolicyId);
                }
                return numUpdated;
            }
            return 0;
        }
    }

    /**
     * If setting policy has schedule assigned to it, add the schedule to the {@link SettingPolicyInfo}.
     * @param context Jooq context to use for transactional purposes
     * @param settingPolicies {@link com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy}
     * to which the schedule is added.
     * @return Stream with updated {@link com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy}
     * objects.
     */
    private Stream<SettingProto.SettingPolicy> addScheduleToSettingPolicy(@Nonnull final DSLContext context,
                    @Nonnull Stream<SettingProto.SettingPolicy> settingPolicies) {
        final Map<Long, SettingProto.SettingPolicy> sPolicyMap = settingPolicies.collect(
            Collectors.toMap(SettingProto.SettingPolicy::getId, Function.identity()));
        final Map<Long, List<Long>> settingPolicySchedules = context.selectFrom(SETTING_POLICY_SCHEDULE)
            .where(SETTING_POLICY_SCHEDULE.SETTING_POLICY_ID.in(sPolicyMap.keySet()))
            .fetchGroups(SETTING_POLICY_SCHEDULE.SETTING_POLICY_ID, SETTING_POLICY_SCHEDULE.SCHEDULE_ID);
        // For now we support at most one schedule per setting policy
        settingPolicySchedules.forEach((settingPolicyId, scheduleIds) -> {
            if (scheduleIds.size() > 1) {
                logger.error("Found {} schedules for setting policy id {}, expected at most one schedule",
                    () -> scheduleIds.size(), () -> settingPolicyId);
                throw new IllegalStateException("More than one schedule found for setting policy " +
                    settingPolicyId);
            }
            if (scheduleIds.size() > 0) {
                final long scheduleId = scheduleIds.get(0);
                sPolicyMap.compute(settingPolicyId, (sPolicyId, sPolicy) -> sPolicy.toBuilder()
                    .setInfo(sPolicy.getInfo().toBuilder().setScheduleId(scheduleId).build())
                    .build());
            }
        });

        return sPolicyMap.values().stream();
    }

    /**
     * Convert a {@link SettingPolicyRecord} retrieved from the database into a
     * {@link SettingPolicy}.
     *
     * @param jooqRecord The record retrieved from the database via jooq.
     * @return An equivalent {@link SettingPolicy}.
     */
    @Nonnull
    private SettingProto.SettingPolicy toSettingPolicy(
            @Nonnull final SettingPolicyRecord jooqRecord) {
        return SettingProto.SettingPolicy.newBuilder()
                .setId(jooqRecord.getId())
                .setSettingPolicyType(
                        SettingPolicyTypeConverter.typeFromDb(jooqRecord.getPolicyType()))
                .setInfo(jooqRecord.getSettingPolicyData())
                .build();
    }


    /**
     * Convert a {@link SettingPolicy} retrieved from the database into a {@link SettingPolicy}.
     *
     * @param jooqSettingPolicy The setting policy retrieved from the database via jooq.
     * @param scheduleId The ID of the schedule used by the policy.
     * @return An equivalent {@link SettingPolicy}.
     */
    @Nonnull
    private SettingProto.SettingPolicy toSettingPolicyWithSchedule(
        @Nonnull final SettingPolicy jooqSettingPolicy, final long scheduleId) {
        final SettingPolicyInfo settingPolicyInfo = jooqSettingPolicy.getSettingPolicyData()
            .toBuilder()
            .setScheduleId(scheduleId)
            .build();
        return SettingProto.SettingPolicy.newBuilder()
            .setId(jooqSettingPolicy.getId())
            .setSettingPolicyType(
                SettingPolicyTypeConverter.typeFromDb(jooqSettingPolicy.getPolicyType()))
            .setInfo(settingPolicyInfo)
            .build();
    }
}
