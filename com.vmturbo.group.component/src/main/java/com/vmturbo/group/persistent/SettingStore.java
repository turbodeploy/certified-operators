package com.vmturbo.group.persistent;

import static com.vmturbo.group.db.Tables.GLOBAL_SETTINGS;
import static com.vmturbo.group.db.Tables.SETTING_POLICY;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.InsertValuesStep4;
import org.jooq.Record1;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.group.GroupDTO.DiscoveredSettingPolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.group.db.enums.SettingPolicyPolicyType;
import com.vmturbo.group.db.tables.pojos.SettingPolicy;
import com.vmturbo.group.db.tables.records.SettingPolicyRecord;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.persistent.TargetCollectionUpdate.TargetPolicyUpdate;
import com.vmturbo.group.persistent.TargetCollectionUpdate.TargetSettingPolicyUpdate;

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

    private final IdentityProvider identityProvider;

    private final SettingPolicyValidator settingPolicyValidator;

    /**
     * Create a new SettingStore.
     *
     * @param settingSpecStore The source, providing the {@link SettingSpec} definitions.
     * @param dslContext A context with which to interact with the underlying datastore.
     * @param identityProvider The identity provider used to assign OIDs.
     */
    public SettingStore(@Nonnull final SettingSpecStore settingSpecStore,
            @Nonnull final DSLContext dslContext, @Nonnull final IdentityProvider identityProvider,
            @Nonnull final SettingPolicyValidator settingPolicyValidator) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.identityProvider = Objects.requireNonNull(identityProvider);

        // read the json config file, and create setting spec instances in memory
        this.settingPolicyValidator = Objects.requireNonNull(settingPolicyValidator);
    }

    /**
     * This is the internal version of {@link SettingStore#createUserSettingPolicy(SettingPolicyInfo)}
     * which allows the specification of the setting policy's type. External classes should NOT
     * use this method.
     *
     * @param settingPolicyInfo See {@link SettingStore#createUserSettingPolicy(SettingPolicyInfo)}.
     * @param type The type of the policy to create.
     * @return See {@link SettingStore#createUserSettingPolicy(SettingPolicyInfo)}.
     * @throws InvalidSettingPolicyException See {@link SettingStore#createUserSettingPolicy(SettingPolicyInfo)}.
     * @throws DuplicateNameException See {@link SettingStore#createUserSettingPolicy(SettingPolicyInfo)}.
     * @throws DataAccessException See {@link SettingStore#createUserSettingPolicy(SettingPolicyInfo)}.
     */
    @Nonnull
    private SettingProto.SettingPolicy internalCreateSettingPolicy(
            @Nonnull final SettingPolicyInfo settingPolicyInfo,
            @Nonnull final SettingProto.SettingPolicy.Type type)
            throws InvalidSettingPolicyException, DuplicateNameException, DataAccessException {
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

                final SettingPolicy jooqSettingPolicy = new SettingPolicy(
                    identityProvider.next(),
                    settingPolicyInfo.getName(),
                    settingPolicyInfo.getEntityType(),
                    settingPolicyInfo,
                    SettingPolicyTypeConverter.typeToDb(type),
                    settingPolicyInfo.getTargetId());
                context.newRecord(SETTING_POLICY, jooqSettingPolicy).store();
                return toSettingPolicy(jooqSettingPolicy);
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
     * @throws InvalidSettingPolicyException If the input setting policy is not valid.
     * @throws DuplicateNameException If there is already a setting policy with the same name as
     * the input setting policy.
     * @throws DataAccessException If there is another problem connecting to the database.
     */
    @Nonnull
    public SettingProto.SettingPolicy createUserSettingPolicy(
            @Nonnull final SettingPolicyInfo settingPolicyInfo)
            throws InvalidSettingPolicyException, DuplicateNameException {
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
     * @throws InvalidSettingPolicyException If the input setting policy is not valid.
     * @throws DuplicateNameException If there is already a setting policy with the same name as
     * the input setting policy.
     * @throws DataAccessException If there is another problem connecting to the database.
     */
    @Nonnull
    public SettingProto.SettingPolicy createDefaultSettingPolicy(
            @Nonnull final SettingPolicyInfo settingPolicyInfo)
            throws InvalidSettingPolicyException, DuplicateNameException {
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
     * @throws InvalidSettingPolicyException If the input setting policy is not valid.
     * @throws DuplicateNameException If there is already a setting policy with the same name as
     * the input setting policy.
     * @throws DataAccessException If there is another problem connecting to the database.
     */
    @Nonnull
    public SettingProto.SettingPolicy createDiscoveredSettingPolicy(
            @Nonnull final SettingPolicyInfo settingPolicyInfo)
            throws InvalidSettingPolicyException, DuplicateNameException {
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
     * @throws InvalidSettingPolicyException If the update attempt would violate constraints on
     * the setting policy.
     * @throws DuplicateNameException If there is already a setting policy with the same name as
     * the new info (other than the policy to update).
     * @throws DataAccessException If there is an error interacting with the database.
     */
    @Nonnull
    public SettingProto.SettingPolicy updateSettingPolicy(final long id,
            @Nonnull final SettingPolicyInfo newInfo)
            throws SettingPolicyNotFoundException, InvalidSettingPolicyException,
            DuplicateNameException, DataAccessException {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);

                final SettingPolicyRecord record =
                        context.fetchOne(SETTING_POLICY, SETTING_POLICY.ID.eq(id));
                if (record == null) {
                    throw new SettingPolicyNotFoundException(id);
                }

                // Explicitly search for an existing policy with the same name that's NOT
                // the policy being edited. We do this because we want to know
                // know when to throw a DuplicateNameException as opposed to a generic
                // DataIntegrityException.
                final Record1<Long> existingId = context.select(SETTING_POLICY.ID)
                        .from(SETTING_POLICY)
                        .where(SETTING_POLICY.NAME.eq(newInfo.getName()))
                        .and(SETTING_POLICY.ID.ne(id))
                        .fetchOne();
                if (existingId != null) {
                    throw new DuplicateNameException(existingId.value1(), newInfo.getName());
                }

                final SettingProto.SettingPolicy.Type type =
                        SettingPolicyTypeConverter.typeFromDb(record.getPolicyType());

                // Validate the setting policy.
                // This should throw an exception if it's invalid.
                settingPolicyValidator.validateSettingPolicy(newInfo, type);

                // Additional update-only validation for default policies
                // to ensure certain fields are not changed.
                if (type.equals(Type.DEFAULT)) {
                    // For default setting policies we don't allow changes to names
                    // or entity types.
                    if (newInfo.getEntityType() != record.getEntityType()) {
                        throw new InvalidSettingPolicyException("Illegal attempt to change the " +
                                " entity type of a default setting policy.");
                    }
                    if (!newInfo.getName().equals(record.getName())) {
                        throw new InvalidSettingPolicyException("Illegal attempt to change the " +
                                " name of a default setting policy.");
                    }
                }

                if (type.equals(Type.DISCOVERED)) {
                    throw new InvalidSettingPolicyException("Illegal attempt to modify a " +
                        "discovered setting policy.");
                }

                record.setEntityType(newInfo.getEntityType());
                record.setName(newInfo.getName());
                record.setSettingPolicyData(newInfo);
                record.setTargetId(newInfo.getTargetId());
                final int modifiedRecords = record.update();
                if (modifiedRecords == 0) {
                    // This should never happen, because we overwrote fields in the record,
                    // and update() should always execute an UPDATE statement if some fields
                    // got overwritten.
                    throw new IllegalStateException("Failed to update record.");
                }
                return toSettingPolicy(record);
            });
        } catch (DataAccessException e) {
            // Jooq will rethrow exceptions thrown in the transactionResult call
            // wrapped in a DataAccessException. Check to see if that's why the transaction failed.
            if (e.getCause() instanceof DuplicateNameException) {
                throw (DuplicateNameException)e.getCause();
            } else if (e.getCause() instanceof SettingPolicyNotFoundException) {
                throw (SettingPolicyNotFoundException)e.getCause();
            } else if (e.getCause() instanceof InvalidSettingPolicyException) {
                throw (InvalidSettingPolicyException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Delete a setting policy.
     *
     * @param id The ID of the setting policy to delete.
     * @param initializedByUser true if initiated by user, false if initiated by some part of the system (ie. a
     *                          {@link TargetSettingPolicyUpdate}.
     * @return The deleted {@link SettingProto.SettingPolicy}.
     * @throws SettingPolicyNotFoundException If the setting policy does not exist.
     * @throws InvalidSettingPolicyException If the setting policy is a DEFAULT policy or it is a user update
     *                                       attempting to delete a DISCOVERED policy.
     */
    @Nonnull
    public SettingProto.SettingPolicy deleteSettingPolicy(final long id,
                                                          boolean initializedByUser)
            throws SettingPolicyNotFoundException, InvalidSettingPolicyException {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);

                final SettingPolicyRecord record =
                        context.fetchOne(SETTING_POLICY, SETTING_POLICY.ID.eq(id));
                if (record == null) {
                    throw new SettingPolicyNotFoundException(id);
                }

                if (record.getPolicyType().equals(SettingPolicyPolicyType.default_)) {
                    throw new InvalidSettingPolicyException("Cannot delete default setting policies.");
                } else if (record.getPolicyType().equals(SettingPolicyPolicyType.discovered)
                    && initializedByUser) {
                    throw new InvalidSettingPolicyException("Cannot delete discovered setting policies.");
                }

                final int modifiedRecords = record.delete();
                if (modifiedRecords == 0) {
                    // This should never happen, because the record definitely exists if we
                    // got to this point.
                    throw new IllegalStateException("Failed to delete record.");
                }

                return toSettingPolicy(record);
            });
        } catch (DataAccessException e) {
            // Jooq will rethrow exceptions thrown in the transactionResult call
            // wrapped in a DataAccessException. Check to see if that's why the transaction failed.
            final Throwable cause = e.getCause();
            if (cause instanceof SettingPolicyNotFoundException) {
                throw (SettingPolicyNotFoundException)cause;
            } else if (cause instanceof InvalidSettingPolicyException) {
                throw (InvalidSettingPolicyException)cause;
            } else {
                throw e;
            }
        }
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
        return dslContext.selectFrom(SETTING_POLICY)
                .where(filter.getConditions())
                .fetch()
                .into(SettingPolicy.class)
                .stream()
                .map(this::toSettingPolicy);
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
     * @param targetId The ID of the target that discovered the setting policies.
     * @param settingPolicyInfos The new set of {@link DiscoveredSettingPolicyInfo}s.
     * @param groupOids a mapping of policy key (name) to policy OID
     * @throws DatabaseException If there is an error interacting with the database.
     */
    public void updateTargetSettingPolicies(long targetId,
                                            @Nonnull final List<DiscoveredSettingPolicyInfo> settingPolicyInfos,
                                            @Nonnull final Map<String, Long> groupOids) throws DatabaseException {
        logger.info("Updating setting policies discovered by {}. Got {} setting policies.",
            targetId, settingPolicyInfos.size());

        DiscoveredSettingPoliciesMapper mapper = new DiscoveredSettingPoliciesMapper(groupOids);
        List<SettingPolicyInfo> discoveredSettingPolicies = settingPolicyInfos.stream()
            .map(info -> mapper.mapToSettingPolicyInfo(info, targetId))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
        final TargetSettingPolicyUpdate update = new TargetSettingPolicyUpdate(targetId, identityProvider,
            discoveredSettingPolicies, getSettingPoliciesDiscoveredByTarget(targetId));
        update.apply(this::storeDiscoveredSettingPolicy, this::deleteSettingPolicyForTargetUpdate);
        logger.info("Finished updating discovered groups.");
    }

    @VisibleForTesting
    @Nonnull
    Collection<SettingProto.SettingPolicy> getSettingPoliciesDiscoveredByTarget(final long targetId)
        throws DatabaseException {
        try {
            return getSettingPolicies(SettingPolicyFilter.newBuilder().withTargetId(targetId).build())
                .collect(Collectors.toList());
        } catch (DataAccessException dae) {
            throw new DatabaseException(
                "Unable to retrieve setting policies discovered by target " + targetId, dae);
        }
    }

    private void storeDiscoveredSettingPolicy(@Nonnull final SettingProto.SettingPolicy settingPolicy)
        throws DatabaseException {
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
            dslContext.newRecord(SETTING_POLICY, jooqSettingPolicy).store();
        } catch (InvalidSettingPolicyException | DataAccessException e) {
            throw new DatabaseException("Unable to store discovered setting policy " + settingPolicy, e);
        }
    }

    private void deleteSettingPolicyForTargetUpdate(final long settingPolicyId) throws DatabaseException {
        try {
            deleteSettingPolicy(settingPolicyId, false);
        } catch (SettingPolicyNotFoundException | InvalidSettingPolicyException e) {
            throw new DatabaseException(
                "Unable to delete discovered setting policy with id " + settingPolicyId, e);
        }
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
    public List<String> collectDiags() throws DiagnosticsException {
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();

        try {
            return Arrays.asList(
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
        dslContext.truncate(SETTING_POLICY).execute();
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
}
