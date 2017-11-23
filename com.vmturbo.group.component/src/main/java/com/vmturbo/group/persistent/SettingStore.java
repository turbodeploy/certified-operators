package com.vmturbo.group.persistent;

import static com.vmturbo.group.db.Tables.SETTING_POLICY;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.group.db.enums.SettingPolicyPolicyType;
import com.vmturbo.group.db.tables.pojos.SettingPolicy;
import com.vmturbo.group.db.tables.records.SettingPolicyRecord;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * The {@link SettingStore} class is used to store settings-related objects, and retrieve them
 * in an efficient way.
 */
public class SettingStore {

    private final Logger logger = LogManager.getLogger();

    /**
     * A DSLContext with which to interact with an underlying persistent datastore.
     */
    private final DSLContext dsl;

    private final IdentityProvider identityProvider;

    private final SettingPolicyValidator settingPolicyValidator;

    /**
     * Create a new SettingStore.
     *
     * @param settingSpecStore The source, providing the {@link SettingSpec} definitions.
     * @param dsl A context with which to interact with the underlying datastore.
     * @param identityProvider The identity provider used to assign OIDs.
     */
    public SettingStore(@Nonnull final SettingSpecStore settingSpecStore,
            @Nonnull final DSLContext dsl, @Nonnull final IdentityProvider identityProvider,
            @Nonnull final SettingPolicyValidator settingPolicyValidator) {
        this.dsl = Objects.requireNonNull(dsl);
        this.identityProvider = Objects.requireNonNull(identityProvider);

        // read the json config file, and create setting spec instances in memory
        this.settingPolicyValidator = Objects.requireNonNull(settingPolicyValidator);
    }

    /**
     * This is the internal version of {@link SettingStore#createSettingPolicy(SettingPolicyInfo)}
     * which allows the specification of the setting policy's type. External classes should NOT
     * use this method.
     *
     * @param settingPolicyInfo See {@link SettingStore#createSettingPolicy(SettingPolicyInfo)}.
     * @param type The type of the policy to create.
     * @return See {@link SettingStore#createSettingPolicy(SettingPolicyInfo)}.
     * @throws InvalidSettingPolicyException See {@link SettingStore#createSettingPolicy(SettingPolicyInfo)}.
     * @throws DuplicateNameException See {@link SettingStore#createSettingPolicy(SettingPolicyInfo)}.
     * @throws DataAccessException See {@link SettingStore#createSettingPolicy(SettingPolicyInfo)}.
     */
    @Nonnull
    private SettingProto.SettingPolicy internalCreateSettingPolicy(
            @Nonnull final SettingPolicyInfo settingPolicyInfo,
            @Nonnull final SettingProto.SettingPolicy.Type type)
            throws InvalidSettingPolicyException, DuplicateNameException, DataAccessException {
        // Validate before doing anything.
        settingPolicyValidator.validateSettingPolicy(settingPolicyInfo, type);

        try {
            return dsl.transactionResult(configuration -> {
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

                final SettingPolicy jooqSettingPolicy =
                        new SettingPolicy(identityProvider.next(), settingPolicyInfo.getName(),
                                settingPolicyInfo.getEntityType(), settingPolicyInfo,
                                type == Type.DEFAULT ? SettingPolicyPolicyType.default_ :
                                        SettingPolicyPolicyType.user);
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
     * Persist a new SettingPolicy in the {@link SettingStore} based on the info in the
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
    public SettingProto.SettingPolicy createSettingPolicy(
            @Nonnull final SettingPolicyInfo settingPolicyInfo)
            throws InvalidSettingPolicyException, DuplicateNameException {
        return internalCreateSettingPolicy(settingPolicyInfo, Type.USER);
    }

    @Nonnull
    public SettingProto.SettingPolicy createDefaultSettingPolicy(
            @Nonnull final SettingPolicyInfo settingPolicyInfo)
            throws InvalidSettingPolicyException, DuplicateNameException {
        return internalCreateSettingPolicy(settingPolicyInfo, Type.DEFAULT);
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
            return dsl.transactionResult(configuration -> {
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

                record.setEntityType(newInfo.getEntityType());
                record.setName(newInfo.getName());
                record.setSettingPolicyData(newInfo);
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
     * @return The deleted {@link SettingProto.SettingPolicy}.
     * @throws SettingPolicyNotFoundException If the setting policy does not exist.
     * @throws InvalidSettingPolicyException If the setting policy is a DEFAULT policy.
     */
    @Nonnull
    public SettingProto.SettingPolicy deleteSettingPolicy(final long id)
            throws SettingPolicyNotFoundException, InvalidSettingPolicyException {
        try {
            return dsl.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);

                final SettingPolicyRecord record =
                        context.fetchOne(SETTING_POLICY, SETTING_POLICY.ID.eq(id));
                if (record == null) {
                    throw new SettingPolicyNotFoundException(id);
                }

                if (record.getPolicyType().equals(SettingPolicyPolicyType.default_)) {
                    throw new InvalidSettingPolicyException("Cannot delete default policies.");
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
        return dsl.selectFrom(SETTING_POLICY)
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
