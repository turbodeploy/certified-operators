package com.vmturbo.group.persistent;

import static com.vmturbo.group.db.Tables.SETTING_POLICY;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpecCollection;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.group.db.tables.pojos.SettingPolicy;
import com.vmturbo.group.db.tables.records.SettingPolicyRecord;
import com.vmturbo.group.identity.IdentityProvider;

/**
 * The {@link SettingStore} class is used to store settings-related objects, and retrieve them
 * in an efficient way.
 */
public class SettingStore {

    private final Logger logger = LogManager.getLogger();

    // map to store the setting spec, indexed by setting name
    private final ImmutableMap<String, SettingSpec> settingSpecMap;

    /**
     * A DSLContext with which to interact with an underlying persistent datastore.
     */
    private final DSLContext dsl;

    private final IdentityProvider identityProvider;

    private final SettingPolicyValidator settingPolicyValidator;

    /**
     * Create a new SettingStore.
     * @param settingSpecJsonFile The name of the file containing the {@link SettingSpec} definitions.
     * @param dsl A context with which to interact with the underlying datastore.
     * @param identityProvider The identity provider used to assign OIDs.
     */
    public SettingStore(@Nonnull final String settingSpecJsonFile,
                        @Nonnull final DSLContext dsl,
                        @Nonnull final IdentityProvider identityProvider,
                        @Nonnull final GroupStore groupStore) {
        this.dsl = Objects.requireNonNull(dsl);
        this.identityProvider = Objects.requireNonNull(identityProvider);

        // read the json config file, and create setting spec instances in memory
        final Map<String, SettingSpec> loadedMap =
                parseSettingSpecJsonFile(Objects.requireNonNull(settingSpecJsonFile));

        settingSpecMap = ImmutableMap.<String, SettingSpec>builder().putAll(loadedMap).build();
        settingPolicyValidator = new DefaultSettingPolicyValidator(this::getSettingSpec, groupStore);
    }

    /**
     * A constructor for testing that allows injection of the {@link SettingPolicyValidator}.
     */
    @VisibleForTesting
    public SettingStore(@Nonnull final String settingSpecJsonFile,
                        @Nonnull final DSLContext dsl,
                        @Nonnull final IdentityProvider identityProvider,
                        @Nonnull final SettingPolicyValidator settingPolicyValidator) {
        this.dsl = Objects.requireNonNull(dsl);
        this.identityProvider = Objects.requireNonNull(identityProvider);

        // read the json config file, and create setting spec instances in memory
        final Map<String, SettingSpec> loadedMap =
                parseSettingSpecJsonFile(Objects.requireNonNull(settingSpecJsonFile));

        settingSpecMap = ImmutableMap.<String, SettingSpec>builder().putAll(loadedMap).build();
        this.settingPolicyValidator = Objects.requireNonNull(settingPolicyValidator);
    }


    /**
     * Gets the {@link SettingSpec} by name.
     *
     * @param specName The setting name.
     * @return The selected {@link SettingSpec}, or an empty optional if the spec doesn't exist in
     *         the store.
     */
    @Nonnull
    public Optional<SettingSpec> getSettingSpec(@Nonnull final String specName) {
        return Optional.ofNullable(settingSpecMap.get(specName));
    }

    /**
     * Gets all the {@link SettingSpec}.
     *
     * @return a collection of {@link SettingSpec}
     */
    public Collection<SettingSpec> getAllSettingSpec() {
        return settingSpecMap.values();
    }

    /**
     * Persist a new SettingPolicy in the {@link SettingStore} based on the info in the
     * {@link SettingPolicyInfo}.
     *
     * Note that setting policies must have unique names.
     *
     * @param settingPolicyInfo The info to be applied to the SettingPolicy.
     * @return A SettingPolicy whose info matches the input {@link SettingPolicyInfo}.
     *         An ID will be assigned to this policy.
     * @throws InvalidSettingPolicyException If the input setting policy is not valid.
     * @throws DuplicateNameException If there is already a setting policy with the same name as
     *                                the input setting policy.
     */
    @Nonnull
    public SettingProto.SettingPolicy createSettingPolicy(
                @Nonnull final SettingPolicyInfo settingPolicyInfo)
            throws InvalidSettingPolicyException, DuplicateNameException {

        // Validate before doing anything.
        settingPolicyValidator.validateSettingPolicy(settingPolicyInfo);

        try {
            return dsl.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                // Explicitly search for an existing policy with the same name, so that we
                // know when to throw a DuplicateNameException as opposed to a generic
                // DataIntegrityException.
                final Record1<Long> existingId =
                        context.select(SETTING_POLICY.ID).from(SETTING_POLICY)
                                .where(SETTING_POLICY.NAME.eq(settingPolicyInfo.getName()))
                                .fetchOne();
                if (existingId != null) {
                    throw new DuplicateNameException(existingId.value1(), settingPolicyInfo.getName());
                }

                final SettingPolicy jooqSettingPolicy = new SettingPolicy(identityProvider.next(),
                        settingPolicyInfo.getName(),
                        settingPolicyInfo.getEntityType(),
                        settingPolicyInfo);
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
     * Get a setting policy by its unique OID.
     *
     * @param oid The OID (object id) of the setting policy to retrieve.
     * @return The {@link SettingProto.SettingPolicy} associated with the name, or an empty policy.
     */
    public Optional<SettingProto.SettingPolicy> getSettingPolicy(final long oid) {
        final SettingPolicyRecord jooqSettingPolicy = dsl.selectFrom(SETTING_POLICY)
                .where(SETTING_POLICY.ID.eq(oid))
                .fetchOne();
        return Optional.ofNullable(jooqSettingPolicy)
                .map(record -> record.into(SettingPolicy.class))
                .map(this::toSettingPolicy);
    }

    /**
     * Get a setting policy by its unique name.
     *
     * @param name The name of the setting policy to retrieve.
     * @return The {@link SettingProto.SettingPolicy} associated with the name, or an empty
     *         policy.
     */
    public Optional<SettingProto.SettingPolicy> getSettingPolicy(@Nonnull final String name) {
        final SettingPolicyRecord jooqSettingPolicy = dsl.selectFrom(SETTING_POLICY)
                .where(SETTING_POLICY.NAME.eq(name))
                .fetchOne();
        return Optional.ofNullable(jooqSettingPolicy)
            .map(record -> record.into(SettingPolicy.class))
            .map(this::toSettingPolicy);
    }

    /**
     * Get all defined setting policies.
     *
     * @return A stream of {@link SettingProto.SettingPolicy} objects, one for every setting policy
     *         known to the {@link SettingStore}.
     */
    @Nonnull
    public Stream<SettingProto.SettingPolicy> getAllSettingPolicies() {
        return dsl.selectFrom(SETTING_POLICY).fetch().into(SettingPolicy.class).stream()
            .map(this::toSettingPolicy);
    }

    /**
     * Parses the json config file, in order to deserialize {@link SettingSpec} objects, and add
     * them to the store.
     *
     * @param settingSpecJsonFile file to parse, containing the setting spec definition
     * @return collection of {@link SettingSpec} objects loaded from the file, indexed by name
     */
    @Nonnull
    private Map<String, SettingSpec> parseSettingSpecJsonFile(
            @Nonnull final String settingSpecJsonFile) {
        logger.debug("Loading setting spec json file: {}", settingSpecJsonFile);
        SettingSpecCollection.Builder collectionBuilder = SettingSpecCollection.newBuilder();

        // open the file and create a reader for it
        try (InputStream inputStream = Thread.currentThread()
            .getContextClassLoader().getResourceAsStream(settingSpecJsonFile);
             InputStreamReader reader = new InputStreamReader(inputStream);
        ) {

            // parse the json file
            JsonFormat.parser().merge(reader, collectionBuilder);

        } catch (IOException e) {
            logger.error("Unable to load SettingSpecs from Json file: {}", settingSpecJsonFile, e);
        }

        final SettingSpecCollection settingSpecCollection = collectionBuilder.build();

        return settingSpecCollection.getSettingSpecsList().stream()
            .collect(Collectors.toMap(SettingSpec::getName, Function.identity()));
    }

    /**
     * Convert a {@link SettingPolicy} retrieved from the database into a {@link SettingPolicy}.
     *
     * @param jooqSettingPolicy The setting policy retrieved from the database via jooq.
     * @return An equivalent {@link SettingPolicy}.
     */
    @Nonnull
    private SettingProto.SettingPolicy toSettingPolicy(@Nonnull final SettingPolicy jooqSettingPolicy) {
        return SettingProto.SettingPolicy.newBuilder()
            .setId(jooqSettingPolicy.getId())
            .setInfo(jooqSettingPolicy.getSettingPolicyData())
            .build();
    }

    /**
     * An interface to abstract away the validation of setting policies in order
     * to make unit testing easier.
     */
    @VisibleForTesting
    @FunctionalInterface
    interface SettingPolicyValidator {
        /**
         * Verify that a {@link SettingPolicyInfo} meets all the requirements - e.g. that all
         * setting names are valid, that the setting values match the expected types, that all
         * required fields are set, etc.
         *
         * @param settingPolicyInfo The {@link SettingPolicyInfo} to validate.
         * @throws InvalidSettingPolicyException If the policy is invalid.
         */
        void validateSettingPolicy(@Nonnull final SettingPolicyInfo settingPolicyInfo)
                throws InvalidSettingPolicyException;
    }

    /**
     * A pretty name for a Function<String, Optional<SettingSpec>> for use by
     * {@link DefaultSettingPolicyValidator}. Useful for unit testing.
     */
    @VisibleForTesting
    @FunctionalInterface
    interface SettingSpecStore {
        Optional<SettingSpec> getSpec(@Nonnull final String name);
    }

    /**
     * The default implementation of {@link SettingPolicyValidator}. This should be the
     * only implementation!
     */
    @VisibleForTesting
    static class DefaultSettingPolicyValidator implements SettingPolicyValidator {
        private final SettingSpecStore settingSpecStore;
        private final GroupStore groupStore;

        @VisibleForTesting
        DefaultSettingPolicyValidator(@Nonnull final SettingSpecStore settingSpecStore,
                                      @Nonnull final GroupStore groupStore) {
            this.settingSpecStore = Objects.requireNonNull(settingSpecStore);
            this.groupStore = Objects.requireNonNull(groupStore);
        }

        /**
         * {@inheritDoc}
         */
        public void validateSettingPolicy(@Nonnull final SettingPolicyInfo settingPolicyInfo)
                throws InvalidSettingPolicyException {
            // We want to collect everything wrong with the input and put that
            // into the description message.
            final List<String> errors = new LinkedList<>();
            if (!settingPolicyInfo.hasName()) {
                errors.add("Setting policy must have a name!");
            }

            if (!settingPolicyInfo.hasEntityType()) {
                errors.add("Setting policy must have an entity type!");
            }

            if (settingPolicyInfo.getSettingsList().stream()
                    .anyMatch(setting -> !setting.hasSettingSpecName())) {
                errors.add("Setting policy has unnamed settings!");
            }

            // If the setting policy is scoped to a set of groups, make sure
            // the groups exist, and are compatible with the policy info.
            if (settingPolicyInfo.hasScope()) {
                try {
                    final Map<Long, Optional<Group>> groupMap =
                            groupStore.getGroups(settingPolicyInfo.getScope().getGroupsList());
                    groupMap.forEach((groupId, groupOpt) -> {
                        if (groupOpt.isPresent()) {
                            final Group group = groupOpt.get();
                            final int policyEntityType = settingPolicyInfo.getEntityType();
                            final int groupEntityType = group.getInfo().getEntityType();
                            if (groupEntityType != policyEntityType) {
                                errors.add("Group " + group.getId() + " with entity type " +
                                        groupEntityType + " does not match entity type " +
                                        policyEntityType + " of the setting policy");
                            }
                        } else {
                            errors.add("Group " + groupId + "for setting policy not found.");
                        }
                    });
                } catch (DatabaseException e) {
                    errors.add("Unable to fetch groups for setting policy due to exception: " +
                            e.getMessage());
                }
            }

            errors.addAll(validateReferencedSpecs(settingPolicyInfo));

            if (!errors.isEmpty()) {
                throw new InvalidSettingPolicyException("Invalid setting policy: " +
                        settingPolicyInfo.getName() + "\n" +
                        StringUtils.join(errors, "\n"));
            }
        }

        @Nonnull
        private List<String> validateReferencedSpecs(
                @Nonnull final SettingPolicyInfo settingPolicyInfo) {
            // We want to collect everything wrong with the input.
            final List<String> errors = new LinkedList<>();

            final Map<Setting, Optional<SettingSpec>> referencedSpecs =
                    settingPolicyInfo.getSettingsList().stream()
                        .filter(Setting::hasSettingSpecName)
                        .collect(Collectors.toMap(Function.identity(),
                                setting -> settingSpecStore.getSpec(setting.getSettingSpecName())));
            referencedSpecs.forEach((setting, specOpt) -> {
                if (!specOpt.isPresent()) {
                    errors.add("Setting " + setting.getSettingSpecName() + " does not exist!");
                } else {
                    final String name = setting.getSettingSpecName();
                    final SettingSpec spec = specOpt.get();

                    if (!spec.hasEntitySettingSpec()) {
                        errors.add("Setting " + name + " is not an entity setting, " +
                                "and can't be overwritten by a setting policy!");
                    } else {
                        // Make sure that the input policy info matches any
                        // entity type restrictions in the setting scope.
                        final int entityType = settingPolicyInfo.getEntityType();
                        final EntitySettingScope scope =
                                spec.getEntitySettingSpec().getEntitySettingScope();
                        if (scope.hasEntityTypeSet() &&
                                !scope.getEntityTypeSet().getEntityTypeList().contains(entityType)) {
                            errors.add("Entity type " + entityType +
                                " not supported by setting spec " + name + ". Must be one of: " +
                                StringUtils.join(scope.getEntityTypeSet().getEntityTypeList(), ", "));
                        }
                    }

                    // Make sure the value of the setting matches the value type in
                    // the related setting spec.
                    final boolean mismatchedValTypes;
                    switch (setting.getValueCase()) {
                        case BOOLEAN_SETTING_VALUE:
                            mismatchedValTypes = !spec.hasBooleanSettingValueType();
                            break;
                        case NUMERIC_SETTING_VALUE:
                            mismatchedValTypes = !spec.hasNumericSettingValueType();
                            if (!mismatchedValTypes) {
                                final NumericSettingValueType type = spec.getNumericSettingValueType();
                                final float value = setting.getNumericSettingValue().getValue();
                                if (type.hasMin() && value < type.getMin()) {
                                    errors.add("Value " + value + " for setting " + name +
                                            " less than minimum!");
                                }
                                if (type.hasMax() && value > type.getMax()) {
                                    errors.add("Value " + value + " for setting " + name +
                                            " more than maximum!");
                                }
                            }
                            break;
                        case STRING_SETTING_VALUE:
                            mismatchedValTypes = !spec.hasStringSettingValueType();
                            if (!mismatchedValTypes) {
                                final StringSettingValueType type = spec.getStringSettingValueType();
                                final String value = setting.getStringSettingValue().getValue();
                                if (type.hasValidationRegex() &&
                                        !Pattern.compile(type.getValidationRegex())
                                                .matcher(value).matches()) {
                                    errors.add("Value " + value + " does not match validation regex " +
                                            type.getValidationRegex());
                                }
                            }
                            break;
                        case ENUM_SETTING_VALUE:
                            mismatchedValTypes = !spec.hasEnumSettingValueType();
                            if (!mismatchedValTypes) {
                                final EnumSettingValueType type = spec.getEnumSettingValueType();
                                final String value = setting.getEnumSettingValue().getValue();
                                if (!type.getEnumValuesList().contains(value)) {
                                    errors.add("Value " + value + " is not in the allowable list: " +
                                            StringUtils.join(type.getEnumValuesList(), ", "));
                                }
                            }
                            break;
                        default:
                            mismatchedValTypes = true;
                            break;
                    }
                    if (mismatchedValTypes) {
                        errors.add("Mismatched value. Got " + setting.getValueCase()
                                + " and expected " + spec.getSettingValueTypeCase());
                    }
                }
            });
            return errors;
        }
    }
}
