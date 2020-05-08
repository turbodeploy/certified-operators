package com.vmturbo.group.setting;

import static com.vmturbo.group.db.Tables.GLOBAL_SETTINGS;
import static com.vmturbo.group.db.Tables.SETTING_POLICY;
import static com.vmturbo.group.db.Tables.SETTING_POLICY_GROUPS;
import static com.vmturbo.group.db.Tables.SETTING_POLICY_SETTING;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Functions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.Status;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Record1;
import org.jooq.Record3;
import org.jooq.TableRecord;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;

import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting.Builder;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting.ValueCase;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.common.ItemNotFoundException.SettingNotFoundException;
import com.vmturbo.group.common.ItemNotFoundException.SettingPolicyNotFoundException;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.enums.SettingPolicyPolicyType;
import com.vmturbo.group.db.tables.pojos.SettingPolicy;
import com.vmturbo.group.db.tables.records.GlobalSettingsRecord;
import com.vmturbo.group.db.tables.records.SettingPolicyGroupsRecord;
import com.vmturbo.group.db.tables.records.SettingPolicyRecord;
import com.vmturbo.group.db.tables.records.SettingPolicySettingOidsRecord;
import com.vmturbo.group.db.tables.records.SettingPolicySettingRecord;
import com.vmturbo.group.db.tables.records.SettingPolicySettingScheduleIdsRecord;
import com.vmturbo.group.service.StoreOperationException;

/**
 * The {@link SettingStore} class is used to store settings-related objects, and retrieve them
 * in an efficient way.
 */
public class SettingStore implements DiagsRestorable {

    /**
     * The file name for the settings dump collected from the {@link SettingStore}.
     * It's a string file, so the "diags" extension is required for compatibility
     * with {@link DiagsZipReader}.
     */
    private static final String SETTINGS_DUMP_FILE = "settings_dump";

    private static final Map<ValueCase, SettingValueConverter> SETTING_VALUE_CONVERTERS;

    private final Logger logger = LogManager.getLogger();

    /**
     * A DSLContext with which to interact with an underlying persistent datastore.
     */
    private final DSLContext dslContext;

    private final SettingSpecStore settingSpecStore;

    private final SettingPolicyValidator settingPolicyValidator;

    private final SettingsUpdatesSender settingsUpdatesSender;

    static {
        final Map<ValueCase, SettingValueConverter> settingValueConverters =
                new EnumMap<>(ValueCase.class);
        settingValueConverters.put(ValueCase.BOOLEAN_SETTING_VALUE,
                new BooleanSettingValueConverter());
        settingValueConverters.put(ValueCase.ENUM_SETTING_VALUE, new EnumSettingValueConverter());
        settingValueConverters.put(ValueCase.NUMERIC_SETTING_VALUE,
                new NumericSettingValueConverter());
        settingValueConverters.put(ValueCase.STRING_SETTING_VALUE,
                new StringSettingValueConverter());
        settingValueConverters.put(ValueCase.SORTED_SET_OF_OID_SETTING_VALUE,
                new OidsSetValueConverter());
        SETTING_VALUE_CONVERTERS = Collections.unmodifiableMap(settingValueConverters);
    }

    /**
     * Create a new SettingStore.
     *
     * @param settingSpecStore The source, providing the {@link SettingSpec} definitions.
     * @param dslContext A context with which to interact with the underlying datastore.
     * @param settingPolicyValidator settingPolicyValidator.
     * @param settingsUpdatesSender broadcaster for settings updates.
     *
     */
    public SettingStore(@Nonnull final SettingSpecStore settingSpecStore,
            @Nonnull final DSLContext dslContext,
            @Nonnull final SettingPolicyValidator settingPolicyValidator,
            @Nonnull final SettingsUpdatesSender settingsUpdatesSender) {
        this.settingSpecStore = Objects.requireNonNull(settingSpecStore);
        this.dslContext = Objects.requireNonNull(dslContext);
        this.settingPolicyValidator = Objects.requireNonNull(settingPolicyValidator);
        this.settingsUpdatesSender = settingsUpdatesSender;
    }

    @Nonnull
    private SettingPolicyRecord createSettingPolicyRecord(long oid,
            @Nonnull SettingPolicyInfo settingPolicyInfo,
            @Nonnull SettingProto.SettingPolicy.Type type) {
        return new SettingPolicyRecord(
                oid,
                settingPolicyInfo.getName(),
                settingPolicyInfo.getEntityType(),
                SettingPolicyTypeConverter.typeToDb(type),
                settingPolicyInfo.hasTargetId() ? settingPolicyInfo.getTargetId() : null,
                settingPolicyInfo.getDisplayName(),
                settingPolicyInfo.getEnabled(),
                settingPolicyInfo.hasScheduleId() ? settingPolicyInfo.getScheduleId() : null
        );
    }

    @Nonnull
    private List<Query> deleteChildRecords(@Nonnull DSLContext context, long policyId) {
        final List<Query> queries = new ArrayList<>(2);
        queries.add(context.deleteFrom(Tables.SETTING_POLICY_GROUPS)
                .where(Tables.SETTING_POLICY_GROUPS.SETTING_POLICY_ID.eq(policyId)));
        queries.add(context.deleteFrom(Tables.SETTING_POLICY_SETTING)
                .where(Tables.SETTING_POLICY_SETTING.POLICY_ID.eq(policyId)));
        return queries;
    }

    /**
     * Creates setting policies.
     *
     * @param context DB transaction context to create policies in
     * @param policies policies to create
     * @throws StoreOperationException if creation operation failed.
     */
    public void createSettingPolicies(@Nonnull DSLContext context,
            @Nonnull Collection<SettingProto.SettingPolicy> policies)
            throws StoreOperationException {
        try {
            for (SettingProto.SettingPolicy policy : policies) {
                settingPolicyValidator.validateSettingPolicy(policy.getInfo(),
                        policy.getSettingPolicyType());
            }
        } catch (InvalidItemException e) {
            throw new StoreOperationException(Status.INVALID_ARGUMENT, e.getMessage(), e);
        }
        final Set<String> duplicates = getDuplicates(policies
                .stream().map(SettingProto.SettingPolicy::getInfo)
                .map(SettingPolicyInfo::getName).iterator());
        if (!duplicates.isEmpty()) {
            throw new StoreOperationException(Status.INVALID_ARGUMENT,
                    "Duplicated policy names found: " + duplicates);
        }
        try {
            for (SettingProto.SettingPolicy policy : policies) {
                settingPolicyValidator.validateSettingPolicy(policy.getInfo(),
                        policy.getSettingPolicyType());
            }
        } catch (InvalidItemException e) {
            throw new StoreOperationException(Status.INVALID_ARGUMENT, e.getMessage(), e);
        }
        final Set<String> namesToCheck = policies.stream()
                .filter(policy -> policy.getSettingPolicyType() != Type.DISCOVERED)
                .map(SettingProto.SettingPolicy::getInfo)
                .map(SettingPolicyInfo::getName)
                .collect(Collectors.toSet());
        final List<String> duplicated = context.select(SETTING_POLICY.NAME)
                .from(SETTING_POLICY)
                .where(SETTING_POLICY.POLICY_TYPE.notEqual(SettingPolicyPolicyType.discovered))
                .and(SETTING_POLICY.NAME.in(namesToCheck))
                .fetch()
                .stream()
                .map(Record1::value1)
                .collect(Collectors.toList());
        if (!duplicated.isEmpty()) {
            throw new StoreOperationException(Status.ALREADY_EXISTS,
                    "Setting policies already exist with names: " + duplicated);
        }
        final Collection<TableRecord<?>> inserts = new ArrayList<>();
        for (SettingProto.SettingPolicy policy : policies) {
            inserts.addAll(createSettingPolicy(policy));
        }
        context.batchInsert(inserts).execute();
    }

    @Nonnull
    private <T> Set<T> getDuplicates(@Nonnull Iterator<T> source) {
        final Set<T> newValues = new HashSet<>();
        final Set<T> duplicates = new HashSet<>();
        while (source.hasNext()) {
            final T next = source.next();
            if (!newValues.add(next)) {
                duplicates.add(next);
            }
        }
        return duplicates;
    }

    @Nonnull
    private Collection<TableRecord<?>> createSettingPolicy(
            @Nonnull SettingProto.SettingPolicy policy) throws StoreOperationException {
        final Collection<TableRecord<?>> records = new ArrayList<>();
        records.add(createSettingPolicyRecord(policy.getId(), policy.getInfo(),
                policy.getSettingPolicyType()));
        records.addAll(attachChildRecords(policy.getId(), policy.getInfo()));
        return records;
    }

    @Nonnull
    private Collection<TableRecord<?>> attachChildRecords(long policyId,
            @Nonnull SettingPolicyInfo policy) throws StoreOperationException {
        final Collection<TableRecord<?>> records = new ArrayList<>();
        records.addAll(attachGroupsToPolicy(policyId, policy.getScope()));
        records.addAll(attachSettingsToPolicy(policyId, policy.getSettingsList()));
        return records;
    }

    @Nonnull
    private Collection<SettingPolicyGroupsRecord> attachGroupsToPolicy(@Nonnull Long policyId,
            @Nonnull Scope policyScope) {
        final Collection<SettingPolicyGroupsRecord> allGroups =
                new ArrayList<>(policyScope.getGroupsCount());
        for (long groupIds : policyScope.getGroupsList()) {
            final SettingPolicyGroupsRecord group =
                    new SettingPolicyGroupsRecord(groupIds, policyId);
            allGroups.add(group);
        }
        return allGroups;
    }

    @Nonnull
    private List<TableRecord<?>> attachSettingsToPolicy(@Nonnull Long policyId,
            @Nonnull Collection<Setting> settings) throws StoreOperationException {
        final List<TableRecord<?>> allSettings = new ArrayList<>(settings.size());
        for (Setting setting : settings) {
            final SettingValueConverter converter =
                    SETTING_VALUE_CONVERTERS.get(setting.getValueCase());
            if (converter == null) {
                throw new StoreOperationException(Status.INVALID_ARGUMENT,
                        "Could not find a suitable converter for setting " + setting);
            }
            allSettings.addAll(converter.createDbRecords(setting, policyId));
        }
        return allSettings;
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
            DataAccessException, StoreOperationException {
        final SettingProto.SettingPolicy existingPolicy = getSettingPolicy(context, id).orElseThrow(
                () -> new SettingPolicyNotFoundException(id));

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

                final SettingProto.SettingPolicy existingPolicy =
                        getSettingPolicy(context, id).orElseThrow(
                                () -> new SettingPolicyNotFoundException(id));
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
                    throw new IllegalArgumentException(
                            "Cannot reset setting policy " + id + " as id does not exist");
                }

                return internalUpdateSettingPolicy(context, existingPolicy.toBuilder()
                    .setInfo(newPolicyInfo)
                    .build());
            });
        } catch (DataAccessException e) {
            // Jooq will rethrow exceptions thrown in the transactionResult call
            // wrapped in a DataAccessException. Check to see if that's why the transaction failed.
            if (e.getCause() instanceof SettingPolicyNotFoundException) {
                throw (SettingPolicyNotFoundException)e.getCause();
            } else if (e.getCause() instanceof InvalidItemException) {
                // This shouldn't happen, because default setting policies created from the
                // setting specs should always be valid. Must be some programming error!
                throw new IllegalStateException("DefaultSettingPolicyCreator produced an " +
                        "invalid setting policy! Error: " + e.getMessage());
            } else if (e.getCause() instanceof IllegalArgumentException) {
                throw (IllegalArgumentException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    @Nonnull
    private Map<Long, Collection<Setting>> getSettings(@Nonnull DSLContext context,
            @Nonnull Collection<Long> oids) throws StoreOperationException {
        final Map<Long, Collection<Setting>> policySettings = new HashMap<>();
        final List<SettingPolicySettingRecord> settingRecords =
                context.selectFrom(SETTING_POLICY_SETTING)
                        .where(SETTING_POLICY_SETTING.POLICY_ID.in(oids))
                        .fetch();

        final Table<Long, String, List<Long>> oidsCollection =
                getSettingsOidsFromSeparateTable(context, oids);

        final Table<Long, String, List<Long>> executionSchedulesCollection =
                getSettingsExecutionSchedulesFromSeparateTable(context, oids);

        for (SettingPolicySettingRecord record : settingRecords) {
            policySettings.computeIfAbsent(record.getPolicyId(), key -> new HashSet<>())
                    .add(convertFromDb(record, oidsCollection, executionSchedulesCollection));
        }
        return Collections.unmodifiableMap(policySettings);
    }

    @Nonnull
    private Table<Long, String, List<Long>> getSettingsOidsFromSeparateTable(
            @Nonnull DSLContext context, @Nonnull Collection<Long> oids) {
        final List<SettingPolicySettingOidsRecord> oidsRecords =
                context.selectFrom(Tables.SETTING_POLICY_SETTING_OIDS)
                        .where(Tables.SETTING_POLICY_SETTING_OIDS.POLICY_ID.in(oids))
                        .orderBy(Tables.SETTING_POLICY_SETTING_OIDS.OID_NUMBER)
                        .fetch();
        final Table<Long, String, List<Long>> oidsCollection = HashBasedTable.create();
        for (SettingPolicySettingOidsRecord oidRecord : oidsRecords) {
            populateMultipleSettingValues(oidsCollection, oidRecord.getPolicyId(), oidRecord.getSettingName(),
                    oidRecord.getOid());
        }
        return oidsCollection;
    }

    @Nonnull
    private Table<Long, String, List<Long>> getSettingsExecutionSchedulesFromSeparateTable(
            @Nonnull DSLContext context, @Nonnull Collection<Long> oids) {
        final List<SettingPolicySettingScheduleIdsRecord> scheduleIdsRecords =
                context.selectFrom(Tables.SETTING_POLICY_SETTING_SCHEDULE_IDS)
                        .where(Tables.SETTING_POLICY_SETTING_SCHEDULE_IDS.POLICY_ID.in(oids))
                        .fetch();
        final Table<Long, String, List<Long>> scheduleIdsCollection = HashBasedTable.create();
        for (SettingPolicySettingScheduleIdsRecord scheduleRecord : scheduleIdsRecords) {
            populateMultipleSettingValues(scheduleIdsCollection, scheduleRecord.getPolicyId(),
                    scheduleRecord.getSettingName(), scheduleRecord.getExecutionScheduleId());
        }
        return scheduleIdsCollection;
    }

    private void populateMultipleSettingValues(@Nonnull Table<Long, String, List<Long>> collection,
            @Nonnull Long policyId, @Nonnull String settingName, @Nonnull Long value) {
        final List<Long> existing = collection.get(policyId, settingName);
        final List<Long> effectiveList;
        if (existing == null) {
            effectiveList = new ArrayList<>();
            collection.put(policyId, settingName, effectiveList);
        } else {
            effectiveList = existing;
        }
        effectiveList.add(value);
    }

    @Nonnull
    private Collection<SettingProto.SettingPolicy> getSettingPoliciesByIds(
            @Nonnull DSLContext context, @Nonnull Collection<Long> oids)
            throws StoreOperationException {
        final List<SettingPolicyRecord> policies =
                context.selectFrom(SETTING_POLICY).where(SETTING_POLICY.ID.in(oids)).fetch();
        final Map<Long, Set<Long>> policyGroups = new HashMap<>();
        context.selectFrom(SETTING_POLICY_GROUPS)
                .where(SETTING_POLICY_GROUPS.SETTING_POLICY_ID.in(oids))
                .fetch()
                .forEach(record -> policyGroups.computeIfAbsent(record.getSettingPolicyId(),
                        key -> new HashSet<>()).add(record.getGroupId()));
        final Collection<SettingProto.SettingPolicy> result = new ArrayList<>(policies.size());
        final Map<Long, Collection<Setting>> policySettings = getSettings(context, oids);
        for (SettingPolicyRecord policy: policies) {
            final SettingProto.SettingPolicy.Builder builder =
                    SettingProto.SettingPolicy.newBuilder();
            builder.setId(policy.getId());
            builder.setSettingPolicyType(
                    SettingPolicyTypeConverter.typeFromDb(policy.getPolicyType()));
            final SettingPolicyInfo.Builder infoBuilder = SettingPolicyInfo.newBuilder();
            infoBuilder.setName(policy.getName());
            if (policy.getDisplayName() != null) {
                infoBuilder.setDisplayName(policy.getDisplayName());
            }
            infoBuilder.addAllSettings(policySettings.getOrDefault(policy.getId(), Collections.emptySet()));
            if (policy.getEntityType() != null) {
                infoBuilder.setEntityType(policy.getEntityType());
            }
            infoBuilder.setEnabled(policy.getEnabled());
            if (policy.getScheduleId() != null) {
                infoBuilder.setScheduleId(policy.getScheduleId());
            }
            infoBuilder.setScope(Scope.newBuilder().addAllGroups(policyGroups.getOrDefault(policy.getId(),
                    Collections.emptySet())));
            if (policy.getTargetId() != null) {
                infoBuilder.setTargetId(policy.getTargetId());
            }
            builder.setInfo(infoBuilder);
            result.add(builder.build());
        }
        return result;
    }

    @Nonnull
    private Setting convertFromDb(@Nonnull SettingPolicySettingRecord record,
            @Nonnull Table<Long, String, List<Long>> oidListValues,
            @Nonnull Table<Long, String, List<Long>> scheduleValues) throws StoreOperationException {
        final String settingName = record.getSettingName();
        final Long policyId = record.getPolicyId();
        final ValueCase valueCase = Objects.requireNonNull(record.getSettingType(),
                "Setting type not found for setting " + settingName + " in policy " + policyId);
        final SettingValueConverter converter =
                Objects.requireNonNull(SETTING_VALUE_CONVERTERS.get(valueCase),
                        "Unexpected type found for setting " + settingName + " in policy "
                                + policyId + ": " + valueCase);
        try {
            final List<Long> multiplySettingValues =
                    getAppropriateMultiplySettingValue(settingName, policyId, oidListValues,
                            scheduleValues);

            return converter.fromDbValue(record.getSettingValue(),
                    multiplySettingValues == null ? Collections.emptyList() : multiplySettingValues)
                    .setSettingSpecName(settingName)
                    .build();
        } catch (NumberFormatException e) {
            throw new StoreOperationException(Status.INTERNAL,
                    String.format("Failed to convert of setting %d-\"%s\" of type %s: %s",
                            policyId, settingName, valueCase,
                            record.getSettingValue()), e);
        }
    }

    @Nullable
    private List<Long> getAppropriateMultiplySettingValue(@Nonnull String settingName,
            @Nonnull Long policyId, @Nonnull Table<Long, String, List<Long>> oidListValues,
            @Nonnull Table<Long, String, List<Long>> scheduleValues) {
        if (EntitySettingSpecs.isExecutionScheduleSetting(settingName)) {
            return scheduleValues.get(policyId, settingName);
        } else {
            return oidListValues.get(policyId, settingName);
        }
    }

    /**
     * Fetch all settings policies that use specified schedule.
     *
     * @param context transaction context to use
     * @param scheduleId ID of the {@link com.vmturbo.group.db.tables.pojos.Schedule} setting
     * policies are being fetched for
     * @return A stream of {@link SettingPolicy} objects that use the schedule.
     * @throws DataAccessException If there is an exception executing the query.
     * @throws StoreOperationException if there was an exception operating with the store
     */
    @Nonnull
    public Collection<SettingProto.SettingPolicy> getSettingPoliciesUsingSchedule(
            @Nonnull DSLContext context, final long scheduleId)
            throws DataAccessException, StoreOperationException {
        final Collection<Long> oids = context.select(SETTING_POLICY.ID)
                .from(SETTING_POLICY)
                .where(SETTING_POLICY.SCHEDULE_ID.eq(scheduleId))
                .fetch(SETTING_POLICY.ID);
        return getSettingPoliciesByIds(context, oids);
    }

    /**
     * Get setting policies matching a filter.
     *
     * @param filter The {@link SettingPolicyFilter}.
     * @return A stream of {@link SettingPolicy} objects that match the filter.
     * @throws DataAccessException If there is an error connecting to the database.
     * @throws StoreOperationException if failed to retrieve setting policy from the DB
     */
    @Nonnull
    public Collection<SettingProto.SettingPolicy> getSettingPolicies(
            @Nonnull final SettingPolicyFilter filter)
            throws DataAccessException, StoreOperationException {
        return getSettingPolicies(dslContext, filter);
    }

    /**
     * Search for policies using the specified filter.
     *
     * @param context transactional context to execute query within
     * @param filter filter to apply on the policies
     * @return all the policies matching the filter.
     * @throws StoreOperationException if store operation failed
     */
    @Nonnull
    public Collection<SettingProto.SettingPolicy> getSettingPolicies(
            @Nonnull final DSLContext context, @Nonnull final SettingPolicyFilter filter)
            throws StoreOperationException {
        final Collection<Long> settingPolicies = context.select(SETTING_POLICY.ID)
                .from(SETTING_POLICY)
                .where(filter.getConditions())
                .fetch(SETTING_POLICY.ID);
        return getSettingPoliciesByIds(context, settingPolicies);
    }

    /**
     * Get a setting policy by its unique OID.
     *
     * @param context transactional context to use
     * @param oid The OID (object id) of the setting policy to retrieve.
     * @return The {@link SettingProto.SettingPolicy} associated with the name, or an empty policy.
     * @throws StoreOperationException if failed to retrieve setting policy from the DB
     */
    public Optional<SettingProto.SettingPolicy> getSettingPolicy(@Nonnull DSLContext context,
            final long oid) throws StoreOperationException {
        final Collection<SettingProto.SettingPolicy> policies =
                getSettingPoliciesByIds(context, Collections.singleton(oid));
        if (policies.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(policies.iterator().next());
        }
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
        record.setDisplayName(policy.getInfo().getDisplayName());
        record.setEntityType(policy.getInfo().getEntityType());
        if (policy.getInfo().hasTargetId()) {
            record.setTargetId(policy.getInfo().getTargetId());
        }
        record.setPolicyType(SettingPolicyTypeConverter.typeToDb(policy.getSettingPolicyType()));
        record.setEnabled(policy.getInfo().getEnabled());
        if (policy.getInfo().hasScheduleId()) {
            record.setScheduleId(policy.getInfo().getScheduleId());
        }

        final Collection<TableRecord<?>> inserts;
        try {
            inserts = attachChildRecords(policy.getId(), policy.getInfo());
        } catch (StoreOperationException e) {
            logger.warn("Could not create records for policy " + policy.getId(), e);
            throw new InvalidItemException(
                    "Could not create records for policy " + policy.getId() + ": " +
                            e.getMessage());
        }

        final int modifiedRecords = record.update();
        if (modifiedRecords == 0) {
            // This should never happen, because we overwrote fields in the record,
            // and update() should always execute an UPDATE statement if some fields
            // got overwritten.
            throw new IllegalStateException("Failed to update record.");
        }
        context.batch(deleteChildRecords(context, policy.getId())).execute();
        context.batchInsert(inserts).execute();
        return policy;
    }

    /**
     * Return the SettingPolicies associated with the Groups.
     *
     * @param groupIds The set of group ids.
     * @return Setting Policies associated with the group.
     */
    @Nonnull
    public Map<Long, Set<SettingProto.SettingPolicy>> getSettingPoliciesForGroups(
            @Nonnull Set<Long> groupIds) {
        return dslContext.transactionResult((configuration -> {
            final DSLContext context = DSL.using(configuration);
            return getSettingPoliciesForGroups(context, groupIds);
        }));
    }

    /**
     * Return the SettingPolicies associated with the Groups.
     *
     * @param context transaction context to use
     * @param groupIds The set of group ids.
     * @return Setting Policies associated with the group.
     * @throws StoreOperationException if store failed to operate
     */
    @Nonnull
    private Map<Long, Set<SettingProto.SettingPolicy>> getSettingPoliciesForGroups(
            @Nonnull DSLContext context, @Nonnull Set<Long> groupIds)
            throws StoreOperationException {
        if (groupIds.isEmpty()) {
            return Collections.emptyMap();
        }
        final List<SettingPolicyGroupsRecord> records = context.selectFrom(SETTING_POLICY_GROUPS)
                .where(SETTING_POLICY_GROUPS.GROUP_ID.in(groupIds))
                .fetch();
        final Map<Long, Set<SettingProto.SettingPolicy>> groupPolicies = new HashMap<>();
        final Set<Long> policiesToRequest = records.stream()
                .map(SettingPolicyGroupsRecord::getSettingPolicyId)
                .collect(Collectors.toSet());
        final Map<Long, SettingProto.SettingPolicy> settingPolicyMap =
                getSettingPoliciesByIds(context, policiesToRequest).stream()
                        .collect(Collectors.toMap(SettingProto.SettingPolicy::getId,
                                Function.identity()));
        for (SettingPolicyGroupsRecord record : records) {
            final SettingProto.SettingPolicy policy =
                    settingPolicyMap.get(record.getSettingPolicyId());
            groupPolicies.computeIfAbsent(record.getGroupId(), key -> new HashSet<>()).add(policy);
        }
        return groupPolicies;
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
    private void updateGlobalSettingInternal(@Nonnull final Collection<Setting> settings)
            throws SQLTransientException, DataAccessException {
        try {
            dslContext.transaction(transactionContext -> {
                final DSLContext transaction = DSL.using(transactionContext);
                final List<GlobalSettingsRecord> globalSettingsRecords = settings.stream()
                    .map(setting -> transaction.newRecord(GLOBAL_SETTINGS,
                        new GlobalSettingsRecord(setting.getSettingSpecName(), setting.toByteArray())))
                    .collect(Collectors.toList());
                transaction.batchUpdate(globalSettingsRecords).execute();
            });

            for (Setting setting : settings) {
                try {
                    settingsUpdatesSender.notifySettingsUpdated(setting);
                } catch (InterruptedException e) {
                    logger.error("Interrupted exception while broadcasting notification for setting {}",
                        setting.getSettingSpecName(), e);
                } catch (CommunicationException e) {
                    logger.error("CommunicationException exception while broadcasting notification for" +
                        " setting {}", setting.getSettingSpecName(), e);
                }
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
            updateGlobalSettingInternal(Collections.singletonList(setting));
        } catch (SQLTransientException e) {
            throw new DataAccessException("Failed to update setting: "
                + setting.getSettingSpecName(), e.getCause());
        }
    }

    /**
     * Reset global settings.
     *
     * @param settingSpecNames the names of settings to reset
     * @throws DataAccessException If there is another problem connecting to the database.
     * @throws SettingNotFoundException If there are settings that are not in the database.
     */
    public void resetGlobalSetting(@Nonnull final Collection<String> settingSpecNames)
            throws DataAccessException, SettingNotFoundException {
        final List<String> settingNotFound = new ArrayList<>();
        // Get default global settings.
        final List<Setting> settings = DefaultGlobalSettingsCreator.defaultSettingsFromSpecs(
            settingSpecNames.stream().map(settingSpecName -> {
                if (settingSpecStore.getSettingSpec(settingSpecName).isPresent()) {
                    return settingSpecStore.getSettingSpec(settingSpecName).get();
                } else {
                    settingNotFound.add(settingSpecName);
                    return null;
                }
            }).filter(Objects::nonNull).collect(Collectors.toSet()));

        if (!settingNotFound.isEmpty()) {
            throw new SettingNotFoundException(settingNotFound);
        }

        try {
            updateGlobalSettingInternal(settings);
        } catch (SQLTransientException e) {
            throw new DataAccessException("Failed to update settings: "
                + settingSpecNames, e.getCause());
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
     * {@inheritDoc}
     */
    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();

        try {
            appender.appendString(gson.toJson(getAllGlobalSettings()));
            appender.appendString(
                    gson.toJson(getSettingPolicies(SettingPolicyFilter.newBuilder().build())));
        } catch (DataAccessException | InvalidProtocolBufferException | StoreOperationException e) {
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

        } catch (DataAccessException | StoreOperationException e) {
            errors.add("Failed to restore setting policies: " + e.getMessage() + ": " +
                ExceptionUtils.getStackTrace(e));
        }

        if (!errors.isEmpty()) {
            throw new DiagnosticsException(errors);
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return SETTINGS_DUMP_FILE;
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
    }

    /**
     * Deletes the setting policies from the store identified by OIDs. {@code allowedType}
     * is an additional restriction to specify which types of policies are expected to be deleted.
     * It will cause a {@link StoreOperationException} if one of the requested OIDs refers to
     * another policy type.
     *
     * @param context transaction context to execute within
     * @param oids OIDs of policies to delete
     * @param allowedType type of the policies. Used as an additional check to ensure, that
     *         client is expecting removal of a specific type of policies only
     * @throws StoreOperationException if some operation failed with this store.
     */
    public void deleteSettingPolcies(@Nonnull DSLContext context, @Nonnull Collection<Long> oids,
            @Nonnull Type allowedType) throws StoreOperationException {
        logger.debug("Deleting policies of type {}: {}", allowedType, oids);
        final Set<Long> forbiddenOids = context.select(SETTING_POLICY.ID)
                .from(SETTING_POLICY)
                .where(SETTING_POLICY.ID.in(oids)
                        .and(SETTING_POLICY.POLICY_TYPE.ne(
                                SettingPolicyTypeConverter.typeToDb(allowedType))))
                .fetchSet(SETTING_POLICY.ID);
        if (!forbiddenOids.isEmpty()) {
            throw new StoreOperationException(Status.INVALID_ARGUMENT,
                    String.format("Could not remove setting policies of types except %s: %s",
                            allowedType, forbiddenOids));
        }
        final int deleted = context.deleteFrom(SETTING_POLICY).where(SETTING_POLICY.ID.in(oids)).execute();
        if (deleted != oids.size()) {
            throw new StoreOperationException(Status.NOT_FOUND,
                    "Failed to remove setting policies. Some of the policies are not found: " +
                            oids);
        }
    }

    /**
     * Insert all the setting policies in the list into the database.
     * For internal use only when restoring setting policies from diagnostics.
     *
     * @param settingPolicies The setting policies to insert.
     * @throws StoreOperationException if failed to insert operation failed.
     */
    private void insertAllSettingPolicies(
            @Nonnull final List<SettingProto.SettingPolicy> settingPolicies)
            throws StoreOperationException {
        final Collection<TableRecord<?>> inserts = new ArrayList<>();
        for (SettingProto.SettingPolicy settingPolicy : settingPolicies) {
            inserts.addAll(createSettingPolicy(settingPolicy));
        }
        dslContext.batchInsert(inserts).execute();
    }

    /**
     * Returns a collection of discovered policies that already exist in the database.
     *
     * @param dslContext transaction context to execute within
     * @return map of policy id by policy name, groupped by target id
     */
    public Map<Long, Map<String, Long>> getDiscoveredPolicies(DSLContext dslContext) {
        final Collection<Record3<String, Long, Long>> discoveredPolicies =
                dslContext.select(SETTING_POLICY.NAME, SETTING_POLICY.ID, SETTING_POLICY.TARGET_ID)
                        .from(SETTING_POLICY)
                        .where(SETTING_POLICY.POLICY_TYPE.eq(SettingPolicyPolicyType.discovered))
                        .fetch();
        final Map<Long, Map<String, Long>> resultMap = new HashMap<>();
        for (Record3<String, Long, Long> policy: discoveredPolicies) {
            final String name = policy.value1();
            final Long id = policy.value2();
            final Long targetId = policy.value3();
            resultMap.computeIfAbsent(targetId, key -> new HashMap<>()).put(name, id);
        }
        return Collections.unmodifiableMap(resultMap);
    }

    /**
     * Interface for a converter of a setting value.
     */
    private interface SettingValueConverter {
        /**
         * Creates insert records for the setting values. It could be a single one record
         * or multiple records
         *
         * @param setting setting value to convert to DB
         * @param policyId policy OID
         * @return a list of table records to insert
         */
        @Nonnull
        List<TableRecord<?>> createDbRecords(@Nonnull Setting setting, long policyId);

        /**
         * Converts a setting Protobuf message from DB representation.
         * @param value single-line value of the setting
         * @param oidListValue list of longs for the setting
         * @return settings builder.
         */
        @Nonnull
        Setting.Builder fromDbValue(@Nonnull String value, @Nullable List<Long> oidListValue);
    }

    /**
     * Abstract setting value converter aimed to store data in a single string field in the DB.
     */
    private abstract static class SimpleValueSettingConverter implements SettingValueConverter {
        @Nonnull
        @Override
        public List<TableRecord<?>> createDbRecords(@Nonnull Setting setting, long policyId) {
            final SettingPolicySettingRecord record =
                    new SettingPolicySettingRecord(policyId, setting.getSettingSpecName(),
                            setting.getValueCase(), toDbValue(setting));
            return Collections.singletonList(record);
        }

        @Nonnull
        protected abstract String toDbValue(@Nonnull Setting setting);
    }

    /**
     * Setting value converter for enum setting values.
     */
    private static class EnumSettingValueConverter extends SimpleValueSettingConverter {
        @Nonnull
        @Override
        protected String toDbValue(@Nonnull Setting setting) {
            return setting.getEnumSettingValue().getValue();
        }

        @Nonnull
        @Override
        public Setting.Builder fromDbValue(@Nonnull String value,
                @Nullable List<Long> oidListValue) {
            return Setting.newBuilder()
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(value));
        }
    }

    /**
     * Settgin value converter for string values.
     */
    private static class StringSettingValueConverter extends SimpleValueSettingConverter {
        @Nonnull
        @Override
        public String toDbValue(@Nonnull Setting setting) {
            return setting.getStringSettingValue().getValue();
        }

        @Nonnull
        @Override
        public Setting.Builder fromDbValue(@Nonnull String value,
                @Nullable List<Long> oidListValue) {
            return Setting.newBuilder()
                    .setStringSettingValue(StringSettingValue.newBuilder().setValue(value));
        }
    }

    /**
     * Setting value converter for boolean values.
     */
    private static class BooleanSettingValueConverter extends SimpleValueSettingConverter {
        @Nonnull
        @Override
        public String toDbValue(@Nonnull Setting setting) {
            return Boolean.toString(setting.getBooleanSettingValue().getValue());
        }

        @Nonnull
        @Override
        public Setting.Builder fromDbValue(@Nonnull String value,
                @Nullable List<Long> oidListValue) {
            return Setting.newBuilder()
                    .setBooleanSettingValue(
                            BooleanSettingValue.newBuilder().setValue(Boolean.parseBoolean(value)));
        }
    }

    /**
     * Value converter for numeric setting values.
     */
    private static class NumericSettingValueConverter extends SimpleValueSettingConverter {
        @Nonnull
        @Override
        public String toDbValue(@Nonnull Setting setting) {
            return Float.toString(setting.getNumericSettingValue().getValue());
        }

        @Nonnull
        @Override
        public Setting.Builder fromDbValue(@Nonnull String value,
                @Nullable List<Long> oidListValue) {
            return Setting.newBuilder()
                    .setNumericSettingValue(
                            NumericSettingValue.newBuilder().setValue(Float.parseFloat(value)));
        }
    }

    /**
     * Value converter for OIDs sorted set. Value is stored in a separate table.
     */
    protected static class OidsSetValueConverter implements SettingValueConverter {
        @Nonnull
        @Override
        public List<TableRecord<?>> createDbRecords(@Nonnull Setting setting, long policyId) {
            final List<TableRecord<?>> result = new ArrayList<>();
            final SettingPolicySettingRecord mainRecord =
                    new SettingPolicySettingRecord(policyId, setting.getSettingSpecName(),
                            setting.getValueCase(), "-");
            result.add(mainRecord);
            if (EntitySettingSpecs.isExecutionScheduleSetting(setting.getSettingSpecName())) {
                addExecutionScheduleRecords(result, setting, policyId);
            } else {
                addOidsRecords(result, setting, policyId);
            }
            return result;
        }

        @Nonnull
        @Override
        public Builder fromDbValue(@Nonnull String value, @Nullable List<Long> oidListValue) {
            return Setting.newBuilder()
                    .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                            .addAllOids(oidListValue)
                            .build());
        }

        private void addOidsRecords(@Nonnull List<TableRecord<?>> result, @Nonnull Setting setting,
                long policyId) {
            int counter = 0;
            for (Long oid : setting.getSortedSetOfOidSettingValue().getOidsList()) {
                final SettingPolicySettingOidsRecord record =
                        new SettingPolicySettingOidsRecord(policyId, setting.getSettingSpecName(),
                                counter++, oid);
                result.add(record);
            }
        }

        private void addExecutionScheduleRecords(@Nonnull List<TableRecord<?>> result,
                @Nonnull Setting setting, long policyId) {
            for (Long oid : setting.getSortedSetOfOidSettingValue().getOidsList()) {
                final SettingPolicySettingScheduleIdsRecord record =
                        new SettingPolicySettingScheduleIdsRecord(policyId,
                                setting.getSettingSpecName(), oid);
                result.add(record);
            }
        }
    }
}
