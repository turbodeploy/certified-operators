package com.vmturbo.group.setting;

import static com.vmturbo.group.db.Tables.ENTITY_SETTINGS;
import static com.vmturbo.group.db.Tables.GLOBAL_SETTINGS;
import static com.vmturbo.group.db.Tables.SETTINGS;
import static com.vmturbo.group.db.Tables.SETTINGS_OIDS;
import static com.vmturbo.group.db.Tables.SETTINGS_POLICIES;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Insert;
import org.jooq.InsertOnDuplicateStep;
import org.jooq.Param;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record4;
import org.jooq.Result;
import org.jooq.TableRecord;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
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
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.group.DiscoveredObjectVersionIdentity;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.common.ItemNotFoundException.SettingNotFoundException;
import com.vmturbo.group.common.ItemNotFoundException.SettingPolicyNotFoundException;
import com.vmturbo.group.db.Tables;
import com.vmturbo.group.db.enums.SettingPolicyPolicyType;
import com.vmturbo.group.db.tables.pojos.SettingPolicy;
import com.vmturbo.group.db.tables.records.EntitySettingsRecord;
import com.vmturbo.group.db.tables.records.GlobalSettingsRecord;
import com.vmturbo.group.db.tables.records.SettingPolicyGroupsRecord;
import com.vmturbo.group.db.tables.records.SettingPolicyRecord;
import com.vmturbo.group.db.tables.records.SettingPolicySettingOidsRecord;
import com.vmturbo.group.db.tables.records.SettingPolicySettingRecord;
import com.vmturbo.group.db.tables.records.SettingPolicySettingScheduleIdsRecord;
import com.vmturbo.group.db.tables.records.SettingsOidsRecord;
import com.vmturbo.group.db.tables.records.SettingsPoliciesRecord;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * The {@link SettingStore} class is used to store settings-related objects, and retrieve them
 * in an efficient way.
 */
public class SettingStore implements DiagsRestorable<DSLContext> {

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

    private static final Set<String> ACTION_WORKFLOW_SETTING_NAMES;

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
        ACTION_WORKFLOW_SETTING_NAMES = Collections.unmodifiableSet(
                ActionSettingSpecs.getActionWorkflowSettingSpecs()
                        .stream()
                        .map(SettingSpec::getName)
                        .collect(Collectors.toSet()));
    }

    /**
     * Create a new SettingStore.
     *  @param settingSpecStore The source, providing the {@link SettingSpec} definitions.
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
        final byte[] hash = SettingPolicyHash.hash(settingPolicyInfo);
        return new SettingPolicyRecord(
                oid,
                settingPolicyInfo.getName(),
                settingPolicyInfo.getEntityType(),
                SettingPolicyTypeConverter.typeToDb(type),
                settingPolicyInfo.hasTargetId() ? settingPolicyInfo.getTargetId() : null,
                settingPolicyInfo.getDisplayName(),
                settingPolicyInfo.getEnabled(),
                settingPolicyInfo.hasScheduleId() ? settingPolicyInfo.getScheduleId() : null,
                hash,
                settingPolicyInfo.hasDeleteAfterScheduleExpiration()
                        ? settingPolicyInfo.getDeleteAfterScheduleExpiration() : null
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
                settingPolicyValidator.validateSettingPolicy(context, policy.getInfo(),
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
        logger.debug("Inserting {} records to add {} setting policies", inserts.size(),
                policies.size());
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
     * @return The updated {@link SettingProto.SettingPolicy} and flag which defines should
     * acceptances and rejections for actions associated with policy be removed or not.
     * @throws StoreOperationException if update failed
     */
    @Nonnull
    public Pair<SettingProto.SettingPolicy, Boolean> updateSettingPolicy(final long id,
            @Nonnull final SettingPolicyInfo newInfo) throws StoreOperationException {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                return updateSettingPolicy(context, id, newInfo);
            });
        } catch (DataAccessException e) {
            // Jooq will rethrow exceptions thrown in the transactionResult call
            // wrapped in a DataAccessException. Check to see if that's why the transaction failed.
            if (e.getCause() instanceof StoreOperationException) {
                throw (StoreOperationException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    @Nonnull
    private Pair<SettingProto.SettingPolicy, Boolean> updateSettingPolicy(@Nonnull DSLContext context,
            final long id, @Nonnull final SettingPolicyInfo newInfo)
            throws StoreOperationException {
        final SettingProto.SettingPolicy existingPolicy = getSettingPolicy(context, id).orElseThrow(
                () -> new StoreOperationException(Status.NOT_FOUND,
                        "Setting Policy " + id + " not found."));

        final SettingProto.SettingPolicy.Type type = existingPolicy.getSettingPolicyType();

        // Additional update-only validation for default policies
        // to ensure certain fields are not changed.
        if (type.equals(Type.DEFAULT)) {
            // For default setting policies we don't allow changes to names
            // or entity types.
            if (newInfo.getEntityType() != existingPolicy.getInfo().getEntityType()) {
                throw new StoreOperationException(Status.INVALID_ARGUMENT, "Illegal attempt to "
                        + "change the entity type of a default setting policy.");
            }
            if (!newInfo.getName().equals(existingPolicy.getInfo().getName())) {
                throw new StoreOperationException(Status.INVALID_ARGUMENT,
                        "Illegal attempt to change the name of a default setting policy "
                                + existingPolicy.getInfo().getName());
            }
            Set<String> newSettingNames = newInfo.getSettingsList()
                    .stream()
                    .map(Setting::getSettingSpecName)
                    .collect(Collectors.toSet());

            final SettingPolicyInfo defaultSettingPolicy = DefaultSettingPolicyCreator
                    .defaultSettingPoliciesFromSpecs(settingSpecStore.getAllSettingSpecs())
                    .get(existingPolicy.getInfo().getEntityType());
            if (defaultSettingPolicy == null) {
                throw new StoreOperationException(Status.INTERNAL, "Cannot get default "
                    + "info for policy" + newInfo.getName()
                    + "entity type " + existingPolicy.getInfo().getEntityType());
            }
            final Map<String, Setting> defaultSettings = defaultSettingPolicy == null
                    ? Collections.emptyMap()
                    : defaultSettingPolicy.getSettingsList().stream()
                    .collect(Collectors.toMap(Setting::getSettingSpecName, Functions.identity()));
            List<Setting> settingsToAdd = new ArrayList<>();

            // calculate removed setting with default value
            List<String> unsentSettingNames = defaultSettings
                .keySet()
                .stream()
                .filter(s -> !newSettingNames.contains(s))
                .collect(Collectors.toList());

            final Map<String, Setting> existingPolicySettings =
                existingPolicy.getInfo().getSettingsList().stream()
                .collect(Collectors.toMap(Setting::getSettingSpecName, Functions.identity()));

            // for those settings that should have a value but not sent from API, use existing value
            for (String unsentSettingName : unsentSettingNames) {
                Setting existingSetting = existingPolicySettings.get(unsentSettingName);
                if (existingSetting != null) {
                    settingsToAdd.add(existingSetting);
                } else {
                    settingsToAdd.add(defaultSettings.get(unsentSettingName));
                }
            }

            // add all the new setting to the list
            settingsToAdd.addAll(newInfo.getSettingsList());

            SettingPolicyInfo newNewInfo = SettingPolicyInfo.newBuilder(newInfo)
                .clearSettings()
                .addAllSettings(settingsToAdd)
                .build();

            boolean removeAcceptancesAndRejectionsForAssociatedActions =
                    shouldAcceptancesAndRejectionsForActionsAssociatedWithPolicyBeRemoved(newInfo,
                            existingPolicy.getInfo());

            return Pair.create(internalUpdateSettingPolicy(context,
                    existingPolicy.toBuilder().setInfo(newNewInfo).build()),
                    removeAcceptancesAndRejectionsForAssociatedActions);
        }

        if (type.equals(Type.DISCOVERED)) {
            throw new StoreOperationException(Status.INVALID_ARGUMENT,
                    "Illegal attempt to modify a discovered setting policy " + id);
        }

        boolean shouldAcceptancesAndRejectionsForAssociatedActionsBeRemoved =
                shouldAcceptancesAndRejectionsForActionsAssociatedWithPolicyBeRemoved(newInfo,
                        existingPolicy.getInfo());

        return Pair.create(internalUpdateSettingPolicy(context,
                existingPolicy.toBuilder().setInfo(newInfo).build()),
                shouldAcceptancesAndRejectionsForAssociatedActionsBeRemoved);
    }

    /**
     * Detects whether acceptances and rejections for actions associated with policy be removed or
     * not, depends on changes inside setting policy.
     * Remove acceptances/rejections in following cases:
     * 1. ExecutionSchedule setting was removed or modified
     * 2. ActionMode setting associated with ExecutionSchedule setting was modified from MANUAL
     * value to another one
     * 3. ActionMode setting with {@link ActionMode#EXTERNAL_APPROVAL} mode was removed
     *
     * @param newInfo new policy info
     * @param existingInfo policy info before updates
     * @return true if acceptances for actions should be removed otherwise false
     */
    private boolean shouldAcceptancesAndRejectionsForActionsAssociatedWithPolicyBeRemoved(
            @Nonnull SettingPolicyInfo newInfo, @Nonnull SettingPolicyInfo existingInfo) {
        final Set<Setting> previousExecutionScheduleSettings = new HashSet<>();
        final Set<Setting> previousExternalApprovalSettings = new HashSet<>();

        for (Setting setting : existingInfo.getSettingsList()) {
            if (ActionSettingSpecs.isExecutionScheduleSetting(setting.getSettingSpecName())) {
                previousExecutionScheduleSettings.add(setting);
            } else if (ActionSettingSpecs.isActionModeSetting(setting.getSettingSpecName())
                    && setting.getEnumSettingValue()
                    .getValue()
                    .equals(ActionMode.EXTERNAL_APPROVAL.name())) {
                previousExternalApprovalSettings.add(setting);
            }
        }

        final Map<String, List<Long>> newExecutionScheduleSettings = new HashMap<>();
        final Set<Setting> newExternalApprovalSettings = new HashSet<>();

        for (Setting setting : newInfo.getSettingsList()) {
            if (ActionSettingSpecs.isExecutionScheduleSetting(setting.getSettingSpecName())) {
                newExecutionScheduleSettings.put(setting.getSettingSpecName(),
                        setting.getSortedSetOfOidSettingValue().getOidsList());
            } else if (ActionSettingSpecs.isActionModeSetting(setting.getSettingSpecName())
                    && setting.getEnumSettingValue()
                    .getValue()
                    .equals(ActionMode.EXTERNAL_APPROVAL.name())) {
                newExternalApprovalSettings.add(setting);
            }
        }

        final Optional<Setting> removedExecutionScheduleSetting =
                previousExecutionScheduleSettings.stream()
                        .filter(setting -> newExecutionScheduleSettings.get(setting.getSettingSpecName()) == null)
                        .findFirst();

        boolean removeAcceptancesAndRejections = removedExecutionScheduleSetting.isPresent()
                || !newExternalApprovalSettings.containsAll(previousExternalApprovalSettings);

        if (!removeAcceptancesAndRejections) {
            final List<String> previousActionModeSettingsNames =
                previousExecutionScheduleSettings.stream()
                    .map(el -> ActionSettingSpecs.getActionModeSettingFromExecutionScheduleSetting(
                        el.getSettingSpecName()))
                    .collect(Collectors.toList());

            final List<String> previousActionModeSettingsWithManualValue =
                    getActionModeSettingsWithManualValue(existingInfo,
                            previousActionModeSettingsNames);

            final List<String> newActionModeSettingsNames = newExecutionScheduleSettings.keySet()
                    .stream()
                    .map(ActionSettingSpecs::getActionModeSettingFromExecutionScheduleSetting)
                    .collect(Collectors.toList());

            final List<String> newActionModeSettingsWithManualValue =
                    getActionModeSettingsWithManualValue(newInfo, newActionModeSettingsNames);
            removeAcceptancesAndRejections = !newActionModeSettingsWithManualValue.containsAll(
                    previousActionModeSettingsWithManualValue);
        }
        return removeAcceptancesAndRejections;
    }

    @Nonnull
    private List<String> getActionModeSettingsWithManualValue(@Nonnull SettingPolicyInfo settingPolicyInfo,
            List<String> actionModeSettingsNames) {
        return settingPolicyInfo.getSettingsList()
                .stream()
                .filter(setting -> actionModeSettingsNames.contains(setting.getSettingSpecName())
                        && setting.getEnumSettingValue()
                        .getValue()
                        .equals(ActionMode.MANUAL.name()))
                .map(Setting::getSettingSpecName)
                .collect(Collectors.toList());
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
    public Pair<SettingProto.SettingPolicy, Boolean> resetSettingPolicy(final long id)
            throws SettingPolicyNotFoundException, IllegalArgumentException {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);

                final SettingProto.SettingPolicy existingPolicy =
                        getSettingPolicy(context, id).orElseThrow(
                                () -> new SettingPolicyNotFoundException(id));
                final Type type = existingPolicy.getSettingPolicyType();

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

                boolean removeAcceptancesAndRejectionsForAssociatedActions =
                        shouldAcceptancesAndRejectionsForActionsAssociatedWithPolicyBeRemoved(
                                newPolicyInfo, existingPolicy.getInfo());

                return Pair.create(internalUpdateSettingPolicy(context,
                        existingPolicy.toBuilder().setInfo(newPolicyInfo).build()),
                        removeAcceptancesAndRejectionsForAssociatedActions);
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
            Boolean deleteAfterExpiration = policy.getDeleteAfterExpiration();
            if (policy.getDeleteAfterExpiration() != null) {
                infoBuilder.setDeleteAfterScheduleExpiration(policy.getDeleteAfterExpiration());
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
        if (ActionSettingSpecs.isExecutionScheduleSetting(settingName)) {
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
            throws StoreOperationException {
        final SettingPolicyRecord record =
                context.fetchOne(SETTING_POLICY, SETTING_POLICY.ID.eq(policy.getId()));
        if (record == null) {
            throw new StoreOperationException(Status.NOT_FOUND,
                    "Setting Policy " + policy.getId() + " not found.");
        }

        // Validate the setting policy.
        // This should throw an exception if it's invalid.
        try {
            settingPolicyValidator.validateSettingPolicy(context, policy.getInfo(),
                policy.getSettingPolicyType());
        } catch (InvalidItemException e) {
            throw new StoreOperationException(Status.INVALID_ARGUMENT, e.getMessage(), e);
        }


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
            throw new StoreOperationException(Status.ALREADY_EXISTS,
                    "Duplicated policy names found: " + policy.getInfo().getName());
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
        } else {
            record.setScheduleId(null);
        }

        final Collection<TableRecord<?>> inserts =
                attachChildRecords(policy.getId(), policy.getInfo());

        final int modifiedRecords = record.update();
        if (modifiedRecords == 0) {
            // This should never happen, because we overwrote fields in the record,
            // and update() should always execute an UPDATE statement if some fields
            // got overwritten.
            throw new StoreOperationException(Status.INVALID_ARGUMENT, "Failed to update record.");
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
     * @param context the context for DB access.
     * @param ignoreFailures if the insert failures should be ignored or not.
     * @throws DataAccessException
     * @throws InvalidProtocolBufferException
     * @throws SQLTransientException
     *
     */
    @Retryable(value = {SQLTransientException.class},
        maxAttempts = 3, backoff = @Backoff(delay = 2000))
    private void insertGlobalSettingsInternal(@Nonnull final List<Setting> settings,
                  @Nonnull DSLContext context, boolean ignoreFailures)
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
            BatchBindStep batch = context.batch(addIgnoreIfNeeded(
                //have to provide dummy values for jooq
                context.insertInto(GLOBAL_SETTINGS, GLOBAL_SETTINGS.NAME, GLOBAL_SETTINGS.SETTING_DATA)
                .values(newSettings.get(0).getSettingSpecName(), newSettings.get(0).toByteArray()),
              ignoreFailures));
            for (Setting setting : newSettings) {
                batch.bind(setting.getSettingSpecName(), setting.toByteArray());
            }

            // log errors if there are failures
            int[] rowsAffected = batch.execute();
            for (int idx = 0; idx < rowsAffected.length; idx++) {
                // all the queries are insert so if no row was changes there is something wrong.
                if (rowsAffected[idx] == 0) {
                    logger.warn("There was an error occurred while inserting global settings. "
                            + "The setting that failed to insert is \"{}\".",
                        newSettings.get(idx));
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

    public void insertGlobalSettings(@Nonnull final List<Setting> settings)
        throws DataAccessException, InvalidProtocolBufferException {

        try {
            insertGlobalSettingsInternal(settings, dslContext, false);
        } catch (SQLTransientException e) {
            throw new DataAccessException("Failed to insert settings into DB", e.getCause());
        }
    }

    /**
     * Inserts global settings into db tables.
     *
     * @param settings the settings that is being inserted
     * @param context the context for db access.
     * @param ignoreFailures if insertion failure should be ignored.
     * @throws DataAccessException when something goes wrong with DB interaction.
     * @throws InvalidProtocolBufferException when we cannot parse protobuf objects.
     */
    public void insertGlobalSettings(@Nonnull final List<Setting> settings,
                 @Nonnull DSLContext context, boolean ignoreFailures)
        throws DataAccessException, InvalidProtocolBufferException {

        try {
            insertGlobalSettingsInternal(settings, context, ignoreFailures);
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
    public void restoreDiags(@Nonnull List<String> collectedDiags, @Nonnull DSLContext context) throws DiagnosticsException {
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
            deleteAllGlobalSettings(context);
            insertGlobalSettings(globalSettingsToRestore, context, true);


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
            deleteAllSettingPolicies(context);
            insertAllSettingPolicies(settingPoliciesToRestore, context, true);

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
     *
     * @param context the dsl context for db access.
     */
    private void deleteAllGlobalSettings(@Nonnull DSLContext context) {
        context.truncate(GLOBAL_SETTINGS).execute();
    }

    /**
     * Delete all setting policies.
     *
     * @param context the dsl context for db access.
     */
    private void deleteAllSettingPolicies(@Nonnull DSLContext context) {
         context.deleteFrom(SETTING_POLICY).execute();
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
    public void deleteSettingPolicies(@Nonnull DSLContext context, @Nonnull Collection<Long> oids,
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
     * @param context The dsl context for accessing db.
     * @param ignoreFailures if the insert failures should be ignored or not.
     * @throws StoreOperationException if failed to insert operation failed.
     */
    private void insertAllSettingPolicies(
            @Nonnull final List<SettingProto.SettingPolicy> settingPolicies,
            @Nonnull DSLContext context,
            boolean ignoreFailures)
            throws StoreOperationException {
        final Collection<TableRecord<?>> records = new ArrayList<>();
        for (SettingProto.SettingPolicy settingPolicy : settingPolicies) {
            records.addAll(createSettingPolicy(settingPolicy));
        }

        // execute inserts
        List<Insert<?>> inserts = records.stream()
            .map(r -> recordToBatchQuery(context, r, ignoreFailures))
            .collect(Collectors.toList());
        int[] rowsAffected = context.batch(inserts).execute();
        for (int idx = 0; idx < rowsAffected.length; idx++) {
            // all the queries are insert so if no row was changes there is something wrong.
            if (rowsAffected[idx] == 0) {
                logger.info("There was an error occurred while inserting setting policies. "
                        + "The insert with statement \"{}\" and params \"{}\" did not execute "
                        + "successfully",
                    inserts.get(idx).getSQL(),
                    inserts.get(idx).getParams().values().stream().map(Param::toString)
                        .collect(Collectors.joining(",")),
                    rowsAffected[idx]);
            }
        }
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

    /**
     * Returns a collection of discovered policies that already exist in the database.
     *
     * @param dslContext transaction context to execute within
     * @return map of policy id by policy name, groupped by target id
     */
    public Map<Long, Map<String, DiscoveredObjectVersionIdentity>> getDiscoveredPolicies(
            @Nonnull DSLContext dslContext) {
        final Collection<Record4<String, Long, byte[], Long>> discoveredPolicies =
                dslContext.select(SETTING_POLICY.NAME, SETTING_POLICY.ID, SETTING_POLICY.HASH,
                        SETTING_POLICY.TARGET_ID).from(SETTING_POLICY).where(
                        SETTING_POLICY.POLICY_TYPE.eq(SettingPolicyPolicyType.discovered)).fetch();
        final Map<Long, Map<String, DiscoveredObjectVersionIdentity>> resultMap = new HashMap<>();
        for (Record4<String, Long, byte[], Long> policy: discoveredPolicies) {
            final String name = policy.value1();
            final Long id = policy.value2();
            final byte[] hash = policy.value3();
            final Long targetId = policy.value4();
            final DiscoveredObjectVersionIdentity identity = new DiscoveredObjectVersionIdentity(id, hash);
            resultMap.computeIfAbsent(targetId, key -> new HashMap<>()).put(name, identity);
        }
        return Collections.unmodifiableMap(resultMap);
    }

    /**
     * Return names of the action workflow settings.
     *
     * @return list of action workflow setting names
     */
    public static Set<String> getActionWorkflowSettingNames() {
        return ACTION_WORKFLOW_SETTING_NAMES;
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
            if (ActionSettingSpecs.isExecutionScheduleSetting(setting.getSettingSpecName())) {
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

    /**
     * Save plan entity settings.
     *
     * @param topologyContextId topology context ID
     * @param entityToSettingMap a multimap that maps entity OID to settings associated to it.
     * @param settingToEntityMap a multimap that maps settings to list of entities that use it.
     */
    public void savePlanEntitySettings(final long topologyContextId,
                                       @Nonnull Multimap<Long, SettingToPolicyId> entityToSettingMap,
                                       @Nonnull Multimap<SettingToPolicyId, Long> settingToEntityMap) {
        // Insert unique settings into database
        final Map<SettingToPolicyId, Integer> settingToIdMap = new HashMap<>();
        settingToEntityMap.keySet().forEach(settingToPolicy -> {
            final SettingAdapter settingAdapter = new SettingAdapter(settingToPolicy.getSetting());
            final Record record = dslContext.insertInto(SETTINGS, SETTINGS.TOPOLOGY_CONTEXT_ID,
                    SETTINGS.SETTING_NAME, SETTINGS.SETTING_TYPE, SETTINGS.SETTING_VALUE)
                    .values(topologyContextId, settingAdapter.getSettingName(),
                            (short)settingAdapter.getSettingType().getNumber(), settingAdapter.getValue())
                    .returning(SETTINGS.ID)
                    .fetchOne();
            final Integer settingId = record.getValue(SETTINGS.ID);
            if (ValueCase.SORTED_SET_OF_OID_SETTING_VALUE.equals(settingAdapter.getSettingType())) {
                final List<TableRecord<?>> oidRecords = new ArrayList<>(settingAdapter.getOidList().size());
                for (Long oid : settingAdapter.getOidList()) {
                    oidRecords.add(new SettingsOidsRecord(settingId, oid));
                }
                dslContext.batchInsert(oidRecords).execute();

                // Save the policy(s) that are responsible for the setting. It can be multiple,
                // for example in the case of compute tier exclusions where excluded compute tiers
                // are the union of the exclusions from each policy.
                final List<TableRecord<?>> policyRecords =
                    new ArrayList<>(settingToPolicy.getSettingPolicyIdList().size());
                for (Long policyId : settingToPolicy.getSettingPolicyIdList()) {
                    policyRecords.add(new SettingsPoliciesRecord(settingId, policyId));
                }
                dslContext.batchInsert(policyRecords).execute();
            }

            settingToIdMap.put(settingToPolicy, settingId);
        });

        // Insert entity to setting association to plan_entity_settings table.
        final List<TableRecord<?>> planEntitySettingRecords = new ArrayList<>();
        entityToSettingMap.forEach((entityId, setting) -> planEntitySettingRecords.add(
                new EntitySettingsRecord(topologyContextId, entityId, settingToIdMap.get(setting))));
        dslContext.batchInsert(planEntitySettingRecords).execute();
    }

    /**
     * Get the settings used by specified entities of a plan.
     *
     * @param topologyContextId topology context ID
     * @param entityIds Entity IDs
     * @return a map that maps entities to their settings.
     * @throws InvalidProtocolBufferException error when loading settings object from database
     */
    public Map<Long, Collection<SettingToPolicyId>> getPlanEntitySettings(long topologyContextId,
                                                                          List<Long> entityIds)
            throws InvalidProtocolBufferException {
        Condition conditions = ENTITY_SETTINGS.TOPOLOGY_CONTEXT_ID.eq(topologyContextId);
        // If no entity IDs are provided, return settings for all entities (VMs) for this plan.
        if (!entityIds.isEmpty()) {
            conditions = conditions.and(ENTITY_SETTINGS.ENTITY_ID.in(entityIds));
        }
        final Result<?> entitySettings = dslContext.select().from(ENTITY_SETTINGS)
                .join(SETTINGS)
                .on(ENTITY_SETTINGS.SETTING_ID.eq(SETTINGS.ID))
                .where(conditions)
                .fetch();

        Set<Integer> settingIds = Sets.newHashSet();
        Set<Integer> oidSettingIds = Sets.newHashSet();
        for (Record record : entitySettings) {
            final Integer settingId = record.get(ENTITY_SETTINGS.SETTING_ID);
            final ValueCase settingType = ValueCase.forNumber(record.get(SETTINGS.SETTING_TYPE));
            if (ValueCase.SORTED_SET_OF_OID_SETTING_VALUE.equals(settingType)) {
                oidSettingIds.add(settingId);
            }
            settingIds.add(settingId);
        }

        // Collect Oids by Setting
        Map<Integer, Set<Long>> settingToOids = Maps.newHashMap();
        if (!oidSettingIds.isEmpty()) {
            final Result<?> settingOids = dslContext.select().from(SETTINGS_OIDS)
                    .where(SETTINGS_OIDS.SETTING_ID.in(oidSettingIds))
                    .fetch();
            settingToOids = settingOids.stream()
                    .collect(Collectors.groupingBy(
                            rec -> rec.get(SETTINGS_OIDS.SETTING_ID),
                            Collectors.mapping(rec -> rec.get(SETTINGS_OIDS.OID),
                                    Collectors.toSet())
                            )
                    );
        }

        // Collect Policies by Setting
        Map<Integer, Set<Long>> settingToPolicyIds = Maps.newHashMap();
        if (!settingIds.isEmpty()) {
            final Result<?> settingsPolicies = dslContext.select().from(SETTINGS_POLICIES)
                    .where(SETTINGS_POLICIES.SETTING_ID.in(settingIds))
                    .fetch();
            settingToPolicyIds = settingsPolicies.stream()
                    .collect(Collectors.groupingBy(
                            rec -> rec.get(SETTINGS_POLICIES.SETTING_ID),
                            Collectors.mapping(rec -> rec.get(SETTINGS_POLICIES.POLICY_ID),
                                    Collectors.toSet())
                            )
                    );
        }

        final Multimap<Long, SettingToPolicyId> entityToSettingsMap = HashMultimap.create();
        for (Record record : entitySettings) {
            final Long entityId = record.get(ENTITY_SETTINGS.ENTITY_ID);
            final String settingName = record.get(SETTINGS.SETTING_NAME);
            final ValueCase settingType = ValueCase.forNumber(record.get(SETTINGS.SETTING_TYPE));
            final String value = record.get(SETTINGS.SETTING_VALUE);
            final Integer settingId = record.get(ENTITY_SETTINGS.SETTING_ID);
            final Set<Long> oids = settingToOids.getOrDefault(settingId, Sets.newHashSet());
            final SettingAdapter settingAdapter = new SettingAdapter(settingName, settingType,
                    value, Lists.newArrayList(oids));
            final Set<Long> policyIds = settingToPolicyIds.getOrDefault(settingId, Sets.newHashSet());
            final SettingToPolicyId settingToPolicyId = SettingToPolicyId.newBuilder()
                    .setSetting(settingAdapter.getSetting())
                    .addAllSettingPolicyId(policyIds)
                    .build();
            entityToSettingsMap.put(entityId, settingToPolicyId);
        }
        return entityToSettingsMap.asMap();
    }

    /**
     * Delete records that saved the settings used by the plan from database.
     *
     * @param topologyContextId topology context ID
     */
    public void deletePlanSettings(long topologyContextId) {
        // Delete records in settings table by topologyContextId.
        // Related records in entity_settings and setting_oids tables
        // will be delete automatically (delete cascade).
        dslContext.delete(SETTINGS)
                .where(SETTINGS.TOPOLOGY_CONTEXT_ID.eq(topologyContextId))
                .execute();
    }

    /**
     * Get all topology context IDs that have associated plan settings.
     *
     * @return a set of topology context IDs
     */
    public Set<Long> getContextsWithSettings() {
        return dslContext.selectDistinct(ENTITY_SETTINGS.TOPOLOGY_CONTEXT_ID)
                .from(ENTITY_SETTINGS)
                .fetch()
                .stream()
                .map(Record1::value1)
                .collect(Collectors.toSet());
    }

    /**
     * Adapter class for Setting Protobuf object to adapt to the corresponding database tables.
     */
    @VisibleForTesting
    static class SettingAdapter {
        private Setting setting;
        private String settingName;
        private ValueCase settingType;
        private String value;
        private List<Long> oidList = new ArrayList<>();

        /**
         * Constructor using a Setting object. Database values are derived from the Setting object.
         *
         * @param setting Setting protobuf object
         */
        SettingAdapter(@Nonnull final Setting setting) {
            this.setting = setting;
            this.settingName = setting.getSettingSpecName();
            this.settingType = setting.getValueCase();
            switch (settingType) {
                case BOOLEAN_SETTING_VALUE:
                    value = Boolean.toString(setting.getBooleanSettingValue().getValue());
                    break;
                case NUMERIC_SETTING_VALUE:
                    value = Float.toString(setting.getNumericSettingValue().getValue());
                    break;
                case STRING_SETTING_VALUE:
                    value = setting.getStringSettingValue().getValue();
                    break;
                case ENUM_SETTING_VALUE:
                    value = setting.getEnumSettingValue().getValue();
                    break;
                case SORTED_SET_OF_OID_SETTING_VALUE:
                    value = "-";
                    oidList = setting.getSortedSetOfOidSettingValue().getOidsList();
                    break;
                default:
                    value = "-";
            }
        }

        /**
         * Constructor using database values as input, and derive the Setting object.
         *
         * @param settingName setting name
         * @param settingType setting type
         * @param value setting value
         * @param oidListValue oid list (empty list if not applicable)
         */
        SettingAdapter(@Nonnull final String settingName, @Nonnull final ValueCase settingType,
                              @Nonnull final String value, @Nonnull final List<Long> oidListValue) {
            this.settingName = settingName;
            this.settingType = settingType;
            this.value = value;
            this.oidList = oidListValue;
            Setting.Builder settingBuilder = Setting.newBuilder().setSettingSpecName(settingName);
            switch (settingType) {
                case BOOLEAN_SETTING_VALUE:
                    settingBuilder.setBooleanSettingValue(
                            BooleanSettingValue.newBuilder().setValue(Boolean.parseBoolean(value)));
                    break;
                case NUMERIC_SETTING_VALUE:
                    settingBuilder.setNumericSettingValue(
                            NumericSettingValue.newBuilder().setValue(Float.parseFloat(value)));
                    break;
                case STRING_SETTING_VALUE:
                    settingBuilder.setStringSettingValue(
                            StringSettingValue.newBuilder().setValue(value));
                    break;
                case ENUM_SETTING_VALUE:
                    settingBuilder.setEnumSettingValue(
                            EnumSettingValue.newBuilder().setValue(value));
                    break;
                case SORTED_SET_OF_OID_SETTING_VALUE:
                    settingBuilder.setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                            .addAllOids(oidListValue)
                            .build());
                    break;
                default:
            }
            this.setting = settingBuilder.build();
        }

        Setting getSetting() {
            return setting;
        }

        String getSettingName() {
            return settingName;
        }

        ValueCase getSettingType() {
            return settingType;
        }

        String getValue() {
            return value;
        }

        List<Long> getOidList() {
            return oidList;
        }
    }
}
