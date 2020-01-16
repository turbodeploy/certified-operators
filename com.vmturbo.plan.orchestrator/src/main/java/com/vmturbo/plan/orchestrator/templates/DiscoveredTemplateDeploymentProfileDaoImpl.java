package com.vmturbo.plan.orchestrator.templates;

import static com.vmturbo.plan.orchestrator.db.Tables.DEPLOYMENT_PROFILE;
import static com.vmturbo.plan.orchestrator.db.Tables.RESERVATION;
import static com.vmturbo.plan.orchestrator.db.Tables.RESERVATION_TO_TEMPLATE;
import static com.vmturbo.plan.orchestrator.db.Tables.TEMPLATE;
import static com.vmturbo.plan.orchestrator.db.Tables.TEMPLATE_TO_DEPLOYMENT_PROFILE;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.plan.orchestrator.db.enums.ReservationStatus;
import com.vmturbo.plan.orchestrator.db.tables.records.ReservationRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.DeploymentProfileInfo;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.UpdateDiscoveredTemplateDeploymentProfileResponse;
import com.vmturbo.common.protobuf.plan.DeploymentProfileDTO.UpdateDiscoveredTemplateDeploymentProfileResponse.TargetProfileIdentities;
import com.vmturbo.common.protobuf.plan.TemplateDTO;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.plan.orchestrator.db.tables.records.DeploymentProfileRecord;
import com.vmturbo.plan.orchestrator.db.tables.records.TemplateRecord;
import com.vmturbo.plan.orchestrator.db.tables.records.TemplateToDeploymentProfileRecord;


/**
 * Store discovered templates and deployment profiles into database. And right now,
 * the relationship between templates with deployment profiles is many to many.
 */
public class DiscoveredTemplateDeploymentProfileDaoImpl {

    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    public DiscoveredTemplateDeploymentProfileDaoImpl(@Nonnull final DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Store all targets discovered templates and deployment profiles into database.
     *
     * @param targetMap Map contains all targets discovered templates and deployment profiles.
     * @param orphanedDeploymentProfileMap Contains a list of discovered deployment profile which
     *                                        have no reference template.
     * @return external to internal identity mapping
     */
    public UpdateDiscoveredTemplateDeploymentProfileResponse setDiscoveredTemplateDeploymentProfile(
        @Nonnull Map<Long, TemplateInfoToDeploymentProfileMap> targetMap,
        @Nonnull Map<Long, List<DeploymentProfileInfo>> orphanedDeploymentProfileMap) {
        Set<Long> discoveredTargetIds = targetMap.entrySet().stream()
            .map(Entry::getKey)
            .collect(Collectors.toSet());
        UpdateDiscoveredTemplateDeploymentProfileResponse.Builder response =
            UpdateDiscoveredTemplateDeploymentProfileResponse
                       .newBuilder();

        dsl.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            // Remove records which target id is not exist in current upload data.
            final List<TemplateRecord> missingTargetTemplateRecords = transactionDsl
                .selectFrom(TEMPLATE)
                .where(TEMPLATE.TARGET_ID.isNotNull().and(TEMPLATE.TARGET_ID.notIn(discoveredTargetIds)))
                .fetch();

            // Find reservation records that used the templates to be deleted.
            final List<ReservationRecord> invalidReservationRecords = transactionDsl.selectFrom(
                    RESERVATION.join(RESERVATION_TO_TEMPLATE)
                            .on(RESERVATION.ID.eq(RESERVATION_TO_TEMPLATE.RESERVATION_ID))
                            .and(RESERVATION_TO_TEMPLATE.TEMPLATE_ID.in(buildTemplateIds(missingTargetTemplateRecords))))
                    .fetch()
                    .into(RESERVATION);
            if (!invalidReservationRecords.isEmpty()) {
                transactionDsl.batchUpdate(invalidateReservation(invalidReservationRecords)).execute();
                logger.warn("Invalidated reservations: {}", invalidReservationRecords.stream()
                        .map(ReservationRecord::getName).collect(Collectors.toList()));
            }

            final List<DeploymentProfileRecord> missingTargetDeployprofileRecords = transactionDsl
                .selectFrom(DEPLOYMENT_PROFILE)
                .where(DEPLOYMENT_PROFILE.TARGET_ID.isNotNull().and(DEPLOYMENT_PROFILE.TARGET_ID.notIn(discoveredTargetIds)))
                .fetch();

            transactionDsl.batchDelete(missingTargetTemplateRecords).execute();
            transactionDsl.batchDelete(missingTargetDeployprofileRecords).execute();

            for (Long targetId : discoveredTargetIds) {
                TargetProfileIdentities identities = setDiscoveredTemplateDeploymentProfileByTarget(targetId,
                    transactionDsl, targetMap.get(targetId), orphanedDeploymentProfileMap.get(targetId));
                response.addTargetProfileIdentities(identities);
            }
        });

        return response.build();
    }

    private Set<Long> buildTemplateIds(List<TemplateRecord> missingTargetTemplateRecords) {
        return missingTargetTemplateRecords.stream().map(TemplateRecord::getId).collect(Collectors.toSet());
    }

    private List<ReservationRecord> invalidateReservation(List<ReservationRecord> oldRecords) {
        return oldRecords.stream()
                .map(r -> new ReservationRecord(r.getId(), r.getName(), r.getStartTime(), r.getExpireTime(),
                        ReservationStatus.invalid, r.getReservationTemplateCollection(), r.getConstraintInfoCollection()))
                .collect(Collectors.toList());
    }

    /**
     * For one target, store its discovered templates and deployment profiles.
     *
     * @param targetId Id of target object.
     * @param transactionDsl Transaction context.
     * @param profileMap Contains relationship between templates to list of attached deployment profiles.
     * @param orphanedDeploymentProfile Contains a list of discovered deployment profile which
     *                                        have no reference template.
     * @return generated identities
     */
    private TargetProfileIdentities setDiscoveredTemplateDeploymentProfileByTarget(@Nonnull long targetId,
                                                                @Nonnull DSLContext transactionDsl,
                                                                @Nonnull TemplateInfoToDeploymentProfileMap profileMap,
                                                                @Nonnull List<DeploymentProfileInfo> orphanedDeploymentProfile) {
        final Set<TemplateInfo> templateInfos = profileMap.keySet();
        final Set<DeploymentProfileInfo> deploymentProfileInfos = profileMap.entrySet().stream()
            .map(Entry::getValue)
            .flatMap(List::stream)
            .collect(Collectors.toSet());
        Map<String, Long> templateInfoIdMap =
            setDiscoveredTemplatesByTarget(targetId, transactionDsl, templateInfos);
        // Add no reference deployment profiles into sets in order to replace all deployment profiles
        deploymentProfileInfos.addAll(orphanedDeploymentProfile);
        Map<String, Long> deploymentProfileIdMap =
            setDiscoveredDeploymentProfileByTarget(targetId, transactionDsl, deploymentProfileInfos);
        updateTemplateToDeploymentProfileTable(profileMap, transactionDsl, templateInfoIdMap,
            deploymentProfileIdMap);
        return TargetProfileIdentities.newBuilder().setTargetOid(targetId)
                        .putAllProfileIdToOid(templateInfoIdMap)
                        .putAllDeploymentProfileIdToOid(deploymentProfileIdMap).build();
    }

    /**
     * Replace discovered templates data in database, and we use probe send template id to differentiate
     * templates. And for same templates, we don't change its template id. And for templates not discovered
     * this time, will be removed from table.
     *
     * @param targetId Id of target object.
     * @param transactionDsl Transaction context.
     * @param templateInfos Contains set of new discovered templates.
     * @return
     */
    private Map<String, Long> setDiscoveredTemplatesByTarget(long targetId,
                                                             DSLContext transactionDsl,
                                                             Set<TemplateInfo> templateInfos) {
        final Map<String, Long> templateInfoIdMap = new HashMap<>();

        final Set<String> templateInstanceProbeIds = templateInfos.stream()
            .map(TemplateInfo::getProbeTemplateId)
            .collect(Collectors.toSet());

        final List<TemplateRecord> existingRecords = dsl.selectFrom(TEMPLATE)
            .where(TEMPLATE.TARGET_ID.eq(targetId).and(TEMPLATE.TARGET_ID.isNotNull()))
            .fetch();

        final Set<String> existingTemplateProbeIds = existingRecords.stream()
            .map(TemplateRecord::getProbeTemplateId)
            .collect(Collectors.toSet());

        final List<TemplateRecord> recordsToDelete = existingRecords.stream()
            .filter(template -> !templateInstanceProbeIds.contains(template.getProbeTemplateId()))
            .collect(Collectors.toList());

        final Map<String, TemplateInfo> recordsToUpdateByProbeIdMap = templateInfos.stream()
            .filter(template -> existingTemplateProbeIds.contains(template.getProbeTemplateId()))
            .collect(Collectors.toMap(template -> template.getProbeTemplateId(), Function.identity()));

        final List<TemplateRecord> recordsToUpdate = existingRecords.stream()
            .filter(template -> recordsToUpdateByProbeIdMap.containsKey(template.getProbeTemplateId()))
            .map(template -> {
                TemplateInfo templateInfo = recordsToUpdateByProbeIdMap.get(template.getProbeTemplateId());
                template.setTemplateInfo(templateInfo);
                template.setName(templateInfo.getName());
                template.setEntityType(templateInfo.getEntityType());
                return template;
            })
            .collect(Collectors.toList());

        transactionDsl.batchDelete(recordsToDelete).execute();
        addTemplateRecordToIdMap(templateInfoIdMap, recordsToUpdate);
        transactionDsl.batchUpdate(recordsToUpdate).execute();

        final List<TemplateRecord> recordsToAdd = templateInfos.stream()
            .filter(template -> !existingTemplateProbeIds.contains(template.getProbeTemplateId()))
            .map(template ->
                transactionDsl.newRecord(TEMPLATE, new TemplateRecord(
                    IdentityGenerator.next(),
                    targetId,
                    template.getName(),
                    template.getEntityType(),
                    template,
                    template.getProbeTemplateId(),
                    TemplateDTO.Template.Type.DISCOVERED.name())))
            .collect(Collectors.toList());
        addTemplateRecordToIdMap(templateInfoIdMap, recordsToAdd);
        transactionDsl.batchInsert(recordsToAdd).execute();
        return templateInfoIdMap;
    }

    /**
     * Replace discovered deployment profile data in database, and we use probe send deployment profile
     * id to differentiate deployment profile. And for same deployment profile, we don't change its
     * id. And for deployment profiles not discovered this time, will be removed from table.
     *
     * @param targetId Id of target object.
     * @param transactionDsl Transaction context.
     * @param deploymentProfileInfos Contains set of new discovered deployment profile.
     * @return
     */
    private Map<String, Long> setDiscoveredDeploymentProfileByTarget(long targetId,
                                                                     DSLContext transactionDsl,
                                                                     Set<DeploymentProfileInfo> deploymentProfileInfos) {
        final Map<String, Long> deploymentProfileIdMap = new HashMap<>();

        final Set<String> deploymentProfileProbeIds = deploymentProfileInfos.stream()
            .map(DeploymentProfileInfo::getProbeDeploymentProfileId)
            .collect(Collectors.toSet());

        final List<DeploymentProfileRecord> existingRecords = dsl.selectFrom(DEPLOYMENT_PROFILE)
            .where(DEPLOYMENT_PROFILE.TARGET_ID.eq(targetId).and(DEPLOYMENT_PROFILE.TARGET_ID.isNotNull()))
            .fetch();

        final Set<String> existingDeploymentProfileProbeIds = existingRecords.stream()
            .map(DeploymentProfileRecord::getProbeDeploymentProfileId)
            .collect(Collectors.toSet());

        final List<DeploymentProfileRecord> recordsToDelete = existingRecords.stream()
            .filter(deploymentProfile ->
                !deploymentProfileProbeIds.contains(deploymentProfile.getProbeDeploymentProfileId()))
            .collect(Collectors.toList());

        final Map<String, DeploymentProfileInfo> recordsToUpdateByProbeIdMap = deploymentProfileInfos.stream()
            .filter(deploymentProfile ->
                existingDeploymentProfileProbeIds.contains(deploymentProfile.getProbeDeploymentProfileId()))
            .collect(Collectors.toMap(profile -> profile.getProbeDeploymentProfileId(), Function.identity(),
                // bifunction to resolve the key duplicates
                (profile1, profile2) -> {
                    // we are keeping the 1st profile only, using profile name lexicographic ordering,
                    // so that every discovery the same profile is maintained and is consistent (and we
                    // are not ping-ponging between profiles).
                    if (profile1.getName().compareTo(profile2.getName()) > 0) {
                        // 2nd profile name is after 1st one. Invert them
                        DeploymentProfileInfo profileTmp = profile1;
                        profile1 = profile2;
                        profile2 = profileTmp;
                    }
                    logger.error("Two deployment profiles have the same ProfileID: {}. Keeping: {}, dropping: {}",
                        profile1.getProbeDeploymentProfileId(), profile1.getName(), profile2.getName());
                    return profile1;
                }));

        final List<DeploymentProfileRecord> recordsToUpdate = existingRecords.stream()
            .filter(deploymentProfile ->
                recordsToUpdateByProbeIdMap.containsKey(deploymentProfile.getProbeDeploymentProfileId()))
            .map(deploymentProfile -> {
                DeploymentProfileInfo deploymentProfileInfo =
                    recordsToUpdateByProbeIdMap.get(deploymentProfile.getProbeDeploymentProfileId());
                deploymentProfile.setName(deploymentProfileInfo.getName());
                deploymentProfile.setDeploymentProfileInfo(deploymentProfileInfo);
                return deploymentProfile;
            })
            .collect(Collectors.toList());

        transactionDsl.batchDelete(recordsToDelete).execute();
        addDeploymentProfileRecordToIdMap(deploymentProfileIdMap, recordsToUpdate);
        transactionDsl.batchUpdate(recordsToUpdate).execute();

        final List<DeploymentProfileRecord> recordsToAdd = deploymentProfileInfos.stream()
            .filter(deploymentProfile ->
                !existingDeploymentProfileProbeIds.contains(deploymentProfile.getProbeDeploymentProfileId()))
            .map(deploymentProfile -> createDeploymentProfileRecord(transactionDsl, targetId, deploymentProfile))
            .collect(Collectors.toList());
        addDeploymentProfileRecordToIdMap(deploymentProfileIdMap, recordsToAdd);
        transactionDsl.batchInsert(recordsToAdd).execute();
        return deploymentProfileIdMap;
    }

    private DeploymentProfileRecord createDeploymentProfileRecord(DSLContext transactionDsl,
                                                                  long targetId,
                                                                  DeploymentProfileInfo deploymentProfileInfo) {
        return transactionDsl.newRecord(DEPLOYMENT_PROFILE, new DeploymentProfileRecord(
            IdentityGenerator.next(),
            targetId,
            deploymentProfileInfo.getProbeDeploymentProfileId(),
            deploymentProfileInfo.getName(),
            deploymentProfileInfo));
    }

    private void addTemplateRecordToIdMap(Map<String, Long> templateInfoIdMap,
                                          List<TemplateRecord> templateRecords) {
        templateRecords.stream()
            .forEach(templateRecord ->
                templateInfoIdMap.put(templateRecord.getProbeTemplateId(), templateRecord.getId()));
    }

    private void addDeploymentProfileRecordToIdMap(Map<String, Long> deploymentProfileIdMap,
                                                   List<DeploymentProfileRecord> deploymentProfileRecords) {
        deploymentProfileRecords.stream()
            .forEach(deploymentProfileRecord ->
                deploymentProfileIdMap.put(deploymentProfileRecord.getProbeDeploymentProfileId(),
                    deploymentProfileRecord.getId()));
    }

    /**
     * Update the relationship table by using current new discovered templates and deployment profiles.
     *
     * @param profileMap Contains relationship between templates to list of attached deployment profiles.
     * @param transactionDsl Transaction context.
     * @param templateInfoIdMap Map contains probe unique template Id to table template id.
     * @param deploymentProfileIdMap Map contains probe unique deployment profile Id to table deployment profile Id.
     */
    private void updateTemplateToDeploymentProfileTable(TemplateInfoToDeploymentProfileMap profileMap,
                                                        DSLContext transactionDsl,
                                                        Map<String, Long> templateInfoIdMap,
                                                        Map<String, Long> deploymentProfileIdMap) {
        Set<Long> templateIdToDelete = templateInfoIdMap.entrySet().stream()
            .map(Entry::getValue)
            .collect(Collectors.toSet());

        Set<Long> deploymentProfileToDelete = deploymentProfileIdMap.entrySet().stream()
            .map(Entry::getValue)
            .collect(Collectors.toSet());

        transactionDsl.deleteFrom(TEMPLATE_TO_DEPLOYMENT_PROFILE)
            .where(TEMPLATE_TO_DEPLOYMENT_PROFILE.TEMPLATE_ID.in(templateIdToDelete)
                .or(TEMPLATE_TO_DEPLOYMENT_PROFILE.DEPLOYMENT_PROFILE_ID.in(deploymentProfileToDelete)))
            .execute();

        List<TemplateToDeploymentProfileRecord> relationRecords = profileMap.entrySet().stream()
            .map(entry -> generateRelationRecord(entry, transactionDsl, templateInfoIdMap, deploymentProfileIdMap))
            .flatMap(List::stream)
            .collect(Collectors.toList());
        transactionDsl.batchInsert(relationRecords).execute();
    }

    private List<TemplateToDeploymentProfileRecord> generateRelationRecord(Entry<TemplateInfo,
                                                                           List<DeploymentProfileInfo>> entry,
                                                                           DSLContext transactionDsl,
                                                                           Map<String, Long> templateInfoIdMap,
                                                                           Map<String, Long> deploymentProfileIdMap) {
        final TemplateInfo templateInfo = entry.getKey();
        final List<TemplateToDeploymentProfileRecord> relationRecords = entry.getValue().stream()
            .map(deploymentProfile -> (
                transactionDsl.newRecord(TEMPLATE_TO_DEPLOYMENT_PROFILE, new TemplateToDeploymentProfileRecord(
                    Optional.ofNullable(templateInfoIdMap.get(templateInfo.getProbeTemplateId()))
                        .orElseThrow(() -> new RuntimeException("Template "
                            + templateInfo.getProbeTemplateId() + " is not found.")),
                    Optional.ofNullable(deploymentProfileIdMap.get(deploymentProfile.getProbeDeploymentProfileId()))
                        .orElseThrow(() -> new RuntimeException("Deployment profile "
                            + deploymentProfile.getProbeDeploymentProfileId() + " is not found"))
                ))
            ))
            .collect(Collectors.toList());
        return relationRecords;
    }

    /**
     * Keep Mapping from discovered templates to a list attached discovered deployment profiles.
     */
    public static class TemplateInfoToDeploymentProfileMap {

        private Map<TemplateInfo, List<DeploymentProfileInfo>> templateInfoListMap;

        public TemplateInfoToDeploymentProfileMap() {
            this.templateInfoListMap = new HashMap<>();
        }

        public void put(TemplateInfo templateInfo, List<DeploymentProfileInfo> deploymentProfileInfos) {
            templateInfoListMap.put(templateInfo, deploymentProfileInfos);
        }

        public List<DeploymentProfileInfo> get(TemplateInfo templateInfo) {
            return templateInfoListMap.getOrDefault(templateInfo, Collections.emptyList());
        }

        public Set<Entry<TemplateInfo, List<DeploymentProfileInfo>>> entrySet() {
            return templateInfoListMap.entrySet();
        }

        public Set<TemplateInfo> keySet() {
            return templateInfoListMap.keySet();
        }

        public void clear() {
            templateInfoListMap.clear();
        }
    }
}
