package com.vmturbo.auth.component.licensing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.google.common.annotations.VisibleForTesting;

import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.component.licensing.LicensedEntitiesCountCalculator.LicensedEntitiesCount;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense.Type;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.TurboLicense;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary.ExternalLicenseSummary;
import com.vmturbo.licensing.License;
import com.vmturbo.licensing.utils.LicenseUtil;

/**
 * REsponsible for creating a {@link LicenseSummary} of a bunch of {@link LicenseDTO}s.
 */
public class LicenseSummaryCreator {

    @VisibleForTesting
    static final String GRAFANA_ADMINS_CLAIM_NAME = "included_admins";

    private static final Logger logger = LogManager.getLogger();

    private final int defaultMaxReportEditors;

    /**
     * Create a new instance.
     *
     * @param defaultMaxReportEditors The number of maximum report editors to use if there is no
     *                                valid grafana license added to the system.
     */
    public LicenseSummaryCreator(final int defaultMaxReportEditors) {
        this.defaultMaxReportEditors = defaultMaxReportEditors;
    }

    /**
     * Convert a {@link License } to a {@link LicenseSummary} object.
     *
     * @param licenses The licenses to include in the summary.
     * @param licensedEntitiesCount The {@link LicensedEntitiesCount}, if any.
     * @return A shiny new LicenseSummary.
     */
    public LicenseSummary createLicenseSummary(Collection<LicenseDTO> licenses,
            Optional<LicensedEntitiesCount> licensedEntitiesCount) {
        LicenseSummary.Builder summaryBuilder = LicenseSummary.newBuilder();

        // Calculate the expiration dates from ALL licenses.
        getLatestExpirationDate(licenses.stream()
                .filter(LicenseDTO::hasTurbo)
                .map(LicenseDTO::getTurbo)
                .map(TurboLicense::getExpirationDate))
                .ifPresent(latestExpiration -> {
                    summaryBuilder.setExpirationDate(latestExpiration);
                    summaryBuilder.setIsExpired(ILicense.isExpired(latestExpiration));
                });

        final Collection<TurboLicense> nonExpiredTurboLicenses = new ArrayList<>();
        final Map<Type, List<ExternalLicense>> externalLicensesByType = new HashMap<>();
        // track if any of the non-expired licenses are not valid
        boolean areAllNonExpiredLicensesValid = true; // until proven guilty
        for (LicenseDTO license : licenses) {
            if (license.hasTurbo()) {
                if (!LicenseUtil.isExpired(license)) {
                    nonExpiredTurboLicenses.add(license.getTurbo());
                    // we'll check the validity here too
                    boolean isThisLicenseValid = LicenseUtil.toModel(license)
                            .map(License::isValid)
                            .orElse(false);
                    // if not valid, log for posterity. Debug, since this is a recurring operation.
                    if (!isThisLicenseValid) {
                        // maybe get the errors at some point too.
                        logger.debug("License {} is detected as invalid.", license.getUuid());
                    }
                    areAllNonExpiredLicensesValid = areAllNonExpiredLicensesValid && isThisLicenseValid;
                }
            } else if (license.hasExternal()) {
                externalLicensesByType.computeIfAbsent(license.getExternal().getType(), k -> new ArrayList<>())
                        .add(license.getExternal());
            }
        }

        licensedEntitiesCount.ifPresent(licensedEntities -> {
            summaryBuilder.setCountedEntity(licensedEntities.getCountedEntity().name());
            summaryBuilder.setNumLicensedEntities(licensedEntities.getNumLicensed());
            licensedEntities.getNumInUse().ifPresent(summaryBuilder::setNumInUseEntities);
            summaryBuilder.setIsOverEntityLimit(licensedEntities.isOverLimit());
        });

        // the aggregate feature set available across all of the stored licenses
        // Only consider non-expired licenses.
        nonExpiredTurboLicenses.stream()
                .filter(l -> !ILicense.isExpired(l.getExpirationDate()))
                .map(TurboLicense::getFeaturesList)
                .flatMap(List::stream)
                .forEach(summaryBuilder::addFeature);

        nonExpiredTurboLicenses.stream()
                .map(TurboLicense::getErrorReasonList)
                .flatMap(List::stream)
                .forEach(summaryBuilder::addErrorReason);

        summaryBuilder.setIsValid(areAllNonExpiredLicensesValid);

        // If there is a non-default extracter from Grafana licenses we set it
        // in the subsequent loop.
        summaryBuilder.setMaxReportEditorsCount(defaultMaxReportEditors);

        externalLicensesByType.forEach((type, externalLicenses) -> {
            final ExternalLicenseSummary.Builder externalTypeSummaryBldr = ExternalLicenseSummary.newBuilder()
                    .setType(type);
            getLatestExpirationDate(externalLicenses.stream()
                    .map(ExternalLicense::getExpirationDate))
                    .ifPresent(latestExpiration -> {
                        externalTypeSummaryBldr.setExpirationDate(latestExpiration);
                        externalTypeSummaryBldr.setIsExpired(ILicense.isExpired(latestExpiration));
                    });

            final int seed = 0xC0FFEE;
            final StreamingXXHash64 hash = XXHashFactory.fastestJavaInstance().newStreamingHash64(seed);
            externalLicenses.forEach(license -> {
                hash.update(license.getPayloadBytes().toByteArray(), 0, license.getPayloadBytes().size());
            });
            externalTypeSummaryBldr.setChecksum(hash.getValue());
            if (type == Type.GRAFANA && externalTypeSummaryBldr.hasIsExpired()
                    && !externalTypeSummaryBldr.getIsExpired()) {
                // support applying only one Grafana license
                final Optional<Integer> reportEditorsCount = getReportEditorsCount(externalLicenses.get(0));
                if (reportEditorsCount.isPresent()) {
                    // this count includes default editor
                    summaryBuilder.setMaxReportEditorsCount(reportEditorsCount.get());
                } else {
                    logger.debug("Value of max allowed count of report editors wasn't found in"
                            + " Grafana license");
                }
            }
            summaryBuilder.addExternalLicensesByType(externalTypeSummaryBldr);
        });

        // set generation date to now.
        summaryBuilder.setGenerationDate(DateTimeUtil.getNow());

        return summaryBuilder.build();
    }


    @Nonnull
    private static Optional<String> getLatestExpirationDate(Stream<String> dates) {
        return dates.filter(StringUtils::isNotBlank)
                .max(Comparator.naturalOrder());
    }

    private static Optional<Integer> getReportEditorsCount(
            @Nonnull ExternalLicense grafanaLicense) {
        if (grafanaLicense.hasPayload()) {
            return getMaxCountOfReportEditors(grafanaLicense.getPayload());
        } else {
            logger.warn("Failed to get count of report editors/admins for Grafana"
                    + " because license's payload is absent");
            return Optional.empty();
        }
    }

    private static Optional<Integer> getMaxCountOfReportEditors(String licensePayload) {
        final DecodedJWT decodedJWT = JWT.decode(licensePayload);
        final Map<String, Claim> jwtClaims = decodedJWT.getClaims();
        if (jwtClaims.containsKey(GRAFANA_ADMINS_CLAIM_NAME)) {
            // The number of admins in the license includes the "default" Grafana admin.
            // We don't want to count that towards the number of allowed report editors.
            return Optional.of(jwtClaims.get(GRAFANA_ADMINS_CLAIM_NAME).asInt());
        } else {
            return Optional.empty();
        }
    }
}
