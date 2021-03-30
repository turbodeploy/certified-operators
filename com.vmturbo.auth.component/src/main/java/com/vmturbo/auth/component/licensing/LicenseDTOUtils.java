package com.vmturbo.auth.component.licensing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.jpountz.xxhash.StreamingXXHash64;
import net.jpountz.xxhash.XXHashFactory;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
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
 * Utility functions for working with License DTO objects.
 */
public class LicenseDTOUtils {
    private static final Logger logger = LogManager.getLogger();

    private LicenseDTOUtils() {}

    /**
     * Triggers license validation using the common license validation routine in {@link LicenseUtil}
     * that is shared with Ops Manager, and also adds XL-specific license checks as well.
     *
     * @param license the {@link ILicense} object to run validation on.
     * @return a set of {@link ErrorReason} objects representing any validation issues discovered.
     */
    public static Set<ErrorReason> validateXLLicense(ILicense license) {
        // run base validations
        Set<ErrorReason> validationErrors = LicenseUtil.validate(license);

        // Now perform XL-specific validations
        // XL only understands VM entity counts, so reject non-VM licenses.
        if (license.getCountedEntity() != CountedEntity.VM) {
            validationErrors.add(ErrorReason.INCOMPATIBLE);
        }
        // XL will reject any licenses that don't have features.
        if (!license.hasFeatures()) {
            validationErrors.add(ErrorReason.INVALID_FEATURE_SET);
        }
        return validationErrors;
    }

    /**
     * Convert a {@link LicenseDTO} object to a {@link License} object.
     *
     * @param licenseDTO the {@link LicenseDTO} to convert.
     * @return the resulting {@link License} object.
     */
    public static Optional<License> licenseDTOtoLicense(LicenseDTO licenseDTO) {
        License license = new License();
        doIf(licenseDTO.hasUuid(), () -> license.setUuid(licenseDTO.getUuid()));

        if (!licenseDTO.hasTurbo()) {
            return Optional.empty();
        }

        TurboLicense turboLicense = licenseDTO.getTurbo();
        doIf(turboLicense.hasEmail(), () -> license.setEmail(turboLicense.getEmail()));
        doIf(turboLicense.hasEdition(), () -> license.setEdition(turboLicense.getEdition()));
        doIf(turboLicense.hasExpirationDate(), () -> license.setExpirationDate(turboLicense.getExpirationDate()));
        doIf(turboLicense.hasLicenseOwner(), () -> license.setLicenseOwner(turboLicense.getLicenseOwner()));
        doIf(turboLicense.hasLicenseKey(), () -> license.setLicenseKey(turboLicense.getLicenseKey()));
        doIf(turboLicense.hasExternalLicenseKey(), () -> license.setExternalLicenseKey(turboLicense.getExternalLicenseKey()));
        doIf(licenseDTO.hasFilename(), () -> license.setFilename(licenseDTO.getFilename()));
        doIf(turboLicense.hasCountedEntity(),
                () -> CountedEntity.valueOfName(turboLicense.getCountedEntity())
                    .ifPresent(license::setCountedEntity));
        doIf(turboLicense.hasNumLicensedEntities(), () -> license.setNumLicensedEntities(turboLicense.getNumLicensedEntities()));
        doIf(turboLicense.getFeaturesCount() > 0, () -> license.addFeatures(turboLicense.getFeaturesList()));
        // we don't need to transcribe isValid() since it's calculated based on error reasons in the
        // shared ILicense objects.
        doIf(turboLicense.getErrorReasonCount() > 0,
            () -> license.setErrorReasons(turboLicense.getErrorReasonList().stream()
                .map(ErrorReason::valueOf)
                .collect(Collectors.toSet())));

        return Optional.of(license);
    }

    static void doIf(final boolean condition, @Nonnull final Runnable doIt) {
        if (condition) {
            doIt.run();
        }
    }

    /**
     * Convert a License to LicenseDTO object.
     * @param license the License to convert
     *
     * @return A corresponding LicenseDTO object that is equivalent to the License
     */
    public static LicenseDTO iLicenseToLicenseDTO(ILicense license) {
        LicenseDTO.Builder retBldr = LicenseDTO.newBuilder();
        if (null != license.getUuid()) {
            retBldr.setUuid(license.getUuid());
        }
        TurboLicense.Builder builder = retBldr.getTurboBuilder();
        if (null != license.getEmail()) {
            builder.setEmail(license.getEmail());
        }
        if (null != license.getEdition()) {
            builder.setEdition(license.getEdition());
        }
        if (null != license.getExpirationDate()) {
            builder.setExpirationDate(license.getExpirationDate());
        }
        if (null != license.getLicenseOwner()) {
            builder.setLicenseOwner(license.getLicenseOwner());
        }
        if (null != license.getLicenseKey()) {
            builder.setLicenseKey(license.getLicenseKey());
        }
        if (null != license.getExternalLicenseKey()) {
            builder.setExternalLicenseKey(license.getExternalLicenseKey());
        }
        if (null != license.getFilename()) {
            retBldr.setFilename(license.getFilename());
        }
        if (null != license.getCountedEntity()) {
            builder.setCountedEntity(license.getCountedEntity().name());
        }
        builder.setNumLicensedEntities(license.getNumLicensedEntities());
        if (null != license.getFeatures()) {
            builder.addAllFeatures(license.getFeatures());
        }

        builder.setIsValid(license.isValid());
        if (license.getErrorReasons().size() > 0) {
            builder.addAllErrorReason(license.getErrorReasons().stream()
                    .map(ErrorReason::name)
                    .collect(Collectors.toList()));
        }
        return retBldr.build();
    }

    /**
     * Convert a {@link License } to a {@link LicenseSummary} object.
     *
     * @param licenses The licenses to include in the summary.
     * @param licensedEntitiesCount The {@link LicensedEntitiesCount}, if any.
     * @return A shiny new LicenseSummary.
     */
    public static LicenseSummary createLicenseSummary(Collection<LicenseDTO> licenses,
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
}
