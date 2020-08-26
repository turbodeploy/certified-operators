package com.vmturbo.auth.component.licensing;

import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.TurboLicense;
import com.vmturbo.common.protobuf.search.Search.CountEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.licensing.utils.LicenseUtil;

/**
 * Utility class responsible for calculating the number of licensed entities in a collection of
 * {@link LicenseDTO}s, and looking up how many entities are actually in use in the system.
 */
public class LicensedEntitiesCountCalculator {

    private static final Logger logger = LogManager.getLogger();

    private final SearchServiceBlockingStub searchServiceClient;

    LicensedEntitiesCountCalculator(@Nonnull final SearchServiceBlockingStub searchServiceClient) {
        this.searchServiceClient = searchServiceClient;
    }

    @Nonnull
    Optional<LicensedEntitiesCount> getLicensedEntitiesCount(@Nonnull final Collection<LicenseDTO> licenses) {
        Optional<CountedEntity> countedEntity = licenses.stream()
                // Expired licenses don't count towards the number of licensed entities.
                .filter(l -> !LicenseUtil.isExpired(l))
                .filter(LicenseDTO::hasTurbo)
                .map(LicenseDTO::getTurbo)
                .filter(TurboLicense::hasCountedEntity)
                .map(TurboLicense::getCountedEntity)
                .map(CountedEntity::valueOfName)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
        return countedEntity.map(ce -> {
            final int numLicensed = licenses.stream()
                    .filter(LicenseDTO::hasTurbo)
                    .map(LicenseDTO::getTurbo)
                    // In the corner case of multiple counted entities, we don't want to sum
                    // the number licensed across different types.
                    .filter(l -> CountedEntity.valueOfName(l.getCountedEntity())
                            .map(lc -> lc == ce)
                            .orElse(false))
                    .mapToInt(TurboLicense::getNumLicensedEntities)
                    .sum();
            return new LicensedEntitiesCount(ce, numLicensed, getWorkloadCount(ce));
        });

    }

    /**
     * Populate the workload count field on a license, based on it's counted entity type, and return
     * whether the limit was exceeded or not.
     *
     * @param countedEntity The type of entities to count.
     * @return the populated license, which is actually the same license instance that was passed in.
     */
    private Optional<Integer> getWorkloadCount(@Nonnull final CountedEntity countedEntity) {
        // get the workload count from the repository and check if we are over the limit
        // we will fetch either all PM's or active VM's depending on counted entity type.
        CountEntitiesRequest.Builder entityCountRequestBuilder = CountEntitiesRequest.newBuilder();
        switch (countedEntity) {
            case VM:
                logger.debug("Counting active VMs");
                entityCountRequestBuilder.setSearch(SearchQuery.newBuilder()
                        .addSearchParameters(SearchParameters.newBuilder()
                                .setStartingFilter(PropertyFilter.newBuilder()
                                        .setPropertyName("entityType")
                                        .setStringFilter(StringFilter.newBuilder()
                                                .setStringPropertyRegex("VirtualMachine")
                                                .build())
                                        .build())
                                .addSearchFilter(SearchFilter.newBuilder()
                                        .setPropertyFilter(PropertyFilter.newBuilder()
                                                .setPropertyName("state")
                                                .setStringFilter(StringFilter.newBuilder()
                                                        .setStringPropertyRegex("ACTIVE"))))
                                .build()));
                break;
            case SOCKET:
                // SOCKET count type is not supported in XL!
                // just log a warning here, since this should have been picked up in the validation
                // process.
                logger.warn("Socket-based licenses are not supported in XL.");
                break;
            default:
                // ditto for any other counted entity type
                logger.warn("Counted Entity type {} not understood -- will not count workloads",
                        countedEntity);
                break;
        }

        // get the appropriate workload count.
        // we're going to check the workload limit here and set a boolean, since it's not
        // flagged in the license objects directly.
        CountEntitiesRequest request = entityCountRequestBuilder.build();
        if (request.getSearch().getSearchParametersCount() == 0) {
            logger.info("Empty entity count request -- will not request workload count.");
            return Optional.empty();
        } else {
            // call using waitForReady -- this is on a worker thread, and we are willing to block
            // until the repository service is up.
            int numInUseEntities = searchServiceClient.withWaitForReady().countEntities(request).getEntityCount();
            logger.debug("Search returned {} entities.", numInUseEntities);

            return Optional.of(numInUseEntities);
        }
    }

    /**
     * The count of licensed and in-use entities.
     */
    public static class LicensedEntitiesCount {
        private final CountedEntity countedEntity;
        private final int numLicensed;
        private final Optional<Integer> numInUse;

        @VisibleForTesting
        LicensedEntitiesCount(@Nonnull final CountedEntity countedEntity,
                final int numLicensed,
                @Nonnull final Optional<Integer> numInUse) {
            this.countedEntity = countedEntity;
            this.numLicensed = numLicensed;
            this.numInUse = numInUse;
        }

        public int getNumLicensed() {
            return numLicensed;
        }

        public CountedEntity getCountedEntity() {
            return countedEntity;
        }

        public Optional<Integer> getNumInUse() {
            return numInUse;
        }

        public boolean isOverLimit() {
            return numInUse
                    .map(inUse -> inUse > numLicensed)
                    .orElse(false);
        }
    }
}
