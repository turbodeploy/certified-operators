package com.vmturbo.clustermgr;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc.LicenseManagerServiceBlockingStub;
import com.vmturbo.common.protobuf.licensing.Licensing.GetLicensesRequest;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.TurboLicense;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.utils.BuildProperties;

/**
 * Responsible for formatting the file name for the compressed diags.
 */
public class DiagEnvironmentSummary {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The prefix to use for the file name.
     */
    private static final String DIAG_FILE_PREFIX = "turbonomic-diags";

    private final BuildProperties buildProperties;

    private final Clock clock;

    private final ChannelFactory channelFactory;

    private final String authHost;

    private final int serverGrpcPort;

    /**
     * The domain of the installation, extracted from the license added to the product.
     * (i.e. if the license is for me@turbonomic.com the domain will be turbonomic.com).
     *
     * <p>Once we find the domain, we only need to set it once - the domain of a particular installation
     * shouldn't change.
     */
    private final SetOnce<Optional<String>> domain = new SetOnce<>();

    DiagEnvironmentSummary(@Nonnull final BuildProperties buildProperties,
                           @Nonnull final Clock clock,
                           @Nonnull final ChannelFactory channelFactory,
                           @Nonnull final String authHost,
                           final int serverGrpcPort) {
        this.buildProperties = buildProperties;
        this.clock = clock;
        this.channelFactory = channelFactory;
        this.authHost = authHost;
        this.serverGrpcPort = serverGrpcPort;
    }

    /**
     * Get the file name to use for the diags.
     *
     * @return A string representing the file name. Note that this does not include the path.
     */
    @Nonnull
    public String getDiagFileName() {
        // The file name will start with turbonomic-diags, followed by some "-"-separated properties.
        // Something like:
        // turbonomic-diags-2019-09-25-turbonomic.com-7.21.0-SNAPSHOT-cd49c13-_1569431182577.zip
        final StringJoiner stringJoiner = new StringJoiner("-");
        stringJoiner.add(DIAG_FILE_PREFIX);
        stringJoiner.add(LocalDateTime.now(clock).toLocalDate().toString());
        getLicenseDomain().ifPresent(stringJoiner::add);
        stringJoiner.add(buildProperties.getVersion());
        stringJoiner.add(buildProperties.getShortCommitId());
        stringJoiner.add("_" + clock.millis());
        return stringJoiner.toString() + ".zip";
    }

    /**
     * Get a summary of the environment for diags collection purposes.
     *
     * <p>At the time of this writing this only returns the build properties used to make this version
     * of the component and the domain name used for the license.
     *
     * @return A string containing the summary of the diagnostics environment.
     */
    @Nonnull
    public String getDiagSummary() {
        final StringJoiner stringJoiner = new StringJoiner("\n");
        stringJoiner.add("Domain: " + getLicenseDomain().orElse("UNKNOWN"));
        stringJoiner.add(buildProperties.toString());
        return stringJoiner.toString();
    }

    /**
     * Get the domain name for this installation, extracted from the license(s) used to activate
     * the product.
     *
     * @return An {@link Optional} containing the domain, if it could be successfully extracted
     *         from the licenses. An empty optional otherwise.
    */
    private Optional<String> getLicenseDomain() {
        domain.trySetValue(() -> {
            try {
                return computeLicenseDomain();
            } catch (StatusRuntimeException e) {
                logger.error("Failed to get licenses. Error: {}", e.getLocalizedMessage());
                // Return null so that we don't save the result, and retry on the next
                // call to this method.
                return null;
            }
        });
        return domain.getValue()
            .filter(Optional::isPresent)
            .map(Optional::get);
    }

    @Nonnull
    private Optional<String> computeLicenseDomain()
            throws StatusRuntimeException {
        final ManagedChannel authChannel = channelFactory.getChannel(authHost, serverGrpcPort);

        final LicenseManagerServiceBlockingStub licenseManagerService =
            LicenseManagerServiceGrpc.newBlockingStub(authChannel);

        List<String> domains;
        try {
            domains = licenseManagerService.getLicenses(GetLicensesRequest.getDefaultInstance())
                .getLicenseDTOList().stream()
                .filter(LicenseDTO::hasTurbo)
                .map(LicenseDTO::getTurbo)
                .filter(TurboLicense::hasEmail)
                .map(TurboLicense::getEmail)
                // remove any trailing white spaces that can be there
                .map(String::trim)
                .map(email -> {
                    String[] components = email.split("@");
                    if (components.length > 1) {
                        return components[1];
                    } else {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                // We want to eliminate duplicates, and then sort.
                .distinct()
                .sorted(Comparator.comparing(Function.identity()))
                .collect(Collectors.toList());
        } finally {
            authChannel.shutdownNow();
        }

        if (domains.isEmpty()) {
            logger.warn("No domain extracted from licenses.");
            return Optional.empty();
        } else {
            return Optional.of(String.join("_", domains));
        }
    }

    /**
     * Factory class for channels to other components in the system.
     * Used to mock out the channel construction dependency for testing.
     */
    @FunctionalInterface
    @VisibleForTesting
    interface ChannelFactory {

        @Nonnull
        ManagedChannel getChannel(@Nonnull String host, int port);

    }
}
