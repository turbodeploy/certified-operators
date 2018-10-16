package com.vmturbo.topology.processor.stitching.journal;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.gson.Gson;

import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTOOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingMergeInformation;
import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;
import com.vmturbo.stitching.journal.JsonSemanticDiffer;
import com.vmturbo.stitching.journal.JsonSemanticDiffer.DiffOutput;
import com.vmturbo.stitching.journal.SemanticDiffer;

/**
 * These diffs are potentially large and are always formatted prettily.
 */
public class StitchingEntitySemanticDiffer implements SemanticDiffer<StitchingEntity> {
    private static final Logger logger = LogManager.getLogger();

    private Verbosity verbosity;
    private JsonSemanticDiffer differ;

    private final Gson gson = ComponentGsonFactory.createGson();
    private final Gson noPrettyGson = ComponentGsonFactory.createGsonNoPrettyPrint();
    private final CommoditySignatureComparator commodityComparator = new CommoditySignatureComparator();

    public StitchingEntitySemanticDiffer(@Nonnull final Verbosity verbosity) {
        this.verbosity = verbosity;
        differ = new JsonSemanticDiffer();
    }

    @Override
    public Verbosity getVerbosity() {
        return verbosity;
    }

    @Override
    public void setVerbosity(@Nonnull final Verbosity verbosity) {
        this.verbosity = Objects.requireNonNull(verbosity);
    }

    @Nonnull
    public JsonSemanticDiffer getJsonSemanticDiffer() {
        return differ;
    }

    @Nonnull
    @Override
    public String semanticDiff(@Nonnull final StitchingEntity entityA,
                               @Nonnull final StitchingEntity entityB,
                               @Nonnull final FormatRecommendation format) {
        try {
            // TODO: Support for compact format.
            Preconditions.checkArgument(format == FormatRecommendation.PRETTY,
                "Only pretty format is supported. Illegal format " + format);

            final StringBuilder builder = new StringBuilder();
            builder.append(entityDescription(entityA));
            builder.append("\n");

            boolean changed = appendLastUpdatedDiff(entityA, entityB, builder);
            changed |= appendMergeInformationDiff(entityA, entityB, builder);
            changed |= appendProviderDiff(entityA, entityB, builder);
            changed |= appendConsumersDiff(entityA, entityB, builder);
            changed |= appendCommoditiesSoldDiff(entityA, entityB, builder);
            changed |= appendCommoditiesBoughtByProviderDiff(entityA, entityB, builder);
            changed |= appendEntityPropertyDiff(entityA, entityB, builder);

            // If nothing changed, return empty.
            return changed ? StringUtils.strip(builder.toString()) : "";
        } catch (Exception e) {
            logger.error("Error when generating semantic differences: ", e);
            return "";
        }
    }

    @Nonnull
    @Override
    public String dumpEntity(@Nonnull StitchingEntity entity) {
        return noPrettyGson.toJson(entity.getEntityBuilder().build());
    }

    @Nonnull
    public static String entityDescription(@Nonnull final StitchingEntity entity) {
        return String.format("%s %s %s %s",
            entity.getEntityType().name(), entity.getLocalId(), entity.getDisplayName(),
            StitchingMergeInformation.formatOidAndTarget(entity.getOid(), entity.getTargetId()));
    }

        private boolean appendLastUpdatedDiff(@Nonnull final StitchingEntity entityA,
                                              @Nonnull final StitchingEntity entityB,
                                              @Nonnull final StringBuilder builder) {
            if (entityA.getLastUpdatedTime() != entityB.getLastUpdatedTime()) {
                builder.append(String.format("  lastUpdatedTime: (([%s] --> [%s]))\n",
                    timeString(entityA.getLastUpdatedTime()),
                    timeString(entityB.getLastUpdatedTime())));

                return true;
            } else if (verbosity == Verbosity.COMPLETE_VERBOSITY) {
                builder.append(String.format("  lastUpdatedTime: [%s]\n",
                    timeString(entityA.getLastUpdatedTime())));

                return true;
            }

            return false;
        }

        private String timeString(final long timestamp) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC).toString();
        }

        private boolean appendMergeInformationDiff(@Nonnull final StitchingEntity entityA,
                                                   @Nonnull final StitchingEntity entityB,
                                                   @Nonnull final StringBuilder builder) {
            if (!entityA.getMergeInformation().equals(entityB.getMergeInformation())) {
                final String aJson = gson.toJson(entityA.getMergeInformation());
                final String bJson = gson.toJson(entityB.getMergeInformation());

                try {
                    final DiffOutput difference = differ.semanticDiff(aJson, bJson, verbosity,
                        FormatRecommendation.PRETTY, 2);
                    if (shouldRecord(difference)) {
                        builder
                            .append("  Merge Information:\n")
                            .append(difference.diff)
                            .append("\n");
                        return true;
                    } else {
                        return false;
                    }
                } catch (IOException e) {
                    logger.error("Error diffing JSON object {} against {}", aJson, bJson);
                    logger.error("Diffing exception: ", e);
                    return false;
                }
            } else if (verbosity == Verbosity.COMPLETE_VERBOSITY) {
                builder
                    .append("  Merge Information: ")
                    .append(gson.toJson(entityA.getMergeInformation()))
                    .append("\n");
                return true;
            }

            return false;
        }

        private boolean appendProviderDiff(@Nonnull final StitchingEntity entityA,
                                           @Nonnull final StitchingEntity entityB,
                                           @Nonnull final StringBuilder builder) {
            final SetView<StitchingEntity> removedProviders =
                Sets.difference(entityA.getProviders(), entityB.getProviders());
            final SetView<StitchingEntity> addedProviders =
                Sets.difference(entityB.getProviders(), entityA.getProviders());

            return appendRemoveAddDiff(builder, removedProviders, addedProviders, "Providers", this::getTargetOidString);
        }

        private boolean appendConsumersDiff(@Nonnull final StitchingEntity entityA,
                                            @Nonnull final StitchingEntity entityB,
                                            @Nonnull final StringBuilder builder) {
            final SetView<StitchingEntity> removedConsumers =
                Sets.difference(entityA.getConsumers(), entityB.getConsumers());
            final SetView<StitchingEntity> addedConsumers =
                Sets.difference(entityB.getConsumers(), entityA.getConsumers());

            return appendRemoveAddDiff(builder, removedConsumers, addedConsumers, "Consumers", this::getTargetOidString);
        }

        private <T> boolean appendRemoveAddDiff(@Nonnull final StringBuilder builder,
                                                @Nonnull final Collection<T> removed,
                                                @Nonnull final Collection<T> added,
                                                @Nonnull final String header,
                                                @Nonnull final Function<T, String> toStringMethod) {
            boolean anyRemoved = !removed.isEmpty();
            boolean anyAdded = !added.isEmpty();
            if (anyRemoved || anyAdded) {
                builder.append(spaces(2)).append(header).append(":");

                if (anyRemoved) {
                    builder.append(" removed=[")
                        .append(removed.stream()
                            .map(toStringMethod::apply)
                            .sorted()
                            .collect(Collectors.joining(", ")))
                        .append("]");
                }
                if (anyAdded) {
                    builder.append(" added=[")
                        .append(added.stream()
                            .map(toStringMethod::apply)
                            .sorted()
                            .collect(Collectors.joining(", ")))
                        .append("]");
                }

                builder.append("\n");
            }

            return anyRemoved || anyAdded;
        }

        private boolean appendCommoditiesSoldDiff(@Nonnull final StitchingEntity entityA,
                                                  @Nonnull final StitchingEntity entityB,
                                                  @Nonnull final StringBuilder builder) {
            final List<Builder> soldByA = entityA.getCommoditiesSold().collect(Collectors.toList());
            final List<Builder> soldByB = entityB.getCommoditiesSold().collect(Collectors.toList());

            // Sort them by type and key so that the array order can be consistent.
            soldByA.sort(commodityComparator);
            soldByB.sort(commodityComparator);

            if (soldByA.equals(soldByB)) {
                if (verbosity == Verbosity.COMPLETE_VERBOSITY) {
                    builder.append("  CommoditiesSold:\n")
                        .append(gson.toJson(soldByA.stream()
                            .map(CommodityDTO.Builder::build)
                            .collect(Collectors.toList())))
                        .append("\n");
                } else {
                    return false; // When they're equal, the diff is empty
                }
            }

            // Convert from builders to built instances so that the diff is presented legibly.
            final String aJson = gson.toJson(soldByA.stream()
                .map(CommodityDTO.Builder::build)
                .collect(Collectors.toList()));
            final String bJson = gson.toJson(soldByB.stream()
                .map(CommodityDTO.Builder::build)
                .collect(Collectors.toList()));

            try {
                final DiffOutput difference = differ.semanticDiff(aJson, bJson, verbosity,
                    FormatRecommendation.PRETTY, 2);
                if (shouldRecord(difference)) {
                    builder.append("  CommoditiesSold:\n")
                        .append(difference.diff)
                        .append("\n");
                    return true;
                } else {
                    return false;
                }

            } catch (IOException e) {
                logger.error("Error diffing JSON object {} against {}", aJson, bJson);
                logger.error("Diffing exception: ", e);
                return false;
            }
        }

        private Map<StitchingEntityIdentifier, List<CommodityDTO>> boughtMap(@Nonnull final StitchingEntity entity) {
            return entity.getCommodityBoughtListByProvider().entrySet().stream()
                .collect(Collectors.toMap(entry ->
                        new StitchingEntityIdentifier(entry.getKey()), entry -> entry.getValue().stream()
                        .flatMap(cb -> cb.getBoughtList().stream())
                        .map(Builder::build)
                        .collect(Collectors.toList())));
        }

        private boolean appendCommoditiesBoughtByProviderDiff(@Nonnull final StitchingEntity entityA,
                                                              @Nonnull final StitchingEntity entityB,
                                                              @Nonnull final StringBuilder builder) {
            final Map<StitchingEntityIdentifier, List<CommodityDTO>> boughtByA = boughtMap(entityA);
            final Map<StitchingEntityIdentifier, List<CommodityDTO>> boughtByB = boughtMap(entityB);

            if (boughtByA.equals(boughtByB)) {
                // No diff to append
                if (verbosity == Verbosity.COMPLETE_VERBOSITY) {
                    builder.append("  CommoditiesBought:\n")
                        .append(gson.toJson(boughtByA))
                        .append("\n");
                } else {
                    return false;
                }
            }

            final String aJson = gson.toJson(boughtByA);
            final String bJson = gson.toJson(boughtByB);

            try {
                final DiffOutput difference = differ.semanticDiff(aJson, bJson, verbosity,
                    FormatRecommendation.PRETTY, 2);
                if (shouldRecord(difference)) {
                    builder.append("  CommoditiesBought:\n")
                        .append(difference.diff)
                        .append("\n");
                    return true;
                } else {
                    return false;
                }
            } catch (IOException e) {
                logger.error("Error diffing JSON object {} against {}", aJson, bJson);
                logger.error("Diffing exception: ", e);
                return false;
            }
        }

        private boolean appendEntityPropertyDiff(@Nonnull final StitchingEntity entityA,
                                                 @Nonnull final StitchingEntity entityB,
                                                 @Nonnull final StringBuilder builder) {
            final EntityDTO a = entityA.getEntityBuilder().build();
            final EntityDTO b = entityB.getEntityBuilder().build();

            if (a.equals(b)) {
                // No diff to append
                if (verbosity == Verbosity.COMPLETE_VERBOSITY) {
                    builder.append("  Entity:\n")
                        .append(gson.toJson(a))
                        .append("\n");
                    return true;
                } else {
                    return false;
                }
            }

            final String aJson = gson.toJson(a);
            final String bJson = gson.toJson(b);

            try {
                final DiffOutput difference = differ.semanticDiff(aJson, bJson, verbosity,
                    FormatRecommendation.PRETTY, 2);
                if (shouldRecord(difference)) {
                    builder.append("  Entity:\n")
                        .append(difference.diff)
                        .append("\n");
                    return true;
                } else {
                    return false;
                }
            } catch (IOException e) {
                logger.error("Error diffing JSON object {} against {}", aJson, bJson);
                logger.error("Diffing exception: ", e);
                return false;
            }
        }

        private String getTargetOidString(@Nonnull final StitchingEntity entity) {
            return Stream.of(entity.getEntityType(), entity.getOid(), entity.getTargetId())
                .map(Object::toString)
                .collect(Collectors.joining("-"));
        }

        @Nonnull
        public static String spaces(int nSpaces) {
            return IntStream.range(0, nSpaces)
                .mapToObj(index -> " ")
                .collect(Collectors.joining());
        }

    private static class CommoditySignatureComparator implements Comparator<CommodityDTOOrBuilder> {
        @Override
        public int compare(@Nonnull final CommodityDTOOrBuilder commodityA,
                           @Nonnull final CommodityDTOOrBuilder commodityB) {
            final int typeComparison = commodityA.getCommodityType().compareTo(commodityB.getCommodityType());
            if (typeComparison == 0) {
                return StringUtils.compare(commodityA.getKey(), commodityB.getKey());
            } else {
                return typeComparison;
            }
        }
    }

    private static class StitchingEntityIdentifier {
        public final long oid;
        public final long targetId;
        public final EntityType entityType;

        public StitchingEntityIdentifier(@Nonnull final StitchingEntity stitchingEntity) {
            this.oid = stitchingEntity.getOid();
            this.targetId = stitchingEntity.getTargetId();
            this.entityType = stitchingEntity.getEntityType();
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(oid, targetId);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof StitchingEntityIdentifier)) {
                return false;
            }
            final StitchingEntityIdentifier other = (StitchingEntityIdentifier)obj;
            return com.google.common.base.Objects.equal(this.targetId, other.targetId)
                && com.google.common.base.Objects.equal(this.oid, other.oid);
        }

        @Override
        public String toString() {
            return Stream.of(entityType, oid, targetId)
                .map(Object::toString)
                .collect(Collectors.joining("-"));
        }
    }
}
