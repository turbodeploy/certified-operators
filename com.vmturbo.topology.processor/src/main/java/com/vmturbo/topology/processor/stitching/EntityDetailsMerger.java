package com.vmturbo.topology.processor.stitching;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityDetail;

/**
 * Entity details merger. This class merges entity details from source entity DTO to
 * destination DTO.
 */
public class EntityDetailsMerger {

    /**
     * Default constructor.
     */
    public EntityDetailsMerger() {}

    /**
     * Merge entity details from one entity DTO to another.
     *
     * @param from Source entity DTO builder.
     * @param onto Destination entity DTO builder.
     */
    public void merge(@Nonnull final EntityDTO.Builder from,
                      @Nonnull final EntityDTO.Builder onto) {
        if (from.getDetailsList().isEmpty()) {
            // No details to stitch
            return;
        }

        final Map<Integer, EntityDetail> existingDetails = onto.getDetailsList()
                .stream()
                .collect(Collectors.toMap(EntityDetail::getKey, Function.identity()));

        // For now don't override existing entity details in the "onto" entity, just add new details
        // from the "from" entity.
        from.getDetailsList().forEach(fromDetail ->
                existingDetails.putIfAbsent(fromDetail.getKey(), fromDetail));

        onto.clearDetails();
        onto.addAllDetails(existingDetails.values());
    }
}
