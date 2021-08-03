package com.vmturbo.stitching.prestitching;

import java.util.Comparator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingEntity;

/**
 * Prestitching operation that replaces data driven stitching for AWS Business Accounts.  Keep the
 * BusinessAccount with DataDiscovered == true and merge any matching Business Accounts onto it.
 */
public class AwsBusinessAccountPreStitchingOperation
        extends SharedEntityDefaultPreStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Create the operation.
     */
    public AwsBusinessAccountPreStitchingOperation() {
        super(factory -> factory.probeEntityTypeScope(SDKProbeType.AWS.getProbeType(),
                EntityType.BUSINESS_ACCOUNT));
    }

    private boolean isDataDiscovered(StitchingEntity entity) {
        final Builder builder = entity.getEntityBuilder();
        if (!builder.hasBusinessAccountData()) {
            return false;
        }
        return builder.getBusinessAccountData().getDataDiscovered();
    }

    @Override
    protected Comparator<StitchingEntity> comparator() {
        // Only one entity should have dataDiscovered==true. That one is considered "larger" for
        // comparison purposes.  If neither has dataDiscovered==true choose lastUpdatedTime to
        // enforce consistent ordering.
        return (entity1, entity2) -> {
            if (isDataDiscovered(entity1)) {
                if (isDataDiscovered(entity2)) {
                    logger.warn("Multiple targets discovered entities for business account {}. "
                            + "Target IDs: {} {}.", entity1.getOid(), entity1.getTargetId(),
                            entity2.getTargetId());
                    return super.comparator().compare(entity1, entity2);
                }
                return 1;
            } else if (isDataDiscovered(entity2)) {
                return -1;
            } else {
                return super.comparator().compare(entity1, entity2);
            }
        };
    }
}
