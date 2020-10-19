package com.vmturbo.cost.component.topology;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.ClearRIDataChecksumRequest;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceBlockingStub;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.topology.processor.api.TargetListener;

/**
 *  Listens for target related changes in cost component and update cost DB accordingly.
 */
public class TopologyProcessorNotificationListener implements TargetListener {

    private static final Logger logger = LogManager.getLogger();
    private final BusinessAccountHelper businessAccountHelper;
    private final BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore;
    private final RIAndExpenseUploadServiceBlockingStub riAndExpenseUploadRpcService;

    /**
     * TopologyProcessorNotificationListener constructor.
     * @param businessAccountHelper {@link BusinessAccountHelper} to store and resolve ba -> targetId.
     * @param businessAccountPriceTableKeyStore businessAccountPriceTableKeyStore backed by DB to
     * @param riAndExpenseUploadRpcService the RI expense upload service.
     */
    public TopologyProcessorNotificationListener(BusinessAccountHelper businessAccountHelper,
                                                 BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore,
                                                 final RIAndExpenseUploadServiceBlockingStub riAndExpenseUploadRpcService) {
        this.businessAccountHelper = businessAccountHelper;
        this.businessAccountPriceTableKeyStore = businessAccountPriceTableKeyStore;
        this.riAndExpenseUploadRpcService = riAndExpenseUploadRpcService;
    }

    @Override
    public void onTargetRemoved(final long targetId) {
        businessAccountHelper.removeTargetForBusinessAccount(targetId);
        Set<ImmutablePair<Long, String>> orphanedBAs = businessAccountHelper.removeBusinessAccountWithNoTargets();
        logger.info("Target removed notification received.");
        try {
            logger.debug("Removing BA for target removed and related price data from CostDB " +
                    "for target : {}", targetId);
            //remove all the pricetablesKey attached to unused BA OIDs.
            businessAccountPriceTableKeyStore.removeBusinessAccountAndPriceTableKeyOid(
                    orphanedBAs.stream().map(p -> p.left).collect(Collectors.toSet()));
            logger.debug("Successfully removed BA and price related data for target : {}", targetId);
            // Invalidate the checksum so as to force the RI upload in the next broadcast.
            riAndExpenseUploadRpcService.clearRIDataChecksum(ClearRIDataChecksumRequest.getDefaultInstance());
            logger.debug("Successfully invalidated the last uploaded RIData checksum");
        } catch (DbException e) {
            logger.error("Could not update cost DB on target removal.", e);
        }
    }
}
