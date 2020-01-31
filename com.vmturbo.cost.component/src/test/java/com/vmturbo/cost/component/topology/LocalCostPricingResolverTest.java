package com.vmturbo.cost.component.topology;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbException;

/**
 * Class for Testing the Local Pricing Resolver.
 */
public class LocalCostPricingResolverTest {
    private static final TopologyEntityDTO BA1 = TopologyEntityDTO.newBuilder()
            .setOid(7)
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .build();

    private static final TopologyEntityDTO BA2 = TopologyEntityDTO.newBuilder()
            .setOid(8)
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .build();

    private static final TopologyEntityDTO BA3 = TopologyEntityDTO.newBuilder()
            .setOid(9)
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .build();

    private static final PriceTable PRICE_TABLE = PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(30L, OnDemandPriceTable.getDefaultInstance())
            .build();

    private static final PriceTable PRICE_TABLE2 = PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(40L, OnDemandPriceTable.getDefaultInstance())
            .build();

    private final BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore = mock(BusinessAccountPriceTableKeyStore.class);

    private final PriceTableStore priceTableStore = mock(PriceTableStore.class);

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    private final DiscountStore discountStore = mock(DiscountStore.class);

    private final DiscountApplicatorFactory discountApplicatorFactory = DiscountApplicator.newFactory();

    private LocalCostPricingResolver localCostPricingResolver;

    private Map<Long, Long> priceTableKeyOidByBusinessAccountOid = new HashMap<>();

    private Map<Long, PriceTable> priceTableKeyOidByPriceTable = new HashMap<>();

    private Long priceTableKeyOid1 = 20L;

    private Long priceTableKeyOid2 = 25L;

    private CloudTopology<TopologyEntityDTO> topology;

    private final TopologyEntityInfoExtractor topologyEntityInfoExtractor = mock(TopologyEntityInfoExtractor.class);

    /**
     * Setup the test.
     */
    @Before
    public void setup() {
        localCostPricingResolver = new LocalCostPricingResolver(priceTableStore,
                businessAccountPriceTableKeyStore, identityProvider, discountStore, discountApplicatorFactory,
                topologyEntityInfoExtractor);

        topology = (CloudTopology<TopologyEntityDTO>)mock(CloudTopology.class);
        List<TopologyEntityDTO> baList = Arrays.asList(BA1, BA2, BA3);

        when(topology.getAllEntitiesOfType(EntityType.BUSINESS_ACCOUNT_VALUE)).thenReturn(baList);
        when(topology.getEntity(BA1.getOid())).thenReturn(Optional.of(BA1));
        when(topology.getEntity(BA2.getOid())).thenReturn(Optional.of(BA2));
        when(topology.getEntity(BA3.getOid())).thenReturn(Optional.of(BA3));
        when(businessAccountPriceTableKeyStore.fetchPriceTableKeyOidsByBusinessAccount(baList.stream().map(s -> s.getOid())
                .collect(Collectors.toSet()))).thenReturn(priceTableKeyOidByBusinessAccountOid);
        when(priceTableStore.getPriceTables(priceTableKeyOidByBusinessAccountOid.values())).thenReturn(priceTableKeyOidByPriceTable);
    }

    /**
     * Test the business account pricing data mapping.
     */
    @Test
    public void testAccountPricingDataByBusinessAccount() {

        populatePriceTableKeybyBusinessAccountOidMap(BA1.getOid(), priceTableKeyOid1);
        populatePriceTableKeybyBusinessAccountOidMap(BA2.getOid(), priceTableKeyOid2);
        populatePriceTableByPriceTableKeyOid(priceTableKeyOid1, PRICE_TABLE);
        populatePriceTableByPriceTableKeyOid(priceTableKeyOid2, PRICE_TABLE2);

        List<Discount> discountList = Arrays.asList(createDiscountByAccount(BA1.getOid()));

        Map<Long, AccountPricingData<TopologyEntityDTO>> accountPricingDataMapByBusinessAccountOid
                = new HashMap<>();

        try {
            when(discountStore.getAllDiscount()).thenReturn(discountList);
            accountPricingDataMapByBusinessAccountOid = localCostPricingResolver.getAccountPricingDataByBusinessAccount(topology);
        } catch (CloudCostDataRetrievalException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        }

        //Test that accountPricingDataMapByBusinessAccountOid map is non empty
        assert (!accountPricingDataMapByBusinessAccountOid.isEmpty());

        //Test that accountPricingDataByBusinessAccountOid has an entry for BA1 with its associated price table
        assert (accountPricingDataMapByBusinessAccountOid.get(BA1.getOid()).getPriceTable().equals(PRICE_TABLE));

        //Test that accountPricingDataByBusinessAccountOid has an entry for BA2 with its associated price table
        assert (accountPricingDataMapByBusinessAccountOid.get(BA2.getOid()).getPriceTable().equals(PRICE_TABLE2));

        // Test Discount
        Optional<Discount> discount = Optional.ofNullable(discountList.get(0));

        // Test Discount for BA1
        assert (accountPricingDataMapByBusinessAccountOid.get(BA1.getOid()).getDiscountApplicator()
                .getDiscount().equals(discount.get()));

        // test no discount for BA 2
        assert (accountPricingDataMapByBusinessAccountOid.get(BA2.getOid()).getDiscountApplicator()
                .getDiscount() == null);
    }

    /**
     * Test mapping of multiple business accounts to the same and different account pricing data objects.
     */
    @Test
    public void testMultipleAccountPricingDataMappedToBusinessAccount() {
        populatePriceTableKeybyBusinessAccountOidMap(BA1.getOid(), priceTableKeyOid1);
        populatePriceTableKeybyBusinessAccountOidMap(BA2.getOid(), priceTableKeyOid1);
        populatePriceTableKeybyBusinessAccountOidMap(BA3.getOid(), priceTableKeyOid1);
        populatePriceTableByPriceTableKeyOid(priceTableKeyOid1, PRICE_TABLE);

        Map<Long, AccountPricingData<TopologyEntityDTO>> accountPricingDataMapByBusinessAccountOid
                = new HashMap<>();
        List<Discount> discountList = Arrays.asList(createDiscountByAccount(BA3.getOid()));

        try {
            when(discountStore.getAllDiscount()).thenReturn(discountList);
            accountPricingDataMapByBusinessAccountOid = localCostPricingResolver.getAccountPricingDataByBusinessAccount(topology);
        } catch (CloudCostDataRetrievalException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        }

        // Assert that both the business accounts are mapped to the same account pricing data
        assert (accountPricingDataMapByBusinessAccountOid.get(BA1.getOid())
                .equals(accountPricingDataMapByBusinessAccountOid.get(BA2.getOid())));

        // Assert that BA1 and BA3 are mapped to different account pricing data. Both use the same price tables
        // but BA3 has a discount whereas BA 1 doesn't.
        assert (!accountPricingDataMapByBusinessAccountOid.get(BA1.getOid())
                .equals(accountPricingDataMapByBusinessAccountOid.get(BA3.getOid())));

    }

    /**
     * Populate the price table key oid by business account oid map.
     *
     * @param businessAccountOid The business account oid.
     * @param priceTableKeyOid The price table key oid.
     */
    public void populatePriceTableKeybyBusinessAccountOidMap(Long businessAccountOid, Long priceTableKeyOid) {
        priceTableKeyOidByBusinessAccountOid.put(businessAccountOid, priceTableKeyOid);
    }

    /**
     * Populate the price table by price table key oid map.
     *
     * @param priceTableKeyOid The price table key oid.
     * @param priceTable The price table.
     */
    public void populatePriceTableByPriceTableKeyOid(Long priceTableKeyOid, PriceTable priceTable) {
        priceTableKeyOidByPriceTable.put(priceTableKeyOid, priceTable);
    }

    /**
     * Create a discount for a given business account oid.
     *
     * @param baOid The business account oid to create discount for.
     *
     * @return The discount.
     */
    public Discount createDiscountByAccount(Long baOid) {
        Discount discount = Discount.newBuilder().setAssociatedAccountId(baOid).build();
        return discount;
    }
}
