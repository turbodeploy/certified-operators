package com.vmturbo.cost.component.rpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.CloudStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.DiscountQueryFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityTypeFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.Builder;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.cost.component.discount.DiscountNotFoundException;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.discount.DuplicateAccountIdException;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.expenses.AccountExpensesStore;
import com.vmturbo.cost.component.reserved.instance.TimeFrameCalculator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.cost.component.utils.BusinessAccountHelper;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.DbException;

@SuppressWarnings("unchecked")
public class RIAndExpenseUploadRpcServiceTest {

    public static final double ACCOUNT_EXPENSE1 = 10.0;
    private static final long ASSOCIATED_ACCOUNT_ID = 1111l;
    private static final double DISCOUNT_PERCENTAGE2 = 20.0;
    private static final double DISCOUNT_PERCENTAGE1 = 10.0;
    private static final long id = 1234L;
    private static final long id2 = 1235L;
    private static final long ASSOCIATED_SERVICE_ID = 4l;
    final DiscountInfo discountInfo1 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE1)
                    .build())
            .setServiceLevelDiscount(DiscountInfo
                    .ServiceLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByServiceId(id, DISCOUNT_PERCENTAGE1)
                    .build())
            .build();
    final DiscountInfo discountInfo2 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE2)
                    .build())
            .setServiceLevelDiscount(DiscountInfo
                    .ServiceLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByServiceId(id, DISCOUNT_PERCENTAGE1)
                    .build())
            .build();
    final DiscountInfo discountInfo3 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE1)
                    .build())
            .setServiceLevelDiscount(DiscountInfo
                    .ServiceLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByServiceId(id, DISCOUNT_PERCENTAGE1)
                    .build())
            .setDisplayName("testname")
            .build();
    final DiscountInfo discountInfo4 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE1)
                    .build())
            .setServiceLevelDiscount(DiscountInfo
                    .ServiceLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByServiceId(id, DISCOUNT_PERCENTAGE1)
                    .build())
            .build();
    final DiscountInfo discountInfo5 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE1)
                    .build())
            .setServiceLevelDiscount(DiscountInfo
                    .ServiceLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByServiceId(id, DISCOUNT_PERCENTAGE1)
                    .build())
            .setTierLevelDiscount(DiscountInfo
                    .TierLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByTierId(id, DISCOUNT_PERCENTAGE1))
            .build();
    final DiscountInfo discountInfoAccountOnly1 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE1)
                    .build())
            .setDisplayName("testname")
            .build();
    final DiscountInfo discountInfoServiceAndTierOnly1 = DiscountInfo.newBuilder()
            .setServiceLevelDiscount(DiscountInfo
                    .ServiceLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByServiceId(id, DISCOUNT_PERCENTAGE1)
                    .build())
            .setTierLevelDiscount(DiscountInfo
                    .TierLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByTierId(id, DISCOUNT_PERCENTAGE1))
            .build();
    final AccountExpenses.AccountExpensesInfo accountExpensesInfo = AccountExpensesInfo.newBuilder()
            .addServiceExpenses(ServiceExpenses
                    .newBuilder()
                    .setAssociatedServiceId(ASSOCIATED_SERVICE_ID)
                    .setExpenses(CurrencyAmount.newBuilder().setAmount(ACCOUNT_EXPENSE1).build())
                    .build())
            .build();
    private final int ASSOCIATED_ENTITY_TYPE1 = 1;
    private final ComponentCost componentCost = ComponentCost.newBuilder()
            .setAmount(CurrencyAmount.newBuilder().setAmount(ACCOUNT_EXPENSE1).setCurrency(1))
            .setCategory(CostCategory.COMPUTE)
            .build();
    private final EntityCost entityCost = EntityCost.newBuilder()
            .setAssociatedEntityId(ASSOCIATED_SERVICE_ID)
            .addComponentCost(componentCost)
            .setTotalAmount(CurrencyAmount.newBuilder().setAmount(1.111).setCurrency(1).build())
            .setAssociatedEntityType(ASSOCIATED_ENTITY_TYPE1)
            .build();

    private final ComponentCost componentCost1 = ComponentCost.newBuilder()
            .setAmount(CurrencyAmount.newBuilder().setAmount(ACCOUNT_EXPENSE1).setCurrency(1))
            .setCategory(CostCategory.IP)
            .build();
    private final EntityCost entityCost1 = EntityCost.newBuilder()
            .setAssociatedEntityId(ASSOCIATED_SERVICE_ID)
            .addComponentCost(componentCost)
            .addComponentCost(componentCost1)
            .setTotalAmount(CurrencyAmount.newBuilder().setAmount(1.111).setCurrency(1).build())
            .setAssociatedEntityType(ASSOCIATED_ENTITY_TYPE1)
            .build();
    private final EntityCost entityCost2 = EntityCost.newBuilder()
            .setAssociatedEntityId(ASSOCIATED_SERVICE_ID)
            .addComponentCost(componentCost1)
            .setTotalAmount(CurrencyAmount.newBuilder().setAmount(1.111).setCurrency(1).build())
            .setAssociatedEntityType(ASSOCIATED_ENTITY_TYPE1)
            .build();
    public DiscountStore discountStore = mock(DiscountStore.class);

    public AccountExpensesStore accountExpenseStore = mock(AccountExpensesStore.class);

    public EntityCostStore entityCostStore = mock(EntityCostStore.class);

    private TimeFrameCalculator timeFrameCalculator = mock(TimeFrameCalculator.class);

    public BusinessAccountHelper businessAccountHelper = new BusinessAccountHelper();

    private CostRpcService costRpcService;

    @Before
    public void setUp() {
        businessAccountHelper.storeTargetMapping(2, 2);
        costRpcService = new CostRpcService(discountStore, accountExpenseStore, entityCostStore, timeFrameCalculator, businessAccountHelper);
    }

    @Test
    public void testCreateDiscount() throws Exception {
        final CreateDiscountRequest request = CreateDiscountRequest.newBuilder()
                .setId(ASSOCIATED_ACCOUNT_ID)
                .setDiscountInfo(discountInfo1)
                .build();
        final StreamObserver<Cost.CreateDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        Discount discount = Discount.newBuilder()
                .setDiscountInfo(discountInfo1)
                .setId(id)
                .setAssociatedAccountId(ASSOCIATED_ACCOUNT_ID)
                .build();
        given(discountStore.persistDiscount(ASSOCIATED_ACCOUNT_ID, discountInfo1)).willReturn(discount);

        costRpcService.createDiscount(request, mockObserver);
        verify(mockObserver).onNext(CreateDiscountResponse.newBuilder()
                .setDiscount(discount)
                .build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testCreateDiscountNoInfoAndAssociatedAccountId() {
        final StreamObserver<CreateDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        costRpcService.createDiscount(CreateDiscountRequest.newBuilder()
                // No discount info set.
                // No discount associated account id
                .build(), mockObserver);
        verify(mockObserver).onError(any(IllegalStateException.class));
    }

    @Test
    public void testCreateDiscountFailedWihInvalidAccountId() throws Exception {
        final CreateDiscountRequest request = CreateDiscountRequest.newBuilder()
                .setId(ASSOCIATED_ACCOUNT_ID)
                .setDiscountInfo(discountInfo1)
                .build();
        final StreamObserver<Cost.CreateDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        given(discountStore.persistDiscount(ASSOCIATED_ACCOUNT_ID, discountInfo1))
                .willThrow(DuplicateAccountIdException.class);
        costRpcService.createDiscount(request, mockObserver);
        verify(mockObserver).onError(any(IllegalStateException.class));
    }

    @Test
    public void testCreateDiscountFailWithDbException() throws Exception {
        final CreateDiscountRequest request = CreateDiscountRequest.newBuilder()
                .setId(ASSOCIATED_ACCOUNT_ID)
                .setDiscountInfo(discountInfo1)
                .build();
        final StreamObserver<Cost.CreateDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        doThrow(new DbException(id + "")).when(discountStore).persistDiscount(ASSOCIATED_ACCOUNT_ID, discountInfo1);
        costRpcService.createDiscount(request, mockObserver);
        verify(mockObserver).onError(any(IllegalStateException.class));
        final ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INTERNAL).descriptionContains(id + ""));
    }

    @Test
    public void testDeleteDiscount() throws DbException, DiscountNotFoundException {
        final DeleteDiscountRequest deleteDiscountRequest = DeleteDiscountRequest.newBuilder()
                .setAssociatedAccountId(id)
                .build();

        final StreamObserver<DeleteDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        costRpcService.deleteDiscount(deleteDiscountRequest, mockObserver);

        verify(discountStore).deleteDiscountByAssociatedAccountId(id);
        verify(mockObserver).onNext(
                DeleteDiscountResponse.newBuilder().setDeleted(true).build());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }


    @Test
    public void testDeleteDiscountWithDiscountId() throws DbException, DiscountNotFoundException {
        final DeleteDiscountRequest deleteDiscountRequest = DeleteDiscountRequest.newBuilder()
                .setDiscountId(id)
                .build();

        final StreamObserver<DeleteDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        costRpcService.deleteDiscount(deleteDiscountRequest, mockObserver);

        verify(discountStore).deleteDiscountByDiscountId(id);
        verify(mockObserver).onNext(
                DeleteDiscountResponse.newBuilder().setDeleted(true).build());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }


    @Test
    public void testDeleteDiscountFailedWithDiscountNotFoundException() throws DbException, DiscountNotFoundException {
        testDeleteException(new DiscountNotFoundException(id + ""), Code.NOT_FOUND);
    }


    @Test
    public void testDeleteDiscountFailedWithDbException() throws DbException, DiscountNotFoundException {
        testDeleteException(new DbException(id + ""), Code.INTERNAL);
    }

    private void testDeleteException(Exception e, Code code) throws DiscountNotFoundException, DbException {
        final DeleteDiscountRequest discountID = DeleteDiscountRequest.newBuilder()
                .setAssociatedAccountId(id)
                .build();

        final StreamObserver<DeleteDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        doThrow(e).when(discountStore).deleteDiscountByAssociatedAccountId(id);
        costRpcService.deleteDiscount(discountID, mockObserver);

        verify(mockObserver, never()).onCompleted();
        verify(mockObserver, never()).onNext(any());

        final ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(), GrpcExceptionMatcher.hasCode(code)
                .descriptionContains(e.getMessage()));
    }

    @Test
    public void testDeleteDiscountNoAssociatedAccountId() throws Exception {
        final StreamObserver<DeleteDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        costRpcService.deleteDiscount(DeleteDiscountRequest.newBuilder()
                // no associated account id
                .build(), mockObserver);
        verify(mockObserver).onError(any(IllegalStateException.class));
    }

    @Test
    public void testGetDiscountWithAssociatedAccountId() throws Exception {
        final GetDiscountRequest request = GetDiscountRequest.newBuilder()
                .setFilter(DiscountQueryFilter.newBuilder()
                        .addAllAssociatedAccountId(ImmutableSet.of(id)).build())
                .build();
        final StreamObserver<Discount> mockObserver =
                mock(StreamObserver.class);

        final Discount discounts = Discount.newBuilder()
                .setId(id)
                .setDiscountInfo(discountInfo1)
                .build();
        given(discountStore.getDiscountByAssociatedAccountId(id)).willReturn(ImmutableList.of(discounts));

        costRpcService.getDiscounts(request, mockObserver);

        verify(discountStore).getDiscountByAssociatedAccountId(id);
        verify(mockObserver).onNext(Discount.newBuilder().setId(id).setDiscountInfo(discountInfo1).build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetDiscountWithEmptyQueryFilter() throws Exception {
        final GetDiscountRequest request = GetDiscountRequest.newBuilder()
                .setFilter(DiscountQueryFilter.newBuilder()
                        .addAllAssociatedAccountId(ImmutableSet.of()).build())
                .build();
        final StreamObserver<Discount> mockObserver =
                mock(StreamObserver.class);
        costRpcService.getDiscounts(request, mockObserver);
        verify(discountStore, never()).getDiscountByAssociatedAccountId(anyLong());
        verify(mockObserver, never()).onNext(any());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetAllDiscounts() throws Exception {
        final GetDiscountRequest request = GetDiscountRequest.newBuilder()
                .build();
        final StreamObserver<Discount> mockObserver =
                mock(StreamObserver.class);

        final Discount discounts = Discount.newBuilder()
                .setId(id)
                .setDiscountInfo(discountInfo1)
                .build();

        final Discount discounts2 = Discount.newBuilder()
                .setId(id2)
                .setDiscountInfo(discountInfo2)
                .build();
        given(discountStore.getAllDiscount()).willReturn(ImmutableList.of(discounts, discounts2));

        costRpcService.getDiscounts(request, mockObserver);

        verify(discountStore).getAllDiscount();
        verify(mockObserver).onNext(Discount.newBuilder().setId(id).setDiscountInfo(discountInfo1).build());
        verify(mockObserver).onNext(Discount.newBuilder().setId(id2).setDiscountInfo(discountInfo2).build());
        verify(mockObserver).onCompleted();
    }


    @Test
    public void testGetDiscountFailedWithDBException() throws Exception {
        final GetDiscountRequest request = GetDiscountRequest.newBuilder()
                .build();
        final StreamObserver<Discount> mockObserver =
                mock(StreamObserver.class);
        doThrow(new DbException("1")).when(discountStore).getAllDiscount();
        costRpcService.getDiscounts(request, mockObserver);

        verify(mockObserver, never()).onCompleted();
        verify(mockObserver, never()).onNext(any());

        final ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INTERNAL)
                .descriptionContains("1"));
    }

    @Test
    public void testUpdateAccountDiscountOnly() throws Exception {
        final UpdateDiscountRequest request = UpdateDiscountRequest.newBuilder()
                .setAssociatedAccountId(ASSOCIATED_ACCOUNT_ID)
                .setNewDiscountInfo(discountInfoAccountOnly1)
                .build();
        final StreamObserver<UpdateDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        Discount discount = Discount.newBuilder()
                .setId(id)
                .setAssociatedAccountId(ASSOCIATED_ACCOUNT_ID)
                .setDiscountInfo(discountInfo2)
                .build();
        // discountInfoAccountOnly1 from the request has DISCOUNT_PERCENTAGE1 as account level discount
        // discountInfo2 has DISCOUNT_PERCENTAGE2 as account level discount
        given(discountStore.getDiscountByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID))
                .willReturn(ImmutableList.of(discount));
        costRpcService.updateDiscount(request, mockObserver);

        // verify the account level discount is not changed (the account level discount from the request is NOT used)
        verify(discountStore)
                .updateDiscountByAssociatedAccount(ASSOCIATED_ACCOUNT_ID, discountInfo3);
        verify(mockObserver).onNext(
                UpdateDiscountResponse.newBuilder().setUpdatedDiscount(discount).build());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }


    // UI currently doesn't use discount id for updating
    @Test
    public void testUpdateDiscountWithDiscountId() throws Exception {
        final UpdateDiscountRequest request = UpdateDiscountRequest.newBuilder()
                .setDiscountId(id)
                .setNewDiscountInfo(discountInfoAccountOnly1)
                .build();
        final StreamObserver<UpdateDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        Discount discount = Discount.newBuilder()
                .setId(id)
                .setAssociatedAccountId(ASSOCIATED_ACCOUNT_ID)
                .setDiscountInfo(discountInfo2)
                .build();
        // discountInfoAccountOnly1 from the request has DISCOUNT_PERCENTAGE1 as account level discount
        // discountInfo2 has DISCOUNT_PERCENTAGE2 as account level discount
        given(discountStore.getDiscountByDiscountId(id))
                .willReturn(ImmutableList.of(discount));
        costRpcService.updateDiscount(request, mockObserver);

        // verify the account level discount is not changed (the account level discount from the request is NOT used)
        verify(discountStore)
                .updateDiscount(id, discountInfo3);
        verify(mockObserver).onNext(
                UpdateDiscountResponse.newBuilder().setUpdatedDiscount(discount).build());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testUpdateServiceAndTierDiscountOnly() throws Exception {
        final UpdateDiscountRequest request = UpdateDiscountRequest.newBuilder()
                .setAssociatedAccountId(ASSOCIATED_ACCOUNT_ID)
                .setNewDiscountInfo(discountInfoServiceAndTierOnly1)
                .build();
        final StreamObserver<UpdateDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        Discount discount = Discount.newBuilder()
                .setId(id)
                .setAssociatedAccountId(ASSOCIATED_ACCOUNT_ID)
                .setDiscountInfo(discountInfo1)
                .build();
        // discountInfo1 has DISCOUNT_PERCENTAGE1, and should be updated to DISCOUNT_PERCENTAGE2.
        given(discountStore.getDiscountByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID))
                .willReturn(ImmutableList.of(discount));
        costRpcService.updateDiscount(request, mockObserver);

        Discount expectedDiscount = Discount.newBuilder()
                .setId(id)
                .setAssociatedAccountId(ASSOCIATED_ACCOUNT_ID)
                .setDiscountInfo(discountInfo4)
                .build();

        // verify only the account level discount is updated.
        // request asks to update account level discount to DISCOUNT_PERCENTAGE2
        // and discountInfo2 as DISCOUNT_PERCENTAGE2 for account level, and same tier and service
        // level discounts.
        verify(discountStore).updateDiscountByAssociatedAccount(ASSOCIATED_ACCOUNT_ID, discountInfo5);
        verify(mockObserver).onNext(
                UpdateDiscountResponse.newBuilder().setUpdatedDiscount(expectedDiscount).build());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testUpdateDiscountFailedWithInvalidDiscountIdAndInfo() {
        final StreamObserver<UpdateDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        costRpcService.updateDiscount(UpdateDiscountRequest.newBuilder()
                // No discount info set.
                // No discount id
                .build(), mockObserver);
        verify(mockObserver).onError(any(IllegalStateException.class));
    }

    @Test
    public void testUpdateDiscountFailedWithDBException() throws Exception {
        final UpdateDiscountRequest request = UpdateDiscountRequest.newBuilder()
                .setAssociatedAccountId(id)
                .setNewDiscountInfo(discountInfo1)
                .build();
        final StreamObserver<UpdateDiscountResponse> mockObserver =
                mock(StreamObserver.class);
        doThrow(new DbException("1")).when(discountStore).updateDiscount(id, discountInfo1);

        costRpcService.updateDiscount(request, mockObserver);

        verify(mockObserver, never()).onCompleted();
        verify(mockObserver, never()).onNext(any());

        final ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.NOT_FOUND).anyDescription());
    }

    @Test
    public void testGetAveragedEntityStatsGroupByCSP() throws Exception {
        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(StatsFilter.newBuilder().addCommodityRequests(CommodityRequest.newBuilder()
                        .addGroupBy(CostRpcService.CSP)
                        .setRelatedEntityType(CostRpcService.CLOUD_SERVICE).build())
                        .build())
                .build();
        performTest(request, 2);
    }

    @Test
    public void testGetAveragedEntityStatsGroupByCSPWithEmptyBusinessAccountHelperMap() throws Exception {
        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(StatsFilter.newBuilder().addCommodityRequests(CommodityRequest.newBuilder()
                        .addGroupBy(CostRpcService.CSP)
                        .setRelatedEntityType(CostRpcService.CLOUD_SERVICE).build())
                        .build())
                .build();
        final StreamObserver<StatSnapshot> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, AccountExpenses> accountIdToExpenseMap = ImmutableMap.of(2l,
                AccountExpenses.newBuilder()
                        .setAssociatedAccountId(3l)
                        .setAccountExpensesInfo(accountExpensesInfo)
                        .build());
        final Map<Long, Map<Long, AccountExpenses>> snapshotToAccountExpensesMap = ImmutableMap.of(1l, accountIdToExpenseMap);
        given(accountExpenseStore.getLatestExpenses()).willReturn(snapshotToAccountExpensesMap);
        given(accountExpenseStore.getAccountExpenses(any(), any())).willReturn(snapshotToAccountExpensesMap);
        final StatRecord.Builder statRecordBuilder = StatRecord.newBuilder().setName(CostRpcService.COST_PRICE);
        final Builder snapshotBuilder = StatSnapshot.newBuilder();
        snapshotBuilder.setSnapshotDate(DateTimeUtil.toString(1));

        statRecordBuilder.setUnits("$/h");

        // the provider UUID should be missing due to business account id (2) is not in the BusinessAccountHelp map.
        StatValue.Builder statValueBuilder = StatValue.newBuilder();

        statValueBuilder.setAvg((float) ACCOUNT_EXPENSE1);

        statValueBuilder.setTotal((float) ACCOUNT_EXPENSE1);

        // currentValue
        statRecordBuilder.setCurrentValue((float) ACCOUNT_EXPENSE1);

        StatValue statValue = statValueBuilder.build();

        statRecordBuilder.setValues(statValue);
        statRecordBuilder.setUsed(statValue);
        statRecordBuilder.setPeak(statValue);

        snapshotBuilder.addStatRecords(statRecordBuilder.build());
        costRpcService.getAveragedEntityStats(request, mockObserver);
        verify(mockObserver).onNext(snapshotBuilder.build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetAveragedEntityStatsGroupByAccount() throws Exception {
        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(StatsFilter.newBuilder()
                        .setStartDate(1l)
                        .setEndDate(2l)
                        .addCommodityRequests(CommodityRequest.newBuilder()
                                .setRelatedEntityType(CostRpcService.CLOUD_SERVICE)
                                .addGroupBy(CostRpcService.TARGET).build())
                        .build())
                .build();
        performTest(request, 2);
    }

    @Test
    public void testGetAveragedEntityStatsGroupByCloudService() throws Exception {
        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(StatsFilter.newBuilder().addCommodityRequests(CommodityRequest.newBuilder()
                        .addGroupBy(CostRpcService.CLOUD_SERVICE)
                        .setRelatedEntityType(CostRpcService.CLOUD_SERVICE).build())
                        .build())
                .build();
        performTest(request, 4);
    }

    @Test
    public void testGetAveragedEntityStatsGroupByUnknown() throws Exception {
        final GetAveragedEntityStatsRequest request = GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(StatsFilter.newBuilder().addCommodityRequests(CommodityRequest.newBuilder()
                        .addGroupBy("unknown").setRelatedEntityType(CostRpcService.CLOUD_SERVICE).build())
                        .build())
                .build();

        final StreamObserver<StatSnapshot> mockObserver =
                mock(StreamObserver.class);
        costRpcService.getAveragedEntityStats(request, mockObserver);

        verify(mockObserver, never()).onCompleted();
        verify(mockObserver, never()).onNext(any());

        final ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INTERNAL).anyDescription());
    }

    private void performTest(final GetAveragedEntityStatsRequest request, int providerId) throws DbException {
        final StreamObserver<StatSnapshot> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, AccountExpenses> accountIdToExpenseMap = ImmutableMap.of(2l,
                AccountExpenses.newBuilder()
                        .setAssociatedAccountId(2l)
                        .setAccountExpensesInfo(accountExpensesInfo)
                        .build());
        final Map<Long, Map<Long, AccountExpenses>> snapshotToAccountExpensesMap = ImmutableMap.of(1l, accountIdToExpenseMap);
        given(accountExpenseStore.getLatestExpenses()).willReturn(snapshotToAccountExpensesMap);
        given(accountExpenseStore.getAccountExpenses(any(), any())).willReturn(snapshotToAccountExpensesMap);
        final StatRecord.Builder statRecordBuilder = StatRecord.newBuilder().setName(CostRpcService.COST_PRICE);
        final Builder snapshotBuilder = StatSnapshot.newBuilder();
        snapshotBuilder.setSnapshotDate(DateTimeUtil.toString(1));

        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setProviderUuid(String.valueOf(providerId));
        StatValue.Builder statValueBuilder = StatValue.newBuilder();

        statValueBuilder.setAvg((float) ACCOUNT_EXPENSE1);

        statValueBuilder.setTotal((float) ACCOUNT_EXPENSE1);

        // currentValue
        statRecordBuilder.setCurrentValue((float) ACCOUNT_EXPENSE1);

        StatValue statValue = statValueBuilder.build();

        statRecordBuilder.setValues(statValue);
        statRecordBuilder.setUsed(statValue);
        statRecordBuilder.setPeak(statValue);

        snapshotBuilder.addStatRecords(statRecordBuilder.build());
        costRpcService.getAveragedEntityStats(request, mockObserver);
        verify(mockObserver).onNext(snapshotBuilder.build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetCloudCostStatsWithLatestWorkload() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder().build();
        //performEntityCostTest(request, 4);

        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap = ImmutableMap.of(2l,
                entityCost);
        final Map<Long, Map<Long, EntityCost>> snapshotToAccountExpensesMap = ImmutableMap.of(1l, accountIdToExpenseMap);
        given(entityCostStore.getLatestEntityCost()).willReturn(snapshotToAccountExpensesMap);

        final CloudStatRecord.StatRecord.Builder statRecordBuilder = CloudStatRecord.StatRecord.newBuilder();
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        statRecordBuilder.setName(CostRpcService.COST_PRICE);
        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setAssociatedEntityId(4l);
        statRecordBuilder.setAssociatedEntityType(1);
        statRecordBuilder.setCategory(CostCategory.COMPUTE);
        CloudStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudStatRecord.StatRecord.StatValue.newBuilder();

        statValueBuilder.setAvg((float) ACCOUNT_EXPENSE1);

        statValueBuilder.setTotal((float) ACCOUNT_EXPENSE1);
        statValueBuilder.setMax((float) ACCOUNT_EXPENSE1);
        statValueBuilder.setMin((float) ACCOUNT_EXPENSE1);

        statRecordBuilder.setValues(statValueBuilder.build());
        final CloudStatRecord cloudStatRecord = CloudStatRecord.newBuilder()
                .setSnapshotDate(DateTimeUtil.toString(1))
                .addStatRecords(statRecordBuilder.build())
                .build();
        builder.addCloudStatRecord(cloudStatRecord);
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetCloudCostStatsWithWorkload() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .setStartDate(1l)
                .setEndDate(1l)
                .build();


        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap = ImmutableMap.of(2l,
                entityCost);
        final Map<Long, Map<Long, EntityCost>> snapshotToAccountExpensesMap = ImmutableMap.of(1l, accountIdToExpenseMap);
        //given(entityCostStore.getLatestEntityCost()).willReturn(snapshotToAccountExpensesMap);
        given(entityCostStore.getEntityCosts(any())).willReturn(snapshotToAccountExpensesMap);
        final CloudStatRecord.StatRecord.Builder statRecordBuilder = CloudStatRecord.StatRecord.newBuilder();
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        statRecordBuilder.setName(CostRpcService.COST_PRICE);
        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setAssociatedEntityId(4l);
        statRecordBuilder.setAssociatedEntityType(1);
        statRecordBuilder.setCategory(CostCategory.COMPUTE);
        CloudStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudStatRecord.StatRecord.StatValue.newBuilder();

        statValueBuilder.setAvg((float) ACCOUNT_EXPENSE1);

        statValueBuilder.setTotal((float) ACCOUNT_EXPENSE1);
        statValueBuilder.setMax((float) ACCOUNT_EXPENSE1);
        statValueBuilder.setMin((float) ACCOUNT_EXPENSE1);

        statRecordBuilder.setValues(statValueBuilder.build());
        final CloudStatRecord cloudStatRecord = CloudStatRecord.newBuilder()
                .setSnapshotDate(DateTimeUtil.toString(1))
                .addStatRecords(statRecordBuilder.build())
                .build();
        builder.addCloudStatRecord(cloudStatRecord);
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetCloudCostStatsWithWorkloadWithEntityTypeFilter() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .setStartDate(1l)
                .setEndDate(1l)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().build())
                .build();


        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap = ImmutableMap.of(2l,
                entityCost);
        final Map<Long, Map<Long, EntityCost>> snapshotToAccountExpensesMap = ImmutableMap.of(1l, accountIdToExpenseMap);
        given(entityCostStore.getEntityCosts(any())).willReturn(snapshotToAccountExpensesMap);
        final CloudStatRecord.StatRecord.Builder statRecordBuilder = CloudStatRecord.StatRecord.newBuilder();
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        statRecordBuilder.setName(CostRpcService.COST_PRICE);
        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setAssociatedEntityId(4l);
        statRecordBuilder.setAssociatedEntityType(1);
        statRecordBuilder.setCategory(CostCategory.COMPUTE);
        CloudStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudStatRecord.StatRecord.StatValue.newBuilder();

        statValueBuilder.setAvg((float) ACCOUNT_EXPENSE1);

        statValueBuilder.setTotal((float) ACCOUNT_EXPENSE1);
        statValueBuilder.setMax((float) ACCOUNT_EXPENSE1);
        statValueBuilder.setMin((float) ACCOUNT_EXPENSE1);

        statRecordBuilder.setValues(statValueBuilder.build());
        final CloudStatRecord cloudStatRecord = CloudStatRecord.newBuilder()
                .setSnapshotDate(DateTimeUtil.toString(1))
                .addStatRecords(statRecordBuilder.build())
                .build();
        builder.addCloudStatRecord(cloudStatRecord);
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetCloudCostStatsWithWorkloadWithVMEntityTypeFilter() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .setStartDate(1l)
                .setEndDate(1l)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().addFilterId(EntityType.VIRTUAL_MACHINE_VALUE).build())
                .build();


        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap = ImmutableMap.of(2l,
                entityCost);
        final Map<Long, Map<Long, EntityCost>> snapshotToAccountExpensesMap = ImmutableMap.of(1l, accountIdToExpenseMap);
        given(entityCostStore.getEntityCosts(any())).willReturn(snapshotToAccountExpensesMap);
        final CloudStatRecord.StatRecord.Builder statRecordBuilder = CloudStatRecord.StatRecord.newBuilder();
        final CloudStatRecord.StatRecord computeRecord = getCloudStatRecord(statRecordBuilder, CostCategory.COMPUTE, 10, (float) ACCOUNT_EXPENSE1);
        final CloudStatRecord.StatRecord ipRecord = getCloudStatRecord(statRecordBuilder, CostCategory.IP, 10, 0);
        final CloudStatRecord.StatRecord storageRecord = getCloudStatRecord(statRecordBuilder, CostCategory.STORAGE, 10, 0);
        final CloudStatRecord.StatRecord licenseRecord = getCloudStatRecord(statRecordBuilder, CostCategory.LICENSE, 10, 0);

        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        builder.addCloudStatRecord(CloudStatRecord.newBuilder()
                .setSnapshotDate(DateTimeUtil.toString(1))
                .addStatRecords(ipRecord)
                .addStatRecords(computeRecord)
                .addStatRecords(licenseRecord)
                .addStatRecords(storageRecord));
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    public CloudStatRecord.StatRecord getCloudStatRecord(final CloudStatRecord.StatRecord.Builder statRecordBuilder,
                                                                 final CostCategory costCategory,
                                                                 final int entityTypeValue,
                                                                 final float amount) {
    //   final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        statRecordBuilder.setName(CostRpcService.COST_PRICE);
        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setAssociatedEntityType(entityTypeValue);
        statRecordBuilder.setCategory(costCategory);
        CloudStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudStatRecord.StatRecord.StatValue.newBuilder();

        statValueBuilder.setAvg(amount);

        statValueBuilder.setTotal(amount);
        statValueBuilder.setMax(amount);
        statValueBuilder.setMin(amount);

        statRecordBuilder.setValues(statValueBuilder.build());
       return  statRecordBuilder.build();
      //  builder.addCloudStatRecord(cloudStatRecord);
      //  return builder;
    }

    @Test
    public void testGetCloudCostStatsWithWorkloadMultipleCategories() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .setStartDate(1l)
                .setEndDate(1l)
                .build();


        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap1 = ImmutableMap.of(2l,
                entityCost, 3L, entityCost1);
        final Map<Long, EntityCost> accountIdToExpenseMap2 = ImmutableMap.of(2l,
                entityCost1, 3L, entityCost2);
        final Map<Long, Map<Long, EntityCost>> snapshotToAccountExpensesMap =
                ImmutableMap.of(1l, accountIdToExpenseMap1, 99999999l, accountIdToExpenseMap2);
        given(entityCostStore.getEntityCosts(any())).willReturn(snapshotToAccountExpensesMap);

        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        final CloudStatRecord cloudStatRecord = CloudStatRecord.newBuilder()
                .setSnapshotDate(DateTimeUtil.toString(1))
                .addStatRecords(getStatRecordBuilder(CostCategory.COMPUTE))
                .addStatRecords(getStatRecordBuilder(CostCategory.COMPUTE))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP))
                .build();

        final CloudStatRecord cloudStatRecord1 = CloudStatRecord.newBuilder()
                .setSnapshotDate(DateTimeUtil.toString(99999999l))
                .addStatRecords(getStatRecordBuilder(CostCategory.COMPUTE))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP))
                .build();
        builder.addCloudStatRecord(cloudStatRecord).addCloudStatRecord(cloudStatRecord1);
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    private CloudStatRecord.StatRecord.Builder getStatRecordBuilder(CostCategory costCategory) {
        final CloudStatRecord.StatRecord.Builder statRecordBuilder = CloudStatRecord.StatRecord.newBuilder();
        statRecordBuilder.setName(CostRpcService.COST_PRICE);
        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setAssociatedEntityId(4l);
        statRecordBuilder.setAssociatedEntityType(1);
        statRecordBuilder.setCategory(costCategory);
        CloudStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudStatRecord.StatRecord.StatValue.newBuilder();

        statValueBuilder.setAvg((float) ACCOUNT_EXPENSE1);

        statValueBuilder.setTotal((float) ACCOUNT_EXPENSE1);
        statValueBuilder.setMax((float) ACCOUNT_EXPENSE1);
        statValueBuilder.setMin((float) ACCOUNT_EXPENSE1);

        statRecordBuilder.setValues(statValueBuilder.build());
        return statRecordBuilder;
    }

    private void performEntityCostTest(final GetAveragedEntityStatsRequest request, int providerId) throws DbException {
        final StreamObserver<StatSnapshot> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap = ImmutableMap.of(2l,
                entityCost);
        final Map<Long, Map<Long, EntityCost>> snapshotToAccountExpensesMap = ImmutableMap.of(1l, accountIdToExpenseMap);
        given(entityCostStore.getLatestEntityCost()).willReturn(snapshotToAccountExpensesMap);
        given(entityCostStore.getEntityCosts(any(), any())).willReturn(snapshotToAccountExpensesMap);
        final StatRecord.Builder statRecordBuilder = StatRecord.newBuilder().setName(CostRpcService.COST_PRICE);
        final Builder snapshotBuilder = StatSnapshot.newBuilder();
        snapshotBuilder.setSnapshotDate(DateTimeUtil.toString(1));

        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setProviderUuid(String.valueOf(providerId));
        StatValue.Builder statValueBuilder = StatValue.newBuilder();

        statValueBuilder.setAvg((float) ACCOUNT_EXPENSE1);

        statValueBuilder.setTotal((float) ACCOUNT_EXPENSE1);

        // currentValue
        statRecordBuilder.setCurrentValue((float) ACCOUNT_EXPENSE1);

        StatValue statValue = statValueBuilder.build();

        statRecordBuilder.setValues(statValue);
        statRecordBuilder.setUsed(statValue);
        statRecordBuilder.setPeak(statValue);

        snapshotBuilder.addStatRecords(statRecordBuilder.build());
        costRpcService.getAveragedEntityStats(request, mockObserver);
        verify(mockObserver).onNext(snapshotBuilder.build());
        verify(mockObserver).onCompleted();
    }
}