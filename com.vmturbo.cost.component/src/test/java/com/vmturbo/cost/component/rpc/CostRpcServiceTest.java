package com.vmturbo.cost.component.rpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySet;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
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
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudExpenseStatsRequest.GroupByType;
import com.vmturbo.common.protobuf.cost.Cost.GetDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountResponse;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.discount.DiscountNotFoundException;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.discount.DuplicateAccountIdException;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.entity.cost.ProjectedEntityCostStore;
import com.vmturbo.cost.component.expenses.AccountExpensesStore;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.DbException;

@SuppressWarnings("unchecked")
public class CostRpcServiceTest {

    public static final double ACCOUNT_EXPENSE1 = 10.0;
    public static final double ACCOUNT_EXPENSE2 = 5.0;
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
            .addServiceExpenses(ServiceExpenses
                    .newBuilder()
                    .setAssociatedServiceId(ASSOCIATED_SERVICE_ID)
                    .setExpenses(CurrencyAmount.newBuilder().setAmount(ACCOUNT_EXPENSE2).build())
                    .build())
            .build();
    private final int ASSOCIATED_ENTITY_TYPE1 = 1;
    private final ComponentCost componentCost = ComponentCost.newBuilder()
            .setAmount(CurrencyAmount.newBuilder().setAmount(ACCOUNT_EXPENSE1).setCurrency(1))
            .setCategory(CostCategory.ON_DEMAND_COMPUTE)
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
    public ProjectedEntityCostStore projectedEntityCostStore = mock(ProjectedEntityCostStore.class);
    public BusinessAccountHelper businessAccountHelper = new BusinessAccountHelper();
    private TimeFrameCalculator timeFrameCalculator = mock(TimeFrameCalculator.class);

    private Clock clock = new MutableFixedClock(1_000_000);
    private CostRpcService costRpcService;

    @Before
    public void setUp() {
        businessAccountHelper.storeTargetMapping(2, ImmutableList.of(2L));
        costRpcService = new CostRpcService(discountStore, accountExpenseStore, entityCostStore,
                projectedEntityCostStore, timeFrameCalculator, businessAccountHelper, clock);
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
    public void testGetAveragedEntityStatsGroupByCSPWithEmptyBusinessAccountHelperMap() throws Exception {
        final GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
                .setGroupBy(GroupByType.CSP)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().addEntityTypeId(EntityType.CLOUD_SERVICE_VALUE).build())
                .build();

        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, AccountExpenses> accountIdToExpenseMap = ImmutableMap.of(2l,
                AccountExpenses.newBuilder()
                        .setAssociatedAccountId(3l) // 3 is missing in the BusinessAccountHelp map.
                        .setAccountExpensesInfo(accountExpensesInfo)
                        .build());
        final Map<Long, Map<Long, AccountExpenses>> snapshotToAccountExpensesMap = ImmutableMap.of(1l, accountIdToExpenseMap);
        given(accountExpenseStore.getLatestExpenses(anySet(), anySet())).willReturn(snapshotToAccountExpensesMap);
        given(accountExpenseStore.getAccountExpenses(any())).willReturn(snapshotToAccountExpensesMap);
        final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder();
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        statRecordBuilder.setName(StringConstants.COST_PRICE);
        statRecordBuilder.setUnits("$/h");
        //  statRecordBuilder.setAssociatedEntityId(expectedEntityTypeId);
        CloudCostStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudCostStatRecord.StatRecord.StatValue.newBuilder();

        // the associated entity id should be 0 (missing) due to business account id (3) is not in the BusinessAccountHelp map.
        statRecordBuilder.setAssociatedEntityId(0l);
        statRecordBuilder.setAssociatedEntityType(EntityType.CLOUD_SERVICE_VALUE);
        statValueBuilder.setAvg(7.5f);

        statValueBuilder.setTotal(15.0f);
        statValueBuilder.setMax(10.0f);
        statValueBuilder.setMin(5.0f);

        statRecordBuilder.setValues(statValueBuilder.build());
        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(1)
                .addStatRecords(statRecordBuilder.build())
                .build();
        builder.addCloudStatRecord(cloudStatRecord);
        costRpcService.getAccountExpenseStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetAccountExpensesStatsWithFilterGroupByUnknown() throws Exception {
        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().addEntityTypeId(EntityType.CLOUD_SERVICE_VALUE).build())
                .setStartDate(1l)
                .setEndDate(1l)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().build())
                .build();


        costRpcService.getAccountExpenseStats(request, mockObserver);
        final ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INTERNAL).anyDescription());
    }

    @Test
    public void testGetCloudCostStatsWithLatestWorkload() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder().build();
        //performEntityCostTest(request, 4);

        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap = ImmutableMap.of(2l,
                entityCost);
        final Map<Long, Map<Long, EntityCost>> snapshotToAccountExpensesMap = new HashMap<>();
        snapshotToAccountExpensesMap.put(1l, accountIdToExpenseMap);
        given(entityCostStore.getLatestEntityCost(anySet(), anySet())).willReturn(snapshotToAccountExpensesMap);

        final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder();
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        statRecordBuilder.setName(StringConstants.COST_PRICE);
        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setAssociatedEntityId(4l);
        statRecordBuilder.setAssociatedEntityType(1);
        statRecordBuilder.setCategory(CostCategory.ON_DEMAND_COMPUTE);
        CloudCostStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudCostStatRecord.StatRecord.StatValue.newBuilder();

        statValueBuilder.setAvg((float) ACCOUNT_EXPENSE1);

        statValueBuilder.setTotal((float) ACCOUNT_EXPENSE1);
        statValueBuilder.setMax((float) ACCOUNT_EXPENSE1);
        statValueBuilder.setMin((float) ACCOUNT_EXPENSE1);

        statRecordBuilder.setValues(statValueBuilder.build());
        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(1)
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
            .setRequestProjected(true)
            .build();


        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap = new HashMap<>();
        accountIdToExpenseMap.put(2l, entityCost);
        final Map<Long, Map<Long, EntityCost>> snapshotToAccountExpensesMap = new HashMap<>();
        snapshotToAccountExpensesMap.put(1l, accountIdToExpenseMap);
        final Map<Long, EntityCost> projectedEntityCostMap =
                ImmutableMap.of(3l, entityCost1);
        given(entityCostStore.getEntityCosts(any())).willReturn(snapshotToAccountExpensesMap);
        given(projectedEntityCostStore.getAllProjectedEntitiesCosts()).willReturn(projectedEntityCostMap);

        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        builder.addCloudStatRecord(createCloudStatRecord(ImmutableList.of(entityCost), 1l));
        // the Projected stats will be 1 hour ahead
        builder.addCloudStatRecord(createCloudStatRecord(ImmutableList.of(entityCost1),
                1 + TimeUnit.HOURS.toMillis(1)));
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetCloudCostStatsWithWorkloadWithEntityTypeFilter() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
            .setStartDate(1l)
            .setEndDate(1l)
            .setRequestProjected(true)
            .setEntityTypeFilter(EntityTypeFilter.newBuilder().build())
            .build();

        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap = new HashMap<>();
        accountIdToExpenseMap.put(2l, entityCost);
        final Map<Long, Map<Long, EntityCost>> snapshotToAccountExpensesMap = new HashMap<>();
        snapshotToAccountExpensesMap.put(1l, accountIdToExpenseMap);
        final Map<Long, EntityCost> projectedEntityCostMap =
                ImmutableMap.of(3l, entityCost1);
        given(entityCostStore.getEntityCosts(any())).willReturn(snapshotToAccountExpensesMap);
        given(projectedEntityCostStore.getAllProjectedEntitiesCosts()).willReturn(projectedEntityCostMap);
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        builder.addCloudStatRecord(createCloudStatRecord(ImmutableList.of(entityCost), 1l));
        // the Projected stats will be 1 hour ahead
        builder.addCloudStatRecord(createCloudStatRecord(ImmutableList.of(entityCost1),
                1 + TimeUnit.HOURS.toMillis(1)));
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    private CloudCostStatRecord.StatRecord getCloudStatRecord(final CloudCostStatRecord.StatRecord.Builder statRecordBuilder,
                                                              final CostCategory costCategory,
                                                              final int entityTypeValue,
                                                              final float amount) {
        statRecordBuilder.setName(StringConstants.COST_PRICE);
        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setAssociatedEntityType(entityTypeValue);
        statRecordBuilder.setCategory(costCategory);
        CloudCostStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudCostStatRecord.StatRecord.StatValue.newBuilder();

        statValueBuilder.setAvg(amount);

        statValueBuilder.setTotal(amount);
        statValueBuilder.setMax(amount);
        statValueBuilder.setMin(amount);

        statRecordBuilder.setValues(statValueBuilder.build());
        return statRecordBuilder.build();
        //  builder.addCloudStatRecord(cloudStatRecord);
        //  return builder;
    }

    @Test
    public void testGetCloudCostStatsWithWorkloadMultipleCategories() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
            .setStartDate(1l)
            .setEndDate(1000l)
            .setRequestProjected(true)
            .build();


        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap1 = new HashMap<>();
        accountIdToExpenseMap1.put(2l, entityCost);
        accountIdToExpenseMap1.put(3L, entityCost1);
        final Map<Long, EntityCost> accountIdToExpenseMap2 = new HashMap<>();
        accountIdToExpenseMap2.put(2l, entityCost1);
        accountIdToExpenseMap2.put(3L, entityCost2);

        final Map<Long, Map<Long, EntityCost>> snapshotToAccountExpensesMap = new HashMap<>();
        snapshotToAccountExpensesMap.put(1l, accountIdToExpenseMap1);
        snapshotToAccountExpensesMap.put(500l, accountIdToExpenseMap2);
        given(entityCostStore.getEntityCosts(any())).willReturn(snapshotToAccountExpensesMap);

        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(1)
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE))
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP))
                .build();

        final CloudCostStatRecord cloudStatRecord1 = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(500L)
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP))
                .build();
        builder.addCloudStatRecord(cloudStatRecord).addCloudStatRecord(cloudStatRecord1);
        builder.addCloudStatRecord(createCloudStatRecord(Collections.emptyList(),
                1000 + TimeUnit.HOURS.toMillis(1)));
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    private CloudCostStatRecord.StatRecord.Builder getStatRecordBuilder(CostCategory costCategory) {
        final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder();
        statRecordBuilder.setName(StringConstants.COST_PRICE);
        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setAssociatedEntityId(4l);
        statRecordBuilder.setAssociatedEntityType(1);
        statRecordBuilder.setCategory(costCategory);
        CloudCostStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudCostStatRecord.StatRecord.StatValue.newBuilder();

        statValueBuilder.setAvg((float) ACCOUNT_EXPENSE1);

        statValueBuilder.setTotal((float) ACCOUNT_EXPENSE1);
        statValueBuilder.setMax((float) ACCOUNT_EXPENSE1);
        statValueBuilder.setMin((float) ACCOUNT_EXPENSE1);

        statRecordBuilder.setValues(statValueBuilder.build());
        return statRecordBuilder;
    }

    @Test
    public void testGetAccountExpensesStatsWithFilter() throws Exception {
        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
                .setStartDate(1l)
                .setEndDate(1l)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().build())
                .setGroupBy(GroupByType.CSP)
                .build();

        performAccountExpenseTests(mockObserver, request, 2l);
    }

    @Test
    public void testGetAccountExpensesStatsWithFilterGroupByCSP() throws Exception {
        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
                .setGroupBy(GroupByType.CSP)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().addEntityTypeId(EntityType.CLOUD_SERVICE_VALUE).build())
                .setStartDate(1l)
                .setEndDate(1l)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().build())
                .build();

        performAccountExpenseTests(mockObserver, request, 2l);
    }

    @Test
    public void testGetAccountExpensesStatsWithFilterGroupByTarget() throws Exception {
        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
                .setGroupBy(GroupByType.TARGET)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().addEntityTypeId(EntityType.CLOUD_SERVICE_VALUE).build())
                .setStartDate(1l)
                .setEndDate(1l)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().build())
                .build();

        performAccountExpenseTests(mockObserver, request, 2l);
    }

    @Test
    public void testGetAccountExpensesStatsWithFilterGroupByCloudService() throws Exception {
        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
                .setGroupBy(GroupByType.CLOUD_SERVICE)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().addEntityTypeId(EntityType.CLOUD_SERVICE_VALUE).build())
                .setStartDate(1l)
                .setEndDate(1l)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().build())
                .build();

        performAccountExpenseTests(mockObserver, request, 4l);
    }

    private void performAccountExpenseTests(final StreamObserver<GetCloudCostStatsResponse> mockObserver,
                                            final GetCloudExpenseStatsRequest request,
                                            final long expectedEntityTypeId) throws DbException {
        final Map<Long, AccountExpenses> accountIdToExpenseMap = ImmutableMap.of(2l,
                AccountExpenses.newBuilder()
                        .setAssociatedAccountId(2l)
                        .setAccountExpensesInfo(accountExpensesInfo)
                        .build());
        final Map<Long, Map<Long, AccountExpenses>> snapshotToAccountExpensesMap = ImmutableMap.of(1l, accountIdToExpenseMap);
        given(accountExpenseStore.getLatestExpenses(anySet(), anySet())).willReturn(snapshotToAccountExpensesMap);
        given(accountExpenseStore.getAccountExpenses(any())).willReturn(snapshotToAccountExpensesMap);
        final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder();
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        statRecordBuilder.setName(StringConstants.COST_PRICE);
        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setAssociatedEntityId(expectedEntityTypeId);
        statRecordBuilder.setAssociatedEntityType(EntityType.CLOUD_SERVICE_VALUE);
        CloudCostStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudCostStatRecord.StatRecord.StatValue.newBuilder();

        statValueBuilder.setAvg(7.5f)
                .setTotal(15.0f)
                .setMax(10.0f)
                .setMin(5.0f);

        statRecordBuilder.setValues(statValueBuilder.build());
        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(1)
                .addStatRecords(statRecordBuilder.build())
                .build();
        builder.addCloudStatRecord(cloudStatRecord);

        // add projected values.
        if (request.hasStartDate() && request.hasEndDate()) {
            statValueBuilder.setAvg(5.0f)
                    .setTotal(5.0f)
                    .setMax(5.0f)
                    .setMin(5.0f);
            statRecordBuilder.clearValues();
            statRecordBuilder.setValues(statValueBuilder.build());
            final CloudCostStatRecord projectedCloudStatRecord = CloudCostStatRecord.newBuilder()
                    // the Projected stats will be 1 hour ahead
                    .setSnapshotDate(1 + TimeUnit.HOURS.toMillis(1))
                    .addStatRecords(statRecordBuilder.build())
                    .build();
            builder.addCloudStatRecord(projectedCloudStatRecord);
        }

        when(timeFrameCalculator.millis2TimeFrame(request.getStartDate())).thenReturn(TimeFrame.HOUR);
        costRpcService.getAccountExpenseStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    /**
     * Create Cloud Stat record from the entity cost.
     */
    private CloudCostStatRecord createCloudStatRecord(List<EntityCost> entityCosts,
                                                      long snapshotTime) {

        final CloudCostStatRecord.Builder cloudStatRecordBuilder =
                CloudCostStatRecord.newBuilder()
                        .setSnapshotDate(snapshotTime);
        for (EntityCost entityCost : entityCosts) {
            for (ComponentCost componentCost : entityCost.getComponentCostList()) {
                final CloudCostStatRecord.StatRecord.Builder statRecordBuilder =
                        CloudCostStatRecord.StatRecord.newBuilder();
                statRecordBuilder.setName(StringConstants.COST_PRICE);
                statRecordBuilder.setUnits("$/h");
                statRecordBuilder.setAssociatedEntityId(entityCost.getAssociatedEntityId());
                statRecordBuilder.setAssociatedEntityType(entityCost.getAssociatedEntityType());
                statRecordBuilder.setCategory(componentCost.getCategory());
                CloudCostStatRecord.StatRecord.StatValue.Builder statValueBuilder =
                        CloudCostStatRecord.StatRecord.StatValue.newBuilder();
                statValueBuilder.setAvg((float) componentCost.getAmount().getAmount());
                statValueBuilder.setTotal((float) componentCost.getAmount().getAmount());
                statValueBuilder.setMax((float) componentCost.getAmount().getAmount());
                statValueBuilder.setMin((float) componentCost.getAmount().getAmount());
                statRecordBuilder.setValues(statValueBuilder.build());
                cloudStatRecordBuilder.addStatRecords(statRecordBuilder.build());
            }
        }
        return cloudStatRecordBuilder.build();
    }
}
