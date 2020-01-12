package com.vmturbo.cost.component.rpc;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.Channel;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord.Builder;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
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
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesResponse;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.discount.DiscountNotFoundException;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.discount.DuplicateAccountIdException;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.entity.cost.EntityCostToStatRecordConverter;
import com.vmturbo.cost.component.entity.cost.ProjectedEntityCostStore;
import com.vmturbo.cost.component.expenses.AccountExpensesStore;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.cost.component.util.CostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter.EntityCostFilterBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.sql.utils.DbException;

@SuppressWarnings("unchecked")
public class CostRpcServiceTest {

    private static final double ACCOUNT_EXPENSE1 = 10.0;
    private static final double ACCOUNT_EXPENSE2 = 5.0;
    private static final long ASSOCIATED_ACCOUNT_ID = 1111L;
    private static final double DISCOUNT_PERCENTAGE2 = 20.0;
    private static final double DISCOUNT_PERCENTAGE1 = 10.0;
    private static final long ID = 1234L;
    private static final long ID2 = 1235L;
    private static final long ASSOCIATED_SERVICE_ID = 4L;
    private static final long TIME = 1000_000_000L;
    private static final long MID_TIME = TIME + TimeUnit.MINUTES.toMillis(30);
    private static final double DELTA = 0.00001d; // the allowed delta between 'expected' and 'actual'

    private RepositoryClient repositoryClient;
    private SupplyChainServiceBlockingStub serviceBlockingStub;

    private static final long realTimeContextId = 777777L;
    final DiscountInfo discountInfo1 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE1)
                    .build())
            .setServiceLevelDiscount(DiscountInfo
                    .ServiceLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByServiceId(ID, DISCOUNT_PERCENTAGE1)
                    .build())
            .build();
    private final DiscountInfo discountInfo2 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE2)
                    .build())
            .setServiceLevelDiscount(DiscountInfo
                    .ServiceLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByServiceId(ID, DISCOUNT_PERCENTAGE1)
                    .build())
            .build();
    private final DiscountInfo discountInfo3 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE1)
                    .build())
            .setServiceLevelDiscount(DiscountInfo
                    .ServiceLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByServiceId(ID, DISCOUNT_PERCENTAGE1)
                    .build())
            .setDisplayName("testname")
            .build();
    private final DiscountInfo discountInfo4 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE1)
                    .build())
            .setServiceLevelDiscount(DiscountInfo
                    .ServiceLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByServiceId(ID, DISCOUNT_PERCENTAGE1)
                    .build())
            .build();
    private final DiscountInfo discountInfo5 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE1)
                    .build())
            .setServiceLevelDiscount(DiscountInfo
                    .ServiceLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByServiceId(ID, DISCOUNT_PERCENTAGE1)
                    .build())
            .setTierLevelDiscount(DiscountInfo
                    .TierLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByTierId(ID, DISCOUNT_PERCENTAGE1))
            .build();
    private final DiscountInfo discountInfoAccountOnly1 = DiscountInfo.newBuilder()
            .setAccountLevelDiscount(DiscountInfo
                    .AccountLevelDiscount
                    .newBuilder()
                    .setDiscountPercentage(DISCOUNT_PERCENTAGE1)
                    .build())
            .setDisplayName("testname")
            .build();
    private final DiscountInfo discountInfoServiceAndTierOnly1 = DiscountInfo.newBuilder()
            .setServiceLevelDiscount(DiscountInfo
                    .ServiceLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByServiceId(ID, DISCOUNT_PERCENTAGE1)
                    .build())
            .setTierLevelDiscount(DiscountInfo
                    .TierLevelDiscount
                    .newBuilder()
                    .putDiscountPercentageByTierId(ID, DISCOUNT_PERCENTAGE1))
            .build();
    private final AccountExpenses.AccountExpensesInfo accountExpensesInfo = AccountExpensesInfo.newBuilder()
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
    private static final int ASSOCIATED_ENTITY_TYPE1 = 1;
    private final ComponentCost componentCost = ComponentCost.newBuilder()
            .setAmount(CurrencyAmount.newBuilder().setAmount(ACCOUNT_EXPENSE1).setCurrency(1))
            .setCategory(CostCategory.ON_DEMAND_COMPUTE)
            .setCostSource(CostSource.ON_DEMAND_RATE)
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
    private DiscountStore discountStore = mock(DiscountStore.class);

    private AccountExpensesStore accountExpenseStore = mock(AccountExpensesStore.class);

    private EntityCostStore entityCostStore = mock(EntityCostStore.class);
    private ProjectedEntityCostStore projectedEntityCostStore = mock(ProjectedEntityCostStore.class);
    private BusinessAccountHelper businessAccountHelper = new BusinessAccountHelper();
    private TimeFrameCalculator timeFrameCalculator = mock(TimeFrameCalculator.class);

    private Clock clock = new MutableFixedClock(TIME);
    private CostRpcService costRpcService;

    @Before
    public void setUp() {
        businessAccountHelper.storeTargetMapping(2, ImmutableList.of(2L));
        costRpcService = new CostRpcService(discountStore, accountExpenseStore, entityCostStore,
                projectedEntityCostStore, timeFrameCalculator, businessAccountHelper, clock);

            repositoryClient = mock(RepositoryClient.class);
            serviceBlockingStub = SupplyChainServiceGrpc.newBlockingStub(mock(Channel.class));
        when(timeFrameCalculator.millis2TimeFrame(anyLong())).thenReturn(TimeFrame.LATEST);
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
                .setId(ID)
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
                // No discount associated account ID
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

        doThrow(new DbException(ID + "")).when(discountStore).persistDiscount(ASSOCIATED_ACCOUNT_ID, discountInfo1);
        costRpcService.createDiscount(request, mockObserver);
        verify(mockObserver).onError(any(IllegalStateException.class));
        final ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INTERNAL).descriptionContains(ID + ""));
    }

    @Test
    public void testDeleteDiscount() throws DbException, DiscountNotFoundException {
        final DeleteDiscountRequest deleteDiscountRequest = DeleteDiscountRequest.newBuilder()
                .setAssociatedAccountId(ID)
                .build();

        final StreamObserver<DeleteDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        costRpcService.deleteDiscount(deleteDiscountRequest, mockObserver);

        verify(discountStore).deleteDiscountByAssociatedAccountId(ID);
        verify(mockObserver).onNext(
                DeleteDiscountResponse.newBuilder().setDeleted(true).build());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }


    @Test
    public void testDeleteDiscountWithDiscountId() throws DbException, DiscountNotFoundException {
        final DeleteDiscountRequest deleteDiscountRequest = DeleteDiscountRequest.newBuilder()
                .setDiscountId(ID)
                .build();

        final StreamObserver<DeleteDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        costRpcService.deleteDiscount(deleteDiscountRequest, mockObserver);

        verify(discountStore).deleteDiscountByDiscountId(ID);
        verify(mockObserver).onNext(
                DeleteDiscountResponse.newBuilder().setDeleted(true).build());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }


    @Test
    public void testDeleteDiscountFailedWithDiscountNotFoundException() throws DbException, DiscountNotFoundException {
        testDeleteException(new DiscountNotFoundException(ID + ""), Code.NOT_FOUND);
    }


    @Test
    public void testDeleteDiscountFailedWithDbException() throws DbException, DiscountNotFoundException {
        testDeleteException(new DbException(ID + ""), Code.INTERNAL);
    }

    private void testDeleteException(Exception e, Code code) throws DiscountNotFoundException, DbException {
        final DeleteDiscountRequest discountID = DeleteDiscountRequest.newBuilder()
                .setAssociatedAccountId(ID)
                .build();

        final StreamObserver<DeleteDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        doThrow(e).when(discountStore).deleteDiscountByAssociatedAccountId(ID);
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
                // no associated account ID
                .build(), mockObserver);
        verify(mockObserver).onError(any(IllegalStateException.class));
    }

    @Test
    public void testGetDiscountWithAssociatedAccountId() throws Exception {
        final GetDiscountRequest request = GetDiscountRequest.newBuilder()
                .setFilter(DiscountQueryFilter.newBuilder()
                        .addAllAssociatedAccountId(ImmutableSet.of(ID)).build())
                .build();
        final StreamObserver<Discount> mockObserver =
                mock(StreamObserver.class);

        final Discount discounts = Discount.newBuilder()
                .setId(ID)
                .setDiscountInfo(discountInfo1)
                .build();
        given(discountStore.getDiscountByAssociatedAccountId(ID)).willReturn(ImmutableList.of(discounts));

        costRpcService.getDiscounts(request, mockObserver);

        verify(discountStore).getDiscountByAssociatedAccountId(ID);
        verify(mockObserver).onNext(Discount.newBuilder().setId(ID).setDiscountInfo(discountInfo1).build());
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
                .setId(ID)
                .setDiscountInfo(discountInfo1)
                .build();

        final Discount discounts2 = Discount.newBuilder()
                .setId(ID2)
                .setDiscountInfo(discountInfo2)
                .build();
        given(discountStore.getAllDiscount()).willReturn(ImmutableList.of(discounts, discounts2));

        costRpcService.getDiscounts(request, mockObserver);

        verify(discountStore).getAllDiscount();
        verify(mockObserver).onNext(Discount.newBuilder().setId(ID).setDiscountInfo(discountInfo1).build());
        verify(mockObserver).onNext(Discount.newBuilder().setId(ID2).setDiscountInfo(discountInfo2).build());
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
                .setId(ID)
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


    // UI currently doesn't use discount ID for updating
    @Test
    public void testUpdateDiscountWithDiscountId() throws Exception {
        final UpdateDiscountRequest request = UpdateDiscountRequest.newBuilder()
                .setDiscountId(ID)
                .setNewDiscountInfo(discountInfoAccountOnly1)
                .build();
        final StreamObserver<UpdateDiscountResponse> mockObserver =
                mock(StreamObserver.class);

        Discount discount = Discount.newBuilder()
                .setId(ID)
                .setAssociatedAccountId(ASSOCIATED_ACCOUNT_ID)
                .setDiscountInfo(discountInfo2)
                .build();
        // discountInfoAccountOnly1 from the request has DISCOUNT_PERCENTAGE1 as account level discount
        // discountInfo2 has DISCOUNT_PERCENTAGE2 as account level discount
        given(discountStore.getDiscountByDiscountId(ID))
                .willReturn(ImmutableList.of(discount));
        costRpcService.updateDiscount(request, mockObserver);

        // verify the account level discount is not changed (the account level discount from the request is NOT used)
        verify(discountStore)
                .updateDiscount(ID, discountInfo3);
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
                .setId(ID)
                .setAssociatedAccountId(ASSOCIATED_ACCOUNT_ID)
                .setDiscountInfo(discountInfo1)
                .build();
        // discountInfo1 has DISCOUNT_PERCENTAGE1, and should be updated to DISCOUNT_PERCENTAGE2.
        given(discountStore.getDiscountByAssociatedAccountId(ASSOCIATED_ACCOUNT_ID))
                .willReturn(ImmutableList.of(discount));
        costRpcService.updateDiscount(request, mockObserver);

        Discount expectedDiscount = Discount.newBuilder()
                .setId(ID)
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
                // No discount ID
                .build(), mockObserver);
        verify(mockObserver).onError(any(IllegalStateException.class));
    }

    @Test
    public void testUpdateDiscountFailedWithDBException() throws Exception {
        final UpdateDiscountRequest request = UpdateDiscountRequest.newBuilder()
                .setAssociatedAccountId(ID)
                .setNewDiscountInfo(discountInfo1)
                .build();
        final StreamObserver<UpdateDiscountResponse> mockObserver =
                mock(StreamObserver.class);
        doThrow(new DbException("1")).when(discountStore).updateDiscount(ID, discountInfo1);

        costRpcService.updateDiscount(request, mockObserver);

        verify(mockObserver, never()).onCompleted();
        verify(mockObserver, never()).onNext(any());

        final ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.NOT_FOUND).anyDescription());
    }

    // TODO: uncomment once the Cost Breakdown by Cloud Provider is implemented, GroupBy = CSP is
    //  not supported yet
//    @Test
//    public void testGetAveragedEntityStatsGroupByCSPWithEmptyBusinessAccountHelperMap() throws Exception {
//        final GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
//                .setGroupBy(GroupByType.CSP)
//                .setEntityTypeFilter(EntityTypeFilter.newBuilder().addEntityTypeId(EntityType.CLOUD_SERVICE_VALUE).build())
//                .build();
//
//        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
//                mock(StreamObserver.class);
//        final Map<Long, AccountExpenses> accountIdToExpenseMap = ImmutableMap.of(2l,
//                AccountExpenses.newBuilder()
//                        .setAssociatedAccountId(3l) // 3 is missing in the BusinessAccountHelp map.
//                        .setAccountExpensesInfo(accountExpensesInfo)
//                        .build());
//        final Map<Long, Map<Long, AccountExpenses>> snapshotToAccountExpensesMap =
//                ImmutableMap.of(1l, accountIdToExpenseMap);
//        given(accountExpenseStore.getLatestAccountExpensesWithConditions(Collections.emptyList()))
//                .willReturn(snapshotToAccountExpensesMap);
//        given(accountExpenseStore.getAccountExpenses(any())).willReturn(snapshotToAccountExpensesMap);
//        final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder();
//        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
//        statRecordBuilder.setName(StringConstants.COST_PRICE);
//        statRecordBuilder.setUnits("$/h");
//        //  statRecordBuilder.setAssociatedEntityId(expectedEntityTypeId);
//        CloudCostStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudCostStatRecord.StatRecord.StatValue.newBuilder();
//
//        // the associated entity ID should be 0 (missing) due to business account ID (3) is not in the BusinessAccountHelp map.
//        statRecordBuilder.setAssociatedEntityId(0l);
//        statRecordBuilder.setAssociatedEntityType(EntityType.CLOUD_SERVICE_VALUE);
//        statValueBuilder.setAvg(7.5f);
//
//        statValueBuilder.setTotal(15.0f);
//        statValueBuilder.setMax(10.0f);
//        statValueBuilder.setMin(5.0f);
//
//        statRecordBuilder.setValues(statValueBuilder.build());
//        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
//                .setSnapshotDate(1)
//                .addStatRecords(statRecordBuilder.build())
//                .build();
//        builder.addCloudStatRecord(cloudStatRecord);
//        costRpcService.getAccountExpenseStats(request, mockObserver);
//        verify(mockObserver).onNext(builder.build());
//        verify(mockObserver).onCompleted();
//    }

    @Test
    public void testGetAccountExpensesStatsWithFilterGroupByUnknown() throws Exception {
        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().addEntityTypeId(EntityType.CLOUD_SERVICE_VALUE).build())
                .setStartDate(TIME)
                .setEndDate(TIME)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().build())
                .build();


        costRpcService.getAccountExpenseStats(request, mockObserver);
        final ArgumentCaptor<StatusException> exceptionCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INTERNAL).anyDescription());
    }

    @Test
    public void testGetCloudCostStatsWithLatestWorkload() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()).build();
        //performEntityCostTest(request, 4);

        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap = ImmutableMap.of(2L,
                entityCost);
        final Map<Long, Collection<StatRecord>> snapshotToAccountExpensesMap = new HashMap<>();
        snapshotToAccountExpensesMap.put(TIME, EntityCostToStatRecordConverter
                .convertEntityToStatRecord(accountIdToExpenseMap.values()));
        given(entityCostStore.getEntityCostStats(EntityCostFilterBuilder
            .newBuilder(TimeFrame.LATEST)
            .latestTimestampRequested(true)
            .build())
        ).willReturn(snapshotToAccountExpensesMap);

        final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder();
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        statRecordBuilder.setName(StringConstants.COST_PRICE);
        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setAssociatedEntityId(4L);
        statRecordBuilder.setAssociatedEntityType(1);
        statRecordBuilder.setCategory(CostCategory.ON_DEMAND_COMPUTE);
        CloudCostStatRecord.StatRecord.StatValue.Builder statValueBuilder = CloudCostStatRecord.StatRecord.StatValue.newBuilder();

        statValueBuilder.setAvg((float) ACCOUNT_EXPENSE1);

        statValueBuilder.setTotal((float) ACCOUNT_EXPENSE1);
        statValueBuilder.setMax((float) ACCOUNT_EXPENSE1);
        statValueBuilder.setMin((float) ACCOUNT_EXPENSE1);

        statRecordBuilder.setValues(statValueBuilder.build());
        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(TIME)
                .setQueryId(0L)
                .addStatRecords(statRecordBuilder.build())
                .build();
        builder.addCloudStatRecord(cloudStatRecord);
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    /**
     * Tests the account, region, and availability zone filter convert properly
     * to EntityCostFilter.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetEntityCostWithAccountRegionAzFilter() throws Exception {
        // ARRANGE
        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
            mock(StreamObserver.class);

        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
            .setAccountFilter(Cost.AccountFilter.newBuilder().addAccountId(55L))
            .setRegionFilter(Cost.RegionFilter.newBuilder().addRegionId(66L))
            .setAvailabilityZoneFilter(Cost.AvailabilityZoneFilter.newBuilder()
                .addAvailabilityZoneId(77L))
            .build()).build();

        ArgumentCaptor<CostFilter> argumentCaptor =
            ArgumentCaptor.forClass(CostFilter.class);
        when(entityCostStore.getEntityCostStats(argumentCaptor.capture()))
            .thenReturn(Collections.emptyMap());

        // ACT
        costRpcService.getCloudCostStats(request, mockObserver);

        // ASSERT
        assertThat(argumentCaptor.getValue(), is(EntityCostFilter.EntityCostFilterBuilder
            .newBuilder(TimeFrame.LATEST)
            .accountIds(Collections.singleton(55L))
            .regionIds(Collections.singleton(66L))
            .availabilityZoneIds(Collections.singleton(77L))
            .latestTimestampRequested(true)
            .build()
        ));
    }

    @Test
    public void testGetCloudCostStatsWithWorkload() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setStartDate(TIME)
                        .setEndDate(TIME)
                        .setQueryId(0L)
                        .setRequestProjected(true)
                        .build()).build();


        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap = new HashMap<>();
        accountIdToExpenseMap.put(2L, entityCost);
        final Map<Long, Collection<StatRecord>> snapshotToAccountExpensesMap = new HashMap<>();
        snapshotToAccountExpensesMap.put(TIME, EntityCostToStatRecordConverter
                .convertEntityToStatRecord(accountIdToExpenseMap.values()));
        final Map<Long, EntityCost> projectedEntityCostMap =
                ImmutableMap.of(3L, entityCost1);
        given(entityCostStore.getEntityCostStats(any())).willReturn(snapshotToAccountExpensesMap);
        given(projectedEntityCostStore.getProjectedStatRecords(any(EntityCostFilter.class)))
                .willReturn(EntityCostToStatRecordConverter
                        .convertEntityToStatRecord(projectedEntityCostMap.values()));

        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        builder.addCloudStatRecord(createCloudStatRecord(ImmutableList.of(entityCost), TIME));
        // the Projected stats will be 1 hour ahead
        builder.addCloudStatRecord(createCloudStatRecord(ImmutableList.of(entityCost1),
                TIME + TimeUnit.HOURS.toMillis(1)));
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    /**
     * Test to ensure that when there is no projected topology, we get the latest entity cost,
     * if we can correctly retrieve them.
     *
     * @throws Exception Generic Exception that is thrown if any exception is encountered.
     */
    @Test
    public void testGetCloudStatsWithNoProjectedTopology() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setStartDate(TIME)
                        .setQueryId(0L)
                        .setEndDate(TIME)
                        .setRequestProjected(true)
                        .build()).build();
        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap = ImmutableMap.of(2L, entityCost);
        final Map<Long, Collection<StatRecord>> snapshotToAccountExpensesMap = new HashMap<>();
        snapshotToAccountExpensesMap.put(TIME, EntityCostToStatRecordConverter
                .convertEntityToStatRecord(accountIdToExpenseMap.values()));
        given(entityCostStore.getEntityCostStats(EntityCostFilterBuilder
            .newBuilder(TimeFrame.LATEST)
            .duration(TIME, TIME)
            .build())).willReturn(snapshotToAccountExpensesMap);
        given(entityCostStore.getEntityCostStats(EntityCostFilterBuilder
            .newBuilder(TimeFrame.LATEST)
            .latestTimestampRequested(true)
            .build()
        )).willReturn(snapshotToAccountExpensesMap);

        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        builder.addCloudStatRecord(createCloudStatRecord(ImmutableList.of(entityCost), TIME));
        // the Projected stats will be 1 hour ahead
        builder.addCloudStatRecord(createCloudStatRecord(ImmutableList.of(entityCost),
                TIME + TimeUnit.HOURS.toMillis(1)));
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    /**
     * Test to ensure that when there is no projected topology, we try to retrieve the latest entity
     * cost. But if multiple of them are present, we return an empty map since we will not be sure which
     * one to pick up.
     *
     * @throws Exception Generic Exception that is thrown if any exception is encountered.
     */
    @Test
    public void testGetCloudStatsWithNoProjectedTopologyAndMultipleLatestEntityCost() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setStartDate(TIME)
                        .setQueryId(0L)
                        .setEndDate(TIME)
                        .setRequestProjected(true)
                        .build()).build();
        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap = ImmutableMap.of(2L, entityCost);
        final Map<Long, Collection<StatRecord>> snapshotToAccountExpensesMap = new HashMap<>();
        snapshotToAccountExpensesMap.put(TIME, EntityCostToStatRecordConverter
                .convertEntityToStatRecord(accountIdToExpenseMap.values()));
        given(entityCostStore.getEntityCostStats(EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST)
                .duration(TIME, TIME)
                .build())).willReturn(snapshotToAccountExpensesMap);
        final Map<Long, Collection<StatRecord>> latestEntityCostMap = new HashMap<>();
        latestEntityCostMap.put(MID_TIME, EntityCostToStatRecordConverter
                .convertEntityToStatRecord(accountIdToExpenseMap.values()));
        given(entityCostStore.getEntityCostStats(EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST)
                .latestTimestampRequested(true)
                .build())).willReturn(latestEntityCostMap);

        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        builder.addCloudStatRecord(createCloudStatRecord(ImmutableList.of(entityCost), TIME));
        // the Projected stats will be 1 hour ahead
        builder.addCloudStatRecord(createCloudStatRecord(ImmutableList.of(entityCost),
                TIME + TimeUnit.HOURS.toMillis(1)));
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    /**
     * Test to ensure that when there is no projected topology, we get the latest entity cost. If we
     * don't get it, we retrieve an empty map.
     *
     * @throws Exception Generic Exception that is thrown if any exception is encountered.
     */
    @Test
    public void testGetCloudStatsWithNoProjectedTopologyAnNoLatestEntityCost() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setStartDate(TIME)
                        .setEndDate(TIME)
                        .setQueryId(0L)
                        .setRequestProjected(true)
                        .build()).build();
        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                        mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap = ImmutableMap.of(2L, entityCost);
        final Map<Long, Collection<StatRecord>> snapshotToAccountExpensesMap = new HashMap<>();
        snapshotToAccountExpensesMap.put(TIME, EntityCostToStatRecordConverter
                .convertEntityToStatRecord(accountIdToExpenseMap.values()));
        given(entityCostStore.getEntityCostStats(EntityCostFilterBuilder
            .newBuilder(TimeFrame.LATEST)
            .duration(TIME, TIME)
            .build())).willReturn(snapshotToAccountExpensesMap);
        given(entityCostStore.getEntityCostStats(EntityCostFilterBuilder
            .newBuilder(TimeFrame.LATEST)
            .latestTimestampRequested(true)
            .build())).willReturn(Collections.emptyMap());

        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        builder.addCloudStatRecord(createCloudStatRecord(ImmutableList.of(entityCost), TIME));
        // the Projected stats will be 1 hour ahead
        builder.addCloudStatRecord(createCloudStatRecord(Collections.emptyList(),
                TIME + TimeUnit.HOURS.toMillis(1)));
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetTierPriceForEntities() throws DbException {
        final GetTierPriceForEntitiesRequest request = GetTierPriceForEntitiesRequest.newBuilder()
                .setOid(2L).setCostCategory(CostCategory.ON_DEMAND_COMPUTE).build();
        final StreamObserver<GetTierPriceForEntitiesResponse> mockObserver =
                mock(StreamObserver.class);
        Map<Long, EntityCost> beforeEntityCostbyOid = new HashMap<>();
        beforeEntityCostbyOid.put(2L, entityCost);
        Map<Long, EntityCost> afterEntityCostbyOid = new HashMap<>();
        afterEntityCostbyOid.put(2L, entityCost);
        given(projectedEntityCostStore.getProjectedEntityCosts(anySet())).willReturn(afterEntityCostbyOid);
        given(entityCostStore.getEntityCosts(any())).willReturn(Collections.singletonMap(0L,
            beforeEntityCostbyOid));

        final GetTierPriceForEntitiesResponse.Builder builder = GetTierPriceForEntitiesResponse.newBuilder();
        builder.putAfterTierPriceByEntityOid(2L, CurrencyAmount.newBuilder().setAmount(10.0).setCurrency(1).build());
        builder.putBeforeTierPriceByEntityOid(2L, CurrencyAmount.newBuilder().setAmount(10.0).setCurrency(1).build());
        costRpcService.getTierPriceForEntities(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetCloudCostStatsWithWorkloadWithEntityTypeFilter() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
            .setStartDate(TIME)
            .setEndDate(TIME)
            .setRequestProjected(true)
            .setEntityTypeFilter(EntityTypeFilter.newBuilder().build())
            .build()).build();

        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap = new HashMap<>();
        accountIdToExpenseMap.put(2L, entityCost);
        final Map<Long, Collection<StatRecord>> snapshotToAccountExpensesMap = new HashMap<>();
        snapshotToAccountExpensesMap.put(TIME, EntityCostToStatRecordConverter
                .convertEntityToStatRecord(accountIdToExpenseMap.values()));
        final Map<Long, EntityCost> projectedEntityCostMap =
                ImmutableMap.of(3L, entityCost1);
        given(entityCostStore.getEntityCostStats(any())).willReturn(snapshotToAccountExpensesMap);
        given(projectedEntityCostStore.getProjectedStatRecords(any(EntityCostFilter.class)))
            .willReturn(EntityCostToStatRecordConverter.convertEntityToStatRecord(projectedEntityCostMap.values()));
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        builder.addCloudStatRecord(createCloudStatRecord(ImmutableList.of(entityCost), TIME));
        // the Projected stats will be 1 hour ahead
        builder.addCloudStatRecord(createCloudStatRecord(ImmutableList.of(entityCost1),
                TIME + TimeUnit.HOURS.toMillis(1)));
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetCloudStatsMultipleGroupBy() throws DbException {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                        .setQueryId(0L)
                        .setStartDate(1L)
                        .setEndDate(1L)
                        .setRequestProjected(true)
                        .addGroupBy(GroupBy.COST_CATEGORY)
                        .build()).build();
        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        Set<EntityCost> entityCosts = Sets.newHashSet();
        entityCosts.add(EntityCost.newBuilder()
                .addComponentCost(ComponentCost.newBuilder().setCategory(CostCategory.IP)
                        .build())
                .setAssociatedEntityId(1)
                .setTotalAmount(CurrencyAmount.newBuilder().setAmount(2.111).build())
                .build());
        entityCosts.add(EntityCost.newBuilder()
                .addComponentCost(ComponentCost.newBuilder().setCategory(CostCategory.IP)
                        .build())
                .setAssociatedEntityId(2)
                .setTotalAmount(CurrencyAmount.newBuilder().setAmount(2.111).build())
                .build());

        entityCosts.add(EntityCost.newBuilder()
                .addComponentCost(ComponentCost.newBuilder().setCategory(CostCategory.ON_DEMAND_COMPUTE)
                        .build())
                .setAssociatedEntityId(1)
                .setTotalAmount(CurrencyAmount.newBuilder().setAmount(3.111).build())
                .build());
        entityCosts.add(EntityCost.newBuilder()
                .addComponentCost(ComponentCost.newBuilder().setCategory(CostCategory.ON_DEMAND_COMPUTE)
                        .build())
                .setAssociatedEntityId(2)
                .setTotalAmount(CurrencyAmount.newBuilder().setAmount(3.111).build())
                .build());
        final Collection<StatRecord> projectedEntityCostMap = createCloudStatRecordByGroup(Lists.newArrayList(entityCosts),
                1L).getStatRecordsList();
        given(projectedEntityCostStore.getProjectedStatRecordsByGroup(any(), any(EntityCostFilter.class))).willReturn(projectedEntityCostMap);
        final Map<Long, Collection<StatRecord>> snapshotToAccountExpensesMap = new HashMap<>();
        CloudCostStatRecord sampleCloudCostStats = createCloudStatRecordByGroup(Lists.newArrayList(entityCosts),
                1L);
        snapshotToAccountExpensesMap.put(1L,
               sampleCloudCostStats.getStatRecordsList());
        when(entityCostStore.getEntityCostStats(any())).thenReturn(snapshotToAccountExpensesMap);

        costRpcService.getCloudCostStats(request, mockObserver);
        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        builder.addCloudStatRecord(sampleCloudCostStats);
        // the Projected stats will be 1 hour ahead
        builder.addCloudStatRecord(createCloudStatRecordByGroup(Lists.newArrayList(entityCosts),
                1 + TimeUnit.HOURS.toMillis(1)));

        ArgumentCaptor<GetCloudCostStatsResponse> argumentCaptor = ArgumentCaptor.forClass(GetCloudCostStatsResponse.class);
        verify(mockObserver).onNext(argumentCaptor .capture());

        assertThat(argumentCaptor.getValue().getCloudStatRecordList().stream()
                .flatMap(i1 -> i1.getStatRecordsList().stream()).collect(toList()),
                containsInAnyOrder(builder.getCloudStatRecordList()
                .stream().flatMap(i2 -> i2.getStatRecordsList().stream()).toArray()));
        verify(mockObserver).onCompleted();
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetCloudCostStatsWithWorkloadMultipleCategories() throws Exception {
        final GetCloudCostStatsRequest request = GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                .setStartDate(TIME)
                .setEndDate(MID_TIME)
                .setRequestProjected(true)
                .build()).build();

        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final Map<Long, EntityCost> accountIdToExpenseMap1 = new HashMap<>();
        accountIdToExpenseMap1.put(2L, entityCost);
        accountIdToExpenseMap1.put(3L, entityCost1);
        final Map<Long, EntityCost> accountIdToExpenseMap2 = new HashMap<>();
        accountIdToExpenseMap2.put(2L, entityCost1);
        accountIdToExpenseMap2.put(3L, entityCost2);

        final Map<Long, Collection<StatRecord>> snapshotToAccountExpensesMap = new HashMap<>();
        snapshotToAccountExpensesMap.put(TIME, EntityCostToStatRecordConverter
                .convertEntityToStatRecord(accountIdToExpenseMap1.values()));
        snapshotToAccountExpensesMap.put(MID_TIME, EntityCostToStatRecordConverter
                .convertEntityToStatRecord(accountIdToExpenseMap2.values()));
        given(entityCostStore.getEntityCostStats(any())).willReturn(snapshotToAccountExpensesMap);

        final GetCloudCostStatsResponse.Builder builder = GetCloudCostStatsResponse.newBuilder();
        final CloudCostStatRecord cloudStatRecord = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(TIME)
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE))
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP))
                .setQueryId(0L)
                .build();

        final CloudCostStatRecord cloudStatRecord1 = CloudCostStatRecord.newBuilder()
                .setSnapshotDate(MID_TIME)
                .addStatRecords(getStatRecordBuilder(CostCategory.ON_DEMAND_COMPUTE))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP))
                .addStatRecords(getStatRecordBuilder(CostCategory.IP))
                .setQueryId(0L)
                .build();
        builder.addCloudStatRecord(cloudStatRecord).addCloudStatRecord(cloudStatRecord1);
        builder.addCloudStatRecord(createCloudStatRecord(Collections.emptyList(),
                MID_TIME + TimeUnit.HOURS.toMillis(1)));
        costRpcService.getCloudCostStats(request, mockObserver);
        verify(mockObserver).onNext(builder.build());
        verify(mockObserver).onCompleted();
    }

    private CloudCostStatRecord.StatRecord.Builder getStatRecordBuilder(CostCategory costCategory) {
        final CloudCostStatRecord.StatRecord.Builder statRecordBuilder = CloudCostStatRecord.StatRecord.newBuilder();
        statRecordBuilder.setName(StringConstants.COST_PRICE);
        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setAssociatedEntityId(4L);
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
                .setStartDate(TIME)
                .setEndDate(TIME)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().build())
                .setGroupBy(GroupByType.CSP)
                .build();

        performAccountExpenseTests(mockObserver, request, 2L);
    }

    @Test
    public void testGetAccountExpensesStatsWithFilterGroupByCSP() throws Exception {
        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
                .setGroupBy(GroupByType.CSP)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().addEntityTypeId(EntityType.CLOUD_SERVICE_VALUE).build())
                .setStartDate(TIME)
                .setEndDate(TIME)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().build())
                .build();

        performAccountExpenseTests(mockObserver, request, 2L);
    }

    @Test
    public void testGetAccountExpensesStatsWithFilterGroupByBusinessUnit() throws Exception {
        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
                .setGroupBy(GroupByType.BUSINESS_UNIT)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().addEntityTypeId(EntityType.CLOUD_SERVICE_VALUE).build())
                .setStartDate(TIME)
                .setEndDate(TIME)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().build())
                .build();

        performAccountExpenseTests(mockObserver, request, 2L);
    }

    @Test
    public void testGetAccountExpensesStatsWithFilterGroupByCloudService() throws Exception {
        final StreamObserver<GetCloudCostStatsResponse> mockObserver =
                mock(StreamObserver.class);
        final GetCloudExpenseStatsRequest request = GetCloudExpenseStatsRequest.newBuilder()
                .setGroupBy(GroupByType.CLOUD_SERVICE)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().addEntityTypeId(EntityType.CLOUD_SERVICE_VALUE).build())
                .setStartDate(TIME)
                .setEndDate(TIME)
                .setEntityTypeFilter(EntityTypeFilter.newBuilder().build())
                .build();

        performAccountExpenseTests(mockObserver, request, 4L);
    }

    private void performAccountExpenseTests(final StreamObserver<GetCloudCostStatsResponse> mockObserver,
                                            final GetCloudExpenseStatsRequest request,
                                            final long expectedEntityTypeId) throws DbException {
        final Map<Long, AccountExpenses> accountIdToExpenseMap = ImmutableMap.of(2L,
                AccountExpenses.newBuilder()
                        .setAssociatedAccountId(2L)
                        .setAccountExpensesInfo(accountExpensesInfo)
                        .build());
        final Map<Long, Map<Long, AccountExpenses>> snapshotToAccountExpensesMap =
                ImmutableMap.of(TIME, accountIdToExpenseMap);
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
                .setSnapshotDate(TIME)
                .addStatRecords(statRecordBuilder.build())
                .build();
        builder.addCloudStatRecord(cloudStatRecord);

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
                        .setSnapshotDate(snapshotTime)
                        .setQueryId(0L);
        for (EntityCost entityCost : entityCosts) {
            for (ComponentCost componentCost : entityCost.getComponentCostList()) {
                final Builder statRecordBuilder = createStatRecord(entityCost, componentCost);
                cloudStatRecordBuilder.addStatRecords(statRecordBuilder.build());
            }
        }
        return cloudStatRecordBuilder.build();
    }

    private CloudCostStatRecord createCloudStatRecordByGroup(List<EntityCost> entityCosts, long snapshotTime) {
        final CloudCostStatRecord.Builder cloudStatRecordBuilder =
                CloudCostStatRecord.newBuilder()
                        .setSnapshotDate(snapshotTime)
                        .setQueryId(0L);
        cloudStatRecordBuilder.addAllStatRecords(createProjectedStatRecordsByGroup(entityCosts));
        return cloudStatRecordBuilder.build();
    }

    private Iterable<? extends StatRecord> createProjectedStatRecordsByGroup(final List<EntityCost> entityCosts) {
        List<StatRecord> result = new ArrayList<>();
        ProjectedEntityCostStore projectedEntityCostStore = new ProjectedEntityCostStore(repositoryClient, serviceBlockingStub,
                realTimeContextId);
        for (EntityCost entityCost : entityCosts) {
            result.addAll(EntityCostToStatRecordConverter.convertEntityToStatRecord(entityCost));
        }
        return projectedEntityCostStore.aggregateByGroup(Collections.singleton(GroupBy.COST_CATEGORY),
                result);
    }

    private Builder createStatRecord(final EntityCost entityCost, final ComponentCost componentCost) {
        final Builder statRecordBuilder =
                StatRecord.newBuilder();
        statRecordBuilder.setName(StringConstants.COST_PRICE);
        statRecordBuilder.setUnits("$/h");
        statRecordBuilder.setAssociatedEntityId(entityCost.getAssociatedEntityId());
        statRecordBuilder.setAssociatedEntityType(entityCost.getAssociatedEntityType());
        statRecordBuilder.setCategory(componentCost.getCategory());
        StatRecord.StatValue.Builder statValueBuilder =
                StatRecord.StatValue.newBuilder();
        statValueBuilder.setAvg((float)componentCost.getAmount().getAmount());
        statValueBuilder.setTotal((float)componentCost.getAmount().getAmount());
        statValueBuilder.setMax((float)componentCost.getAmount().getAmount());
        statValueBuilder.setMin((float)componentCost.getAmount().getAmount());
        statRecordBuilder.setValues(statValueBuilder.build());
        return statRecordBuilder;
    }

    /**
     * Test that we convert the stats records to the correct units and values by day.
     */
    @Test
    public void testAggregatedStatsByDay() {
        TimeFrame timeFrame = TimeFrame.DAY;
        final double multiplier = 24d;

        // BUILD
        List<CostRpcService.AccountExpenseStat> accountExpenseStats = Collections.singletonList(
                new CostRpcService.AccountExpenseStat(ID, ACCOUNT_EXPENSE1)
        );

        // RUN
        costRpcService.aggregateStatRecords(accountExpenseStats, timeFrame)
                //ASSERT
                .forEach(statRecord -> {
                    assertEquals(statRecord.getUnits(), timeFrame.getUnits());
                    assertEquals(statRecord.getValues().getTotal(),
                            ACCOUNT_EXPENSE1 * multiplier, DELTA);
                });
    }

    /**
     * Test that we convert the stats records to the correct units and values by day.
     */
    @Test
    public void testAggregatedStatsByMonth() {
        TimeFrame timeFrame = TimeFrame.MONTH;
        final double multiplier = 730d;

        // BUILD
        List<CostRpcService.AccountExpenseStat> accountExpenseStats = Collections.singletonList(
                new CostRpcService.AccountExpenseStat(ID, ACCOUNT_EXPENSE1)
        );

        // RUN
        costRpcService.aggregateStatRecords(accountExpenseStats, timeFrame)
                //ASSERT
                .forEach(statRecord -> {
                    assertEquals(statRecord.getUnits(), timeFrame.getUnits());
                    assertEquals(statRecord.getValues().getTotal(),
                            ACCOUNT_EXPENSE1 * multiplier, DELTA);
                });
    }
}
