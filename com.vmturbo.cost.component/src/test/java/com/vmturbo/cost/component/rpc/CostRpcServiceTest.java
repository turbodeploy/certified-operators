package com.vmturbo.cost.component.rpc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.GetDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountResponse;
import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Cost.DeleteDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.DiscountInfo;
import com.vmturbo.common.protobuf.cost.Cost.DiscountQueryFilter;
import com.vmturbo.common.protobuf.cost.Cost.CreateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountRequest;
import com.vmturbo.common.protobuf.cost.Cost.UpdateDiscountResponse;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.cost.component.discount.DiscountNotFoundException;
import com.vmturbo.cost.component.discount.DiscountStore;
import com.vmturbo.cost.component.discount.DuplicateAccountIdException;
import com.vmturbo.sql.utils.DbException;

@SuppressWarnings("unchecked")
public class CostRpcServiceTest {

    public static final long ASSOCIATED_ACCOUNT_ID = 1111l;
    public static final double DISCOUNT_PERCENTAGE2 = 20.0;
    public static final double DISCOUNT_PERCENTAGE1 = 10.0;
    private static final long id = 1234L;
    private static final long id2 = 1235L;
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

    public DiscountStore discountStore = mock(DiscountStore.class);
    private CostRpcService costRpcService;

    @Before
    public void setUp() {
        costRpcService = new CostRpcService(discountStore);
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
}