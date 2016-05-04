package com.vmturbo.platform.analysis.translators;

import java.util.List;
import java.util.function.DoubleBinaryOperator;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderSettings;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommoditySoldSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommoditySpecificationTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.analysis.protobuf.PriceFunctionDTOs.PriceFunctionTO;
import com.vmturbo.platform.analysis.protobuf.QuantityUpdatingFunctionDTOs.QuantityUpdatingFunctionTO;
import com.vmturbo.platform.analysis.topology.Topology;

/**
 * A class containing methods to convert Protobuf messages to java classes used by analysis.
 *
 * <p>
 *  This is intended to contain only static methods.
 * </p>
 */
public final class ProtobufToAnalysis {
    // Methods for converting PriceFunctionDTOs.

    /**
     * Converts a {@link PriceFunctionTO} to a {@link PriceFunction}.
     *
     * @param input The {@link PriceFunctionTO} to convert.
     * @return The resulting {@link PriceFunction}.
     */
    public static PriceFunction priceFunction(@NonNull PriceFunctionTO input) {
        switch (input.getPriceFunctionTypeCase()) {
            case STANDARD_WEIGHTED:
                return PriceFunction.Cache.createStandardWeightedPriceFunction(input.getStandardWeighted().getWeight());
            case CONSTANT:
                return PriceFunction.Cache.createConstantPriceFunction(input.getConstant().getValue());
            case STEP:
                return PriceFunction.Cache.createStepPriceFunction(input.getStep().getStepAt(),
                    input.getStep().getPriceBelow(), input.getStep().getPriceAbove());
            case PRICEFUNCTIONTYPE_NOT_SET:
            default:
                throw new IllegalArgumentException("input = " + input);
        }
    }

    // Methods for converting QuantityUpdatingFunctionDTOs.

    /**
     * Converts a {@link QuantityUpdatingFunctionTO} to a {@link DoubleBinaryOperator quantity
     * updating function}.
     *
     * @param input The {@link QuantityUpdatingFunctionTO} to convert.
     * @return The resulting {@link DoubleBinaryOperator quantity updating function}.
     */
    public static @NonNull DoubleBinaryOperator quantityUpdatingFunction(@NonNull QuantityUpdatingFunctionTO input) {
        switch (input.getQuantityUpdatingFunctionTypeCase()) {
            case MAX:
                return Math::max;
            case QUANTITYUPDATINGFUNCTIONTYPE_NOT_SET:
            default:
                throw new IllegalArgumentException("input = " + input);
        }
    }

    // Methods for converting EconomyDTOs.

    /**
     * Converts a {@link CommoditySpecificationTO} to a {@link CommoditySpecification}.
     *
     * @param input The {@link CommoditySpecificationTO} to convert.
     * @return The resulting {@link CommoditySpecification}.
     */
    public static @NonNull CommoditySpecification commoditySpecification(@NonNull CommoditySpecificationTO input) {
        return new CommoditySpecification(input.getType(),input.getQualityLowerBound(),input.getQualityUpperBound());
    }

    /**
     * Converts a list of {@link CommoditySpecificationTO}s to a {@link Basket}.
     *
     * @param input The list of {@link CommoditySpecificationTO}s to convert.
     * @return The resulting {@link Basket}.
     *
     * @see #basket(TraderTO)
     * @see #basket(ShoppingListTO)
     */
    public static @NonNull Basket basket(@NonNull List<CommoditySpecificationTO> input) {
        return new Basket(input.stream().map(ProtobufToAnalysis::commoditySpecification).collect(Collectors.toList()));
    }

    /**
     * Creates a {@link Basket} from a {@link ShoppingListTO} for use as a basket bought.
     *
     * @param input The {@link ShoppingListTO} from which to create the {@link Basket}.
     * @return The resulting {@link Basket}.
     *
     * @see #basket(List)
     * @see #basket(TraderTO)
     */
    public static @NonNull Basket basket(@NonNull ShoppingListTO input) {
        return new Basket(input.getCommoditiesBoughtList().stream()
           .map(cb -> commoditySpecification(cb.getSpecification())).collect(Collectors.toList()));
    }

    /**
     * Creates a {@link Basket} from a {@link TraderTO} for use as a basket sold.
     *
     * @param input The {@link TraderTO} from which to create the {@link Basket}.
     * @return The resulting {@link Basket}.
     *
     * @see #basket(List)
     * @see #basket(ShoppingListTO)
     */
    public static @NonNull Basket basket(@NonNull TraderTO input) {
        return new Basket(input.getCommoditiesSoldList().stream()
           .map(cs -> commoditySpecification(cs.getSpecification())).collect(Collectors.toList()));
    }

    /**
     * Adds a new {@link BuyerParticipation} to a {@link Trader} that is already in a
     * {@link Topology} given a {@link ShoppingListTO}.
     *
     * @param topology The {@link Topology} which contains the {@link Trader}.
     * @param buyer The {@link Trader} to add the {@link BuyerParticipation} to.
     * @param shoppingList The {@link ShoppingListTO} describing the {@link BuyerParticipation}.
     * @return The resulting {@link BuyerParticipation}.
     */
    public static @NonNull BuyerParticipation addShoppingList(@NonNull Topology topology, @NonNull Trader buyer,
                                                  @NonNull ShoppingListTO shoppingList) {
        @NonNull Basket basketBought = basket(shoppingList);
        @NonNull BuyerParticipation participation = shoppingList.hasSupplier()
            ? topology.addBasketBought(shoppingList.getOid(), buyer, basketBought, shoppingList.getSupplier())
            : topology.addBasketBought(shoppingList.getOid(), buyer, basketBought);

        participation.setMovable(shoppingList.getMovable());

        for (CommodityBoughtTO commodityBought : shoppingList.getCommoditiesBoughtList()) {
            int index = basketBought.indexOf(commoditySpecification(commodityBought.getSpecification()));
            participation.setQuantity(index, commodityBought.getQuantity());
            participation.setPeakQuantity(index, commodityBought.getPeakQuantity());
        }

        return participation;
    }

    /**
     * Populates the fields of a {@link CommoditySoldSettings} instance from information in a
     * {@link CommoditySoldSettingsTO}.
     *
     * @param source The {@link CommoditySoldSettingsTO} from which to get the settings.
     * @param destination The {@link CommoditySoldSettings} instance to put the settings to.
     */
    public static void populateCommoditySoldSettings(@NonNull CommoditySoldSettingsTO source,
                                                     @NonNull CommoditySoldSettings destination) {
        destination.setResizable(source.getResizable());
        destination.setCapacityLowerBound(source.getCapacityLowerBound());
        destination.setCapacityUpperBound(source.getCapacityUpperBound());
        destination.setCapacityIncrement(source.getCapacityIncrement());
        destination.setUtilizationUpperBound(source.getUtilizationUpperBound());
        destination.setPriceFunction(priceFunction(source.getPriceFunction()));
    }

    /**
     * Populates the fields of a {@link CommoditySold} from information in a {@link CommoditySoldTO}.
     *
     * @param source The {@link CommoditySoldTO} from which to get the data.
     * @param destination The {@link CommoditySold} to put the data to.
     */
    public static void populateCommoditySold(@NonNull CommoditySoldTO source,
                                             @NonNull CommoditySold destination) {
        destination.setQuantity(source.getQuantity());
        destination.setPeakQuantity(source.getPeakQuantity());
        destination.setCapacity(source.getCapacity());
        destination.setThin(source.getThin());
        populateCommoditySoldSettings(source.getSettings(), destination.getSettings());
    }

    /**
     * Populates the fields of a {@link TraderSettings} instance from information in a
     * {@link TraderSettingsTO}.
     *
     * @param source The {@link TraderSettingsTO} from which to get the settings.
     * @param destination The {@link TraderSettings} instance to put the settings to.
     */
    public static void populateTraderSettings(@NonNull TraderSettingsTO source,
                                              @NonNull TraderSettings destination) {
        destination.setCloneable(source.getClonable());
        destination.setSuspendable(source.getSuspendable());
        destination.setMinDesiredUtil(source.getMinDesiredUtilization());
        destination.setMaxDesiredUtil(source.getMaxDesiredUtilization());
    }

    /**
     * Converts a {@link TraderStateTO} to a {@link TraderState} instance.
     *
     * @param input The {@link TraderStateTO} to convert.
     * @return The resulting {@link TraderState} instance.
     */
    public static TraderState traderState(@NonNull TraderStateTO input) {
        switch (input) {
            case ACTIVE:
                return TraderState.ACTIVE;
            case INACTIVE:
                return TraderState.INACTIVE;
            default:
                throw new IllegalArgumentException("input = " + input);
        }
    }

    /**
     * Adds a new {@link Trader} to a {@link Topology} given a {@link TraderTO}.
     *
     * @param topology The {@link Topology} to add the {@link Trader} to.
     * @param input The {@link TraderTO} describing the {@link Trader}.
     * @return The resulting {@link Trader}.
     */
    public static @NonNull Trader addTrader(@NonNull Topology topology, @NonNull TraderTO input) {
        @NonNull Basket basketSold = basket(input);
        @NonNull Trader output = topology.addTrader(input.getOid(), input.getType(), traderState(input.getState()), basketSold);

        populateTraderSettings(input.getSettings(), output.getSettings());

        for (CommoditySoldTO commoditySold : input.getCommoditiesSoldList()) {
            populateCommoditySold(commoditySold, output.getCommoditySold(commoditySpecification(commoditySold.getSpecification())));
        }

        for (ShoppingListTO shoppingList : input.getShoppingListsList()) {
            addShoppingList(topology, output, shoppingList);
        }

        return output;
    }

    // Methods for converting ActionDTOs.

    /**
     * Converts an {@link ActionTO} to an {@link Action} given some additional context.
     *
     * @param input The {@link ActionTO} to convert.
     * @param economy The {@link Economy} containing the acted on objects.
     * @param participation A function mapping OIDs to {@link BuyerParticipation}s in <b>economy</b>.
     * @param trader A function mapping OIDs to {@link Trader}s in <b>economy</b>.
     * @return The resulting {@link Action}.
     */
    public static @NonNull Action action(@NonNull ActionTO input, @NonNull Economy economy,
            LongFunction<BuyerParticipation> participation, LongFunction<Trader> trader) {
        switch (input.getActionTypeCase()) {
            case MOVE:
                return new Move(economy, participation.apply(input.getMove().getShoppingListToMove()),
                    input.getMove().hasSource() ? trader.apply(input.getMove().getSource()) : null,
                    input.getMove().hasDestination() ? trader.apply(input.getMove().getDestination()) : null);
            case RECONFIGURE:
                return new Reconfigure(economy, participation.apply(input.getReconfigure().getShoppingListToReconfigure()));
            case ACTIVATE:
                return new Activate(trader.apply(input.getActivate().getTraderToActivate()),
                                    economy.getMarket(basket(input.getActivate().getTriggeringBasketList())));
            case DEACTIVATE:
                return new Deactivate(trader.apply(input.getDeactivate().getTraderToDeactivate()),
                                      economy.getMarket(basket(input.getDeactivate().getTriggeringBasketList())));
            case PROVISION_BY_DEMAND:
                return new ProvisionByDemand(economy, participation.apply(input.getProvisionByDemand().getModelBuyer()));
            case PROVISION_BY_SUPPLY:
                return new ProvisionBySupply(economy, trader.apply(input.getProvisionBySupply().getModelSeller()));
            case RESIZE:
                return new Resize(trader.apply(input.getResize().getSellingTrader()),
                    commoditySpecification(input.getResize().getSpecification()),
                    input.getResize().getOldCapacity(),input.getResize().getNewCapacity());
            case ACTIONTYPE_NOT_SET:
            default:
                throw new IllegalArgumentException("input = " + input);
        }
    }

} // end ProtobufToAnalysis class
