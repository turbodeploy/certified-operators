package com.vmturbo.platform.analysis.ede;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.RawMaterials;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs;
import com.vmturbo.platform.analysis.topology.Topology;

public class TestCommon {

    static final int COMMODITY_TYPE_CPU = 0;
    static final int COMMODITY_TYPE_MEM = 1;
    static final int COMMODITY_TYPE_STORAGE_1 = 2;
    static final int COMMODITY_TYPE_STORAGE_2 = 3;
    static final int COMMODITY_TYPE_SORAGE_AMOUNT = 4;
    static final int COMMODITY_TYPE_STORAGE_DSPM = 5;
    static final int COMMODITY_TYPE_VCPU = 6;
    static final int COMMODITY_TYPE_VMEM = 7;

    static final int TRADER_TYPE_VM = 0;
    static final int TRADER_TYPE_PM = 1;
    static final int TRADER_TYPE_STORAGE = 2;
    static final int TRADER_TYPE_APP = 3;

    static final long TRADER_OID_1 = 1L;
    static final long TRADER_OID_2 = 2L;
    static final long TRADER_OID_3 = 3L;
    static final long TRADER_OID_4 = 4L;
    static final long TRADER_OID_5 = 5L;
    static final long TRADER_OID_6 = 6L;

    static final CommoditySpecification CPU = new CommoditySpecification(COMMODITY_TYPE_CPU);
    static final CommoditySpecification MEM = new CommoditySpecification(COMMODITY_TYPE_MEM);
    static final Basket PMtoVM = new Basket(CPU, MEM,
                                new CommoditySpecification(COMMODITY_TYPE_STORAGE_1), // Datastore commodity
                                new CommoditySpecification(COMMODITY_TYPE_STORAGE_2));// Datastore commodity
    static final Basket STtoVM = new Basket(
                                new CommoditySpecification(COMMODITY_TYPE_SORAGE_AMOUNT), // Storage Amount
                                new CommoditySpecification(COMMODITY_TYPE_STORAGE_DSPM));// DSPM access commodity
    // VM Basket
    static final CommoditySpecification VCPU = new CommoditySpecification(COMMODITY_TYPE_VCPU);
    static final CommoditySpecification VMEM = new CommoditySpecification(COMMODITY_TYPE_VMEM);
    static final Basket VMtoApp = new Basket(VCPU, VMEM);

    private @NonNull Economy first;
    private @NonNull Economy cloned;
    private @NonNull Topology firstTopology;
    private @NonNull Trader vm;
    private @NonNull Trader pm1;
    private @NonNull Trader pm2;
    private @NonNull Trader app1;
    private @NonNull Trader st1;
    private @NonNull Trader st2;

    private @NonNull Map<@NonNull Long, @NonNull Trader> traderOids = new HashMap<>();

    public TestCommon() throws IllegalArgumentException {
        try {
            createEconomy();
        } catch (Exception e) {
            throw new IllegalArgumentException();
        }
    }

    public void createEconomy() throws NoSuchFieldException, IllegalAccessException {
        first = new Economy();
        Map<Integer, RawMaterials> rawMap = first.getModifiableRawCommodityMap();
        rawMap.put(COMMODITY_TYPE_VCPU,
                new RawMaterials(Lists.newArrayList(CommunicationDTOs.EndDiscoveredTopology.RawMaterial
                        .newBuilder().setCommodityType(COMMODITY_TYPE_CPU).build())));
        rawMap.put(COMMODITY_TYPE_VMEM,
                new RawMaterials(Lists.newArrayList(CommunicationDTOs.EndDiscoveredTopology.RawMaterial
                        .newBuilder().setCommodityType(COMMODITY_TYPE_MEM).build())));

        vm = first.addTrader(TRADER_TYPE_VM, TraderState.ACTIVE, VMtoApp, PMtoVM, STtoVM, STtoVM);
        pm1 = first.addTrader(TRADER_TYPE_PM, TraderState.ACTIVE, PMtoVM);
        pm2 = first.addTrader(TRADER_TYPE_PM, TraderState.ACTIVE, PMtoVM);
        st1 = first.addTrader(TRADER_TYPE_STORAGE, TraderState.ACTIVE, STtoVM);
        st2 = first.addTrader(TRADER_TYPE_STORAGE, TraderState.ACTIVE, STtoVM);
        app1 = first.addTrader(TRADER_TYPE_APP, TraderState.ACTIVE, new Basket(), VMtoApp);

        vm.setOid(TRADER_OID_1);
        pm1.setOid(TRADER_OID_2);
        pm2.setOid(TRADER_OID_3);
        st1.setOid(TRADER_OID_4);
        st2.setOid(TRADER_OID_5);
        app1.setOid(TRADER_OID_6);

        traderOids.put(TRADER_OID_1, vm);
        traderOids.put(TRADER_OID_2, pm1);
        traderOids.put(TRADER_OID_3, pm2);
        traderOids.put(TRADER_OID_4, st1);
        traderOids.put(TRADER_OID_5, st2);
        traderOids.put(TRADER_OID_6, app1);

        vm.setDebugInfoNeverUseInCode("VirtualMachine|" + TRADER_OID_1);
        pm1.setDebugInfoNeverUseInCode("PhysicalMachine|" + TRADER_OID_2);
        pm2.setDebugInfoNeverUseInCode("PhysicalMachine|" + TRADER_OID_3);
        st1.setDebugInfoNeverUseInCode("Storage|" + TRADER_OID_4);
        st2.setDebugInfoNeverUseInCode("Storage|" + TRADER_OID_5);
        app1.setDebugInfoNeverUseInCode("Application|" + TRADER_OID_6);

        first.populateMarketsWithSellersAndMergeConsumerCoverage();

        firstTopology = new Topology();
        first.setTopology(firstTopology);
        Field traderOidField = Topology.class.getDeclaredField("tradersByOid_");
        traderOidField.setAccessible(true);
        traderOidField.set(firstTopology, traderOids);
        Field unmodifiableTraderOidField = Topology.class
                                                   .getDeclaredField("unmodifiableTradersByOid_");
        unmodifiableTraderOidField.setAccessible(true);
        unmodifiableTraderOidField.set(firstTopology, traderOids);
    }

    public Economy getEconomy() {
        try {
            cloned = cloneEconomy(first);
            return cloned;
        } catch (Exception e) {
            throw new IllegalArgumentException();
        }
    }

    public static @NonNull Economy cloneEconomy(@NonNull Economy economy)
                    throws IOException, ClassNotFoundException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutputStream out = new ObjectOutputStream(bos)) {
             out.writeObject(economy);
             try (ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
                      ObjectInputStream in = new ObjectInputStream(bis)) {
                return (Economy)in.readObject();
            }
        }
    }

    /*
     * Should be called after {@link getEconomy()} is called which clones
     * the initially created {@link Economy}
     */
    public Trader getVm() {
        return cloned.getTraders().get(vm.getEconomyIndex());
    }

    /*
     * Should be called after {@link getEconomy()} is called which clones
     * the initially created {@link Economy}
     */
    public Trader[] getPms() {
        return new Trader[] {cloned.getTraders().get(pm1.getEconomyIndex()),
                             cloned.getTraders().get(pm2.getEconomyIndex())};
    }

    /*
     * Should be called after {@link getEconomy()} is called which clones
     * the initially created {@link Economy}
     */
    public Trader[] getStgs() {
        return new Trader[] {cloned.getTraders().get(st1.getEconomyIndex()),
                             cloned.getTraders().get(st2.getEconomyIndex())};
    }

    /*
     * Should be called after {@link getEconomy()} is called which clones
     * the initially created {@link Economy}
     */
    public Trader getApps() {
        return cloned.getTraders().get(app1.getEconomyIndex());
    }
}
