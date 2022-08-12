package com.vmturbo.platform.analysis.benchmarks;

import java.util.ArrayList;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ede.Ede;

/**
 * A benchmark to determine the performance of the 'market algorithms' on Economies of varying
 * size, with some typical supply chain.
 *
 * <p>
 *  May be useful to run with one or more of the following JVM options:
 *  -verbose:gc -verbose:class -verbose:jni
 *  -Xdiag -XX:+UnlockDiagnosticVMOptions -XX:+PrintCompilation -XX:+PrintInlining
 *  -Xmx16G -Xms16G -Xmn6G
 * </p>
 */
public class TypicalSupplyChain {
    // Constants
    private static final @NonNull CommoditySpecification CPU = new CommoditySpecification(0);
    private static final @NonNull CommoditySpecification MEM = new CommoditySpecification(1);
    private static final @NonNull CommoditySpecification STORE = new CommoditySpecification(2);
    private static final @NonNull CommoditySpecification CPU_PROVISIONED = new CommoditySpecification(3);
    private static final @NonNull CommoditySpecification MEM_PROVISIONED = new CommoditySpecification(4);
    private static final @NonNull CommoditySpecification STORE_PROVISIONED = new CommoditySpecification(5);
    private static final @NonNull CommoditySpecification VCPU = new CommoditySpecification(6);
    private static final @NonNull CommoditySpecification VMEM = new CommoditySpecification(7);
    private static final @NonNull CommoditySpecification VSTORE = new CommoditySpecification(8);

    private static final @NonNull Basket EMPTY = new Basket();

    private static final int CLUSTER_BASE = 100;
    private static final double UTILIZATION = 0.7;
    private static final double PROVISION_FACTOR = 10;
    private static final double CPU_CAPACITY = 10000;
    private static final double MEM_CAPACITY = 32000;
    private static final double STORE_CAPACITY = 1000000;

    // Methods

    public static void main(String[] args) {
        System.out.print("Populating Economies: ");
        long start = System.nanoTime();
        @NonNull Economy economy = generateEconomy(20, 500, 100, 50);
        System.out.printf("%,20dns\n", System.nanoTime()-start);
        System.out.printf("Number of traders: %,8d\n", economy.getTraders().size());
        System.out.printf("Number of markets: %,8d\n", economy.getMarkets().size());

        System.out.print("Producing Actions:    ");
        final @NonNull Ede ede = new Ede();
        start = System.nanoTime();
        List<Action> actions = ede.generateActions(economy, true, true, true, true,
                                                   "typicalsupplychain");
        System.out.printf("%,20dns\n", System.nanoTime()-start);
        System.out.printf("Number of actions: %,8d\n", actions.size());
        System.out.println("Done!");
    }

    /**
     * Creates and returns a new economy with the given size parameters.
     *
     * <p>
     *  All clusters are assumed to contain the same number of virtual machines, physical machines
     *  and storages. Each application is assumed to be tied to a single virtual machine. Virtual
     *  machines are uniformly distributed over physical machines and storages.
     * </p>
     *
     * @param nClusters The number of clusters in the generated economy.
     * @param nVMsPerCluster The number of virtual machines in each cluster.
     * @param nPMsPerCluster The number of physical machines in each cluster.
     * @param nSTsPerCluster The number of storages in each cluster.
     * @return The generated economy.
     */
    private static @NonNull Economy generateEconomy(final int nClusters, final int nVMsPerCluster,
                                                    final int nPMsPerCluster, final int nSTsPerCluster) {
        final @NonNull Economy economy = new Economy();

        for (int cluster = 0 ; cluster < nClusters ; ++cluster) {
            final @NonNull CommoditySpecification CLUSTER = new CommoditySpecification(CLUSTER_BASE+cluster);

            // generate physical machines
            final @NonNull List<@NonNull Trader> pms = new ArrayList<>();
            final @NonNull Basket VM_PM = new Basket(CPU,MEM,CPU_PROVISIONED,MEM_PROVISIONED,CLUSTER);
            for (int i = 0 ; i < nPMsPerCluster ; ++i) {
                final @NonNull Trader pm = economy.addTrader(0, TraderState.ACTIVE, VM_PM);

                pm.getCommoditySold(CLUSTER).setCapacity(1);
                pm.getCommoditySold(CPU).setCapacity(CPU_CAPACITY);
                pm.getCommoditySold(MEM).setCapacity(MEM_CAPACITY);
                pm.getCommoditySold(CPU_PROVISIONED).setCapacity(CPU_CAPACITY*PROVISION_FACTOR);
                pm.getCommoditySold(MEM_PROVISIONED).setCapacity(MEM_CAPACITY*PROVISION_FACTOR);

                pm.getCommoditySold(CPU).setQuantity(CPU_CAPACITY*UTILIZATION);
                pm.getCommoditySold(CPU).setPeakQuantity(CPU_CAPACITY*UTILIZATION);
                pm.getCommoditySold(MEM).setQuantity(MEM_CAPACITY*UTILIZATION);
                pm.getCommoditySold(MEM).setPeakQuantity(MEM_CAPACITY*UTILIZATION);
                pm.getCommoditySold(CPU_PROVISIONED).setQuantity(CPU_CAPACITY*UTILIZATION*PROVISION_FACTOR);
                pm.getCommoditySold(CPU_PROVISIONED).setPeakQuantity(CPU_CAPACITY*UTILIZATION*PROVISION_FACTOR);
                pm.getCommoditySold(MEM_PROVISIONED).setQuantity(MEM_CAPACITY*UTILIZATION*PROVISION_FACTOR);
                pm.getCommoditySold(MEM_PROVISIONED).setPeakQuantity(MEM_CAPACITY*UTILIZATION*PROVISION_FACTOR);

                pms.add(pm);
            }

            // generate storages
            final @NonNull List<@NonNull Trader> sts = new ArrayList<>();
            final @NonNull Basket VM_ST = new Basket(STORE,STORE_PROVISIONED,CLUSTER);
            for (int i = 0 ; i < nSTsPerCluster ; ++i) {
                final @NonNull Trader st = economy.addTrader(1, TraderState.ACTIVE, VM_ST);

                st.getCommoditySold(CLUSTER).setCapacity(1);
                st.getCommoditySold(STORE).setCapacity(STORE_CAPACITY);
                st.getCommoditySold(STORE_PROVISIONED).setCapacity(STORE_CAPACITY*PROVISION_FACTOR);

                st.getCommoditySold(STORE).setQuantity(STORE_CAPACITY*UTILIZATION);
                st.getCommoditySold(STORE).setPeakQuantity(STORE_CAPACITY*UTILIZATION);
                st.getCommoditySold(STORE_PROVISIONED).setQuantity(STORE_CAPACITY*UTILIZATION*PROVISION_FACTOR);
                st.getCommoditySold(STORE_PROVISIONED).setPeakQuantity(STORE_CAPACITY*UTILIZATION*PROVISION_FACTOR);

                sts.add(st);
            }

            // generate virtual machines and applications
            for (int i = 0 ; i < nVMsPerCluster ; ++i) {
                final @NonNull Basket APP_VM = new Basket(VCPU, VMEM, VSTORE);
                final @NonNull Trader vm = economy.addTrader(2, TraderState.ACTIVE, APP_VM);

                // Fill-in quantities and capacities sold by the virtual machine
                final double PMsPerVM = nPMsPerCluster / nVMsPerCluster; // inverse of VMs per PM
                vm.getCommoditySold(VCPU).setCapacity(PMsPerVM*UTILIZATION*CPU_CAPACITY);
                vm.getCommoditySold(VMEM).setCapacity(PMsPerVM*UTILIZATION*MEM_CAPACITY);
                final double STsPerVM = nSTsPerCluster / nVMsPerCluster; // inverse of VMs per ST
                vm.getCommoditySold(VSTORE).setCapacity(STsPerVM*UTILIZATION*STORE_CAPACITY);

                vm.getCommoditySold(VCPU).setQuantity(PMsPerVM*UTILIZATION*UTILIZATION*CPU_CAPACITY);
                vm.getCommoditySold(VCPU).setPeakQuantity(PMsPerVM*UTILIZATION*UTILIZATION*CPU_CAPACITY);
                vm.getCommoditySold(VMEM).setQuantity(PMsPerVM*UTILIZATION*UTILIZATION*MEM_CAPACITY);
                vm.getCommoditySold(VMEM).setPeakQuantity(PMsPerVM*UTILIZATION*UTILIZATION*MEM_CAPACITY);
                vm.getCommoditySold(VSTORE).setQuantity(STsPerVM*UTILIZATION*UTILIZATION*STORE_CAPACITY);
                vm.getCommoditySold(VSTORE).setPeakQuantity(STsPerVM*UTILIZATION*UTILIZATION*STORE_CAPACITY);

                // Start buying from a physical machine and fill-in quantities bought
                final @NonNull ShoppingList pmShoppingList = economy.addBasketBought(vm, VM_PM);

                pmShoppingList.setMovable(true);
                pmShoppingList.move(pms.get(i % nPMsPerCluster));

                pmShoppingList.setQuantity(VM_PM.indexOf(CPU), PMsPerVM*UTILIZATION*CPU_CAPACITY);
                pmShoppingList.setPeakQuantity(VM_PM.indexOf(CPU), PMsPerVM*UTILIZATION*CPU_CAPACITY);
                pmShoppingList.setQuantity(VM_PM.indexOf(CPU_PROVISIONED), PMsPerVM*UTILIZATION*CPU_CAPACITY);
                pmShoppingList.setPeakQuantity(VM_PM.indexOf(CPU_PROVISIONED), PMsPerVM*UTILIZATION*CPU_CAPACITY);
                pmShoppingList.setQuantity(VM_PM.indexOf(MEM), PMsPerVM*UTILIZATION*MEM_CAPACITY);
                pmShoppingList.setPeakQuantity(VM_PM.indexOf(MEM), PMsPerVM*UTILIZATION*MEM_CAPACITY);
                pmShoppingList.setQuantity(VM_PM.indexOf(MEM_PROVISIONED), PMsPerVM*UTILIZATION*MEM_CAPACITY);
                pmShoppingList.setPeakQuantity(VM_PM.indexOf(MEM_PROVISIONED), PMsPerVM*UTILIZATION*MEM_CAPACITY);

                // Start buying from a storage and fill-in quantities bought
                final @NonNull ShoppingList stShoppingList = economy.addBasketBought(vm, VM_ST);

                stShoppingList.setMovable(true);
                stShoppingList.move(sts.get(i % nSTsPerCluster));

                stShoppingList.setQuantity(VM_ST.indexOf(STORE), STsPerVM*UTILIZATION*STORE_CAPACITY);
                stShoppingList.setPeakQuantity(VM_ST.indexOf(STORE), STsPerVM*UTILIZATION*STORE_CAPACITY);
                stShoppingList.setQuantity(VM_ST.indexOf(STORE_PROVISIONED), STsPerVM*UTILIZATION*STORE_CAPACITY);
                stShoppingList.setPeakQuantity(VM_ST.indexOf(STORE_PROVISIONED), STsPerVM*UTILIZATION*STORE_CAPACITY);

                // Add an application and tie it to the virtual machine.
                final @NonNull Trader app = economy.addTrader(3, TraderState.ACTIVE, EMPTY);
                final @NonNull ShoppingList vmShoppingList = economy.addBasketBought(app, APP_VM);
                vmShoppingList.move(vm);

                // Fill-in quantities bought by the application
                vmShoppingList.setQuantity(APP_VM.indexOf(VCPU), PMsPerVM*UTILIZATION*UTILIZATION*CPU_CAPACITY);
                vmShoppingList.setPeakQuantity(APP_VM.indexOf(VCPU), PMsPerVM*UTILIZATION*UTILIZATION*CPU_CAPACITY);
                vmShoppingList.setQuantity(APP_VM.indexOf(VMEM), PMsPerVM*UTILIZATION*UTILIZATION*MEM_CAPACITY);
                vmShoppingList.setPeakQuantity(APP_VM.indexOf(VMEM), PMsPerVM*UTILIZATION*UTILIZATION*MEM_CAPACITY);
                vmShoppingList.setQuantity(APP_VM.indexOf(VSTORE), STsPerVM*UTILIZATION*UTILIZATION*STORE_CAPACITY);
                vmShoppingList.setPeakQuantity(APP_VM.indexOf(VSTORE), STsPerVM*UTILIZATION*UTILIZATION*STORE_CAPACITY);
            }
        }

        return economy;
    }

} // end TypicalSupplyChain class
