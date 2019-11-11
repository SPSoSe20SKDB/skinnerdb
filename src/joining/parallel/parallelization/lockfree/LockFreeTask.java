package joining.parallel.parallelization.lockfree;

import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import joining.parallel.join.DPJoin;
import joining.parallel.parallelization.EndPlan;
import joining.parallel.uct.DPNode;
import preprocessing.Context;
import query.QueryInfo;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class LockFreeTask implements Callable<LockFreeResult>{

    private final QueryInfo query;
    private final Context context;
    private DPNode root;
    private final EndPlan endPlan;
    private final AtomicBoolean finish;
    private final ReentrantLock lock;
    private final DPJoin joinOp;


    public LockFreeTask(QueryInfo query, Context context, DPNode root, EndPlan endPlan,
                        AtomicBoolean finish, ReentrantLock lock, DPJoin dpJoin) {
        this.query = query;
        this.context = context;
        this.root = root;
        this.endPlan = endPlan;
        this.finish = finish;
        this.lock = lock;
        this.joinOp = dpJoin;
    }

    @Override
    public LockFreeResult call() throws Exception {
        long timer1 = System.currentTimeMillis();
        // Initialize counters and variables
        int tid = joinOp.tid;
        int[] joinOrder = new int[query.nrJoined];
        long roundCtr = 0;

        // Get default action selection policy
        SelectionPolicy policy = SelectionPolicy.UCB1;
        // Initialize counter until scale down
        long nextScaleDown = 1;
        // Initialize counter until memory loss
        long nextForget = 1;
        // Initialize plot counter
        int plotCtr = 0;
        // Iterate until join result was generated
        double accReward = 0;

        while (true) {
            long start = System.currentTimeMillis();
            ++roundCtr;
            double reward;
            int finalTable = endPlan.getSplitTable();
            if (finalTable != -1) {
                joinOrder = endPlan.getJoinOrder();
//                joinOp.budget = Integer.MAX_VALUE;
                reward = joinOp.execute(joinOrder, finalTable, (int) roundCtr);
            }
            else {
                reward = root.sample(roundCtr, joinOrder, joinOp, policy);
//                System.arraycopy(new int[]{3, 5, 4, 2, 7, 1, 6, 0}, 0, joinOrder, 0, query.nrJoined);
//                reward = joinOp.execute(joinOrder, 2, (int) roundCtr);
            }
            // Count reward except for final sample
            if (!joinOp.isFinished()) {
                accReward += reward;
            }
            // broadcasting the finished plan.
            else {
                int splitTable = joinOp.lastTable;
                if (!finish.get()) {
                    lock.lock();
                    if (!finish.get()) {
                        System.out.println(tid + " shared: " + Arrays.toString(joinOrder) + " splitting " + splitTable);
                        endPlan.setJoinOrder(joinOrder);
                        endPlan.setSplitTable(splitTable);
                        finish.set(true);
                    }
                    lock.unlock();
                }
                if (splitTable == endPlan.getSplitTable()) {
                    break;
                } else {
                    System.out.println(tid + ": bad restart");
                }
            }
            long end = System.currentTimeMillis();
//            joinOp.writeLog("Episode Time: " + (end - start) + "\tReward: " + reward);

//            switch (JoinConfig.EXPLORATION_POLICY) {
//                case REWARD_AVERAGE:
//                    double avgReward = accReward/roundCtr;
//                    JoinConfig.EXPLORATION_WEIGHT = avgReward;
//                    log("Avg. reward: " + avgReward);
//                    break;
//                case SCALE_DOWN:
//                    if (roundCtr == nextScaleDown) {
//                        JoinConfig.EXPLORATION_WEIGHT /= 10.0;
//                        nextScaleDown *= 10;
//                    }
//                    break;
//                case STATIC:
//                case ADAPT_TO_SAMPLE:
//                    // Nothing to do
//                    break;
//            }
            // Consider memory loss
//            if (JoinConfig.FORGET && roundCtr==nextForget && ParallelConfig.EXE_THREADS == 1) {
//                root = new DPNode(roundCtr, query, true, 1);
//                nextForget *= 10;
//            }
            // Generate logging entries if activated
//            log("Selected join order " + Arrays.toString(joinOrder));
//            log("Obtained reward:\t" + reward);
//            log("Table offsets:\t" + Arrays.toString(joinOp.tracker.tableOffset));
//            log("Table cardinalities:\t" + Arrays.toString(joinOp.cardinalities));
        }

        // Update statistics
//        JoinStats.nrSamples = roundCtr;
//        JoinStats.avgReward = accReward/roundCtr;
//        JoinStats.maxReward = maxReward;
//        JoinStats.totalWork = 0;
//        for (int tableCtr=0; tableCtr<query.nrJoined; ++tableCtr) {
//            if (tableCtr == joinOrder[0]) {
//                JoinStats.totalWork += 1;
//            } else {
//                JoinStats.totalWork += Math.max(
//                        joinOp.tracker.tableOffset[tableCtr],0)/
//                        (double)joinOp.cardinalities[tableCtr];
//            }
//        }
//        // Output final stats if join logging enabled
//        if (LoggingConfig.MAX_JOIN_LOGS > 0) {
//            System.out.println("Exploration weight:\t" +
//                    JoinConfig.EXPLORATION_WEIGHT);
//            System.out.println("Nr. rounds:\t" + roundCtr);
//            System.out.println("Table offsets:\t" +
//                    Arrays.toString(joinOp.tracker.tableOffset));
//            System.out.println("Table cards.:\t" +
//                    Arrays.toString(joinOp.cardinalities));
//        }
        // Materialize result table
        long timer2 = System.currentTimeMillis();
        joinOp.roundCtr = roundCtr;
        System.out.println("Thread " + tid + " " + (timer2 - timer1) + "\t Round: " + roundCtr);
        Collection<ResultTuple> tuples = joinOp.result.getTuples();
        return new LockFreeResult(tuples, joinOp.logs, tid);
    }

    /**
     * Print out log entry if the maximal number of log
     * entries has not been reached yet.
     *
     * @param logEntry	log entry to print
     */
    static void log(String logEntry) {
        System.out.println(logEntry);
    }
}