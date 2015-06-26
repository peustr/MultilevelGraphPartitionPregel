package gr.hua.dit.mgp;

import static gr.hua.dit.mgp.GraphPartitionConstants.AggregatorConstants.ACTIVE_VERTICES_AGGREGATOR;
import static gr.hua.dit.mgp.GraphPartitionConstants.AggregatorConstants.COARSENING_AGGREGATOR;
import static gr.hua.dit.mgp.GraphPartitionConstants.AggregatorConstants.COMPUTATION_PHASE_AGGREGATOR;
import static gr.hua.dit.mgp.GraphPartitionConstants.AggregatorConstants.DISTRIBUTING_EDGES_AGGREGATOR;
import static gr.hua.dit.mgp.GraphPartitionConstants.AggregatorConstants.FOLDING_VERTICES_AGGREGATOR;
import static gr.hua.dit.mgp.GraphPartitionConstants.AggregatorConstants.INPUT_GRAPH_AGGREGATOR;
import static gr.hua.dit.mgp.GraphPartitionConstants.AggregatorConstants.MAXIMUM_WEIGHTED_MATCHING_AGGREGATOR;
import static gr.hua.dit.mgp.GraphPartitionConstants.AggregatorConstants.MIGRATION_CANDIDATE_AGGREGATOR;
import static gr.hua.dit.mgp.GraphPartitionConstants.AggregatorConstants.OUTPUT_GRAPH_AGGREGATOR;
import static gr.hua.dit.mgp.GraphPartitionConstants.AggregatorConstants.PARTITIONING_AGGREGATOR;
import static gr.hua.dit.mgp.GraphPartitionConstants.AggregatorConstants.REFINING_LOCALLY_AGGREGATOR;
import static gr.hua.dit.mgp.GraphPartitionConstants.AggregatorConstants.SPLITTING_VERTICES_AGGREGATOR;
import static gr.hua.dit.mgp.GraphPartitionConstants.ComputationConstants.DISTRIBUTING_EDGES;
import static gr.hua.dit.mgp.GraphPartitionConstants.ComputationConstants.FOLDING_VERTICES;
import static gr.hua.dit.mgp.GraphPartitionConstants.ComputationConstants.MAXIMUM_WEIGHTED_MATCHING;
import static gr.hua.dit.mgp.GraphPartitionConstants.ComputationConstants.PARTITIONING;
import static gr.hua.dit.mgp.GraphPartitionConstants.ComputationConstants.REFINING_LOCALLY;
import static gr.hua.dit.mgp.GraphPartitionConstants.ComputationConstants.SPLITTING_VERTICES;
import gr.hua.dit.mgp.aggregators.LongToDoubleMapAggregator;
import gr.hua.dit.mgp.aggregators.LongToLongMapAggregator;
import gr.hua.dit.mgp.io.LongToDoubleMapWritable;
import gr.hua.dit.mgp.io.LongToLongMapWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;
import org.apache.giraph.aggregators.BooleanOverwriteAggregator;
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author peustr
 */
public class GraphPartitionMasterCompute extends DefaultMasterCompute {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphPartitionMasterCompute.class);

    private long k;
    private int computationPhase;
    private boolean coarsening;

    public GraphPartitionMasterCompute() {
        computationPhase = MAXIMUM_WEIGHTED_MATCHING;
        coarsening = true;
    }

    @Override
    public void compute() {

        k = Long.valueOf(getConf().get("partitions"));
        if (getSuperstep() == 0) {
            LOGGER.debug("Starting algorithm for " + k + " partitions");
        } else {
            long active = getActiveVertices();
            LOGGER.debug("## Superstep: " + getSuperstep());
            if (coarsening) {
                // We use 2*k because that's the least number of vertices we can have
                // so that if they all fold, they will not become less than k
                if (active < 2 * k && coarsening) {
                    LOGGER.debug(active + " active vertices left. Partitioning...");
                    coarsening = false;
                    computationPhase = PARTITIONING;
                } // If graph not small enough, examine computation phase
                else if (computationPhase == MAXIMUM_WEIGHTED_MATCHING) {
                    long matching = getMatchingActiveVertices();
                    LOGGER.debug(matching + " vertices active on matching");
                    if (matching == 0) {
                        LOGGER.debug("Changing to folding vertices");
                        computationPhase = FOLDING_VERTICES;
                    }
                } else if (computationPhase == FOLDING_VERTICES) {
                    long folding = getFoldingActiveVertices();
                    LOGGER.debug(folding + " vertices active on folding");
                    if (folding == 0) {
                        LOGGER.debug("Changing to distributing edges");
                        computationPhase = DISTRIBUTING_EDGES;
                    }
                } else if (computationPhase == DISTRIBUTING_EDGES) {
                    long distributing = getDistributingActiveVertices();
                    LOGGER.debug(distributing + " vertices active on distributing edges");
                    if (distributing == 0) {
                        LOGGER.debug("Changing to maximum weighted matching");
                        computationPhase = MAXIMUM_WEIGHTED_MATCHING;
                    }
                }
            } else {
                if (computationPhase == PARTITIONING) {
                    long partitioning = getPartitioningActiveVertices();
                    LOGGER.debug(partitioning + " vertices active on partitioning");
                    LongToDoubleMapWritable inGraph = getAggregatedValue(INPUT_GRAPH_AGGREGATOR);
                    if (!inGraph.getData().isEmpty()) {
                        generatePartitions(inGraph, k);
                    }
                    if (partitioning == 0) {
                        LOGGER.debug("Changing to splitting vertices");
                        computationPhase = SPLITTING_VERTICES;
                    }
                } else if (computationPhase == SPLITTING_VERTICES) {
                    long splitting = getSplittingActiveVertices();
                    LOGGER.debug(splitting + " vertices active on splitting");
                    if (splitting == 0) {
                        LOGGER.debug("Changing to local refinement");
                        computationPhase = REFINING_LOCALLY;
                    }
                } else if (computationPhase == REFINING_LOCALLY) {
                    long refining = getRefiningActiveVertices();
                    LOGGER.debug(refining + " vertices active on refining");
                    if (refining == 0) {
                        LOGGER.debug("Changing to splitting vertices");
                        computationPhase = SPLITTING_VERTICES;
                    }
                }
            }
        }
        setAggregatedValue(COMPUTATION_PHASE_AGGREGATOR, new IntWritable(computationPhase));
        setAggregatedValue(COARSENING_AGGREGATOR, new BooleanWritable(coarsening));
    }

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        registerPersistentAggregator(COMPUTATION_PHASE_AGGREGATOR, IntOverwriteAggregator.class);
        registerPersistentAggregator(COARSENING_AGGREGATOR, BooleanOverwriteAggregator.class);
        registerAggregator(ACTIVE_VERTICES_AGGREGATOR, LongSumAggregator.class);
        registerAggregator(MAXIMUM_WEIGHTED_MATCHING_AGGREGATOR, LongSumAggregator.class);
        registerAggregator(FOLDING_VERTICES_AGGREGATOR, LongSumAggregator.class);
        registerAggregator(DISTRIBUTING_EDGES_AGGREGATOR, LongSumAggregator.class);
        registerAggregator(PARTITIONING_AGGREGATOR, LongSumAggregator.class);
        registerAggregator(SPLITTING_VERTICES_AGGREGATOR, LongSumAggregator.class);
        registerAggregator(REFINING_LOCALLY_AGGREGATOR, LongSumAggregator.class);
        registerPersistentAggregator(INPUT_GRAPH_AGGREGATOR, LongToDoubleMapAggregator.class);
        registerPersistentAggregator(OUTPUT_GRAPH_AGGREGATOR, LongToLongMapAggregator.class);
        k = Long.valueOf(getConf().get("partitions"));
        for (long i = 1; i <= k; i++) {
            registerAggregator(MIGRATION_CANDIDATE_AGGREGATOR + i, LongSumAggregator.class);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(k);
        out.writeInt(computationPhase);
        out.writeBoolean(coarsening);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        k = in.readLong();
        computationPhase = in.readInt();
        coarsening = in.readBoolean();
    }

    private long getActiveVertices() {
        return ((LongWritable) getAggregatedValue(ACTIVE_VERTICES_AGGREGATOR)).get();
    }

    private long getMatchingActiveVertices() {
        return ((LongWritable) getAggregatedValue(MAXIMUM_WEIGHTED_MATCHING_AGGREGATOR)).get();
    }

    private long getFoldingActiveVertices() {
        return ((LongWritable) getAggregatedValue(FOLDING_VERTICES_AGGREGATOR)).get();
    }

    private long getDistributingActiveVertices() {
        return ((LongWritable) getAggregatedValue(DISTRIBUTING_EDGES_AGGREGATOR)).get();
    }

    private long getPartitioningActiveVertices() {
        return ((LongWritable) getAggregatedValue(PARTITIONING_AGGREGATOR)).get();
    }

    private long getSplittingActiveVertices() {
        return ((LongWritable) getAggregatedValue(SPLITTING_VERTICES_AGGREGATOR)).get();
    }

    private long getRefiningActiveVertices() {
        return ((LongWritable) getAggregatedValue(REFINING_LOCALLY_AGGREGATOR)).get();
    }

    private void generatePartitions(LongToDoubleMapWritable inGraph, long k) {
        LongToLongMapWritable outGraph = new LongToLongMapWritable();
        long count = 1l;
        for (Entry<Long, Double> vertex : inGraph.getData().entrySet()) {
            if (count <= k) {
                outGraph.getData().put(vertex.getKey(), count);
                count++;
            }
        }
        setAggregatedValue(OUTPUT_GRAPH_AGGREGATOR, outGraph);
    }

}
