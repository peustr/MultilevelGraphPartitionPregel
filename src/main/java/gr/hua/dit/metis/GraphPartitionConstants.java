package gr.hua.dit.metis;

/**
 *
 * @author peustr
 */
public class GraphPartitionConstants {

    public static class ComputationConstants {

        public static final int MAXIMUM_WEIGHTED_MATCHING = -1;
        public static final int FOLDING_VERTICES = -2;
        public static final int DISTRIBUTING_EDGES = -3;
        public static final int PARTITIONING = -4;
        public static final int SPLITTING_VERTICES = -5;
        public static final int REFINING_LOCALLY = -6;

    }

    public static class MessageConstants {

        public static final int MATCH_MESSAGE = -1;
        public static final int HIDE_MESSAGE = -2;
        public static final int CHILD_MESSAGE = -3;
        public static final int WAKEUP_MESSAGE = -4;
        public static final int PARTITION_MESSAGE = -5;

    }

    public static class AggregatorConstants {

        public static final String COMPUTATION_PHASE_AGGREGATOR = "COMPUTATION_PHASE_AGGREGATOR";
        public static final String COARSENING_AGGREGATOR = "COARSENING_AGGREGATOR";
        public static final String ACTIVE_VERTICES_AGGREGATOR = "ACTIVE_VERTICES_AGGREGATOR";
        public static final String MAXIMUM_WEIGHTED_MATCHING_AGGREGATOR = "MAXIMUM_WEIGHTED_MATCHING_AGGREGATOR";
        public static final String FOLDING_VERTICES_AGGREGATOR = "FOLDING_VERTICES_AGGREGATOR";
        public static final String DISTRIBUTING_EDGES_AGGREGATOR = "DISTRIBUTING_EDGES_AGGREGATOR";
        public static final String PARTITIONING_AGGREGATOR = "PARTITIONING_AGGREGATOR";
        public static final String SPLITTING_VERTICES_AGGREGATOR = "SPLITTING_VERTICES_AGGREGATOR";
        public static final String REFINING_LOCALLY_AGGREGATOR = "REFINING_LOCALLY_AGGREGATOR";
        public static final String INPUT_GRAPH_AGGREGATOR = "INPUT_GRAPH_AGGREGATOR";
        public static final String OUTPUT_GRAPH_AGGREGATOR = "OUTPUT_GRAPH_AGGREGATOR";

    }

}
