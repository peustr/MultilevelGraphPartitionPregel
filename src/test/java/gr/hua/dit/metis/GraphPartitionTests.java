package gr.hua.dit.metis;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author peustr
 */
public class GraphPartitionTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphPartitionTests.class);

    @Test
    public void metisOnTinyUnweightedGraph() throws Exception {

        String[] tinyGraph = {
            "1 2", "1 3",
            "2 1", "2 4",
            "3 1", "3 4",
            "4 2", "4 3", "4 5", "4 6",
            "5 4", "5 7",
            "6 4", "6 7",
            "7 5", "7 6"
        };

        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(GraphPartitionComputation.class);
        conf.setMasterComputeClass(GraphPartitionMasterCompute.class);
        conf.setEdgeInputFormatClass(GraphPartitionEdgeInputFormat.class);
        conf.setVertexInputFormatClass(GraphPartitionVertexValueInputFormat.class);
        conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
        conf.set("partitions", "3");

        Iterable<String> results = InternalVertexRunner.run(conf, tinyGraph, tinyGraph);
        for (String result : results) {
            LOGGER.debug(result);
        }

    }

    @Test
    public void metisOnTinyWeightedGraph() throws Exception {

        String[] tinyGraph = {
            "1 2 5", "1 3 3",
            "2 1 5", "2 4 1",
            "3 1 3", "3 4 1",
            "4 2 1", "4 3 1", "4 5 2", "4 6 6",
            "5 4 2", "5 7 2",
            "6 4 6", "6 7 5",
            "7 5 2", "7 6 5"
        };

        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(GraphPartitionComputation.class);
        conf.setMasterComputeClass(GraphPartitionMasterCompute.class);
        conf.setEdgeInputFormatClass(GraphPartitionEdgeInputFormat.class);
        conf.setVertexInputFormatClass(GraphPartitionVertexValueInputFormat.class);
        conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
        conf.set("partitions", "3");

        Iterable<String> results = InternalVertexRunner.run(conf, tinyGraph, tinyGraph);
        for (String result : results) {
            LOGGER.debug(result);
        }

    }
}
