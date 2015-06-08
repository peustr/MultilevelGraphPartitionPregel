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
            "1 2 5", "1 3 5", "1 6 8",
            "2 1 5", "2 5 1",
            "3 1 5", "3 7 1",
            "4 5 4", "4 12 2",
            "5 2 1", "5 4 4", "5 6 2", "5 9 3",
            "6 1 8", "6 5 2", "6 7 2", "6 11 4",
            "7 3 1", "7 6 2", "7 8 4", "7 10 3",
            "8 7 4", "8 14 2",
            "9 5 3", "9 11 5",
            "10 7 3", "10 11 5",
            "11 6 4", "11 9 5", "11 10 5", "11 12 1", "11 13 1", "11 14 1",
            "12 4 2", "12 11 1", "12 15 1", "12 16 2",
            "13 11 1", "13 17 1", "13 18 2",
            "14 8 2", "14 11 1", "14 19 1", "14 20 2",
            "15 12 1", "15 16 8",
            "16 12 2", "16 15 8", "16 17 4",
            "17 13 1", "17 16 4", "17 18 8",
            "18 13 2", "18 17 8", "18 19 4",
            "19 14 1", "19 18 4", "19 20 8",
            "20 14 2", "20 19 8"
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
