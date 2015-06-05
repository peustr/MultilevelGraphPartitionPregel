package gr.hua.dit.metis;

import static gr.hua.dit.metis.GraphPartitionConstants.AggregatorConstants.ACTIVE_VERTICES_AGGREGATOR;
import static gr.hua.dit.metis.GraphPartitionConstants.AggregatorConstants.COARSENING_AGGREGATOR;
import static gr.hua.dit.metis.GraphPartitionConstants.AggregatorConstants.COMPUTATION_PHASE_AGGREGATOR;
import static gr.hua.dit.metis.GraphPartitionConstants.AggregatorConstants.DISTRIBUTING_EDGES_AGGREGATOR;
import static gr.hua.dit.metis.GraphPartitionConstants.AggregatorConstants.FOLDING_VERTICES_AGGREGATOR;
import static gr.hua.dit.metis.GraphPartitionConstants.AggregatorConstants.INPUT_GRAPH_AGGREGATOR;
import static gr.hua.dit.metis.GraphPartitionConstants.AggregatorConstants.MAXIMUM_WEIGHTED_MATCHING_AGGREGATOR;
import static gr.hua.dit.metis.GraphPartitionConstants.AggregatorConstants.OUTPUT_GRAPH_AGGREGATOR;
import static gr.hua.dit.metis.GraphPartitionConstants.AggregatorConstants.PARTITIONING_AGGREGATOR;
import static gr.hua.dit.metis.GraphPartitionConstants.AggregatorConstants.REFINING_LOCALLY_AGGREGATOR;
import static gr.hua.dit.metis.GraphPartitionConstants.AggregatorConstants.SPLITTING_VERTICES_AGGREGATOR;
import static gr.hua.dit.metis.GraphPartitionConstants.ComputationConstants.DISTRIBUTING_EDGES;
import static gr.hua.dit.metis.GraphPartitionConstants.ComputationConstants.FOLDING_VERTICES;
import static gr.hua.dit.metis.GraphPartitionConstants.ComputationConstants.MAXIMUM_WEIGHTED_MATCHING;
import static gr.hua.dit.metis.GraphPartitionConstants.ComputationConstants.PARTITIONING;
import static gr.hua.dit.metis.GraphPartitionConstants.ComputationConstants.REFINING_LOCALLY;
import static gr.hua.dit.metis.GraphPartitionConstants.ComputationConstants.SPLITTING_VERTICES;
import static gr.hua.dit.metis.GraphPartitionConstants.MessageConstants.CHILD_MESSAGE;
import static gr.hua.dit.metis.GraphPartitionConstants.MessageConstants.HIDE_MESSAGE;
import static gr.hua.dit.metis.GraphPartitionConstants.MessageConstants.MATCH_MESSAGE;
import static gr.hua.dit.metis.GraphPartitionConstants.MessageConstants.PARTITION_MESSAGE;
import static gr.hua.dit.metis.GraphPartitionConstants.MessageConstants.WAKEUP_MESSAGE;
import gr.hua.dit.metis.io.LongToDoubleMapWritable;
import gr.hua.dit.metis.io.LongToLongMapWritable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author peustr
 */
public class GraphPartitionComputation extends BasicComputation<LongWritable, GraphPartitionVertexData, DoubleWritable, GraphPartitionMessageData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphPartitionComputation.class);

    @Override
    public void compute(Vertex<LongWritable, GraphPartitionVertexData, DoubleWritable> vertex, Iterable<GraphPartitionMessageData> messages) throws IOException {

        aggregate(ACTIVE_VERTICES_AGGREGATOR, new LongWritable(1));
        if (vertex.getValue().getComputationPhase() == MAXIMUM_WEIGHTED_MATCHING && !coarsening()) {
            voteToPartition(vertex);
        }
        if (vertexPrepared(vertex, MAXIMUM_WEIGHTED_MATCHING)) {
            aggregate(MAXIMUM_WEIGHTED_MATCHING_AGGREGATOR, new LongWritable(1));
            if (!hasMatched(vertex)) {
                // Pick maximum weighted neighbor 'u'
                long pickedVertex = Long.MAX_VALUE;
                double pickedEdge = -Double.MAX_VALUE;
                for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
                    if (!vertex.getValue().isHidden(edge.getTargetVertexId().get())) {
                        // Solve ties by picking the smaller Id
                        if (edge.getValue().get() == pickedEdge) {
                            pickedVertex = edge.getTargetVertexId().get() < pickedVertex ? edge.getTargetVertexId().get() : pickedVertex;
                        } else if (edge.getValue().get() > pickedEdge) {
                            pickedVertex = edge.getTargetVertexId().get();
                            pickedEdge = edge.getValue().get();
                        }
                    }
                }
                // If I found maximum weighted neighbor
                if (pickedVertex != Long.MAX_VALUE) {
                    LOGGER.debug(vertex.getId() + " wants " + pickedVertex + " to match it");
                    sendMessage(new LongWritable(pickedVertex), new GraphPartitionMessageData(MATCH_MESSAGE, vertex.getId().get(), vertex.getValue().getWeight()));
                } // Else, I am alone
                else {
                    voteToFold(vertex);
                }
                for (GraphPartitionMessageData message : messages) {
                    if (message.getMessageType() == MATCH_MESSAGE && message.getSenderId() == pickedVertex) {
                        LOGGER.debug(vertex.getId() + " picked " + pickedVertex);
                        vertex.getValue().setPickedVertex(pickedVertex);
                        vertex.getValue().setPickedVertexWeight(message.getDoubleData());
                        vertex.getValue().setPickedEdgeWeight(pickedEdge);
                        for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
                            if (!vertex.getValue().isHidden(edge.getTargetVertexId().get()) && edge.getTargetVertexId().get() != pickedVertex) {
                                LOGGER.debug(vertex.getId() + " wants " + edge.getTargetVertexId() + " to hide it");
                                sendMessage(edge.getTargetVertexId(), new GraphPartitionMessageData(HIDE_MESSAGE, vertex.getId().get()));
                            }
                        }
                    }
                }
            } // If matched no reason to run
            else {
                voteToFold(vertex);
            }
            // Handle messages
            for (GraphPartitionMessageData message : messages) {
                if (message.getMessageType() == HIDE_MESSAGE) {
                    // "Hide" neighbor
                    vertex.getValue().hide(message.getSenderId());
                }
            }
        } else if (vertexPrepared(vertex, FOLDING_VERTICES)) {
            aggregate(FOLDING_VERTICES_AGGREGATOR, new LongWritable(1));
            if (!hasFolded(vertex)) {
                // If alone
                if (!hasMatched(vertex)) {
                    // Create new vertex
                    Map<Long, Double> creators = new HashMap<>();
                    creators.put(vertex.getId().get(), vertex.getValue().getWeight());
                    long uuid = generateUniqueId();
                    double totalWeight = vertex.getValue().getWeight();
                    addVertexRequest(new LongWritable(uuid), new GraphPartitionVertexData(DISTRIBUTING_EDGES, totalWeight, creators));
                    LOGGER.debug(vertex.getId() + " created " + uuid);
                    vertex.getValue().setChildVertex(uuid);
                    // Send message to all
                    sendMessageToAllEdges(vertex, new GraphPartitionMessageData(CHILD_MESSAGE, vertex.getId().get(), uuid));
                } // The vertex with the larger id is responsible for creating the new one
                else if (vertex.getId().get() > vertex.getValue().getPickedVertex()) {
                    // Create new vertex
                    Map<Long, Double> creators = new HashMap<>();
                    creators.put(vertex.getId().get(), vertex.getValue().getWeight());
                    creators.put(vertex.getValue().getPickedVertex(), vertex.getValue().getPickedVertexWeight());
                    long uuid = generateUniqueId();
                    double totalWeight = vertex.getValue().getWeight() + vertex.getValue().getPickedVertexWeight();
                    addVertexRequest(new LongWritable(uuid), new GraphPartitionVertexData(DISTRIBUTING_EDGES, totalWeight, creators));
                    LOGGER.debug(vertex.getId() + " created " + uuid);
                    vertex.getValue().setChildVertex(uuid);
                    // Send message to all
                    sendMessageToAllEdges(vertex, new GraphPartitionMessageData(CHILD_MESSAGE, vertex.getId().get(), uuid));
                }
            } else {
                voteToDistribute(vertex);
            }
            // Handle messages
            for (GraphPartitionMessageData message : messages) {
                if (message.getMessageType() == CHILD_MESSAGE) {
                    // If matched vertex add to children
                    if (message.getSenderId() == vertex.getValue().getPickedVertex()) {
                        vertex.getValue().setChildVertex(message.getLongData());
                    } // If other, add to next level's neighbors
                    else {
                        // We need the edge to the nth level neighbor
                        vertex.getValue().addnLevelNeighbor(message.getLongData(), vertex.getEdgeValue(new LongWritable(message.getSenderId())).get());
                    }
                }
            }
        } else if (vertexPrepared(vertex, DISTRIBUTING_EDGES)) {
            aggregate(DISTRIBUTING_EDGES_AGGREGATOR, new LongWritable(1));
            if (!hasFolded(vertex)) {
                voteToMatch(vertex);
            } else {
                // For every nth level neighbor create edges
                for (Entry<Long, Double> entry : vertex.getValue().getnLevelNeighbors().getData().entrySet()) {
                    Edge<LongWritable, DoubleWritable> e1 = EdgeFactory.create(new LongWritable(entry.getKey()), new DoubleWritable(entry.getValue()));
                    Edge<LongWritable, DoubleWritable> e2 = EdgeFactory.create(new LongWritable(vertex.getValue().getChildVertex()), new DoubleWritable(entry.getValue()));
                    LOGGER.debug("Requesting a bidirectional edge between " + vertex.getValue().getChildVertex() + " and " + entry.getKey());
                    addEdgeRequest(new LongWritable(vertex.getValue().getChildVertex()), e1);
                    addEdgeRequest(new LongWritable(entry.getKey()), e2);
                }
                vertex.getValue().setComputationPhase(SPLITTING_VERTICES);
                vertex.voteToHalt();
            }
        } else if (vertexPrepared(vertex, PARTITIONING)) {
            aggregate(PARTITIONING_AGGREGATOR, new LongWritable(1));
            if (!hasPartition(vertex)) {
                LongToLongMapWritable outGraph = getAggregatedValue(OUTPUT_GRAPH_AGGREGATOR);
                if (outGraph.getData().isEmpty()) {
                    aggregate(INPUT_GRAPH_AGGREGATOR, new LongToDoubleMapWritable(vertex.getId().get(), vertex.getValue().getWeight()));
                } else {
                    Long partition = outGraph.getData().get(vertex.getId().get());
                    if (partition != null) {
                        vertex.getValue().setPartition(partition);
                        LOGGER.debug(vertex.getId() + " was assigned to partition " + partition);
                        // Inform all neighbors of assigned partition in case someone
                        // was not assigned one (see generatePartitions() in MasterCompute)
                        sendMessageToAllEdges(vertex, new GraphPartitionMessageData(PARTITION_MESSAGE, vertex.getId().get(), partition, vertex.getValue().getWeight()));
                    } else {
                        double min = Double.MAX_VALUE;
                        long minId = Long.MAX_VALUE;
                        // Inherit partition of minimum weighted neighbor
                        // and solve ties by minimum vertex id
                        for (GraphPartitionMessageData message : messages) {
                            if (message.getMessageType() == PARTITION_MESSAGE) {
                                if ((message.getDoubleData() < min) || (message.getDoubleData() == min && message.getSenderId() < minId)) {
                                    min = message.getDoubleData();
                                    minId = message.getSenderId();
                                    vertex.getValue().setPartition(message.getLongData());
                                    LOGGER.debug(vertex.getId() + " was assigned to partition " + message.getLongData());
                                }
                            }
                        }
                    }
                }
            } else {
                voteToSplit(vertex);
            }
        } else if (vertexPrepared(vertex, SPLITTING_VERTICES)) {
            aggregate(SPLITTING_VERTICES_AGGREGATOR, new LongWritable(1));
            if (!hasPartition(vertex)) {
                // Handle messages
                for (GraphPartitionMessageData message : messages) {
                    if (message.getMessageType() == WAKEUP_MESSAGE) {
                        vertex.getValue().setPartition(message.getLongData());
                    }
                }
                // If we're back to the original graph halt computation
                if (vertex.getValue().getCreators().getData().isEmpty()) {
                    vertex.voteToHalt();
                } // Else, KL local refinement
                else {
                    voteToRefine(vertex);
                }
            } else {
                for (Long creator : vertex.getValue().getCreators().getData().keySet()) {
                    sendMessage(new LongWritable(creator), new GraphPartitionMessageData(WAKEUP_MESSAGE, vertex.getId().get(), vertex.getValue().getPartition()));
                }
                // Delete vertex after waking up creators
                LOGGER.debug("Removing " + vertex.getId());
                removeVertexRequest(vertex.getId());
            }
        } else if (vertexPrepared(vertex, REFINING_LOCALLY)) {
            aggregate(REFINING_LOCALLY_AGGREGATOR, new LongWritable(1));
            // TODO: Implement local refinement
            voteToSplit(vertex);
        }
    }

    // Helper functions
    private boolean vertexPrepared(Vertex<LongWritable, GraphPartitionVertexData, DoubleWritable> vertex, int phase) {
        return vertex.getValue().getComputationPhase() == phase && ((IntWritable) getAggregatedValue(COMPUTATION_PHASE_AGGREGATOR)).get() == phase;
    }

    private boolean coarsening() {
        return ((BooleanWritable) getAggregatedValue(COARSENING_AGGREGATOR)).get();
    }

    private boolean hasMatched(Vertex<LongWritable, GraphPartitionVertexData, DoubleWritable> vertex) {
        return vertex.getValue().getPickedVertex() != Long.MAX_VALUE;
    }

    private boolean hasFolded(Vertex<LongWritable, GraphPartitionVertexData, DoubleWritable> vertex) {
        return vertex.getValue().getChildVertex() != Long.MAX_VALUE;
    }

    private boolean hasPartition(Vertex<LongWritable, GraphPartitionVertexData, DoubleWritable> vertex) {
        return vertex.getValue().getPartition() != Long.MAX_VALUE;
    }

    private long generateUniqueId() {
        return Math.abs(UUID.randomUUID().getLeastSignificantBits());
    }

    private long generatePartition(Vertex<LongWritable, GraphPartitionVertexData, DoubleWritable> vertex) {
        Random gen = new Random();
        return gen.nextInt();
    }

    // Helprer vote functions
    private void voteToMatch(Vertex<LongWritable, GraphPartitionVertexData, DoubleWritable> vertex) {
        vertex.getValue().setComputationPhase(MAXIMUM_WEIGHTED_MATCHING);
    }

    private void voteToFold(Vertex<LongWritable, GraphPartitionVertexData, DoubleWritable> vertex) {
        vertex.getValue().setComputationPhase(FOLDING_VERTICES);
    }

    private void voteToDistribute(Vertex<LongWritable, GraphPartitionVertexData, DoubleWritable> vertex) {
        vertex.getValue().setComputationPhase(DISTRIBUTING_EDGES);
    }

    private void voteToPartition(Vertex<LongWritable, GraphPartitionVertexData, DoubleWritable> vertex) {
        vertex.getValue().setComputationPhase(PARTITIONING);
    }

    private void voteToSplit(Vertex<LongWritable, GraphPartitionVertexData, DoubleWritable> vertex) {
        vertex.getValue().setComputationPhase(SPLITTING_VERTICES);
    }

    private void voteToRefine(Vertex<LongWritable, GraphPartitionVertexData, DoubleWritable> vertex) {
        vertex.getValue().setComputationPhase(REFINING_LOCALLY);
    }

}
