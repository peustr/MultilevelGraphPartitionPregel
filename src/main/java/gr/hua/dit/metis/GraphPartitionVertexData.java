package gr.hua.dit.metis;

import gr.hua.dit.metis.io.LongToDoubleMapWritable;
import gr.hua.dit.metis.io.LongSetWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author peustr
 */
public class GraphPartitionVertexData implements Writable {

    private int computationPhase;
    private long pickedVertex, childVertex, partition, partitionCandidate;
    private double weight, pickedVertexWeight, pickedEdgeWeight;
    private LongSetWritable hiddenNeighbors;
    private LongToDoubleMapWritable creators, nLevelNeighbors, partitionWeights;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(computationPhase);
        out.writeLong(pickedVertex);
        out.writeLong(childVertex);
        out.writeLong(partition);
        out.writeLong(partitionCandidate);
        out.writeDouble(weight);
        out.writeDouble(pickedVertexWeight);
        out.writeDouble(pickedEdgeWeight);
        hiddenNeighbors.write(out);
        creators.write(out);
        nLevelNeighbors.write(out);
        partitionWeights.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        computationPhase = in.readInt();
        pickedVertex = in.readLong();
        childVertex = in.readLong();
        partition = in.readLong();
        partitionCandidate = in.readLong();
        weight = in.readDouble();
        pickedVertexWeight = in.readDouble();
        pickedEdgeWeight = in.readDouble();
        hiddenNeighbors.readFields(in);
        creators.readFields(in);
        nLevelNeighbors.readFields(in);
        partitionWeights.readFields(in);
    }

    public GraphPartitionVertexData() {
        hiddenNeighbors = new LongSetWritable();
        creators = new LongToDoubleMapWritable();
        nLevelNeighbors = new LongToDoubleMapWritable();
        partitionWeights = new LongToDoubleMapWritable();
    }

    public GraphPartitionVertexData(int computationPhase, double weight) {
        this.computationPhase = computationPhase;
        this.weight = weight;
        pickedVertex = Long.MAX_VALUE;
        childVertex = Long.MAX_VALUE;
        partition = Long.MAX_VALUE;
        partitionCandidate = Long.MAX_VALUE;
        pickedEdgeWeight = -Double.MAX_VALUE;
        hiddenNeighbors = new LongSetWritable();
        creators = new LongToDoubleMapWritable();
        nLevelNeighbors = new LongToDoubleMapWritable();
        partitionWeights = new LongToDoubleMapWritable();
    }

    public GraphPartitionVertexData(int computationPhase, double weight, Map<Long, Double> creators) {
        this.computationPhase = computationPhase;
        this.weight = weight;
        pickedVertex = Long.MAX_VALUE;
        childVertex = Long.MAX_VALUE;
        partition = Long.MAX_VALUE;
        partitionCandidate = Long.MAX_VALUE;
        pickedEdgeWeight = -Double.MAX_VALUE;
        hiddenNeighbors = new LongSetWritable();
        this.creators = new LongToDoubleMapWritable();
        this.creators.setData(creators);
        nLevelNeighbors = new LongToDoubleMapWritable();
        partitionWeights = new LongToDoubleMapWritable();
    }

    public int getComputationPhase() {
        return computationPhase;
    }

    public void setComputationPhase(int computationPhase) {
        this.computationPhase = computationPhase;
    }

    public long getPickedVertex() {
        return pickedVertex;
    }

    public void setPickedVertex(long pickedVertex) {
        this.pickedVertex = pickedVertex;
    }

    public long getChildVertex() {
        return childVertex;
    }

    public void setChildVertex(long childVertex) {
        this.childVertex = childVertex;
    }

    public long getPartition() {
        return partition;
    }

    public void setPartition(long partition) {
        this.partition = partition;
    }

    public long getPartitionCandidate() {
        return partitionCandidate;
    }

    public void setPartitionCandidate(long partitionCandidate) {
        this.partitionCandidate = partitionCandidate;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public double getPickedVertexWeight() {
        return pickedVertexWeight;
    }

    public void setPickedVertexWeight(double pickedVertexWeight) {
        this.pickedVertexWeight = pickedVertexWeight;
    }

    public double getPickedEdgeWeight() {
        return pickedEdgeWeight;
    }

    public void setPickedEdgeWeight(double pickedEdge) {
        this.pickedEdgeWeight = pickedEdge;
    }

    public LongSetWritable getHiddenNeighbors() {
        return hiddenNeighbors;
    }

    public void setHiddenNeighbors(LongSetWritable hiddenNeighbors) {
        this.hiddenNeighbors = hiddenNeighbors;
    }

    public LongToDoubleMapWritable getCreators() {
        return creators;
    }

    public void setCreators(LongToDoubleMapWritable creators) {
        this.creators = creators;
    }

    public LongToDoubleMapWritable getnLevelNeighbors() {
        return nLevelNeighbors;
    }

    public void setnLevelNeighbors(LongToDoubleMapWritable nLevelNeighbors) {
        this.nLevelNeighbors = nLevelNeighbors;
    }

    public LongToDoubleMapWritable getPartitionWeights() {
        return partitionWeights;
    }

    public void setPartitionWeights(LongToDoubleMapWritable partitionWeights) {
        this.partitionWeights = partitionWeights;
    }

    @Override
    public String toString() {
        return String.valueOf(partition);
    }

    // Utility
    public void hide(long vertexId) {
        hiddenNeighbors.getData().add(vertexId);
    }

    public boolean isHidden(long vertexId) {
        return hiddenNeighbors.getData().contains(vertexId);
    }

    public void addnLevelNeighbor(long vertexId, double edgeWeight) {
        if (nLevelNeighbors.getData().containsKey(vertexId)) {
            double curWeight = nLevelNeighbors.getData().get(vertexId);
            nLevelNeighbors.getData().put(vertexId, curWeight + edgeWeight);
        } else {
            nLevelNeighbors.getData().put(vertexId, edgeWeight);
        }
    }

    public void appendPartitionWeights(Long l, Double d) {
        if (partitionWeights.getData().get(l) != null) {
            partitionWeights.getData().put(l, partitionWeights.getData().get(l) + d);
        } else {
            partitionWeights.getData().put(l, d);
        }
    }
}
