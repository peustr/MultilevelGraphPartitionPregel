package gr.hua.dit.metis;

import gr.hua.dit.metis.io.LongDoubleMapWritable;
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
    private long pickedVertex, childVertex, partition;
    private double weight, pickedVertexWeight, pickedEdgeWeight;
    private LongSetWritable hiddenNeighbors;
    private LongDoubleMapWritable creators, nLevelNeighbors;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(computationPhase);
        out.writeLong(pickedVertex);
        out.writeLong(childVertex);
        out.writeLong(partition);
        out.writeDouble(weight);
        out.writeDouble(pickedVertexWeight);
        out.writeDouble(pickedEdgeWeight);
        hiddenNeighbors.write(out);
        creators.write(out);
        nLevelNeighbors.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        computationPhase = in.readInt();
        pickedVertex = in.readLong();
        childVertex = in.readLong();
        partition = in.readLong();
        weight = in.readDouble();
        pickedVertexWeight = in.readDouble();
        pickedEdgeWeight = in.readDouble();
        hiddenNeighbors.readFields(in);
        creators.readFields(in);
        nLevelNeighbors.readFields(in);
    }

    public GraphPartitionVertexData() {
        hiddenNeighbors = new LongSetWritable();
        creators = new LongDoubleMapWritable();
        nLevelNeighbors = new LongDoubleMapWritable();
    }

    public GraphPartitionVertexData(int computationPhase, double weight) {
        this.computationPhase = computationPhase;
        this.weight = weight;
        pickedVertex = Long.MAX_VALUE;
        childVertex = Long.MAX_VALUE;
        partition = Long.MAX_VALUE;
        pickedEdgeWeight = -Double.MAX_VALUE;
        hiddenNeighbors = new LongSetWritable();
        creators = new LongDoubleMapWritable();
        nLevelNeighbors = new LongDoubleMapWritable();
    }

    public GraphPartitionVertexData(int computationPhase, double weight, Map<Long, Double> creators) {
        this.computationPhase = computationPhase;
        this.weight = weight;
        pickedVertex = Long.MAX_VALUE;
        childVertex = Long.MAX_VALUE;
        partition = Long.MAX_VALUE;
        pickedEdgeWeight = -Double.MAX_VALUE;
        hiddenNeighbors = new LongSetWritable();
        this.creators = new LongDoubleMapWritable();
        this.creators.setData(creators);
        nLevelNeighbors = new LongDoubleMapWritable();
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

    public LongDoubleMapWritable getCreators() {
        return creators;
    }

    public void setCreators(LongDoubleMapWritable creators) {
        this.creators = creators;
    }

    public LongDoubleMapWritable getnLevelNeighbors() {
        return nLevelNeighbors;
    }

    public void setnLevelNeighbors(LongDoubleMapWritable nLevelNeighbors) {
        this.nLevelNeighbors = nLevelNeighbors;
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
}
