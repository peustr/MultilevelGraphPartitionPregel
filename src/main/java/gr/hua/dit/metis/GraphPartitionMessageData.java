package gr.hua.dit.metis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author peustr
 */
public class GraphPartitionMessageData implements Writable {

    private int messageType;
    private long senderId, longData;
    private double doubleData;

    public GraphPartitionMessageData() {
    }

    public GraphPartitionMessageData(int messageType, long senderId) {
        this.messageType = messageType;
        this.senderId = senderId;
    }

    public GraphPartitionMessageData(int messageType, long senderId, long longData) {
        this.messageType = messageType;
        this.senderId = senderId;
        this.longData = longData;
    }

    public GraphPartitionMessageData(int messageType, long senderId, double doubleData) {
        this.messageType = messageType;
        this.senderId = senderId;
        this.doubleData = doubleData;
    }

    public GraphPartitionMessageData(int messageType, long senderId, long longData, double doubleData) {
        this.messageType = messageType;
        this.senderId = senderId;
        this.longData = longData;
        this.doubleData = doubleData;
    }

    public int getMessageType() {
        return messageType;
    }

    public void setMessageType(int messageType) {
        this.messageType = messageType;
    }

    public long getSenderId() {
        return senderId;
    }

    public void setSenderId(long senderId) {
        this.senderId = senderId;
    }

    public long getLongData() {
        return longData;
    }

    public void setLongData(long longData) {
        this.longData = longData;
    }

    public double getDoubleData() {
        return doubleData;
    }

    public void setDoubleData(double doubleData) {
        this.doubleData = doubleData;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(messageType);
        out.writeLong(senderId);
        out.writeLong(longData);
        out.writeDouble(doubleData);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        messageType = in.readInt();
        senderId = in.readLong();
        longData = in.readLong();
        doubleData = in.readDouble();
    }

}
