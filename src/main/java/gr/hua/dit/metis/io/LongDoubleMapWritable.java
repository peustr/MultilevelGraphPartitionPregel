package gr.hua.dit.metis.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

public class LongDoubleMapWritable implements Writable {

    private Map<Long, Double> data;

    public LongDoubleMapWritable() {
        data = new HashMap<>();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        data.clear();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            data.put(in.readLong(), in.readDouble());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(data.size());
        for (Entry<Long, Double> each : data.entrySet()) {
            out.writeLong(each.getKey());
            out.writeDouble(each.getValue());
        }
    }

    public Map<Long, Double> getData() {
        return data;
    }

    public void setData(Map<Long, Double> data) {
        this.data = data;
    }

}
