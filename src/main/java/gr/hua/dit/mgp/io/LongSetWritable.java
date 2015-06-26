package gr.hua.dit.mgp.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class LongSetWritable implements Writable {

    private Set<Long> data;

    public LongSetWritable() {
        data = new HashSet<>();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        data.clear();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            data.add(in.readLong());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(data.size());
        for (Long each : data) {
            out.writeLong(each);
        }
    }

    public Set<Long> getData() {
        return data;
    }

    public void setData(Set<Long> data) {
        this.data = data;
    }

}
