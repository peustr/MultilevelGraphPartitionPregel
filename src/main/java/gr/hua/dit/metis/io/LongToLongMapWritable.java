package gr.hua.dit.metis.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author peustr
 */
public class LongToLongMapWritable implements Writable {

    private Map<Long, Long> data;

    public LongToLongMapWritable() {
        data = new HashMap<>();
    }

    public LongToLongMapWritable(long l1, long l2) {
        data = new HashMap<>();
        data.put(l1, l2);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        data.clear();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            data.put(in.readLong(), in.readLong());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(data.size());
        for (Map.Entry<Long, Long> each : data.entrySet()) {
            out.writeLong(each.getKey());
            out.writeLong(each.getValue());
        }
    }

    public Map<Long, Long> getData() {
        return data;
    }

    public void setData(Map<Long, Long> data) {
        this.data = data;
    }

}
