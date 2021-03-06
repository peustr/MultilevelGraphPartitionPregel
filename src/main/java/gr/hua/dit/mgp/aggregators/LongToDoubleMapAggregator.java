package gr.hua.dit.mgp.aggregators;

import gr.hua.dit.mgp.io.LongToDoubleMapWritable;
import java.util.Map.Entry;
import org.apache.giraph.aggregators.BasicAggregator;

/**
 *
 * @author peustr
 */
public class LongToDoubleMapAggregator extends BasicAggregator<LongToDoubleMapWritable> {

    @Override
    public void aggregate(LongToDoubleMapWritable value) {
        for (Entry<Long, Double> vertex : value.getData().entrySet()) {
            getAggregatedValue().getData().put(vertex.getKey(), vertex.getValue());
        }
    }

    @Override
    public LongToDoubleMapWritable createInitialValue() {
        return new LongToDoubleMapWritable();
    }

}
