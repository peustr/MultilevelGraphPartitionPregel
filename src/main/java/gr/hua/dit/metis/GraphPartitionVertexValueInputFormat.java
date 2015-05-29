package gr.hua.dit.metis;

import static gr.hua.dit.metis.GraphPartitionConstants.ComputationConstants.MAXIMUM_WEIGHTED_MATCHING;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author peustr
 */
public class GraphPartitionVertexValueInputFormat extends TextVertexValueInputFormat<LongWritable, GraphPartitionVertexData, DoubleWritable> {

    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexValueReader createVertexValueReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new GraphPartitionVertexValueReader();
    }

    public class GraphPartitionVertexValueReader extends
            TextVertexValueReaderFromEachLineProcessed<String[]> {

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            return SEPARATOR.split(line.toString());
        }

        @Override
        protected LongWritable getId(String[] line) throws IOException {
            return new LongWritable(Long.valueOf(line[0]));
        }

        @Override
        protected GraphPartitionVertexData getValue(String[] line) throws IOException {
            return new GraphPartitionVertexData(MAXIMUM_WEIGHTED_MATCHING, 1.0);
        }

    }

}
