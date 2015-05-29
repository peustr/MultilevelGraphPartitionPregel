package gr.hua.dit.metis;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author peustr
 */
public class GraphPartitionEdgeInputFormat extends TextEdgeInputFormat<LongWritable, DoubleWritable> {

    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public EdgeReader<LongWritable, DoubleWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new GraphPartitionTextEdgeReader();
    }

    public class GraphPartitionTextEdgeReader extends TextEdgeReaderFromEachLineProcessed<String[]> {

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            return SEPARATOR.split(line.toString());
        }

        @Override
        protected LongWritable getSourceVertexId(String[] line) throws IOException {
            return new LongWritable(Long.valueOf(line[0]));
        }

        @Override
        protected LongWritable getTargetVertexId(String[] line) throws IOException {
            return new LongWritable(Long.valueOf(line[1]));
        }

        @Override
        protected DoubleWritable getValue(String[] line) throws IOException {
            if (line.length == 2) {
                return new DoubleWritable(1.0);
            } else if (line.length == 3) {
                return new DoubleWritable(Double.valueOf(line[2]));
            } else {
                throw new IOException("Unsupported edge format");
            }
        }

    }
}
