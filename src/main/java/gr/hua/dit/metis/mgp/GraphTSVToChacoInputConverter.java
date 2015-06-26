package gr.hua.dit.metis.mgp;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.StringTokenizer;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author michail
 */
public class GraphTSVToChacoInputConverter {

    public static void main(String args[]) throws Exception {

        File f = new File(args[0]);
        int linenum = 0;

        Writer writer = new FileWriter(new File(args[1]));

        long n, m;
        String format = "0";
        for (String line : FileUtils.readLines(f)) {
            if (linenum == 0) {
                StringTokenizer tokenizer = new StringTokenizer(line, " \t");
                if (!tokenizer.hasMoreTokens()) {
                    throw new IOException("No number of nodes");
                }
                n = Long.valueOf(tokenizer.nextToken());
                if (!tokenizer.hasMoreTokens()) {
                    throw new IOException("No number of edges");
                }
                m = Long.valueOf(tokenizer.nextToken());
                if (tokenizer.hasMoreTokens()) {
                    format = tokenizer.nextToken();
                }
                linenum++;
                continue;
            }

            StringTokenizer tokenizer = new StringTokenizer(line, " \t");
            switch (format) {
                case "0": {
                    while (tokenizer.hasMoreTokens()) {
                        writer.append(linenum + " " + tokenizer.nextToken() + " 1" + "\n");
                    }
                    break;
                }
                case "1": {
                    while (tokenizer.hasMoreTokens()) {
                        writer.append(linenum + " " + tokenizer.nextToken() + " " + tokenizer.nextToken() + "\n");
                    }
                    break;
                }
                case "10": {
                    // TODO: Support
//                    String nodeWeight = tokenizer.nextToken();
//                    while (tokenizer.hasMoreTokens()) {
//                        writer.append(linenum + " " + tokenizer.nextToken() + " 1" + "\n");
//                    }
                    break;
                }
                case "11": {
                    // TODO: Support
//                    String nodeWeight = tokenizer.nextToken();
//                    while (tokenizer.hasMoreTokens()) {
//                        writer.append(linenum + " " + tokenizer.nextToken() + " " + tokenizer.nextToken() + "\n");
//                    }
                    break;
                }
                default:
                    break;
            }

            linenum++;
        }

        writer.flush();
        writer.close();

    }

}
