package chicago.energy;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GHGMapper
        extends Mapper<Object, Text, Text, DoubleWritable> {

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (CSVUtils.isHeader(line) || line.isEmpty()) {
            return;
        }

        String[] fields = CSVUtils.parseLine(line);
        if (fields.length < 25) {
            return;
        }
        String community = CSVUtils.clean(fields[8]);
        Double ghg = CSVUtils.parseDouble(fields[24]);
        if (community.isEmpty() || ghg == null || ghg <= 0) {
            return;
        }

        context.write(
                new Text(community),
                new DoubleWritable(ghg)
        );
    }
}