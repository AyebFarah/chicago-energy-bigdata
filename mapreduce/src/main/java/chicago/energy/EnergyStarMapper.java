package chicago.energy;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Mapper Job 2 — Nombre de bâtiments avec ENERGY STAR Score par année
public class EnergyStarMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (CSVUtils.isHeader(line) || line.isEmpty()) {
            return;
        }

        String[] fields = CSVUtils.parseLine(line);
        if (fields.length < 15) {
            return;
        }
        String year = CSVUtils.clean(fields[0]);
        Double score = CSVUtils.parseDouble(fields[14]);
        if (year.isEmpty() || score == null || score <= 0) {
            return;
        }
        context.write(new Text(year), ONE);
    }
}