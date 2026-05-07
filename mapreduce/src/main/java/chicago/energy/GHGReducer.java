package chicago.energy;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer Job 3 — Somme les émissions GHG
 * par Community Area
 *
 * Entrée  : (community, [ghg1, ghg2, ...])
 * Sortie  : (community, total_ghg)
 */
public class GHGReducer
        extends Reducer<Text, DoubleWritable,
        Text, DoubleWritable> {

    @Override
    public void reduce(Text key,
                       Iterable<DoubleWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        double total = 0.0;
        int count = 0;

        for (DoubleWritable val : values) {
            total += val.get();
            count++;
        }

        double rounded = Math.round(total * 100.0) / 100.0;

        System.out.println(
                "[JOB3] Community: " + key +
                        " | Buildings: " + count +
                        " | Total GHG: " + rounded
        );

        context.write(
                key,
                new DoubleWritable(rounded)
        );
    }
}