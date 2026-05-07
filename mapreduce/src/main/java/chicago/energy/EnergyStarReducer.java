package chicago.energy;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer Job 2 — Compte le nombre de bâtiments
 * ayant un ENERGY STAR Score valide par année
 *
 * Entrée  : (year, [1,1,1,...])
 * Sortie  : (year, total)
 */
public class EnergyStarReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable val : values) {
            sum += val.get();
        }

        System.out.println(
                "[JOB2] Year: " + key +
                        " | Buildings with ENERGY STAR Score: " + sum
        );

        context.write(key, new IntWritable(sum));
    }
}