package chicago.energy;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer Job 1 — Average Site EUI by Building Type
 */
public class EnergyAvgReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key,
                       Iterable<DoubleWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        double sum = 0.0;
        int count = 0;

        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }

        if (count == 0) {
            return;
        }

        double avg = sum / count;

        // Arrondi à 2 décimales
        avg = Math.round(avg * 100.0) / 100.0;

        System.out.println(
                "[JOB1] Type=" + key +
                        " Count=" + count +
                        " AvgEUI=" + avg
        );

        context.write(key, new DoubleWritable(avg));
    }
}