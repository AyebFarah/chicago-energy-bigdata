package chicago.energy;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper Job 2 — Nombre de bâtiments avec ENERGY STAR Score par année
 *
 * Entrée  : ligne CSV
 * Sortie  : (Data Year, 1)
 *
 * Colonnes utilisées :
 *   index 0  = Data Year
 *   index 14 = ENERGY STAR Score
 */
public class EnergyStarMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Ignorer header et lignes vides
        if (CSVUtils.isHeader(line) || line.isEmpty()) {
            return;
        }

        String[] fields = CSVUtils.parseLine(line);

        // Vérification nombre minimal de colonnes
        if (fields.length < 15) {
            return;
        }

        // Data Year
        String year = CSVUtils.clean(fields[0]);

        // ENERGY STAR Score
        Double score = CSVUtils.parseDouble(fields[14]);

        /*
         * On garde uniquement :
         * - année valide
         * - score valide
         * - score > 0
         */
        if (year.isEmpty() || score == null || score <= 0) {
            return;
        }

        context.write(new Text(year), ONE);
    }
}