package chicago.energy;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper Job 3 — Émissions GHG par Community Area
 *
 * Entrée  : ligne CSV
 * Sortie  : (Community Area, Total GHG Emissions)
 *
 * Colonnes utilisées :
 *   index 8  = Community Area
 *   index 24 = Total GHG Emissions (Metric Tons CO2e)
 */
public class GHGMapper
        extends Mapper<Object, Text, Text, DoubleWritable> {

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Ignorer header et lignes vides
        if (CSVUtils.isHeader(line) || line.isEmpty()) {
            return;
        }

        String[] fields = CSVUtils.parseLine(line);

        // Vérifier le nombre minimal de colonnes
        if (fields.length < 25) {
            return;
        }

        // Community Area
        String community = CSVUtils.clean(fields[8]);

        // Total GHG Emissions
        Double ghg = CSVUtils.parseDouble(fields[24]);

        /*
         * Garder uniquement :
         * - Community Area valide
         * - GHG valide
         * - GHG positif
         */
        if (community.isEmpty() || ghg == null || ghg <= 0) {
            return;
        }

        context.write(
                new Text(community),
                new DoubleWritable(ghg)
        );
    }
}