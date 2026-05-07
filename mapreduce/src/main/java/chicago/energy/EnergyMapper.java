package chicago.energy;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper Job 1 — Average Site EUI by Building Type
 *
 * Entrée :
 *   ligne CSV du dataset Chicago Energy Benchmarking
 *
 * Sortie :
 *   (Primary Property Type, Site EUI)
 *
 * Colonnes utilisées :
 *   index 9  = Primary Property Type
 *   index 20 = Site EUI (kBtu/sq ft)
 */
public class EnergyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // Ignorer header et lignes vides
        if (CSVUtils.isHeader(line) || line.isEmpty()) {
            return;
        }

        String[] fields = CSVUtils.parseLine(line);

        // Vérification sécurité
        if (!CSVUtils.hasColumn(fields, 20)) {
            return;
        }

        // Colonnes correctes
        String buildingType = CSVUtils.clean(fields[9]);

        Double eui = CSVUtils.parseDouble(fields[20]);

        // Validation
        if (buildingType.isEmpty()
                || eui == null
                || eui <= 0) {
            return;
        }

        context.write(
                new Text(buildingType),
                new DoubleWritable(eui)
        );
    }
}