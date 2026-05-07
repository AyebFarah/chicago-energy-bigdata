package chicago.energy;

/**
 * Utilitaires CSV pour le dataset Chicago Energy Benchmarking
 *
 * Cette classe fournit :
 * - un parsing CSV robuste avec gestion des guillemets
 * - le nettoyage des champs texte
 * - la détection du header
 * - la conversion sécurisée des valeurs numériques
 */
public class CSVUtils {

    /**
     * Parse une ligne CSV en tenant compte des guillemets.
     *
     * Exemple :
     * 2022,"Willis Tower","123 Main St"
     *
     * Retour :
     * [2022, Willis Tower, 123 Main St]
     */
    public static String[] parseLine(String line) {

        if (line == null || line.trim().isEmpty()) {
            return new String[0];
        }

        /*
         * Regex CSV robuste :
         * sépare uniquement les virgules qui ne sont PAS
         * à l'intérieur des guillemets.
         */
        return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    }

    /**
     * Nettoie un champ CSV :
     * - supprime les guillemets
     * - supprime les espaces inutiles
     * - transforme null en chaîne vide
     */
    public static String clean(String field) {

        if (field == null) {
            return "";
        }

        return field
                .replace("\"", "")
                .trim();
    }

    /**
     * Vérifie si une ligne correspond au header du CSV.
     */
    public static boolean isHeader(String line) {

        if (line == null) {
            return false;
        }

        return line.startsWith("Data Year")
                || line.startsWith("\"Data Year\"");
    }

    /**
     * Parse un entier de manière sécurisée.
     *
     * Retourne null si :
     * - champ vide
     * - valeur invalide
     */
    public static Integer parseInt(String field) {

        String cleaned = clean(field);

        if (cleaned.isEmpty()
                || cleaned.equalsIgnoreCase("null")
                || cleaned.equalsIgnoreCase("N/A")) {
            return null;
        }

        /*
         * Supprime les virgules éventuelles
         * Exemple : "12,500"
         */
        cleaned = cleaned.replace(",", "");

        try {
            return Integer.parseInt(cleaned);

        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Parse un double de manière sécurisée.
     *
     * Gère :
     * - champs vides
     * - virgules dans les nombres
     * - valeurs invalides
     *
     * Exemple :
     * "65,250.8" -> 65250.8
     */
    public static Double parseDouble(String field) {

        String cleaned = clean(field);

        if (cleaned.isEmpty()
                || cleaned.equalsIgnoreCase("null")
                || cleaned.equalsIgnoreCase("N/A")) {
            return null;
        }

        /*
         * Important :
         * Supprime les séparateurs de milliers
         */
        cleaned = cleaned.replace(",", "");

        try {
            return Double.parseDouble(cleaned);

        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Vérifie qu'un index existe dans le tableau CSV.
     */
    public static boolean hasColumn(String[] fields, int index) {

        return fields != null
                && index >= 0
                && index < fields.length;
    }
}