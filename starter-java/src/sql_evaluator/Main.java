package sql_evaluator;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.io.FileWriter;
import java.io.File;
import java.util.ArrayList;

public final class Main {
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: COMMAND <table-folder> <sql-json-file> <output-file>");
            System.exit(1); return;
        }

        String tableFolder = args[0];
        String sqlJsonFile = args[1];
        String outputFile = args[2];

        Query query;
        try {
            query = JacksonUtil.readFully(sqlJsonFile, Query.class);
        } catch (JsonProcessingException ex) {
            System.err.println("Error loading \"" + sqlJsonFile + "\" as query JSON: " + ex.getMessage());
            System.exit(1); return;
        }

        ArrayList<Table> tables = new ArrayList<>();
        for (TableRef tableRef : query.from) {
            String path = tableFolder + File.separator + (tableRef.source + ".table.json");
            Table table;
            try {
                table = JacksonUtil.readFully(path, Table.class);
            } catch (JsonProcessingException ex) {
                System.err.println("Error loading \"" + path + "\" as table JSON: " + ex.getMessage());
                System.exit(1); return;
            }
            tables.add(table);
        }

        // TODO: Actually evaluate query.
        // For now, just dump the input back out.
        try (FileWriter out = new FileWriter(outputFile)) {
            JacksonUtil.writeLines(out, query);

            for (Table table : tables) {
                out.write("[\n");

                out.write("    ");
                JacksonUtil.write(out, table.columns);

                for (Object[] row : table.rows) {
                    out.write(",\n    ");
                    JacksonUtil.write(out, row);
                }

                out.write("\n]\n");
            }
        }
    }
}
