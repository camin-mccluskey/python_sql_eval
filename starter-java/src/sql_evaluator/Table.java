package sql_evaluator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Represents the data loaded from a ".table.json" file.
 */
@JsonDeserialize(using=Table.Deserializer.class)
@JsonSerialize(using=Table.Serializer.class)
public final class Table extends Node {
    public final ColumnDef[] columns;
    public final Object[][] rows;  // Each value is either a String or Integer object.

    public Table(ColumnDef[] columns, Object[][] rows) {
        this.columns = columns;
        this.rows = rows;
    }

    @JsonFormat(shape=JsonFormat.Shape.ARRAY)
    public static final class ColumnDef extends Node {
        public final String name;
        public final SqlType type;

        @JsonCreator
        public ColumnDef(@JsonProperty("name") String name, @JsonProperty("type") SqlType type) {
            if (name == null) throw new IllegalArgumentException("'name' can't be null");
            if (type == null) throw new IllegalArgumentException("'type' can't be null");
            this.name = name;
            this.type = type;
        }
    }

    public static final class Deserializer extends StdDeserializer<Table> {
        public Deserializer() {
            super(Table.class);
        }

        @Override
        public Table deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {
            if (!jp.isExpectedStartArrayToken()) {
                throw new JsonParseException(jp, "expecting start of an array (for table)");
            }
            jp.nextToken();

            ColumnDef[] columns = jp.readValueAs(ColumnDef[].class);
            jp.nextToken();

            ArrayList<Object[]> rows = new ArrayList<>();
            while (jp.getCurrentToken() != JsonToken.END_ARRAY) {
                rows.add(readRow(jp, columns));
            }
            jp.nextToken();

            return new Table(columns, rows.toArray(new Object[rows.size()][]));
        }

        private Object[] readRow(JsonParser jp, ColumnDef[] columns) throws IOException {
            if (!jp.isExpectedStartArrayToken()) {
                throw new JsonParseException(jp, "expecting start of an array (for table row), got" + jp.getCurrentToken());
            }
            jp.nextToken();

            Object[] row = new Object[columns.length];

            for (int i = 0; i < row.length; i++) {
                // If there aren't enough cells...
                if (jp.currentToken() == JsonToken.END_ARRAY) {
                    throw new JsonParseException(jp, "row only has " + i + " values, but there are " + row.length + " columns");
                }

                row[i] = readCell(jp, columns[i]);
            }

            // If there are too many cells...
            if (jp.currentToken() != JsonToken.END_ARRAY) {
                throw new JsonParseException(jp, "row has more than " + columns.length + " values, but there are only " + columns.length + " columns");
            }
            jp.nextToken();

            return row;
        }

        private Object readCell(JsonParser jp, ColumnDef columnDef) throws IOException {
            Object value;

            switch (columnDef.type) {
                case STR:
                    if (jp.currentToken() != JsonToken.VALUE_STRING) {
                        throw new JsonParseException(jp, "got invalid cell value for column \"" + columnDef.name + "\"; expecting a string");
                    }
                    value = jp.getText();
                    jp.nextToken();
                    break;
                case INT:
                    if (jp.currentToken() != JsonToken.VALUE_NUMBER_INT) {
                        throw new JsonParseException(jp, "got invalid cell value for column \"" + columnDef.name + "\"; expecting an integer");
                    }
                    value = jp.getIntValue();
                    jp.nextToken();
                    break;
                default:
                    throw new AssertionError("unhandled SqlType: " + columnDef.type);
            }

            return value;
        }
    }

    public static final class Serializer extends StdSerializer<Table> {
        public Serializer() {
            super(Table.class);
        }

        @Override
        public void serialize(Table t, JsonGenerator g, SerializerProvider serializerProvider) throws IOException {
            g.writeStartArray(t.rows.length + 1);

            g.writeObject(t.columns);

            for (int i = 0; i < t.rows.length; i++) {
                Object[] row = t.rows[i];
                if (row.length != t.columns.length) {
                    throw new AssertionError("row " + (i+1) + " has " + row.length + " cells, but the table has " + t.columns.length + " columns");
                }

                g.writeStartArray(row.length);
                for (Object cell : row) {
                    if (cell instanceof String) {
                        g.writeString((String) cell);
                    } else if (cell instanceof Integer) {
                        g.writeNumber((Integer) cell);
                    } else {
                        throw new AssertionError("row " + (i+1) + " has bad cell value type: " + cell.getClass().getName());
                    }
                }
                g.writeEndArray();
            }

            g.writeEndArray();
        }
    }
}
