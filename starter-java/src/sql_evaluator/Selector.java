package sql_evaluator;

import com.fasterxml.jackson.annotation.*;

/**
 * These appear in the SELECT clause.
 */
public final class Selector extends Node {
    public final ColumnRef column;
    public final String as;

    @JsonCreator
    public Selector(@JsonProperty("column") ColumnRef column, @JsonProperty("as") String as) {
        if (column == null) throw new IllegalArgumentException("'column' can't be null");
        if (as == null) throw new IllegalArgumentException("'as' can't be null");
        this.column = column;
        this.as = as;
    }
}

