package sql_evaluator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The top-level node for a query: SELECT ... FROM ... WHERE.
 */
public final class Query extends Node {
    public final Selector[] select;
    public final TableRef[] from;
    public final Condition[] where;  // Might be empty.

    @JsonCreator
    public Query(@JsonProperty("select") Selector[] select, @JsonProperty("from") TableRef[] from, @JsonProperty("where") Condition[] where) {
        if (select == null) throw new IllegalArgumentException("'select' can't be null");
        if (from == null) throw new IllegalArgumentException("'from' can't be null");
        if (where == null) throw new IllegalArgumentException("'where' can't be null");
        if (select.length == 0) throw new IllegalArgumentException("'select' can't be empty");
        if (from.length == 0) throw new IllegalArgumentException("'from' can't be empty");
        this.select = select;
        this.from = from;
        this.where = where;
    }
}
