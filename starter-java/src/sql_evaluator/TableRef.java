package sql_evaluator;

import com.fasterxml.jackson.annotation.*;

/**
 * These appear in the FROM clause.
 */
public final class TableRef extends Node {
    public final String source;
    public final String as;

    @JsonCreator
    public TableRef(@JsonProperty("source") String source, @JsonProperty("as") String as) {
        if (source == null) throw new IllegalArgumentException("'source' can't be null");
        if (as == null) throw new IllegalArgumentException("'as' can't be null");
        this.source = source;
        this.as = as;
    }
}
