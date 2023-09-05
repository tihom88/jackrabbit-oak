package org.apache.jackrabbit.oak.index.indexer.document.incrementalstore;

public enum IncrementalOperand {
    ADD("a"),
    MODIFY("m"),
    DELETE("d");
    private final String operand;

    IncrementalOperand(String operand) {
        this.operand = operand;
    }

    @Override
    public String toString() {
        return operand;
    }
}