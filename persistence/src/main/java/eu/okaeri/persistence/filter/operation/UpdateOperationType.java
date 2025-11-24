package eu.okaeri.persistence.filter.operation;

public enum UpdateOperationType {
    // Field operations
    SET,
    UNSET,
    INCREMENT,
    MULTIPLY,
    MIN,
    MAX,
    CURRENT_DATE,

    // Array operations
    PUSH,
    POP_FIRST,
    POP_LAST,
    PULL,
    PULL_ALL,
    ADD_TO_SET
}
