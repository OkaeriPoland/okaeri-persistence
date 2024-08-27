package eu.okaeri.persistence.raw;

public enum PersistencePropertyMode {
    NATIVE, // readByProperty
    TOSTRING, // streamAll + prefilter + filter
    EQUALS // streamAll + filter
}
