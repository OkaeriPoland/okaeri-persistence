package eu.okaeri.persistence.filter.operation;

public interface UpdateOperation {

    /**
     * @return The field path to update (e.g., "exp", "profile.age", "items")
     */
    String getField();

    /**
     * @return The type of update operation
     */
    UpdateOperationType getType();
}
