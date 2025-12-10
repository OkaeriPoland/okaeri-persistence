package eu.okaeri.persistence.filter;

import eu.okaeri.configs.migrate.view.RawConfigView;
import eu.okaeri.configs.serdes.SerdesRegistry;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.filter.operation.*;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.util.*;

import static eu.okaeri.persistence.document.DocumentValueUtils.compareEquals;
import static eu.okaeri.persistence.document.DocumentValueUtils.compareForSort;

/**
 * Evaluates update operations in-memory for backends that don't support native atomic updates.
 * Used as a fallback when backend throws UnsupportedOperationException.
 * Uses RawConfigView pattern to leverage OkaeriConfig's automatic type handling.
 */
@RequiredArgsConstructor
public class InMemoryUpdateEvaluator {

    private final SerdesRegistry serdesRegistry;

    /**
     * Apply update operations to a document in-memory.
     * Uses RawConfigView pattern: document.set() handles type conversion automatically.
     *
     * @param document   The document to update (must have configurer already set)
     * @param operations List of update operations to apply
     * @return true if document was modified, false otherwise
     */
    public boolean applyUpdate(@NonNull Document document, @NonNull List<UpdateOperation> operations) {
        if (operations.isEmpty()) {
            return false;
        }

        if (document.getConfigurer() == null) {
            throw new IllegalStateException("Document must have configurer set - backend implementation bug?");
        }

        RawConfigView view = new RawConfigView(document);
        boolean modified = false;

        // Apply operations in order
        for (UpdateOperation op : operations) {
            boolean changed = this.applyOperation(view, op);
            modified = modified || changed;
        }

        return modified;
    }

    /**
     * Apply a single operation using RawConfigView.
     */
    protected boolean applyOperation(@NonNull RawConfigView view, @NonNull UpdateOperation operation) {
        switch (operation.getType()) {
            case SET:
                return this.applySet(view, (SetOperation) operation);
            case UNSET:
                return this.applyUnset(view, (UnsetOperation) operation);
            case INCREMENT:
                return this.applyIncrement(view, (IncrementOperation) operation);
            case MULTIPLY:
                return this.applyMultiply(view, (MultiplyOperation) operation);
            case MIN:
                return this.applyMin(view, (MinOperation) operation);
            case MAX:
                return this.applyMax(view, (MaxOperation) operation);
            case CURRENT_DATE:
                return this.applyCurrentDate(view, (CurrentDateOperation) operation);
            case PUSH:
                return this.applyPush(view, (PushOperation) operation);
            case POP_FIRST:
                return this.applyPopFirst(view, (PopFirstOperation) operation);
            case POP_LAST:
                return this.applyPopLast(view, (PopLastOperation) operation);
            case PULL:
                return this.applyPull(view, (PullOperation) operation);
            case PULL_ALL:
                return this.applyPullAll(view, (PullAllOperation) operation);
            case ADD_TO_SET:
                return this.applyAddToSet(view, (AddToSetOperation) operation);
            default:
                throw new UnsupportedOperationException("Unsupported operation type: " + operation.getType());
        }
    }

    // ===== FIELD OPERATIONS =====

    protected boolean applySet(@NonNull RawConfigView view, @NonNull SetOperation op) {
        Object oldValue = view.get(op.getField());
        view.set(op.getField(), op.getValue());  // OkaeriConfig handles type conversion
        return !Objects.equals(oldValue, op.getValue());
    }

    protected boolean applyUnset(@NonNull RawConfigView view, @NonNull UnsetOperation op) {
        if (!view.exists(op.getField())) {
            return false;
        }
        view.set(op.getField(), null);
        return true;
    }

    protected boolean applyIncrement(@NonNull RawConfigView view, @NonNull IncrementOperation op) {
        Object currentValue = view.get(op.getField());
        Number current = (currentValue instanceof Number) ? (Number) currentValue : 0;

        // Perform increment based on types
        Number newValue;
        if ((op.getDelta() instanceof Double) || (current instanceof Double)) {
            newValue = current.doubleValue() + op.getDelta().doubleValue();
        } else if ((op.getDelta() instanceof Long) || (current instanceof Long)) {
            newValue = current.longValue() + op.getDelta().longValue();
        } else {
            newValue = current.intValue() + op.getDelta().intValue();
        }

        view.set(op.getField(), newValue);  // OkaeriConfig handles type conversion
        return true;
    }

    protected boolean applyMultiply(@NonNull RawConfigView view, @NonNull MultiplyOperation op) {
        Object currentValue = view.get(op.getField());
        Number current = (currentValue instanceof Number) ? (Number) currentValue : 1;

        double newValue = current.doubleValue() * op.getFactor();
        view.set(op.getField(), newValue);  // OkaeriConfig handles type conversion
        return true;
    }

    protected boolean applyMin(@NonNull RawConfigView view, @NonNull MinOperation op) {
        Object currentValue = view.get(op.getField());

        // If field doesn't exist, set to the new value
        if (currentValue == null) {
            view.set(op.getField(), op.getValue());
            return true;
        }

        // Use compareForSort for type-safe comparison (handles Number coercion)
        if (compareForSort(op.getValue(), currentValue) < 0) {
            view.set(op.getField(), op.getValue());  // OkaeriConfig handles type conversion
            return true;
        }

        return false;
    }

    protected boolean applyMax(@NonNull RawConfigView view, @NonNull MaxOperation op) {
        Object currentValue = view.get(op.getField());

        if (currentValue == null) {
            view.set(op.getField(), op.getValue());
            return true;
        }

        // Use compareForSort for type-safe comparison (handles Number coercion)
        if (compareForSort(op.getValue(), currentValue) > 0) {
            view.set(op.getField(), op.getValue());  // OkaeriConfig handles type conversion
            return true;
        }

        return false;
    }

    protected boolean applyCurrentDate(@NonNull RawConfigView view, @NonNull CurrentDateOperation op) {
        view.set(op.getField(), Instant.now().toString());
        return true;
    }

    // ===== ARRAY OPERATIONS =====

    @SuppressWarnings("unchecked")
    protected boolean applyPush(@NonNull RawConfigView view, @NonNull PushOperation op) {
        Object currentValue = view.get(op.getField());

        // Create new list if field doesn't exist or is null
        List<Object> list;
        if (currentValue == null) {
            list = new ArrayList<>();
        } else if (currentValue instanceof List) {
            list = new ArrayList<>((List<?>) currentValue);
        } else {
            throw new IllegalArgumentException("Field is not an array: " + op.getField());
        }

        // Add values
        if (op.isSingleValue()) {
            list.add(op.getSingleValue());
        } else {
            list.addAll(op.getValues());
        }

        view.set(op.getField(), list);  // OkaeriConfig handles type conversion
        return true;
    }

    @SuppressWarnings("unchecked")
    protected boolean applyPopFirst(@NonNull RawConfigView view, @NonNull PopFirstOperation op) {
        Object currentValue = view.get(op.getField());

        if (currentValue == null) {
            return false;
        }

        if (!(currentValue instanceof List)) {
            throw new IllegalArgumentException("Field is not an array: " + op.getField());
        }

        List<Object> list = new ArrayList<>((List<?>) currentValue);
        if (list.isEmpty()) {
            return false;
        }

        list.remove(0);
        view.set(op.getField(), list);
        return true;
    }

    @SuppressWarnings("unchecked")
    protected boolean applyPopLast(@NonNull RawConfigView view, @NonNull PopLastOperation op) {
        Object currentValue = view.get(op.getField());

        if (currentValue == null) {
            return false;
        }

        if (!(currentValue instanceof List)) {
            throw new IllegalArgumentException("Field is not an array: " + op.getField());
        }

        List<Object> list = new ArrayList<>((List<?>) currentValue);
        if (list.isEmpty()) {
            return false;
        }

        list.remove(list.size() - 1);
        view.set(op.getField(), list);
        return true;
    }

    @SuppressWarnings("unchecked")
    protected boolean applyPull(@NonNull RawConfigView view, @NonNull PullOperation op) {
        Object currentValue = view.get(op.getField());

        if (currentValue == null) {
            return false;
        }

        if (!(currentValue instanceof List)) {
            throw new IllegalArgumentException("Field is not an array: " + op.getField());
        }

        List<Object> list = new ArrayList<>((List<?>) currentValue);
        boolean modified = false;

        // Remove all occurrences using compareEquals for type-safe comparison
        Iterator<Object> iter = list.iterator();
        while (iter.hasNext()) {
            Object item = iter.next();
            if (compareEquals(item, op.getValue())) {
                iter.remove();
                modified = true;
            }
        }

        if (modified) {
            view.set(op.getField(), list);
        }

        return modified;
    }

    @SuppressWarnings("unchecked")
    protected boolean applyPullAll(@NonNull RawConfigView view, @NonNull PullAllOperation op) {
        Object currentValue = view.get(op.getField());

        if (currentValue == null) {
            return false;
        }

        if (!(currentValue instanceof List)) {
            throw new IllegalArgumentException("Field is not an array: " + op.getField());
        }

        List<Object> list = new ArrayList<>((List<?>) currentValue);
        boolean modified = false;

        Iterator<Object> iter = list.iterator();
        while (iter.hasNext()) {
            Object item = iter.next();
            // Check if item matches any value to remove
            for (Object removeValue : op.getValues()) {
                if (compareEquals(item, removeValue)) {
                    iter.remove();
                    modified = true;
                    break;
                }
            }
        }

        if (modified) {
            view.set(op.getField(), list);
        }

        return modified;
    }

    @SuppressWarnings("unchecked")
    protected boolean applyAddToSet(@NonNull RawConfigView view, @NonNull AddToSetOperation op) {
        Object currentValue = view.get(op.getField());

        // Create new list if field doesn't exist
        List<Object> list;
        if (currentValue == null) {
            list = new ArrayList<>();
        } else if (currentValue instanceof List) {
            list = new ArrayList<>((List<?>) currentValue);
        } else {
            throw new IllegalArgumentException("Field is not an array: " + op.getField());
        }

        boolean modified = false;

        // Get values to add
        List<Object> valuesToAdd = op.isSingleValue()
            ? Collections.singletonList(op.getSingleValue())
            : op.getValues();

        // Add each value only if not already present
        for (Object value : valuesToAdd) {
            boolean exists = false;
            for (Object existing : list) {
                if (compareEquals(existing, value)) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                list.add(value);
                modified = true;
            }
        }

        if (modified) {
            view.set(op.getField(), list);
        }

        return modified;
    }
}
