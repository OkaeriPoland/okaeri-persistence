package eu.okaeri.persistence.mongo.filter;

import eu.okaeri.persistence.filter.operation.*;
import lombok.NonNull;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Renders update operations to MongoDB update document format.
 */
public class MongoUpdateRenderer {

    /**
     * Render a list of update operations to a MongoDB update document.
     * Groups operations by MongoDB operator ($set, $inc, $push, etc.).
     *
     * @param operations List of update operations
     * @return MongoDB update document
     */
    public Document render(@NonNull List<UpdateOperation> operations) {
        Document updateDoc = new Document();

        // Group operations by type for efficient MongoDB update
        Map<UpdateOperationType, List<UpdateOperation>> grouped = new HashMap<>();
        for (UpdateOperation op : operations) {
            grouped.computeIfAbsent(op.getType(), k -> new ArrayList<>()).add(op);
        }

        // Render each operation type
        for (Map.Entry<UpdateOperationType, List<UpdateOperation>> entry : grouped.entrySet()) {
            switch (entry.getKey()) {
                case SET:
                    this.renderSet(updateDoc, entry.getValue());
                    break;
                case UNSET:
                    this.renderUnset(updateDoc, entry.getValue());
                    break;
                case INCREMENT:
                    this.renderIncrement(updateDoc, entry.getValue());
                    break;
                case MULTIPLY:
                    this.renderMultiply(updateDoc, entry.getValue());
                    break;
                case MIN:
                    this.renderMin(updateDoc, entry.getValue());
                    break;
                case MAX:
                    this.renderMax(updateDoc, entry.getValue());
                    break;
                case CURRENT_DATE:
                    this.renderCurrentDate(updateDoc, entry.getValue());
                    break;
                case PUSH:
                    this.renderPush(updateDoc, entry.getValue());
                    break;
                case POP_FIRST:
                    this.renderPopFirst(updateDoc, entry.getValue());
                    break;
                case POP_LAST:
                    this.renderPopLast(updateDoc, entry.getValue());
                    break;
                case PULL:
                    this.renderPull(updateDoc, entry.getValue());
                    break;
                case PULL_ALL:
                    this.renderPullAll(updateDoc, entry.getValue());
                    break;
                case ADD_TO_SET:
                    this.renderAddToSet(updateDoc, entry.getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported update operation type: " + entry.getKey());
            }
        }

        return updateDoc;
    }

    private void renderSet(Document updateDoc, List<UpdateOperation> operations) {
        Document setDoc = updateDoc.get("$set", Document.class);
        if (setDoc == null) {
            setDoc = new Document();
            updateDoc.put("$set", setDoc);
        }

        for (UpdateOperation op : operations) {
            SetOperation setOp = (SetOperation) op;
            setDoc.put(setOp.getField(), setOp.getValue());
        }
    }

    private void renderUnset(Document updateDoc, List<UpdateOperation> operations) {
        Document unsetDoc = updateDoc.get("$unset", Document.class);
        if (unsetDoc == null) {
            unsetDoc = new Document();
            updateDoc.put("$unset", unsetDoc);
        }

        for (UpdateOperation op : operations) {
            UnsetOperation unsetOp = (UnsetOperation) op;
            unsetDoc.put(unsetOp.getField(), "");
        }
    }

    private void renderIncrement(Document updateDoc, List<UpdateOperation> operations) {
        Document incDoc = updateDoc.get("$inc", Document.class);
        if (incDoc == null) {
            incDoc = new Document();
            updateDoc.put("$inc", incDoc);
        }

        for (UpdateOperation op : operations) {
            IncrementOperation incOp = (IncrementOperation) op;
            incDoc.put(incOp.getField(), incOp.getDelta());
        }
    }

    private void renderMultiply(Document updateDoc, List<UpdateOperation> operations) {
        Document mulDoc = updateDoc.get("$mul", Document.class);
        if (mulDoc == null) {
            mulDoc = new Document();
            updateDoc.put("$mul", mulDoc);
        }

        for (UpdateOperation op : operations) {
            MultiplyOperation mulOp = (MultiplyOperation) op;
            mulDoc.put(mulOp.getField(), mulOp.getFactor());
        }
    }

    private void renderMin(Document updateDoc, List<UpdateOperation> operations) {
        Document minDoc = updateDoc.get("$min", Document.class);
        if (minDoc == null) {
            minDoc = new Document();
            updateDoc.put("$min", minDoc);
        }

        for (UpdateOperation op : operations) {
            MinOperation minOp = (MinOperation) op;
            minDoc.put(minOp.getField(), minOp.getValue());
        }
    }

    private void renderMax(Document updateDoc, List<UpdateOperation> operations) {
        Document maxDoc = updateDoc.get("$max", Document.class);
        if (maxDoc == null) {
            maxDoc = new Document();
            updateDoc.put("$max", maxDoc);
        }

        for (UpdateOperation op : operations) {
            MaxOperation maxOp = (MaxOperation) op;
            maxDoc.put(maxOp.getField(), maxOp.getValue());
        }
    }

    private void renderCurrentDate(Document updateDoc, List<UpdateOperation> operations) {
        Document setDoc = updateDoc.get("$set", Document.class);
        if (setDoc == null) {
            setDoc = new Document();
            updateDoc.put("$set", setDoc);
        }

        for (UpdateOperation op : operations) {
            CurrentDateOperation dateOp = (CurrentDateOperation) op;
            // Use $currentDate for true timestamps, or set Instant.now() directly
            setDoc.put(dateOp.getField(), Instant.now().toString());
        }
    }

    private void renderPush(Document updateDoc, List<UpdateOperation> operations) {
        Document pushDoc = updateDoc.get("$push", Document.class);
        if (pushDoc == null) {
            pushDoc = new Document();
            updateDoc.put("$push", pushDoc);
        }

        for (UpdateOperation op : operations) {
            PushOperation pushOp = (PushOperation) op;
            if (pushOp.isSingleValue()) {
                // Single value: $push: { field: value }
                pushDoc.put(pushOp.getField(), pushOp.getSingleValue());
            } else {
                // Multiple values: $push: { field: { $each: [values] } }
                pushDoc.put(pushOp.getField(), new Document("$each", pushOp.getValues()));
            }
        }
    }

    private void renderPopFirst(Document updateDoc, List<UpdateOperation> operations) {
        Document popDoc = updateDoc.get("$pop", Document.class);
        if (popDoc == null) {
            popDoc = new Document();
            updateDoc.put("$pop", popDoc);
        }

        for (UpdateOperation op : operations) {
            PopFirstOperation popOp = (PopFirstOperation) op;
            popDoc.put(popOp.getField(), -1); // -1 removes first element
        }
    }

    private void renderPopLast(Document updateDoc, List<UpdateOperation> operations) {
        Document popDoc = updateDoc.get("$pop", Document.class);
        if (popDoc == null) {
            popDoc = new Document();
            updateDoc.put("$pop", popDoc);
        }

        for (UpdateOperation op : operations) {
            PopLastOperation popOp = (PopLastOperation) op;
            popDoc.put(popOp.getField(), 1); // 1 removes last element
        }
    }

    private void renderPull(Document updateDoc, List<UpdateOperation> operations) {
        Document pullDoc = updateDoc.get("$pull", Document.class);
        if (pullDoc == null) {
            pullDoc = new Document();
            updateDoc.put("$pull", pullDoc);
        }

        for (UpdateOperation op : operations) {
            PullOperation pullOp = (PullOperation) op;
            pullDoc.put(pullOp.getField(), pullOp.getValue());
        }
    }

    private void renderPullAll(Document updateDoc, List<UpdateOperation> operations) {
        Document pullAllDoc = updateDoc.get("$pullAll", Document.class);
        if (pullAllDoc == null) {
            pullAllDoc = new Document();
            updateDoc.put("$pullAll", pullAllDoc);
        }

        for (UpdateOperation op : operations) {
            PullAllOperation pullAllOp = (PullAllOperation) op;
            pullAllDoc.put(pullAllOp.getField(), pullAllOp.getValues());
        }
    }

    private void renderAddToSet(Document updateDoc, List<UpdateOperation> operations) {
        Document addToSetDoc = updateDoc.get("$addToSet", Document.class);
        if (addToSetDoc == null) {
            addToSetDoc = new Document();
            updateDoc.put("$addToSet", addToSetDoc);
        }

        for (UpdateOperation op : operations) {
            AddToSetOperation addOp = (AddToSetOperation) op;
            if (addOp.isSingleValue()) {
                // Single value: $addToSet: { field: value }
                addToSetDoc.put(addOp.getField(), addOp.getSingleValue());
            } else {
                // Multiple values: $addToSet: { field: { $each: [values] } }
                addToSetDoc.put(addOp.getField(), new Document("$each", addOp.getValues()));
            }
        }
    }
}
