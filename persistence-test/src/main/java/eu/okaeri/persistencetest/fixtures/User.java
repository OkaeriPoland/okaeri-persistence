package eu.okaeri.persistencetest.fixtures;

import eu.okaeri.persistence.document.Document;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class User extends Document {

    public enum Status {
        ACTIVE, INACTIVE, PENDING, BANNED
    }

    private String name;
    private int exp;
    private boolean verified;
    private List<String> tags;
    private List<Integer> scores;
    private Status status;
    private UUID referenceId;

    public User(String name, int exp) {
        this.name = name;
        this.exp = exp;
    }

    public User(String name, int exp, boolean verified) {
        this.name = name;
        this.exp = exp;
        this.verified = verified;
    }

    public User(String name, int exp, Status status) {
        this.name = name;
        this.exp = exp;
        this.status = status;
    }

    public User(String name, int exp, UUID referenceId) {
        this.name = name;
        this.exp = exp;
        this.referenceId = referenceId;
    }

    public UUID getId() {
        return this.getPath().toUUID();
    }
}
