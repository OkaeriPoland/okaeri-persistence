package eu.okaeri.persistencetest.fixtures;

import eu.okaeri.persistence.document.Document;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class User extends Document {

    private String name;
    private int exp;
    private boolean verified;

    public User(String name, int exp) {
        this.name = name;
        this.exp = exp;
    }
}
