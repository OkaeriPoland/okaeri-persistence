package eu.okaeri.persistencetestjdbc.entity;

import eu.okaeri.persistence.document.Document;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.UUID;

@Data
@EqualsAndHashCode(callSuper = false)
public class User extends Document {
    private UUID id;
    private String shortId;
    private UserMeta meta;
}
