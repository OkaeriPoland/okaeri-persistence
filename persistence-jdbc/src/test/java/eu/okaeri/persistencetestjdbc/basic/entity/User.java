package eu.okaeri.persistencetestjdbc.basic.entity;

import eu.okaeri.persistence.document.Document;
import java.util.UUID;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class User extends Document {
  private UUID id;
  private String shortId;
  private UserMeta meta;
}
