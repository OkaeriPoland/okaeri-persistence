package eu.okaeri.persistencetestmem.basic.entity;

import eu.okaeri.configs.annotation.Exclude;
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

  // in-memory documents can store unserializable entities
  // it is however required to mark them as excluded
  // as indexing requires document deconstruction
  @Exclude private UnknownEntity entity;
}
