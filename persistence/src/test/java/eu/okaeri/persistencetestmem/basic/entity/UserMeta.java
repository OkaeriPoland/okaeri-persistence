package eu.okaeri.persistencetestmem.basic.entity;

import eu.okaeri.persistence.document.Document;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class UserMeta extends Document {
  private String name;
  private String fullName;
  private String description;
}
