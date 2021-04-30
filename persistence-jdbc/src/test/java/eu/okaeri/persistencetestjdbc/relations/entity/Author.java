package eu.okaeri.persistencetestjdbc.relations.entity;

import eu.okaeri.persistence.document.Document;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = false)
public class Author extends Document {
    private String name;
//    private List<Book> books;
}
