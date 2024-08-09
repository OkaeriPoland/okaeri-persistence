package eu.okaeri.persistencetestjdbc.refs;

import com.zaxxer.hikari.HikariConfig;
import eu.okaeri.configs.json.simple.JsonSimpleConfigurer;
import eu.okaeri.persistence.PersistenceCollection;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.document.ref.EagerRef;
import eu.okaeri.persistence.document.ref.Ref;
import eu.okaeri.persistence.jdbc.H2Persistence;
import eu.okaeri.persistence.jdbc.commons.JdbcHelper;
import eu.okaeri.persistence.repository.RepositoryDeclaration;
import eu.okaeri.persistencetestjdbc.refs.entity.Author;
import eu.okaeri.persistencetestjdbc.refs.entity.Book;
import eu.okaeri.persistencetestjdbc.refs.repository.AuthorRepository;
import eu.okaeri.persistencetestjdbc.refs.repository.BookRepository;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestRefsJdbc {

  private static final long CREATE_USERS = 1_000;

  private DocumentPersistence persistence;
  private PersistenceCollection bookCollection;
  private PersistenceCollection authorCollection;
  private BookRepository bookRepository;
  private AuthorRepository authorRepository;
  private Author author1;
  private Author author2;
  private Author author3;
  private Author author4;
  private Book book1;
  private Book book2;
  private Book book3;

  @BeforeAll
  public void setup() {

    // setup hikari
    final HikariConfig config =
        JdbcHelper.configureHikari("jdbc:h2:mem:test;mode=mysql", "org.h2.Driver");

    // create collections
    this.bookCollection = PersistenceCollection.of(BookRepository.class);
    this.authorCollection = PersistenceCollection.of(AuthorRepository.class);

    // prepare persistence backend
    this.persistence =
        new DocumentPersistence(
            new H2Persistence(PersistencePath.of("storage"), config), JsonSimpleConfigurer::new);
    this.persistence.registerCollection(this.bookCollection);
    this.persistence.registerCollection(this.authorCollection);

    // create repository instance
    this.bookRepository =
        RepositoryDeclaration.of(BookRepository.class)
            .newProxy(this.persistence, this.bookCollection, TestRefsJdbc.class.getClassLoader());
    this.authorRepository =
        RepositoryDeclaration.of(AuthorRepository.class)
            .newProxy(this.persistence, this.authorCollection, TestRefsJdbc.class.getClassLoader());
  }

  @BeforeEach
  public void prepareDb() {

    // flush current
    this.persistence.deleteAll(this.bookCollection);

    // fill with data
    this.author1 = this.authorRepository.findOrCreateByPath(UUID.randomUUID());
    this.author1.setName("Some Author1");
    this.author2 = this.authorRepository.findOrCreateByPath(UUID.randomUUID());
    this.author2.setName("Some Author2");
    this.author3 = this.authorRepository.findOrCreateByPath(UUID.randomUUID());
    this.author3.setName("Some Author3");
    this.author4 = this.authorRepository.findOrCreateByPath(UUID.randomUUID());
    this.author4.setName("Some Author4");

    this.book1 = this.bookRepository.findOrCreateByPath(UUID.randomUUID());
    this.book1.setTitle("Some book: The title of single author book");
    this.book1.setAuthors(Collections.singletonList(EagerRef.of(this.author1)));

    this.book2 = this.bookRepository.findOrCreateByPath(UUID.randomUUID());
    this.book2.setTitle("Some book: The title of two author book");
    this.book2.setAuthors(Arrays.asList(EagerRef.of(this.author1), EagerRef.of(this.author2)));

    this.book3 = this.bookRepository.findOrCreateByPath(UUID.randomUUID());
    this.book3.setTitle("Some book: The title of three author book");
    this.book3.setAuthors(
        Arrays.asList(
            EagerRef.of(this.author2), EagerRef.of(this.author3), EagerRef.of(this.author4)));

    Stream.of(
            this.author1,
            this.author2,
            this.author3,
            this.author4,
            this.book1,
            this.book1,
            this.book3)
        .forEach(Document::save);
  }

  @Test
  public void test_write() {

    this.book3.save();
    System.out.println(this.book3);
    System.out.println(this.book3.saveToString());
    System.out.println();

    final Book book = this.bookRepository.findByPath(this.book3.getPath().toUUID()).get();
    System.out.println(book);

    for (final Ref<Author> authorRef : book.getAuthors()) {
      final Author author = authorRef.get().orElse(null);
      System.out.println(author);
    }
  }
}
