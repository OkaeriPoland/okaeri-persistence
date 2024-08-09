package eu.okaeri.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import eu.okaeri.configs.annotation.CustomKey;
import eu.okaeri.configs.json.gson.JsonGsonConfigurer;
import eu.okaeri.configs.serdes.commons.SerdesCommons;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import eu.okaeri.persistence.mongo.MongoPersistence;
import eu.okaeri.persistence.repository.DocumentRepository;
import eu.okaeri.persistence.repository.RepositoryDeclaration;
import eu.okaeri.persistence.repository.annotation.DocumentCollection;
import eu.okaeri.persistence.repository.annotation.DocumentIndex;
import eu.okaeri.persistence.repository.annotation.DocumentPath;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Logger;
import lombok.Data;
import lombok.EqualsAndHashCode;

final class IgnoreCaseTest {

  private IgnoreCaseTest() {}

  public static void main(final String[] args) {
    final MongoClient mongoClient =
        MongoClients.create(
            "mongodb://127.0.0.1:27017/vanitymc?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.2.12");

    final DocumentPersistence persistence =
        new DocumentPersistence(
            new MongoPersistence(PersistencePath.of("template"), mongoClient, "template"),
            JsonGsonConfigurer::new,
            new SerdesCommons());

    final PersistenceCollection persistenceCollection =
        PersistenceCollection.of(ProfileRepository.class);
    persistence.registerCollection(persistenceCollection);

    final ProfileRepository profileRepository =
        RepositoryDeclaration.of(ProfileRepository.class)
            .newProxy(persistence, persistenceCollection, IgnoreCaseTest.class.getClassLoader());

    final Collection<UUID> uuids = new ArrayList<>();
    final Collection<String> names = new ArrayList<>();

    for (int i = 0; i < 16; i++) {
      final UUID uuid = UUID.randomUUID();
      final String formatted = String.format("Profile%d", i);
      final Profile profile = profileRepository.findOrCreate(uuid, formatted);
      Logger.getGlobal().info(String.format("Created %s", i));

      profile.save();

      uuids.add(uuid);
      names.add(profile.getName());

      Logger.getGlobal().info(String.format("Saved %s", i));
    }

    uuids.stream()
        .map(profileRepository::findByPath)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(it -> Logger.getGlobal().info("by uuid: " + it.saveToString()));

    names.stream()
        .map(String::toLowerCase)
        .map(profileRepository::findByName)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(it -> Logger.getGlobal().info("by name: " + it.saveToString()));
  }

  @DocumentCollection(path = "profiles", keyLength = 36, indexes = @DocumentIndex(path = "name", maxLength = 16))
  public interface ProfileRepository extends DocumentRepository<UUID, Profile> {

    @DocumentPath(value = "name", ignoreCase = true)
    Optional<Profile> findByName(final String name);

    default Profile findOrCreate(final UUID uuid, final String profileName) {

      final Profile profile = this.findOrCreateByPath(uuid);
      if (profileName != null) {
        profile.setName(profileName);
      }

      return profile;
    }
  }

  @Data
  @EqualsAndHashCode(callSuper = false)
  public static class Profile extends Document {

    @CustomKey("name")
    private String name;

    public UUID getUniqueId() {
      return this.getPath().toUUID();
    }
  }
}
