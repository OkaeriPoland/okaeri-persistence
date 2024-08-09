package eu.okaeri.persistence.document.ref;

import eu.okaeri.configs.schema.GenericsDeclaration;
import eu.okaeri.configs.serdes.DeserializationData;
import eu.okaeri.configs.serdes.ObjectSerializer;
import eu.okaeri.configs.serdes.SerializationData;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class EagerRefSerializer implements ObjectSerializer<EagerRef<? extends Document>> {

  private final DocumentPersistence persistence;

  @Override
  public boolean supports(@NonNull final Class clazz) {
    return EagerRef.class.isAssignableFrom(clazz);
  }

  @Override
  public void serialize(
      @NonNull final EagerRef eagerRef,
      @NonNull final SerializationData serializationData,
      @NonNull final GenericsDeclaration genericsDeclaration) {
    serializationData.add("_id", eagerRef.getId().getValue());
    serializationData.add("_collection", eagerRef.getCollection().getValue());
  }

  @Override
  @SuppressWarnings("unchecked")
  public EagerRef<? extends Document> deserialize(
      @NonNull final DeserializationData deserializationData,
      @NonNull final GenericsDeclaration genericsDeclaration) {

    final PersistencePath id = PersistencePath.of(deserializationData.get("_id", String.class));
    final PersistencePath collection =
        PersistencePath.of(deserializationData.get("_collection", String.class));

    final GenericsDeclaration subtype = genericsDeclaration.getSubtypeAtOrNull(0);
    if (subtype == null) {
      throw new IllegalArgumentException("cannot create LazyRef from " + genericsDeclaration);
    }
    final Class<? extends Document> type = (Class<? extends Document>) subtype.getType();

    final EagerRef<Document> ref =
        new EagerRef<>(id, collection, type, null, false, this.persistence);
    ref.fetch();

    return ref;
  }
}
