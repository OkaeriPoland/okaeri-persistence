package eu.okaeri.persistence.document.ref;

import eu.okaeri.configs.schema.GenericsDeclaration;
import eu.okaeri.configs.serdes.*;
import eu.okaeri.persistence.PersistencePath;
import eu.okaeri.persistence.document.Document;
import eu.okaeri.persistence.document.DocumentPersistence;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor
public class LazyRefSerializer implements ObjectSerializer<LazyRef<? extends Document>> {

    private final DocumentPersistence persistence;

    @Override
    public boolean supports(Class clazz) {
        return LazyRef.class.isAssignableFrom(clazz);
    }

    @Override
    public void serialize(LazyRef lazyRef, SerializationData serializationData) {
        serializationData.add("_id", lazyRef.getId().getValue());
        serializationData.add("_collection", lazyRef.getCollection().getValue());
    }

    @Override
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public LazyRef<? extends Document> deserialize(DeserializationData deserializationData, GenericsDeclaration genericsDeclaration) {

        PersistencePath id = PersistencePath.of(deserializationData.get("_id", String.class));
        PersistencePath collection = PersistencePath.of(deserializationData.get("_collection", String.class));

        GenericsDeclaration subtype = genericsDeclaration.getSubtypeAtOrNull(0);
        if (subtype == null) throw new IllegalArgumentException("cannot create LazyRef from " + genericsDeclaration);
        Class<? extends Document> type = (Class<? extends Document>) subtype.getType();

        return new LazyRef<>(id, collection, type, null, false, this.persistence);
    }
}