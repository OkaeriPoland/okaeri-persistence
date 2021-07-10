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
public class LazyRefSerializer implements ObjectSerializer<Ref<? extends Document>> {

    private final DocumentPersistence persistence;

    @Override
    public boolean supports(@NonNull Class clazz) {
        return LazyRef.class.isAssignableFrom(clazz);
    }

    @Override
    public void serialize(@NonNull Ref lazyRef, @NonNull SerializationData serializationData) {
        serializationData.add("_id", lazyRef.getId().getValue());
        serializationData.add("_collection", lazyRef.getCollection().getValue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Ref<? extends Document> deserialize(@NonNull DeserializationData deserializationData, @NonNull GenericsDeclaration genericsDeclaration) {

        PersistencePath id = PersistencePath.of(deserializationData.get("_id", String.class));
        PersistencePath collection = PersistencePath.of(deserializationData.get("_collection", String.class));

        GenericsDeclaration subtype = genericsDeclaration.getSubtypeAtOrNull(0);
        if (subtype == null) throw new IllegalArgumentException("cannot create LazyRef from " + genericsDeclaration);
        Class<? extends Document> type = (Class<? extends Document>) subtype.getType();

        return new LazyRef<>(id, collection, type, null, false, this.persistence);
    }
}