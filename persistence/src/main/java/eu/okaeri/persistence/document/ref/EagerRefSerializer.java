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
    public boolean supports(@NonNull Class clazz) {
        return EagerRef.class.isAssignableFrom(clazz);
    }

    @Override
    public void serialize(@NonNull EagerRef eagerRef, @NonNull SerializationData serializationData, @NonNull GenericsDeclaration genericsDeclaration) {
        serializationData.add("_id", eagerRef.getId().getValue());
        serializationData.add("_collection", eagerRef.getCollection().getValue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public EagerRef<? extends Document> deserialize(@NonNull DeserializationData deserializationData, @NonNull GenericsDeclaration genericsDeclaration) {

        PersistencePath id = PersistencePath.of(deserializationData.get("_id", String.class));
        PersistencePath collection = PersistencePath.of(deserializationData.get("_collection", String.class));

        GenericsDeclaration subtype = genericsDeclaration.getSubtypeAtOrNull(0);
        if (subtype == null) throw new IllegalArgumentException("cannot create LazyRef from " + genericsDeclaration);
        Class<? extends Document> type = (Class<? extends Document>) subtype.getType();

        EagerRef<Document> ref = new EagerRef<>(id, collection, type, null, false, this.persistence);
        ref.fetch();

        return ref;
    }
}
