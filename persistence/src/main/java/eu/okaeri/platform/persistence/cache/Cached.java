package eu.okaeri.platform.persistence.cache;

import lombok.*;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

@ToString
@EqualsAndHashCode
public abstract class Cached<T> {

    public static <A> Cached<A> of(Supplier<A> supplier) {
        return of(null, supplier);
    }

    public static <A> Cached<A> of(Duration ttl, Supplier<A> supplier) {
        return new Cached<A>() {

            private Instant loaded;

            @Override
            public A get() {

                if (this.getValue() == null) {
                    this.loaded = Instant.now();
                    return this.update();
                }

                if (this.getTtl() == null) {
                    return this.getValue();
                }

                Duration timeLived = Duration.between(this.loaded, Instant.now());
                if (timeLived.compareTo(this.getTtl()) > 0) {
                    this.loaded = Instant.now();
                    return this.update();
                }

                return this.getValue();
            }

            @Override
            public A resolve() {
                return supplier.get();
            }
        };
    }

    @Getter @Setter private Duration ttl;
    @Getter private T value;

    public T get() {
        return (this.value == null)
                ? this.update()
                : this.value;
    }

    public T update() {
        this.value = this.resolve();
        return this.value;
    }

    public abstract T resolve();
}
