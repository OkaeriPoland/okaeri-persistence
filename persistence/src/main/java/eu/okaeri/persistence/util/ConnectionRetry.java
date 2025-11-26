package eu.okaeri.persistence.util;

import lombok.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Utility for retrying connection establishment with exponential backoff.
 * <p>
 * Global defaults can be configured via system properties:
 * <ul>
 *   <li>{@code okaeri.persistence.connectRetry.initialBackoffMs} - initial backoff in milliseconds (default: 1000)</li>
 *   <li>{@code okaeri.persistence.connectRetry.maxBackoffMs} - maximum backoff in milliseconds (default: 30000)</li>
 *   <li>{@code okaeri.persistence.connectRetry.multiplier} - backoff multiplier (default: 2.0)</li>
 *   <li>{@code okaeri.persistence.connectRetry.timeoutMs} - total timeout in milliseconds, 0 for no timeout (default: 300000)</li>
 * </ul>
 */
public final class ConnectionRetry<T> {

    private static final Logger LOGGER = Logger.getLogger(ConnectionRetry.class.getSimpleName());

    // Global defaults from system properties
    private static final Duration DEFAULT_INITIAL_BACKOFF = Duration.ofMillis(
        Long.parseLong(System.getProperty("okaeri.persistence.connectRetry.initialBackoffMs", "1000")));
    private static final Duration DEFAULT_MAX_BACKOFF = Duration.ofMillis(
        Long.parseLong(System.getProperty("okaeri.persistence.connectRetry.maxBackoffMs", "30000")));
    private static final double DEFAULT_MULTIPLIER =
        Double.parseDouble(System.getProperty("okaeri.persistence.connectRetry.multiplier", "2.0"));
    private static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(
        Long.parseLong(System.getProperty("okaeri.persistence.connectRetry.timeoutMs", "300000")));

    private final String contextName;
    private final Callable<T> connector;
    private Duration initialBackoff = DEFAULT_INITIAL_BACKOFF;
    private Duration maxBackoff = DEFAULT_MAX_BACKOFF;
    private double multiplier = DEFAULT_MULTIPLIER;
    private Duration timeout = DEFAULT_TIMEOUT;
    private Consumer<Integer> onRetry;

    private ConnectionRetry(@NonNull String contextName, @NonNull Callable<T> connector) {
        this.contextName = contextName;
        this.connector = connector;
    }

    public static Builder of(@NonNull String contextName) {
        return new Builder(contextName);
    }

    public ConnectionRetry<T> initialBackoff(@NonNull Duration backoff) {
        this.initialBackoff = backoff;
        return this;
    }

    public ConnectionRetry<T> maxBackoff(@NonNull Duration backoff) {
        this.maxBackoff = backoff;
        return this;
    }

    public ConnectionRetry<T> multiplier(double multiplier) {
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("Multiplier must be >= 1.0");
        }
        this.multiplier = multiplier;
        return this;
    }

    public ConnectionRetry<T> timeout(@NonNull Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public ConnectionRetry<T> noTimeout() {
        this.timeout = Duration.ZERO;
        return this;
    }

    public ConnectionRetry<T> onRetry(@NonNull Consumer<Integer> callback) {
        this.onRetry = callback;
        return this;
    }

    public T connect() {
        Instant startTime = Instant.now();
        Instant deadline = this.timeout.isZero() ? null : startTime.plus(this.timeout);
        Duration currentBackoff = this.initialBackoff;
        int attempt = 0;

        while (true) {
            attempt++;
            try {
                return this.connector.call();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectionException("Connection interrupted for " + this.contextName, e);
            } catch (Exception exception) {
                Duration elapsed = Duration.between(startTime, Instant.now());

                if ((deadline != null) && Instant.now().isAfter(deadline)) {
                    throw new ConnectionException(
                        "Failed to connect to " + this.contextName + " after " + formatDuration(elapsed) +
                            " (" + attempt + " attempts)", exception);
                }

                String cause = (exception.getCause() != null)
                    ? (" caused by " + exception.getCause().getMessage())
                    : "";

                String timeoutInfo = (deadline != null)
                    ? (", timeout in " + formatDuration(Duration.between(Instant.now(), deadline)))
                    : "";

                LOGGER.severe("[" + this.contextName + "] Cannot connect (attempt " + attempt +
                    ", waiting " + formatDuration(currentBackoff) + timeoutInfo + "): " +
                    exception.getMessage() + cause);

                if (this.onRetry != null) {
                    this.onRetry.accept(attempt);
                }

                try {
                    Thread.sleep(currentBackoff.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ConnectionException("Connection interrupted for " + this.contextName, e);
                }

                // Calculate next backoff with exponential increase, capped at maxBackoff
                long nextBackoffMs = (long) (currentBackoff.toMillis() * this.multiplier);
                currentBackoff = Duration.ofMillis(Math.min(nextBackoffMs, this.maxBackoff.toMillis()));
            }
        }
    }

    private static String formatDuration(Duration duration) {
        long seconds = duration.getSeconds();
        if (seconds < 60) {
            return seconds + "s";
        }
        long minutes = seconds / 60;
        long remainingSeconds = seconds % 60;
        if (remainingSeconds == 0) {
            return minutes + "m";
        }
        return minutes + "m" + remainingSeconds + "s";
    }

    /**
     * Builder for creating ConnectionRetry instances.
     */
    public static final class Builder {
        private final String contextName;

        private Builder(@NonNull String contextName) {
            this.contextName = contextName;
        }

        public <T> ConnectionRetry<T> connector(@NonNull Callable<T> connector) {
            return new ConnectionRetry<>(this.contextName, connector);
        }
    }

    /**
     * Exception thrown when connection establishment fails.
     */
    public static class ConnectionException extends RuntimeException {
        public ConnectionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
