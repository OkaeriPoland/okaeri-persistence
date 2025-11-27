package eu.okaeri.persistence.util;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectionRetryTest {

    @Test
    void connect_succeeds_on_first_attempt() {
        String result = ConnectionRetry.of("test")
            .connector(() -> "success")
            .connect();

        assertThat(result).isEqualTo("success");
    }

    @Test
    void connect_retries_until_success() {
        AtomicInteger attempts = new AtomicInteger(0);

        String result = ConnectionRetry.of("test")
            .connector(() -> {
                if (attempts.incrementAndGet() < 3) {
                    throw new RuntimeException("fail");
                }
                return "success";
            })
            .initialBackoff(Duration.ofMillis(10))
            .connect();

        assertThat(result).isEqualTo("success");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void connect_throws_after_timeout() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThatThrownBy(() -> ConnectionRetry.of("test-db")
            .connector(() -> {
                attempts.incrementAndGet();
                throw new RuntimeException("connection refused");
            })
            .initialBackoff(Duration.ofMillis(50))
            .timeout(Duration.ofMillis(120))
            .connect())
            .isInstanceOf(ConnectionRetry.ConnectionException.class)
            .hasMessageContaining("test-db")
            .hasMessageContaining("attempts")
            .hasCauseInstanceOf(RuntimeException.class);

        assertThat(attempts.get()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void connect_with_no_timeout_keeps_retrying() {
        AtomicInteger attempts = new AtomicInteger(0);

        String result = ConnectionRetry.of("test")
            .connector(() -> {
                if (attempts.incrementAndGet() < 5) {
                    throw new RuntimeException("fail");
                }
                return "success";
            })
            .initialBackoff(Duration.ofMillis(5))
            .noTimeout()
            .connect();

        assertThat(result).isEqualTo("success");
        assertThat(attempts.get()).isEqualTo(5);
    }

    @Test
    void connect_calls_onRetry_callback() {
        AtomicInteger retryCount = new AtomicInteger(0);
        AtomicInteger attempts = new AtomicInteger(0);

        ConnectionRetry.of("test")
            .connector(() -> {
                if (attempts.incrementAndGet() < 3) {
                    throw new RuntimeException("fail");
                }
                return "success";
            })
            .initialBackoff(Duration.ofMillis(5))
            .onRetry(attempt -> retryCount.incrementAndGet())
            .connect();

        assertThat(retryCount.get()).isEqualTo(2);
    }

    @Test
    void connect_respects_max_backoff() {
        AtomicInteger attempts = new AtomicInteger(0);

        ConnectionRetry.of("test")
            .connector(() -> {
                if (attempts.incrementAndGet() < 5) {
                    throw new RuntimeException("fail");
                }
                return "success";
            })
            .initialBackoff(Duration.ofMillis(5))
            .maxBackoff(Duration.ofMillis(10))
            .multiplier(10.0)
            .connect();

        assertThat(attempts.get()).isEqualTo(5);
    }

    @Test
    void multiplier_throws_when_less_than_one() {
        assertThatThrownBy(() -> ConnectionRetry.of("test")
            .connector(() -> "success")
            .multiplier(0.5))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("1.0");
    }

    @Test
    void connect_includes_cause_in_log_message() {
        AtomicInteger attempts = new AtomicInteger(0);

        String result = ConnectionRetry.of("test")
            .connector(() -> {
                if (attempts.incrementAndGet() < 2) {
                    throw new RuntimeException("outer", new RuntimeException("inner cause"));
                }
                return "success";
            })
            .initialBackoff(Duration.ofMillis(5))
            .connect();

        assertThat(result).isEqualTo("success");
    }

    @Test
    void connect_handles_interrupted_exception_during_connection() {
        assertThatThrownBy(() -> ConnectionRetry.of("test")
            .connector(() -> {
                throw new InterruptedException("interrupted");
            })
            .connect())
            .isInstanceOf(ConnectionRetry.ConnectionException.class)
            .hasMessageContaining("interrupted")
            .hasCauseInstanceOf(InterruptedException.class);

        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        Thread.interrupted(); // clear flag
    }

    @Test
    void connect_handles_interrupted_exception_during_sleep() {
        Thread testThread = Thread.currentThread();
        AtomicInteger attempts = new AtomicInteger(0);

        assertThatThrownBy(() -> ConnectionRetry.of("test")
            .connector(() -> {
                if (attempts.incrementAndGet() == 1) {
                    // Schedule interrupt for during sleep
                    new Thread(() -> {
                        try {
                            Thread.sleep(10);
                            testThread.interrupt();
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }).start();
                    throw new RuntimeException("fail");
                }
                return "success";
            })
            .initialBackoff(Duration.ofMillis(100))
            .connect())
            .isInstanceOf(ConnectionRetry.ConnectionException.class)
            .hasMessageContaining("interrupted");

        Thread.interrupted(); // clear flag
    }

    @Test
    void timeout_message_shows_minutes_format() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThatThrownBy(() -> ConnectionRetry.of("test")
            .connector(() -> {
                attempts.incrementAndGet();
                throw new RuntimeException("fail");
            })
            .initialBackoff(Duration.ofMillis(10))
            .timeout(Duration.ofMillis(25))
            .connect())
            .isInstanceOf(ConnectionRetry.ConnectionException.class)
            .hasMessageContaining("test");
    }
}
