package com.udacity.webcrawler.profiler;

import javax.inject.Inject;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Objects;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

/**
 * Concrete implementation of the Profiler interface.
 * Responsible for:
 *  - Wrapping objects using Java Dynamic Proxy
 *  - Writing profiling results to a file
 */
final class ProfilerImpl implements Profiler {

    // Clock used for consistent time measurement (important for testing)
    private final Clock clock;

    // Thread-safe storage of profiling data
    private final ProfilingState state = new ProfilingState();

    // Time when this profiler run started
    private final ZonedDateTime startTime;

    @Inject
    ProfilerImpl(Clock clock) {
        this.clock = Objects.requireNonNull(clock);
        this.startTime = ZonedDateTime.now(clock);
    }

    /**
     * Checks whether the given interface has at least one @Profiled method.
     * Required by the project specification.
     */
    private boolean hasProfiledMethods(Class<?> klass) {
        return Arrays.stream(klass.getMethods())
                .anyMatch(method -> method.isAnnotationPresent(Profiled.class));
    }

    /**
     * Wraps the given delegate in a dynamic proxy that intercepts method calls.
     */
    @Override
    public <T> T wrap(Class<T> klass, T delegate) {
        Objects.requireNonNull(klass);
        Objects.requireNonNull(delegate);

        // Java Proxy works ONLY with interfaces
        if (!klass.isInterface()) {
            throw new IllegalArgumentException("Can only wrap interfaces");
        }

        // Must have at least one @Profiled method
        if (!hasProfiledMethods(klass)) {
            throw new IllegalArgumentException(
                    klass.getName() + " has no @Profiled methods");
        }

        // Create and return proxy instance
        return klass.cast(
                Proxy.newProxyInstance(
                        klass.getClassLoader(),
                        new Class<?>[]{klass},
                        new ProfilingMethodInterceptor(clock, delegate, state)
                )
        );
    }

    /**
     * Writes profiling data to a file.
     * Appends data if file already exists.
     */
    @Override
    public void writeData(Path path) {
        Objects.requireNonNull(path);
        try (FileWriter writer = new FileWriter(path.toFile(), true)) {
            writeData(writer);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write profiling data", e);
        }
    }

    /**
     * Writes profiling data to a Writer.
     */
    @Override
    public void writeData(Writer writer) throws IOException {
        writer.write("Run at " + RFC_1123_DATE_TIME.format(startTime));
        writer.write(System.lineSeparator());
        state.write(writer);
        writer.write(System.lineSeparator());
    }
}
