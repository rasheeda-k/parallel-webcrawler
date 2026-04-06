package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Intercepts method calls and records execution time
 * for methods annotated with @Profiled.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {

    private final Clock clock;
    private final Object delegate;
    private final ProfilingState state;

    ProfilingMethodInterceptor(Clock clock, Object delegate, ProfilingState state) {
        this.clock = Objects.requireNonNull(clock);
        this.delegate = Objects.requireNonNull(delegate);
        this.state = Objects.requireNonNull(state);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        if (method.getDeclaringClass() == Object.class) {
            return method.invoke(delegate, args);
        }

        boolean profiled = method.isAnnotationPresent(Profiled.class);
        Instant startTime = profiled ? clock.instant() : null;

        try {
            // Call the actual method
            return method.invoke(delegate, args);
        } catch (InvocationTargetException e) {
            // Always rethrow the ORIGINAL exception
            throw e.getCause();
        } finally {
            // Record execution time EVEN IF exception occurs
            if (profiled) {
                Instant endTime = clock.instant();
                state.record(
                        delegate.getClass(),
                        method,
                        Duration.between(startTime, endTime)
                );
            }
        }
    }
}
