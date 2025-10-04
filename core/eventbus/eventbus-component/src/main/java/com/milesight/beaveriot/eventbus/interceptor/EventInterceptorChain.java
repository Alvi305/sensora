package com.milesight.beaveriot.eventbus.interceptor;

import com.milesight.beaveriot.base.utils.TypeUtil;
import com.milesight.beaveriot.eventbus.api.Event;
import com.milesight.beaveriot.eventbus.api.EventInterceptor;
import com.milesight.beaveriot.eventbus.api.EventResponse;
import com.milesight.beaveriot.eventbus.api.IdentityKey;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Builds and executes a type-specific chain of EventInterceptor(s).
 * - At initialization, it discovers EventInterceptor beans and groups them by the Event type they target.
 * - At runtime, it runs before/after hooks around event handling for matching interceptors.
 * @author leon
 * Notes:
 * - Matching is based on the event's exact runtime class (no subclass polymorphism).
 * - Before hooks run in registration order, after hooks run in reverse order.
 * - Any before hook can short-circuit handling by returning false.
 */
public class EventInterceptorChain<T extends Event<? extends IdentityKey>> implements InitializingBean {

    // Spring provider for lazily retrieving all EventInterceptor<T> beans in order
    private ObjectProvider<EventInterceptor<T>> objectProvider;

    public EventInterceptorChain(ObjectProvider<EventInterceptor<T>> objectProvider) {
        this.objectProvider = objectProvider;
    }

    private Map<Class<?>, List<EventInterceptor<T>>> interceptorMaps = new HashMap<>();

    /**
     * Runs "before" hooks for the given event.
     * - If no interceptors are registered for the event class, allow processing (return true).
     * - For each interceptor that matches(event), call beforeHandle.
     * - If any beforeHandle returns false, stop and return false to indicate the caller should skip handling.
     */
    public boolean preHandle(T event) {
        if (ObjectUtils.isEmpty(interceptorMaps) || !interceptorMaps.containsKey(event.getClass())) {
            return true;
        }

        for (EventInterceptor<T> interceptor : interceptorMaps.get(event.getClass())) {
            if (interceptor.match(event)) {
                // short-circuit
                boolean isContinue = interceptor.beforeHandle(event);
                if (!isContinue) {
                    return false;
                }
            }
        }
        // All applicable interceptors allowed processing to continue
        return true;
    }

    /**
     * Runs "after" hooks for the given event, in reverse order (LIFO).
     * - If no interceptors registered for the event class:
     *   - If an exception was thrown during handling, rethrow it (so errors aren't swallowed).
     *   - Otherwise, just return.
     * - For registered interceptors:
     *   - Only run those whose match(event) is true.
     *   - Run in reverse registration order, typical for "unwinding" behavior.
     * - If an exception exists and no interceptor matched, rethrow it.
     */
    public void afterHandle(T event, EventResponse eventResponse, Exception exception) throws Exception {

        if (ObjectUtils.isEmpty(interceptorMaps) || !interceptorMaps.containsKey(event.getClass())) {
            if (exception != null) {
                throw exception;
            } else {
                return;
            }
        }

        boolean matchInterceptor = false;

        // Execute in reverse order to mirror stack unwinding
        for (int i = interceptorMaps.get(event.getClass()).size() - 1; i >= 0; i--) {
            EventInterceptor<T> interceptor = interceptorMaps.get(event.getClass()).get(i);
            if (interceptor.match(event)) {
                matchInterceptor = true;
                interceptor.afterHandle(event, eventResponse, exception);
            }
        }

        // If any interceptor throws an exception, we need to rethrow it
        if (exception != null && !matchInterceptor) {
            throw exception;
        }
    }


    /**
     * Lifecycle hook called by Spring after dependencies are set.
     * - Discovers all EventInterceptor<T> beans in ordered sequence.
     * - Resolves their target Event type (generic parameter) even if proxied by Spring AOP.
     * - Groups them into interceptorMaps by concrete Event class.
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        if (objectProvider != null) {
            objectProvider.orderedStream().forEach(interceptor -> {

                // AopUtils ensures the target class is inspected, not the proxy
                Type eventType = TypeUtil.getTypeArgument(AopUtils.getTargetClass(interceptor), 0);

                // Fail fast if the interceptor didn't declare a concrete Event subtype
                Assert.isTrue(eventType != null , "EventInterceptor must have a class type that is a subclass of Event, interceptor: " + interceptor.getClass().getName());

                // Bucket by the exact Event class
                @SuppressWarnings("unchecked")
                Class<?> key = (Class<?>) eventType;
                List<EventInterceptor<T>> eventInterceptors =
                        interceptorMaps.computeIfAbsent(key, k -> new ArrayList<>());

                // Preserve discovery order (affects before/after execution)
                eventInterceptors.add(interceptor);
            });
        }
    }
}
