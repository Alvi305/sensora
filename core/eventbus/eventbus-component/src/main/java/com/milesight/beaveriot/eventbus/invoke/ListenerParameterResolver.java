package com.milesight.beaveriot.eventbus.invoke;

import com.milesight.beaveriot.base.exception.ConfigurationException;
import com.milesight.beaveriot.context.integration.model.ExchangePayload;
import com.milesight.beaveriot.context.integration.model.event.ExchangeEvent;
import com.milesight.beaveriot.context.integration.proxy.ExchangePayloadProxy;
import com.milesight.beaveriot.eventbus.api.Event;
import com.milesight.beaveriot.eventbus.api.IdentityKey;
import lombok.*;
import org.springframework.util.Assert;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

/**
 *  @author leon
 * Resolves listener method parameter details and adapts events accordingly.
 * Responsibilities:
 * - If a listener expects a specific ExchangePayload subtype, build an ExchangeEvent
 *   with a payload filtered to the matched keys and proxied to the expected subtype.
 * - Determine the concrete Event class a listener method handles (for bucketing in caches).
 * - Extract the generic payload type (IdentityKey subtype) from Event<T> parameters.
 */
public class ListenerParameterResolver {


    /**
     * Adapt a generic Event<? extends ExchangePayload> to a concrete ExchangeEvent with:
     * - payload filtered down to only matchMultiKeys
     * - payload proxied as the requested parameterType (ExchangePayload subtype)
     *
     * @param parameterType  expected ExchangePayload subtype on the listener method
     * @param event          original event (with ExchangePayload payload)
     * @param matchMultiKeys the subset of payload keys that matched the listener
     * @return a new ExchangeEvent with same event type and adapted payload
     */
    public ExchangeEvent resolveEvent(@NonNull Class<? extends ExchangePayload> parameterType, Event<? extends ExchangePayload> event, String[] matchMultiKeys) {
        //filter key
        ExchangePayload payload = event.getPayload();
        ExchangePayload newPayload = ExchangePayload.createFrom(payload, List.of(matchMultiKeys));

        newPayload = new ExchangePayloadProxy<>(newPayload, parameterType).proxy();
        return ExchangeEvent.of(event.getEventType(), newPayload);
    }

    /**
     * Determine the concrete Event class a listener method handles.
     *
     * Rules:
     * - The method must have at least 1 parameter.
     * - The first parameter must be Event or Event<T>.
     * - If the parameter is an interface (e.g., Event<T>) and T is ExchangePayload or its subtype,
     *   we consider the concrete event class to be ExchangeEvent (used at runtime).
     * - Otherwise, we return the parameter's class itself.
     *
     * This is used by EventBusDispatcher to bucket listeners per event class.
     */
    public <T extends Event<? extends IdentityKey>> Class<T> resolveActualEventType(Method method) {
        if (method.getParameterTypes().length == 0) {
            throw new ConfigurationException("EventBus method param-number invalid, method:" + method);
        }

        Class<?> clazz = method.getParameterTypes()[0];

        Assert.isTrue(Event.class.isAssignableFrom(clazz), "The EventBus method input parameter must be an implementation of Event, or an implementation of Event containing generic parameters, method:" + method.toGenericString());

        Class<?> eventClass = clazz;
        if (clazz.isInterface()) {
            Class<?> actualTypeArgument = resolveParameterTypes(method);
            if (ExchangePayload.class.isAssignableFrom(actualTypeArgument)) {
                eventClass = ExchangeEvent.class;
            }
        }

        Assert.notNull(eventClass, "The EventBus method input parameter must be an implementation of Event, or an implementation of Event containing generic parameters, method:" + method.toGenericString());

        //noinspection unchecked
        return (Class<T>) eventClass;
    }

    /**
     * Extract the generic parameter type T from a method parameter declared as Event<T>.
     * Example:
     *   void handle(Event<MyPayload> event)
     *   -> returns MyPayload.class (must implement IdentityKey)
     * If the first parameter is not parameterized or not Event<T> with a single type arg,
     * returns null.
     */
    public Class<?> resolveParameterTypes(Method method) {

        if (method.getGenericParameterTypes()[0] instanceof ParameterizedType parameterizedType) {

            Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
            if (actualTypeArguments.length == 1) {
                Class<?> actualTypeArgument = (Class<?>) actualTypeArguments[0];
                Assert.isTrue(IdentityKey.class.isAssignableFrom(actualTypeArgument), "parameter type must be an implementation of IdentityKey, method:" + method.toGenericString());
                return actualTypeArgument;
            }
        }
        return null;
    }


}
