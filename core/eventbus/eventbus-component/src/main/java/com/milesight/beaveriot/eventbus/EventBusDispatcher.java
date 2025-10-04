package com.milesight.beaveriot.eventbus;

import com.milesight.beaveriot.base.exception.EventBusExecutionException;
import com.milesight.beaveriot.eventbus.annotations.EventSubscribe;
import com.milesight.beaveriot.eventbus.api.Event;
import com.milesight.beaveriot.eventbus.api.EventResponse;
import com.milesight.beaveriot.eventbus.api.IdentityKey;
import com.milesight.beaveriot.eventbus.configuration.ExecutionOptions;
import com.milesight.beaveriot.eventbus.interceptor.EventInterceptorChain;
import com.milesight.beaveriot.eventbus.invoke.EventInvoker;
import com.milesight.beaveriot.eventbus.invoke.EventSubscribeInvoker;
import com.milesight.beaveriot.eventbus.invoke.ListenerParameterResolver;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;



/**
 * Central dispatcher for the EventBus.
 *
 * Responsibilities:
 * - Register listeners (annotation-based and dynamic) into in-memory caches.
 * - Resolve which listeners match an incoming event (by eventType and payloadKey pattern).
 * - Execute matching listeners:
 *     publish(): asynchronously via an Executor.
 *     handle(): synchronously, aggregating EventResponse and errors.
 * - Surround execution with interceptors (preHandle/afterHandle).
 *
 * Listener registration (annotation-based):
 *   Developer method + @EventSubscribe
 *     -> registerAnnotationSubscribe(...)
 *     -> resolve eventClass and parameterType
 *     -> build ListenerCacheKey (payloadKeyExpression, eventType[])
 *     -> annotationSubscribeCache[eventClass][ListenerCacheKey] += EventSubscribeInvoker
 *
 * Event dispatch (publish/handle):
 *   Event arrives
 *     -> createInvocationHolders(event)
 *     -> from annotation cache:
 *        for each ListenerCacheKey:
 *         if eventType matches AND payloadKeyExpression matches any key in payloadKey:
 *           add InvocationHolder(invoker, matchedKeys, event)
 *     -> from dynamic cache: same matching process
 *     -> if holders not empty: preHandle via EventInterceptorChain; if false -> stop
 *     -> execute invokers
 *        publish: async via Executor
 *        handle:  sync, gather EventResponse and exceptions
 *     -> handle only: afterHandle via EventInterceptorChain (with exception if any)
 *     -> return EventResponse (handle)
 *
 * @author leon
 */
@Slf4j
public class EventBusDispatcher<T extends Event<? extends IdentityKey>> implements EventBus<T>, ApplicationContextAware {

    // Cache of annotation-based subscriptions:
    // For each event class, map a ListenerCacheKey (filters) -> list of invokers (methods)
    private final Map<Class<T>, Map<ListenerCacheKey, List<EventInvoker<T>>>> annotationSubscribeCache = new ConcurrentHashMap<>();

    // Cache of dynamic subscriptions:
    // For each event class, map a unique key -> single invoker
    private final Map<Class<T>, Map<UniqueListenerCacheKey, EventInvoker<T>>> dynamicSubscribeCache = new ConcurrentHashMap<>();
    private ExecutionOptions executionOptions;
    private ListenerParameterResolver parameterResolver;
    private ApplicationContext applicationContext;

    private EventInterceptorChain<T> eventInterceptorChain;

    public EventBusDispatcher(ExecutionOptions executionOptions, ListenerParameterResolver parameterResolver, EventInterceptorChain<T> eventInterceptorChain) {
        this.executionOptions = executionOptions;
        this.parameterResolver = parameterResolver;
        this.eventInterceptorChain = eventInterceptorChain;
    }

    /**
     * Asynchronously publish an event to all matching listeners.
     * - Builds invocation list
     * - Runs preHandle interceptors (if any listener exists)
     * - Submits each invocation to the Executor
     * - Errors are logged (not rethrown) since it's async
     */
    @Override
    public void publish(T event) {

        List<InvocationHolder> invocationHolders = createInvocationHolders(event);

        Executor executor = getEventBusExecutor();

        log.debug("Ready to publish EventBus events {}, hit Invocation size：{}", event.getEventType(), invocationHolders.size());

        // If there are listeners, run pre-interceptor check. If false, skip publishing.
        if (!ObjectUtils.isEmpty(invocationHolders)) {
            boolean isContinue = eventInterceptorChain.preHandle(event);
            if (!isContinue) {
                log.warn("EventInterceptor preHandle return false, skip publish event: {}", event.getPayloadKey());
                return;
            }
        }

        // Submit each invocation to the executor
        invocationHolders.forEach(invocationHolder -> executor.execute(() -> {
            try {
                log.debug("Publish EventBus events, match invocation key: {}, invoker: {}", invocationHolder.getMatchMultiKeys(), invocationHolder.getInvoker());
                invocationHolder.getInvoker().invoke(event, invocationHolder.getMatchMultiKeys());
            } catch (Throwable e) {
                log.error("EventSubscribe method invoke error, method: {}", e);
            }
        }));
    }

    /**
     * Resolve the Executor bean by name from ExecutionOptions.
     * Throws if the bean name is missing or not found.
     */
    private Executor getEventBusExecutor() {
        String eventBusTaskExecutor = executionOptions.getEventBusTaskExecutor();
        if (ObjectUtils.isEmpty(eventBusTaskExecutor) || !applicationContext.containsBean(eventBusTaskExecutor)) {
            throw new EventBusExecutionException("EventBusTaskExecutor not found");
        }
        return (Executor) applicationContext.getBean(eventBusTaskExecutor);
    }


    /**
     * Synchronously handle an event.
     * - Builds invocation list
     * - Runs preHandle interceptors
     * - Invokes listeners on the calling thread
     * - Aggregates EventResponse from listeners and collects exceptions
     * - Calls afterHandle with aggregated outcome
     * - Returns the combined EventResponse
     */
    @SneakyThrows
    @Override
    public EventResponse handle(T event) {

        EventResponse eventResponses = new EventResponse();

        List<Throwable> causes = new ArrayList<>();

        List<InvocationHolder> invocationHolders = createInvocationHolders(event);

        log.debug("Ready to handle EventBus events, hit Invocation size：{}", invocationHolders.size());

        if (!ObjectUtils.isEmpty(invocationHolders)) {
            boolean isContinue = eventInterceptorChain.preHandle(event);
            if (!isContinue) {
                log.warn("EventInterceptor preHandle return false, skip handle event: {}", event.getPayloadKey());
                return eventResponses;
            }
        }

        invocationHolders.forEach(invocationHolder -> {
            try {
                log.debug("Handle EventBus events, match invocation key: {}, invoker: {}", invocationHolder.getMatchMultiKeys(), invocationHolder.getInvoker());
                Object invoke = invocationHolder.getInvoker().invoke(event, invocationHolder.getMatchMultiKeys());
                if (eventResponses != null && invoke instanceof EventResponse eventResponse) {
                    eventResponses.putAll(eventResponse);
                }
            } catch (Throwable e) {
                Throwable throwable = e.getCause() != null ? e.getCause() : e;
                causes.add(throwable);
                log.error("EventSubscribe method invoke error, method: {}", invocationHolder, e);
            }
        });

        if (!CollectionUtils.isEmpty(causes)) {
            EventBusExecutionException exception = new EventBusExecutionException("EventSubscribe method invoke error", causes);
            eventInterceptorChain.afterHandle(event, eventResponses, exception);
        } else {
            eventInterceptorChain.afterHandle(event, eventResponses, null);
        }
        return eventResponses;
    }

    /**
     * Build the list of InvocationHolder for this event.
     * - Pulls matching invokers from:
     *   1) annotationSubscribeCache (ListenerCacheKey -> List<Invoker>)
     *   2) dynamicSubscribeCache   (UniqueListenerCacheKey -> Invoker)
     * - Matching is based on:
     *   - event type (string equals) via ListenerCacheKey.matchEventType
     *   - payload key pattern via ListenerCacheKey.matchMultiKeys (supports multiple comma-separated keys)
     */
    private List<InvocationHolder> createInvocationHolders(T event) {
        Map<ListenerCacheKey, List<EventInvoker<T>>> listenerCacheKeyListMap = annotationSubscribeCache.get(event.getClass());

        List<InvocationHolder> invocationHolders = new ArrayList<>();

        //invoke annotation subscribe
        if (!ObjectUtils.isEmpty(listenerCacheKeyListMap)) {
            List<InvocationHolder> annotationInvocationHolders = createInvocationHoldersFromMultiInvoker(listenerCacheKeyListMap, event);
            invocationHolders.addAll(annotationInvocationHolders);
        }

        //invoke dynamic subscribe
        if (dynamicSubscribeCache.containsKey(event.getClass())) {
            List<InvocationHolder> dynamicInvocationHolders = createInvocationHoldersFromUniqueInvoker(dynamicSubscribeCache.get(event.getClass()), event);
            invocationHolders.addAll(dynamicInvocationHolders);
        }
        return invocationHolders;
    }

    /**
     * Convert dynamic subscriptions (unique key -> single invoker) into InvocationHolders
     * if they match the current event (by eventType + payloadKey pattern).
     */
    private List<InvocationHolder> createInvocationHoldersFromUniqueInvoker(Map<UniqueListenerCacheKey, EventInvoker<T>> uniqueListenerCacheKeyEventInvokerMap, T event) {
        List<InvocationHolder> invocationHolders = new ArrayList<>();
        for (Map.Entry<UniqueListenerCacheKey, EventInvoker<T>> cacheSubscribeEntry : uniqueListenerCacheKeyEventInvokerMap.entrySet()) {
            String[] matchMultiKeys = filterMatchMultiKeys(event, cacheSubscribeEntry.getKey());
            if (!ObjectUtils.isEmpty(matchMultiKeys)) {
                invocationHolders.add(new InvocationHolder(cacheSubscribeEntry.getValue(), matchMultiKeys, event));
            }
        }
        return invocationHolders;
    }

    /**
     * Convert annotation subscriptions (ListenerCacheKey -> list of invokers) into InvocationHolders
     * for all invokers whose key matches the event.
     */
    private List<InvocationHolder> createInvocationHoldersFromMultiInvoker(Map<ListenerCacheKey, List<EventInvoker<T>>> listenerCacheKeyListMap, T event) {
        List<InvocationHolder> invocationHolders = new ArrayList<>();
        for (Map.Entry<ListenerCacheKey, List<EventInvoker<T>>> listenerCacheKeyListEntry : listenerCacheKeyListMap.entrySet()) {
            String[] matchMultiKeys = filterMatchMultiKeys(event, listenerCacheKeyListEntry.getKey());
            if (!ObjectUtils.isEmpty(matchMultiKeys)) {
                invocationHolders.addAll(listenerCacheKeyListEntry.getValue().stream().map(invoker -> new InvocationHolder(invoker, matchMultiKeys, event)).toList());
            }
        }
        return invocationHolders;
    }

    /**
     * Register a dynamic listener at runtime.
     * Stores a single invoker under a unique key for the given event class.
     */
    public void registerDynamicSubscribe(Class<T> eventClass, UniqueListenerCacheKey listenerCacheKey, EventInvoker<T> eventInvoker) {
        dynamicSubscribeCache.computeIfAbsent(eventClass, k -> new ConcurrentHashMap<>()).put(listenerCacheKey, eventInvoker);
    }

    /**
     * Deregister a dynamic listener at runtime.
     */
    public void deregisterDynamicSubscribe(Class<T> eventClass, UniqueListenerCacheKey listenerCacheKey) {
        if (dynamicSubscribeCache.containsKey(eventClass)) {
            dynamicSubscribeCache.get(eventClass).remove(listenerCacheKey);
        }
    }

    /**
     * Register an annotation-based listener using the @EventSubscribe annotation.
     * Extracts keyExpression and eventType[] and delegates to the overload.
     */
    public void registerAnnotationSubscribe(EventSubscribe eventSubscribe, Object bean, Method executeMethod) {

        registerAnnotationSubscribe(eventSubscribe.payloadKeyExpression(), eventSubscribe.eventType(), bean, executeMethod);
    }

    /**
     * Register an annotation-based listener using explicit keyExpression and eventType[].
     * - Resolve method parameter type and concrete event class for bucketing.
     * - Create ListenerCacheKey(keyExpression, eventType[]).
     * - Insert EventSubscribeInvoker into annotationSubscribeCache[eventClass][ListenerCacheKey].
     */
    public void registerAnnotationSubscribe(String keyExpression, String[] eventType, Object bean, Method executeMethod) {

        Class<?> parameterTypes = parameterResolver.resolveParameterTypes(executeMethod);

        Class<T> eventClass = parameterResolver.resolveActualEventType(executeMethod);

        ListenerCacheKey listenerCacheKey = new ListenerCacheKey(keyExpression, eventType);

        log.debug("registerAsyncSubscribe: {}, subscriber expression: {}", executeMethod, listenerCacheKey);

        annotationSubscribeCache.computeIfAbsent(eventClass, k -> new ConcurrentHashMap<>());

        annotationSubscribeCache.get(eventClass).computeIfAbsent(listenerCacheKey, k -> new ArrayList<>()).add(new EventSubscribeInvoker<>(bean, executeMethod, parameterTypes, parameterResolver));
    }

    /**
     * Decide whether a listener (represented by ListenerCacheKey) matches this event.
     * - First, check if eventType matches (or no types specified).
     * - Then, return the subset of payload keys that match the payloadKeyExpression.
     *   (getPayloadKey() may contain multiple comma-separated keys.)
     */
    private String[] filterMatchMultiKeys(T event, ListenerCacheKey cacheKey) {
        if (!cacheKey.matchEventType(event.getEventType())) {
            return new String[0];
        }
        return cacheKey.matchMultiKeys(event.getPayloadKey());
    }

    /**
     * Provided by Spring to give access to the ApplicationContext
     * so we can look up the Executor bean by name.
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * A small holder object bundling:
     * - the invoker to call,
     * - the exact payload keys that matched (matchMultiKeys),
     * - and the current event.
     *
     * This carries all info needed to execute a listener.
     */
    @Getter
    @AllArgsConstructor
    public class InvocationHolder {

        private EventInvoker<T> invoker;
        private String[] matchMultiKeys;
        private Event<? extends IdentityKey> event;
    }
}
