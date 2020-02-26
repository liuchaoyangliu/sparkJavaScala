package com.lcy.java.flume.interceptor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class TypeInterceptor implements Interceptor {
    private List<Event> addHeaderEvents;
    
    private TypeInterceptor() {
    }
    
    public void initialize() {
        this.addHeaderEvents = new ArrayList();
    }
    
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());
        if (body.contains("hello")) {
            headers.put("topic", "first");
        } else {
            headers.put("topic", "second");
        }
        
        return event;
    }
    
    public List<Event> intercept(List<Event> events) {
        this.addHeaderEvents.clear();
        Iterator var2 = events.iterator();
        
        while(var2.hasNext()) {
            Event event = (Event)var2.next();
            this.addHeaderEvents.add(this.intercept(event));
        }
        
        return this.addHeaderEvents;
    }
    
    public void close() {
    }
    
    public static class Builder implements org.apache.flume.interceptor.Interceptor.Builder {
        public Builder() {
        }
        
        public Interceptor build() {
            return new TypeInterceptor();
        }
        
        public void configure(Context context) {
        }
    }
}

