package com.lcy.java.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

public class MySource extends AbstractSource implements Configurable, PollableSource {
    private String prefix;
    private String subfix;
    
    public MySource() {
    }
    
    public void configure(Context context) {
        this.prefix = context.getString("prefix");
        this.subfix = context.getString("subfix", "atguigu");
    }
    
    public Status process() throws EventDeliveryException {
        Status status = null;
        
        try {
            for(int i = 0; i < 5; ++i) {
                SimpleEvent event = new SimpleEvent();
                event.setBody((this.prefix + "--" + i + "--" + this.subfix).getBytes());
                this.getChannelProcessor().processEvent(event);
                status = Status.READY;
            }
        } catch (Exception var5) {
            var5.printStackTrace();
            status = Status.BACKOFF;
        }
        
        try {
            Thread.sleep(2000L);
        } catch (InterruptedException var4) {
            var4.printStackTrace();
        }
        
        return status;
    }
    
    public long getBackOffSleepIncrement() {
        return 0L;
    }
    
    public long getMaxBackOffSleepInterval() {
        return 0L;
    }
}
