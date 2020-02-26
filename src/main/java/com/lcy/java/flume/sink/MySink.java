package com.lcy.java.flume.sink;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.Sink.Status;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {
    private Logger logger = LoggerFactory.getLogger(MySink.class);
    private String prefix;
    private String subfix;
    
    public MySink() {
    }
    
    public void configure(Context context) {
        this.prefix = context.getString("prefix");
        this.subfix = context.getString("subfix", "atguigu");
    }
    
    public Status process() throws EventDeliveryException {
        Status status = null;
        Channel channel = this.getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        
        try {
            Event event = channel.take();
            if (event != null) {
                String body = new String(event.getBody());
                this.logger.error(this.prefix + body + this.subfix);
            }
            
            transaction.commit();
            status = Status.READY;
        } catch (ChannelException var9) {
            var9.printStackTrace();
            transaction.rollback();
            status = Status.BACKOFF;
        } finally {
            transaction.close();
        }
        
        return status;
    }
}

