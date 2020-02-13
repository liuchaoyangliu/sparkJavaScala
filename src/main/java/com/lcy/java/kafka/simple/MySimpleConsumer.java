package com.lcy.java.kafka.simple;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

public class MySimpleConsumer {
    public MySimpleConsumer() {
    }
    
    public static void main(String[] args) {
        SimpleConsumer simpleConsumer = new SimpleConsumer(" ", 9092, 2000, 4096, "");
        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(""));
        TopicMetadataResponse topicMetadataResponse = simpleConsumer.send(topicMetadataRequest);
        List<TopicMetadata> topicsMetadata = topicMetadataResponse.topicsMetadata();
        Iterator var5 = topicsMetadata.iterator();
        
        while(var5.hasNext()) {
            TopicMetadata topicMetadatum = (TopicMetadata)var5.next();
            List<PartitionMetadata> partitionMetadata = topicMetadatum.partitionsMetadata();
            
            PartitionMetadata partitionMetadatum;
            String var10;
            for(Iterator var8 = partitionMetadata.iterator(); var8.hasNext(); var10 = partitionMetadatum.leader().host()) {
                partitionMetadatum = (PartitionMetadata)var8.next();
            }
        }
        
        SimpleConsumer host = new SimpleConsumer("host", 9092, 2000, 6144, "");
        FetchRequest fetchRequest = (new FetchRequestBuilder()).addFetch("", 0, 1000L, 20480).build();
        FetchResponse fetchResponse = host.fetch(fetchRequest);
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet("", 0);
        Iterator var15 = messageAndOffsets.iterator();
        
        while(var15.hasNext()) {
            MessageAndOffset messageAndOffset = (MessageAndOffset)var15.next();
            messageAndOffset.message().payload();
        }
        
    }
}

