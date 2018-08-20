package com.maiya.kafka.consumer;

import kafka.api.*;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.ConsumerMetadataResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.network.BlockingChannel;

import java.io.IOException;
import java.util.*;

/**
 * Created by lubinsu
 * Date: 2018/2/13 10:49
 * Desc: kafka offset 监控程序
 */
public class KafkaOffsetMonitor {

    public static void getOffsets(TopicAndPartition topicAndPartition, String groupName, int correlationId, String MY_CLIENTID, BlockingChannel channel) {
        // How to fetch offsets

        List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
        partitions.add(topicAndPartition);
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                groupName,
                partitions,
                (short) 1 /* version */, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
                correlationId,
                MY_CLIENTID);
        channel.send(fetchRequest.underlying());
        OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.receive().buffer());
        OffsetMetadataAndError result = fetchResponse.offsets().get(topicAndPartition);
        short offsetFetchErrorCode = result.error();
        if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
            channel.disconnect();
            // Go to step 1 and retry the offset fetch
        } /*else if (errorCode == ErrorMapping.OffsetsLoadInProgress()) {
            // retry the offset fetch (after backoff)
        }*/ else {
            long retrievedOffset = result.offset();
            System.out.println(retrievedOffset);
        }
    }

    public static void main(String[] args) {


        for (String arg : args) {
            System.out.println(arg);
        }
        BlockingChannel channel = new BlockingChannel(args[2], 9092,
                BlockingChannel.UseDefaultBufferSize(),
                BlockingChannel.UseDefaultBufferSize(),
                5000 /* read timeout in millis */);
        channel.connect();
        final String MY_GROUP = args[1];
        final String MY_CLIENT_ID = "democlientid";
        int correlationId = 0;
        final TopicAndPartition testPartition0 = new TopicAndPartition(args[0], 0);
        final TopicAndPartition testPartition1 = new TopicAndPartition(args[0], 1);
        final TopicAndPartition testPartition2 = new TopicAndPartition(args[0], 2);
        final TopicAndPartition testPartition3 = new TopicAndPartition(args[0], 3);

        getOffsets(testPartition0, MY_GROUP, correlationId++, MY_CLIENT_ID, channel);
        getOffsets(testPartition1, MY_GROUP, correlationId++, MY_CLIENT_ID, channel);
        getOffsets(testPartition2, MY_GROUP, correlationId++, MY_CLIENT_ID, channel);
        getOffsets(testPartition3, MY_GROUP, correlationId++, MY_CLIENT_ID, channel);
    }
}
