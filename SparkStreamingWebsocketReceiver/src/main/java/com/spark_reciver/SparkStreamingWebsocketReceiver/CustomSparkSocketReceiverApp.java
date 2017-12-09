package com.spark_reciver.SparkStreamingWebsocketReceiver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Function;
import scala.collection.Seq;

public class CustomSparkSocketReceiverApp {

	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf();
		conf.setAppName("SparkStreamingWebsocketReceiver");
		conf.setMaster("local[*]");
		JavaStreamingContext streamContext = new JavaStreamingContext(conf, Durations.seconds(1));
		JavaDStream<String> websocketReceiverStream = streamContext
				.receiverStream(new WebsocketReceiver("wss://api.hitbtc.com/api/2/ws",
						"{\n\"method\": \"subscribeOrderbook\",\n\"params\": {\n\"symbol\": \"ETHBTC\"\n},\n\"id\": 123\n}\n",
						"E:/Experiement/data/data.json", StorageLevel.MEMORY_AND_DISK_2()));
		websocketReceiverStream.print();

		streamContext.start();
		streamContext.awaitTermination();
	}

}
