package com.hardstifler.flink.tasks.fraud;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

import java.io.Serializable;

public class FrundDetectorTask  implements Serializable {

    public  FrundDetectorTask(StreamExecutionEnvironment env){
        DataStream<Transaction> transactions = env
                .addSource(new TransactionSource())
                .name("transactions");


        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        alerts
                .addSink(new AlertSink())
                .name("send-alerts");

    }
}
