package com.hardstifler.flink.tasks.watermarks;

import com.hardstifler.flink.model.Person;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * 练习Watermarks相关
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/learn-flink/streaming_analytics/
 * https://www.cnblogs.com/rossiXYZ/p/12286407.html
 * 这个假设是触发窗口计算的基础，只有水位线越过窗口对应的结束时间，窗口才会关闭和进行计算。
 */
public class WatermarksTask implements Serializable {
    public WatermarksTask(StreamExecutionEnvironment env) {


        List<Person> list = new LinkedList();

        for (int i = 0; i < 100; i++) {

        }
        DataStream<Person> pStream = env.fromCollection(list);

        //指定水位策略
        WatermarkStrategy<Person> strategy = WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ofSeconds(10)).
                withTimestampAssigner((event, timestamp) -> event.getTimeStamp());

        //指定水位策略
        SingleOutputStreamOperator<Person> streamOperator = pStream.assignTimestampsAndWatermarks(strategy);

        //keyby window
        streamOperator.keyBy(getKey()).
                window(getAssigner()).allowedLateness(Time.seconds(10)).process(getFunction());
//                apply(getFunction());


    }

    private ProcessWindowFunction<Person, Person, Integer, TimeWindow> getFunction() {
        return new ProcessWindowFunction<Person, Person, Integer, TimeWindow>() {
            @Override
            public void process(Integer integer, Context context, Iterable<Person> elements, Collector<Person> out) throws Exception {

            }
        };
    }

//    private WindowFunction<Person, Person, Integer, TimeWindow> getFunction() {
//        return new WindowFunction<Person, Person, Integer, TimeWindow>() {
//            @Override
//            public void apply(Integer integer, TimeWindow window, Iterable<Person> input, Collector<Person> out) throws Exception {
//
//            }
//        };
//    }

    private WindowAssigner<Person, TimeWindow> getAssigner() {
        return new WindowAssigner<Person, TimeWindow>() {
            @Override
            public Collection<TimeWindow> assignWindows(Person element, long timestamp, WindowAssignerContext context) {
                return null;
            }

            @Override
            public Trigger<Person, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
                return null;
            }

            @Override
            public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
                return null;
            }

            @Override
            public boolean isEventTime() {
                return false;
            }
        };
    }

    private KeySelector<Person, Integer> getKey() {
        return new KeySelector<Person, Integer>() {

            @Override
            public Integer getKey(Person value) throws Exception {
                return null;
            }
        };
    }
}
