package com.hardstifler.flink.tasks.watermarks;

import com.alibaba.fastjson.JSON;
import com.hardstifler.flink.model.Person;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

/**
 * 练习Watermarks相关
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/learn-flink/streaming_analytics/
 * https://www.cnblogs.com/rossiXYZ/p/12286407.html
 * 这个假设是触发窗口计算的基础，只有水位线越过窗口对应的结束时间，窗口才会关闭和进行计算。
 */
public class WatermarksTask implements Serializable {
    public WatermarksTask(StreamExecutionEnvironment env) {


        DataStreamSource<String> pStream = env.socketTextStream("127.0.0.1", 3333);
        SingleOutputStreamOperator<Person> mstream = pStream.map(new MapFunction<String, Person>() {
            private final Logger LOG = LoggerFactory.getLogger(this.getClass());

            @Override
            public Person map(String value) throws Exception {
                Person p = JSON.parseObject(value, Person.class);
                if (Objects.isNull(p)) {
                    LOG.error(value);
                    return null;
                }
                return p;
            }
        }).filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person value) throws Exception {
                return value.getAge() > 100;
            }
        });

        /****/
         //指定水位策略 等待4秒
         WatermarkStrategy<Person> strategy = WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ofSeconds(5)).
         withTimestampAssigner((event, timestamp) -> event.getTimeStamp());

         //指定水位策略
         SingleOutputStreamOperator<Person> streamOperator = mstream.assignTimestampsAndWatermarks(strategy);


        streamOperator.keyBy(getKey()).window(TumblingEventTimeWindows.of(Time.seconds(5))).allowedLateness(Time.seconds(2)).process(getFunction()).addSink(getSinkFunction());

    }

    private SinkFunction<Person> getSinkFunction() {
        return new SinkFunction<Person>() {
            private final Logger LOG = LoggerFactory.getLogger(this.getClass());

            @Override
            public void invoke(Person value, Context context) throws Exception {
                LOG.info("sink out {}", value);
            }
        };
    }


    private ProcessWindowFunction<Person, Person, Integer, TimeWindow> getFunction() {
        return new ProcessWindowFunction<Person, Person, Integer, TimeWindow>() {
            private final Logger LOG = LoggerFactory.getLogger(this.getClass());

            @Override
            public void process(Integer key, Context context, Iterable<Person> elements, Collector<Person> out) throws Exception {
                LOG.info("ProcessWindowFunction precess called !");
                Integer maxAge = 0;
                Person index = null;
                Iterator<Person> iterator = elements.iterator();
                if (iterator.hasNext()) {
                    Person p = iterator.next();
                    LOG.info("precess {} {}", key, p);
                    if (p.getAge() > maxAge) {
                        maxAge = p.getAge();
                        index = p;
                    }
                }

                LOG.info("precess max  key {} window {} maxage {}", key, context.currentWatermark(), maxAge);
                out.collect(index);
            }
        };
    }



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
                return true;
            }
        };
    }

    private KeySelector<Person, Integer> getKey() {
        return new KeySelector<Person, Integer>() {
            private final Logger LOG = LoggerFactory.getLogger(this.getClass());
            @Override
            public Integer getKey(Person value) throws Exception {
                LOG.info("keyed {}", value);
                //模拟分区四个
                return value.getKey().hashCode() % 4;
            }
        };
    }
}
