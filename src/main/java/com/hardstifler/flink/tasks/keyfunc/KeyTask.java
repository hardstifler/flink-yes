package com.hardstifler.flink.tasks.keyfunc;

import com.hardstifler.flink.model.Person;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


/**
 * 自定义分组条件以及过滤方法
 * */
public class KeyTask implements Serializable {


    public KeyTask(StreamExecutionEnvironment env) {

        DataStream<Person> pStream = env.fromElements(
                new Person("1", 1, "1", "1"),
                new Person("10", 1, "1", "1"),
                new Person("11", 1, "1", "1"),
                new Person("12", 1, "1", "1"),
                new Person("22", 1, "1", "1"),
                new Person("33", 1, "1", "1"),
                new Person("44", 1, "1", "1"),
                new Person("45", 1, "1", "1"),
                new Person("32", 1, "1", "1")
        );

        DataStream<Person> pfStream = pStream.
                filter(MyFilter());
        pStream.
                keyBy(MyKey()).
                process(MyProcessFunction()).
                addSink(MySinkFunction()).uid("keyd-filter-precess-sink");
    }

    /**
     * sink
     * */
    private SinkFunction<Person> MySinkFunction() {
        return new SinkFunction<Person>() {
            private final Logger LOG = LoggerFactory.getLogger(this.getClass());

            @Override
            public void invoke(Person value, Context context) throws Exception {
                LOG.info("sink out {}", value);
            }
        };
    }


    /**
     * precessfunc
     * */
    private KeyedProcessFunction<Integer, Person, Person> MyProcessFunction() {
        return new KeyedProcessFunction<Integer, Person, Person>() {

            private final Logger LOG = LoggerFactory.getLogger(this.getClass());

            @Override
            public void processElement(Person value, Context ctx, Collector<Person> out) throws Exception {
                LOG.info("key {}", ctx.getCurrentKey());
                LOG.info("value {}", value);
                out.collect(value);
            }
        };
    }

    /**
     * keyd func
     * */
    private KeySelector<Person, Integer> MyKey() {
        return new KeySelector<Person, Integer>() {
            private final Logger LOG = LoggerFactory.getLogger(this.getClass());

            @Override
            public Integer getKey(Person value) throws Exception {
                Integer code = value.getKey().hashCode();
                LOG.info("origin code {}", code);
                return value.getKey().hashCode() % 4;
            }
        };
    }

    /**
     * filter func
     * */
    private FilterFunction<Person> MyFilter() {
        return new FilterFunction<Person>() {

            @Override
            public boolean filter(Person value) throws Exception {
                return value.getAge() > 10;
            }
        };
    }
}
