package com.hardstifler.flink.model;

public class Person {
    private String Key;
    private Integer age;
    private String name;
    private String address;

    public long getTimeStamp() {
        return timeStamp;
    }

    private  long timeStamp;

    public String getKey() {

        return this.Key;
    }

    public Integer getAge() {
        return this.age;
    }



    public Person(String key, Integer age, String name, String address) {
        this.Key = key;
        this.age = age;
        this.name = name;
        this.address = address;
    }

    @Override
    public String toString() {
        return "Person{" +
                "Key='" + Key + '\'' +
                ", age=" + age +
                ", name='" + name + '\'' +
                ", address='" + address + '\'' +
                '}';
    }
}
