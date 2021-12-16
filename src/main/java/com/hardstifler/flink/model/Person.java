package com.hardstifler.flink.model;

public class Person {
    private String Key;
    private Integer age;
    private String name;
    private String address;
    private  long timeStamp;

    public long getTimeStamp() {
        return timeStamp;
    }


    public String getKey() {

        return this.Key;
    }

    public Integer getAge() {
        return this.age;
    }



    public Person(String key, Integer age, String name, String address, long timeStamp) {
        this.Key = key;
        this.age = age;
        this.name = name;
        this.address = address;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "Person{" +
                "Key='" + Key + '\'' +
                ", age=" + age +
                ", name='" + name + '\'' +
                ", address='" + address + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
