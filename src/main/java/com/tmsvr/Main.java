package com.tmsvr;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        Main m = new Main();
        m.start();
    }

    public void start() throws IOException {
        DataStore ds = new DataStore();

        ds.store("key1", "value1");
        ds.store("key2", "value2");
        ds.store("key3", "value3");
        ds.store("key4", "value4");
        ds.store("key5", "value5");
        ds.store("key1", "value1-modified");

        ds.flush();

        ds.store("key6", "value6");
        ds.store("key7", "value7");
        ds.store("key3", "value3-modified");
        ds.store("key5", "value5-modified");

        ds.flush();

        ds.store("key8", "value8");
        ds.store("key9", "value9");
        ds.store("key9", "value9-modified");

        System.out.println("key1 - " + ds.get("key1").orElse("not found"));
        System.out.println("key2 - " + ds.get("key2").orElse("not found"));
        System.out.println("key3 - " + ds.get("key3").orElse("not found"));
        System.out.println("key5 - " + ds.get("key5").orElse("not found"));
        System.out.println("key6 - " + ds.get("key6").orElse("not found"));
        System.out.println("key8 - " + ds.get("key8").orElse("not found"));
        System.out.println("key9 - " + ds.get("key9").orElse("not found"));
    }
}