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

        ds.flush();

        ds.store("key6", "value6");
        ds.store("key7", "value7");

        ds.flush();

        ds.store("key8", "value8");
        ds.store("key9", "value9");

        System.out.println(ds.get("key2"));
        System.out.println(ds.get("key6"));
        System.out.println(ds.get("key8"));
    }
}