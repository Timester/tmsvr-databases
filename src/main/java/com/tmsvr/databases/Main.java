package com.tmsvr.databases;

import com.tmsvr.databases.lsmtree.LsmDataStore;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        Main m = new Main();
        m.start();
    }

    public void start() throws IOException {
        DataStore ds = new LsmDataStore();

        ds.put("key1", "value1");
        ds.put("key2", "value2");
        ds.put("key3", "value3");
        ds.put("key4", "value4");
        ds.put("key5", "value5");
        ds.put("key1", "value1-modified");

      //  ds.flush();

        ds.put("key6", "value6");
        ds.put("key7", "value7");
        ds.put("key3", "value3-modified");
        ds.put("key5", "value5-modified");

      //  ds.flush();

        ds.put("key8", "value8");
        ds.put("key9", "value9");
        ds.put("key9", "value9-modified");
        ds.delete("key5");

        System.out.println("key1 - " + ds.get("key1").orElse("not found"));
        System.out.println("key2 - " + ds.get("key2").orElse("not found"));
        System.out.println("key3 - " + ds.get("key3").orElse("not found"));
        System.out.println("key5 - " + ds.get("key5").orElse("not found"));
        System.out.println("key6 - " + ds.get("key6").orElse("not found"));
        System.out.println("key8 - " + ds.get("key8").orElse("not found"));
        System.out.println("key9 - " + ds.get("key9").orElse("not found"));
        System.out.println("key10 - " + ds.get("key10").orElse("not found"));
    }
}