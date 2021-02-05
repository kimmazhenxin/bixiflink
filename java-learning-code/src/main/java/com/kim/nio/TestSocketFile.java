package com.kim.nio;

import com.kim.nio.TsetBlockingNIO2;
public class TestSocketFile {

    public static void main(String[] args) {
        TestBlockingNIO blockingNIO = new TestBlockingNIO();
        blockingNIO.client("127.0.0.1", 6669);


    }
}
