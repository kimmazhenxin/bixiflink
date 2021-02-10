package com.kim.nio;

import com.kim.nio.TsetBlockingNIO2;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TestSocketFile {


    private static void testChannel() throws Exception {
        FileChannel channel = FileChannel.open(Paths.get("D:/WorkSpace/IDEA/bixiflink/java-learning-code/src/main/resources/test.txt"),
                StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        String string = "BBB";
        byte[] string2 = "ABAA".getBytes();
        buffer.put(string.getBytes());
        System.out.println("------------------write ------------------");
        buffer.flip();
        channel.write(buffer);
        System.out.println(new String(buffer.array()));
        buffer.flip();

        buffer.put(string2, 0, string2.length);

        //buffer.clear();
        System.out.println(buffer.position());
        System.out.println(buffer.limit());
        System.out.println(new String(buffer.array()));
        channel.read(buffer);
        System.out.println("------------------read ------------------");
        //buffer.flip();
        System.out.println(new String(buffer.array()));

        channel.close();
    }



    public static void main(String[] args) throws Exception {
//        TestBlockingNIO blockingNIO = new TestBlockingNIO();
//        blockingNIO.client("127.0.0.1", 6669);

//        TsetBlockingNIO2 blockingNIO2 = new TsetBlockingNIO2();
//        blockingNIO2.client("127.0.0.1", 6669);

//        TestNonBlockingNIO nonBlockingNIO = new TestNonBlockingNIO();
//        nonBlockingNIO.client("127.0.0.1", 6669);
        testChannel();
    }
}
