package com.kim.nio;

import org.junit.Test;

import java.nio.ByteBuffer;


/**
 * @Author: kim
 * @Date: 2021/1/30 21:37
 * @Version: 1.0
 */
public class TestBufferTest {

	@Test
	public void testBuffer2() {
		//1. 分配
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		String str = "abcde";
		buffer.put(str.getBytes());
		byte[] bytes = new byte[buffer.limit()];
		buffer.flip();
		buffer.get(bytes, 0, 2);
		System.out.println(new String(bytes, 0, 2));
		System.out.println("position:	" + buffer.position());
		// mark: 标记position
		buffer.mark();
		buffer.get(bytes, 2, 2);
		System.out.println(new String(bytes, 2, 2));
		System.out.println("position:	" + buffer.position());
		// reset:
		buffer.reset();
		System.out.println("position:	" + buffer.position());

	}


	@Test
	public void test1() {
		//1. 分配
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		System.out.println("---------------------ByteBuffer allocate ----------------------");
		System.out.println("capacity:	" + buffer.capacity());
		System.out.println("limit:	" + buffer.limit());
		System.out.println("position:	" + buffer.position());
		System.out.println("mark:	" + buffer.mark());

		//2. 利用put() 存入数据到缓冲区中
		String str = "abcde";
		buffer.put(str.getBytes());
		System.out.println("---------------------ByteBuffer put ----------------------");
		System.out.println("capacity:	" + buffer.capacity());
		System.out.println("limit:	" + buffer.limit());
		System.out.println("position:	" + buffer.position());
		System.out.println("mark:	" + buffer.mark());

		//3. 切换读取数据模式
		buffer.flip();
		System.out.println("---------------------ByteBuffer flip ----------------------");
		System.out.println("capacity:	" + buffer.capacity());
		System.out.println("limit:	" + buffer.limit());
		System.out.println("position:	" + buffer.position());
		System.out.println("mark:	" + buffer.mark());

		//4. 利用get() 读取缓冲区中的数据
		byte[] bytes = new byte[buffer.limit()];
		// 将缓冲区中的数据读取到bytes数组中
		buffer.get(bytes);
		System.out.println(new String(bytes, 0, bytes.length));
		System.out.println("---------------------ByteBuffer get ----------------------");
		System.out.println("capacity:	" + buffer.capacity());
		System.out.println("limit:	" + buffer.limit());
		System.out.println("position:	" + buffer.position());
		System.out.println("mark:	" + buffer.mark());

		//5. rewind()：可重复读,将位置设为为0，取消设置的mark
		buffer.rewind();
		System.out.println("---------------------ByteBuffer rewind ----------------------");
		System.out.println("capacity:	" + buffer.capacity());
		System.out.println("limit:	" + buffer.limit());
		System.out.println("position:	" + buffer.position());
		System.out.println("mark:	" + buffer.mark());

		//6. clear()：清空缓冲区
		buffer.clear();
		System.out.println("---------------------ByteBuffer clear ----------------------");
		System.out.println("capacity:	" + buffer.capacity());
		System.out.println("limit:	" + buffer.limit());
		System.out.println("position:	" + buffer.position());
		System.out.println("mark:	" + buffer.mark());
		System.out.println("--------------------缓冲区数据被clear后依然存在 ---------------");
		System.out.println((char) buffer.get());

	}

}


