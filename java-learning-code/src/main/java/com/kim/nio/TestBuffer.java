package com.kim.nio;


import java.nio.ByteBuffer;

/**
 * Buffer基本属性、方法使用
 * 一、NIO缓冲区(Buffer)：在 Java NIO 中负责数据的存取。缓冲区就是数组。用于存储不同数据类型的数据
 * 根据数据类型不同(boolean 除外) ，有以下Buffer 常用子类：
 * ByteBuffer、CharBuffer、ShortBuffer、IntBuffer、LongBuffer、FloatBuffer、DoubleBuffer
 *
 * 二、缓冲区存储数据的两个核心方法：
 * put(): 存入数据到缓冲区中
 * get(): 获取数据到缓冲区中
 *
 *
 * 三、缓冲区的四个核心属性：
 * capacity: 表示Buffer 最大数据容量，缓冲区容量不能为负，并且创建后不能更改。
 * limit: 第一个不应该读取或写入的数据的索引，即位于limit 后的数据不可读写。缓冲区的限制不能为负，并且不能大于其容量。
 * position: 下一个要读取或写入的数据的索引。缓冲区的位置不能为负，并且不能大于其限制
 * mark: 标记是一个索引,表示记录当前position 的位置,通过Buffer 中的mark() 方法指定Buffer 中一个特定的position，之后可以通过调用reset() 方法恢复到这个position.
 *
 * 注意：0<=mark<=position<=limit<=capacity
 *
 * 四、直接缓冲区与非直接缓冲区：
 * 非直接缓冲区：通过 allocate() 方法分配缓冲区，将缓冲区建立在 JVM内存中
 * 直接缓冲区：通够 allocateDirect() 方法分配直接缓冲区，将缓冲区建立在物理内存中，可以提高效率
 *
 * @Author: kim
 * @Date: 2021/1/30 21:11
 * @Version: 1.0
 */
public class TestBuffer {

	public void testDirectBuffer() {
		// 分配直接缓冲区
		ByteBuffer directBuffer = ByteBuffer.allocateDirect(1024);
		// 可以判断是不是直接缓冲区
		System.out.println(directBuffer.isDirect());
	}




	public void testBuffer2() {
		//1. 分配
		ByteBuffer buffer = ByteBuffer.allocate(1024);

		String str = "abcde";
		buffer.put(str.getBytes());

		buffer.flip();

		byte[] bytes = new byte[buffer.limit()];
		buffer.get(bytes, 0, 2);
		System.out.println(new String(bytes, 0, 2));
		System.out.println("position:	" + buffer.position());

		// mark: 标记position
		buffer.mark();
		buffer.get(bytes, 2, 2);
		System.out.println(new String(bytes, 2, 2));
		System.out.println("position:	" + buffer.position());

		// reset: 重置,恢复到之前mark标记的位置
		buffer.reset();
		System.out.println("position:	" + buffer.position());

		// hasRemaining: 判断缓冲区中是否还有剩余数据
		if (buffer.hasRemaining()) {
			// 获取缓冲区中可以操作的数量
			System.out.println(buffer.remaining());
		}

	}


	public void testBuffer1() {
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

		//6. clear()：清空缓冲区,但是缓冲区里面的数据并没有被清空,依然存在，
		// 但是出于“被遗忘”状态(指得是里面一些属性回到了最初状态,比如position、limit、capacity,无法被正确读取)
		buffer.clear();
		System.out.println("---------------------ByteBuffer clear ----------------------");
		System.out.println("capacity:	" + buffer.capacity());
		System.out.println("limit:	" + buffer.limit());
		System.out.println("position:	" + buffer.position());
		System.out.println("mark:	" + buffer.mark());
		System.out.println("--------------------缓冲区数据被clear后依然存在 ---------------");
		System.out.println((char) buffer.get());

	}



	public static void main(String[] args) {


	}
}
