package com.kim.nio;

import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @Author: kim
 * @Date: 2021/1/31 9:47
 * @Version: 1.0
 */
public class TestChannelTest {

	@Test
	public void test1() {
		FileInputStream fis = null;
		FileOutputStream fos = null;

		//1) 获取通道
		FileChannel inChanel = null;
		FileChannel outChannel = null;

		try {
			fis = new FileInputStream("src/main/resources/1.PNG");
			fos = new FileOutputStream("src/main/resources/2.PNG");

			inChanel = fis.getChannel();
			outChannel = fos.getChannel();

			//2) 分配指定大小的缓冲区
			ByteBuffer buffer = ByteBuffer.allocate(1024);

			//3) 将通道中的数据存入缓冲区中
			while (inChanel.read(buffer) != -1) {
				// 切换读取数据的模式
				buffer.flip();
				//4) 将缓冲区中的数据写入通道中
				outChannel.write(buffer);
				//5) 情况缓冲区,为了下次继续读写
				buffer.clear();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != fis) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (null != fos) {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (null != outChannel) {
				try {
					outChannel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (null != inChanel) {
				try {
					inChanel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void test4() {
		RandomAccessFile raf1 = null;
		RandomAccessFile raf2 = null;
		FileChannel channel1 = null;
		FileChannel channel2 = null;
		try {
			raf1 = new RandomAccessFile("src/main/resources/1.txt", "rw");

			//1.获取通道
			channel1 = raf1.getChannel();


			//2.分配指定大小的缓冲区
			ByteBuffer buffer1 = ByteBuffer.allocate(1024);
			ByteBuffer buffer2 = ByteBuffer.allocate(3096);

			//3.分散读取
			ByteBuffer[] bufs = {buffer1, buffer2};
			channel1.read(bufs);
			for (ByteBuffer buffer: bufs) {
				buffer.flip();
			}
			System.out.println(new String(bufs[0].array(), 0, bufs[0].limit()));
			System.out.println(new String(bufs[1].array(), 0, bufs[1].limit()));

			//4. 聚集写入
			raf2 = new RandomAccessFile("src/main/resources/2.txt", "rw");
			channel2 = raf2.getChannel();

			channel2.write(bufs);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != raf1) {
				try {
					raf1.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (null != raf2) {
				try {
					raf1.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (null != channel1) {
				try {
					raf1.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (null != channel2) {
				try {
					raf1.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}