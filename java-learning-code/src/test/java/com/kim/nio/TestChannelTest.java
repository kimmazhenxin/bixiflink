package com.kim.nio;

import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
}