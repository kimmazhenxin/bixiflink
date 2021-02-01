package com.kim.nio;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Channel使用
 * 一、通道（Channel）：用于源节点与目标节点的连接。在Java NIO 中负责缓冲区中数据的传输。Channel 本身不存储数据，因此需要配合缓冲区进行传输。
 *
 * 二、通道的主要实现类
 * java.nio.channels.Channel 接口：
 *  * •FileChannel：用于读取、写入、映射和操作文件的通道。
 *  * •DatagramChannel：通过UDP 读写网络中的数据通道。
 *  * •SocketChannel：通过TCP 读写网络中的数据。
 *  * •ServerSocketChannel：可以监听新进来的TCP 连接，对每一个新进来的连接都会创建一个SocketChannel。
 *
 *
 * 三、获取通道
 * 1. Java 针对支持通道的类提供了 getChannel()方法
 * 		本地IO：
 * 		FileInputStream / FileOutStream
 * 		RandomAccessFile
 *
 * 		网络IO：
 * 		Socket
 * 		ServerSocket
 * 		DatagramSocket
 *
 * 2. 在 JDK 中NIO.2 针对各个通道提供了静态方法 open() 打开并返回指定通道
 * 3. 在 JDK 中NIO.2 的Files工具类的newByteChannel() 获取字节通道
 *
 * 四、通道之间的数据传输
 * transferFrom()
 * transferTo()
 *
 * 五、分散（Scatter）于聚集（Gather）
 * 分散读取(Scattering Reads)：将通道中的数据分散到多个缓冲区中
 * 聚集写入(Gathering Writes)：将缓冲区中的数据聚集到通道中
 *
 * @Author: kim
 * @Date: 2021/1/31 9:12
 * @Version: 1.0
 */
public class TestChannel {

	//1. 利用通道完成文件的复制（非直接缓冲区）
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


	//2. 利用直接缓冲区完成文件的复制(内存映射文件)
	// 注意：内存映射文件只有ByteBuffer方式支持
	//
	public void test2() {
		FileChannel inChannel = null;
		FileChannel outChannel = null;
		try {
			inChannel = FileChannel.open(Paths.get("src/main/resources/1.PNG"), StandardOpenOption.READ);
			outChannel = FileChannel.open(Paths.get("src/main/resources/1.PNG"), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);


			// 内存映射文件,分配直接缓冲区,和ByteBuffer.allocateDirectBuffer()是一样的
			MappedByteBuffer inMappedBuffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
			MappedByteBuffer outMappedBuffer = outChannel.map(FileChannel.MapMode.READ_WRITE, 0, inChannel.size());


			// 直接对缓冲区进行数据的读写操作,这时候就不要Channel进行传输了,和test1有本质的区别
			byte[] bytes = new byte[inMappedBuffer.limit()];
			inMappedBuffer.get(bytes);
			outMappedBuffer.put(bytes);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != outChannel) {
				try {
					outChannel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (null != inChannel) {
				try {
					inChannel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
	}


	//3. 通道之间的数据传输(底层也是使用直接缓冲区)
	public void test3() {
		FileChannel inChannel = null;
		FileChannel outChannel = null;
		try {
			inChannel = FileChannel.open(Paths.get("src/main/resources/1.PNG"), StandardOpenOption.READ);
			outChannel = FileChannel.open(Paths.get("src/main/resources/1.PNG"), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);

			// 从in-->out
			inChannel.transferTo(0, inChannel.size(), outChannel);

			// 从in-->out
			outChannel.transferFrom(inChannel, 0, inChannel.size());

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != outChannel) {
				try {
					outChannel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (null != inChannel) {
				try {
					inChannel.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}

	}
}
