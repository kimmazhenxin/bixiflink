/**
 * @Author: kim
 * @Date: 2020/6/21 21:17
 * @Version: 1.0
 */
public class Test1 {
    public static void main(String[] args) {
        System.out.println("begin time:" +System.currentTimeMillis());
        for (int i = 0; i < 100; i = i+2) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("this i is :"+i);
        }
        System.out.println("end time:" +System.currentTimeMillis());
    }
}
