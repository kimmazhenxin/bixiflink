/**
 * @Author: kim
 * @Date: 2020/6/21 21:18
 * @Version: 1.0
 */
public class Test2 {
    public static void main(String[] args) {
        System.out.println("begin2 time:" +System.currentTimeMillis());
        for (int j = 1; j < 100; j = j+2) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("this j is :"+j);
        }
        System.out.println("end2 time:" +System.currentTimeMillis());
    }
}


