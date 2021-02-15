package com.kim.callback;


/**
 * 回调函数使用
 * @Author: kim
 * @Date: 2021/2/10 12:27
 * @Version: 1.0
 */
public class CallBackTest {


    public static void makeInformation() throws Exception {
        System.out.println("A");
        System.out.println("B");

        int num = getNum();
        getControl(num);
    }


    public static void makeInformationUseCallBack(GetControlFunction getControl) throws Exception {
        System.out.println("A");
        System.out.println("B");

        getNumUseCallBack(getControl);
    }

    //求和
    public static int getNum() throws Exception {
        int sum = 0;
        for (int i = 0; i < 500; i++) {
            Thread.sleep(100);
            sum += i;
        }
        System.out.println("sum...... " + sum);
        return sum;
    }


    public static int getNumUseCallBack(GetControlFunction getControl) throws InterruptedException {
        //第一部分: 求和
        int sum = 0;
        for (int i = 0; i < 500; i++) {
            Thread.sleep(100);
            sum += i;
        }
        System.out.println("sum...... " + sum);

        //第二部分: 使用回调函数getControl
        int control = getControl.getControl(sum);

        return control;
    }



    public static int getControl(int num) {
        int a;
        if (num %2 == 0) {
            a = num;
            System.out.println(a);
        } else {
            a = num +1;
            System.out.println(a);
        }
        System.out.println("C");
        System.out.println("D");
        return a;
    }


    public static void main(String[] args) throws Exception {

        // 第一种: 未使用回调函数
        //makeInformation();

        // 第二种: 使用回调函数
        GetControlFunction getControlFunction = (int num) -> {
            int a;
            if (num % 2 == 0) {
                a = num;
                System.out.println(a);
            } else {
                a = num + 1;
                System.out.println(a);
            }
            return a;
        };




        makeInformationUseCallBack(getControlFunction);
    }
}
