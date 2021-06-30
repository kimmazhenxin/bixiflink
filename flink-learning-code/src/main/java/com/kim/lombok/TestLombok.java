package com.kim.lombok;

import lombok.Data;

/**
 * lombok测试使用
 * @Author: kim
 * @Date: 2020/6/20 23:34
 * @Version: 1.0
 */

public class TestLombok {
    public static void main(String[] args) {
        long l = System.currentTimeMillis();
        long l1 = System.currentTimeMillis() - 500L;
        System.out.println("l:" + l + ", l1:"+l1);
//        Teacher teacher = new Teacher();
//        teacher.setDesc("优秀");
//        teacher.setId("001");
//        System.out.println(teacher);
    }
}



@Data
class Teacher {
    private String id;
    private String desc;

}



