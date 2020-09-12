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
        Teacher teacher = new Teacher();
        teacher.setDesc("优秀");
        teacher.setId("001");
        System.out.println(teacher);
    }
}



@Data
class Teacher {
    private String id;
    private String desc;

}



