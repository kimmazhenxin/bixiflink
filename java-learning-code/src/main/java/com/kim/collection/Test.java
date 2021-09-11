package com.kim.collection;

import com.kim.domain.Person;
import lombok.extern.slf4j.Slf4j;
import java.util.HashMap;
import java.util.Map;

/**
 * 验证集合中引用类型属性修改后的变化
 * @Author: kim
 * @Date: 2021/1/30 18:27
 * @Version: 1.0
 */
@Slf4j(topic = "c.Test")
public class Test {

	public static void main(String[] args) {
		Map<Long, Person> map = new HashMap<>();
		Person person = new Person();
		person.setId(1L);
		person.setName("kim");
		map.put(person.getId(), person);

		for (Map.Entry<Long, Person> entry : map.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}

		Person person1 = map.get(1L);
		System.out.println("person1: " + person1);

		System.out.println(person.equals(person1));

		System.out.println("set name........");
		person1.setName("updateKim");
		System.out.println(person1);
		System.out.println(person);

		System.out.println("map........");
		Person person2 = map.put(1L, person1);
		System.out.println("before person1 put : " + person2);
		for (Map.Entry<Long, Person> entry : map.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}
	}
}
