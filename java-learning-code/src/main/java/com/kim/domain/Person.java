package com.kim.domain;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @Author: kim
 * @Description:
 * @Date: 2021/9/11 10:51
 * @Version: 1.0
 */
@Setter
@Getter
@ToString
public class Person{

	private Long id;

	private String name;

	private Double height;

	private String country;

	public Person() {
	}
}
