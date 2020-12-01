package com.kim.parquet;

/**
 * @Author: kim
 * @Date: 2020/11/29 12:36
 * @Version: 1.0
 */
public class ParquetPojo {

	private String word;

	private Long count;

	public ParquetPojo() {
	}

	public ParquetPojo(String word, Long count) {
		this.word = word;
		this.count = count;
	}


	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "ParquetPojo{" +
				"word='" + word + '\'' +
				", count=" + count +
				'}';
	}
}
