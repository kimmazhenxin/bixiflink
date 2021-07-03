package com.kim.table.retract;

import java.util.Date;

/**
 * @Author: kim
 * @Description: 浏览器浏览网页
 * @Date: 2021/6/19 10:25
 * @Version: 1.0
 */
public class WebVisit {

	private String browser;

	private String ip;

	private Date openTime;

	private String pageUrl;

	private String cookieId;

	public WebVisit() {
	}

	public String getBrowser() {
		return browser;
	}

	public void setBrowser(String browser) {
		this.browser = browser;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Date getOpenTime() {
		return openTime;
	}

	public void setOpenTime(Date openTime) {
		this.openTime = openTime;
	}

	public String getPageUrl() {
		return pageUrl;
	}

	public void setPageUrl(String pageUrl) {
		this.pageUrl = pageUrl;
	}

	public String getCookieId() {
		return cookieId;
	}

	public void setCookieId(String cookieId) {
		this.cookieId = cookieId;
	}

	@Override
	public String toString() {
		return "WebVisit{" +
				"browser='" + browser + '\'' +
				", ip='" + ip + '\'' +
				", openTime=" + openTime +
				", pageUrl='" + pageUrl + '\'' +
				", cookieId='" + cookieId + '\'' +
				'}';
	}
}
