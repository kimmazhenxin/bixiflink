package NetworkFlowAnalysis.bean;

/**
 * @Author: kim
 * @Description: 页面点击统计
 * @Date: 15:51 2021/7/6
 * @Version: 1.0
 */
public class PageViewCount {

    // 页面url
    private String url;

    // 窗口结束时间戳
    private long windowEnd;

    // 页面在该窗口的点击量
    private long viewCount;


    public PageViewCount() {
    }

    public PageViewCount(String url, long windowEnd, long viewCount) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.viewCount = viewCount;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public long getViewCount() {
        return viewCount;
    }

    public void setViewCount(long viewCount) {
        this.viewCount = viewCount;
    }

    @Override
    public String toString() {
        return "PageViewCount{" +
                "url='" + url + '\'' +
                ", windowEnd=" + windowEnd +
                ", viewCount=" + viewCount +
                '}';
    }
}
