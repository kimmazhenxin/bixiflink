package HotItemAnalysis.hotitems;

/**
 * 商品点击量(窗口操作的输出类型)
 * @Author: mazhenxin
 * @File: ItemViewCount.java
 * @Date: 2021/07/03 16:04
 */
public class ItemViewCount {

    // 商品ID
    private long itemId;

    // 窗口结束时间戳
    private long windowEnd;

    // 商品在该窗口的点击量
    private long viewCount;


    public ItemViewCount(long itemId, long windowEnd, long viewCount) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.viewCount = viewCount;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
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
        return "ItemViewCount{" +
                "itemId=" + itemId +
                ", windowEnd=" + windowEnd +
                ", viewCount=" + viewCount +
                '}';
    }
}
