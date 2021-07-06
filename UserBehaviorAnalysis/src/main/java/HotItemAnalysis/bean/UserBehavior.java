package HotItemAnalysis.bean;

/**
 * 用户行为数据结构
 * 578814,176722,982926,pv,1511658000
 * @Author: mazhenxin
 * @File: UserBehavior.java
 * @Date: 2021/07/03 16:04
 */
public class UserBehavior {

    // 用户ID
    private long userId;

    // 商品ID
    private long itemId;

    // 商品类目ID
    private int categoryId;

    // 用户行为, 包括("pv", "buy", "cart", "fav")
    private String behavior;

    // 行为发生的时间戳，单位秒
    private long timestamp;


    public UserBehavior() {

    }

    public UserBehavior(long userId, long itemId, int categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }


    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
