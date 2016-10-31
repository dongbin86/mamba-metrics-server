package mamba.metrics;


/**
 * Created by sanbing on 10/10/16.
 */
public class TopNConfig {


    Integer topN;
    String topNFunction;
    Boolean isBottomN;

    public TopNConfig(Integer topN, String topNFunction, Boolean isBottomN) {
        this.setTopN(topN);
        this.setTopNFunction(topNFunction);
        this.setIsBottomN(isBottomN);
    }


    public Integer getTopN() {
        return topN;
    }

    public void setTopN(Integer topN) {
        this.topN = topN;
    }


    public String getTopNFunction() {
        return topNFunction;
    }

    public void setTopNFunction(String topNFunction) {
        this.topNFunction = topNFunction;
    }

    public Boolean getIsBottomN() {
        return isBottomN;
    }

    public void setIsBottomN(Boolean isBottomN) {
        this.isBottomN = isBottomN;
    }
}
