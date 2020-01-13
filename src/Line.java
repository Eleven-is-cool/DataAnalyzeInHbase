import java.awt.Color;
import java.awt.Font;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.StandardChartTheme;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;

public class Line {
  public static void main(String[] args) {
    StandardChartTheme mChartTheme = new StandardChartTheme("CN");
    mChartTheme.setLargeFont(new Font("黑体", Font.BOLD, 20));
    mChartTheme.setExtraLargeFont(new Font("宋体", Font.PLAIN, 15));
    mChartTheme.setRegularFont(new Font("宋体", Font.PLAIN, 15));
    ChartFactory.setChartTheme(mChartTheme);		
    CategoryDataset mDataset = GetDataset();
    JFreeChart mChart = ChartFactory.createLineChart(
        "2009~2016年USA部分评分变化折线图",//图名字
        "年份",//横坐标
        "百分比",//纵坐标
        mDataset,//数据集
        PlotOrientation.VERTICAL,
        true, // 显示图例
        true, // 采用标准生成器 
        false);// 是否生成超链接
    
    CategoryPlot mPlot = (CategoryPlot)mChart.getPlot();
    mPlot.setBackgroundPaint(Color.LIGHT_GRAY);
    mPlot.setRangeGridlinePaint(Color.BLUE);//背景底部横虚线
    mPlot.setOutlinePaint(Color.RED);//边界线
    
    ChartFrame mChartFrame = new ChartFrame("2009~2016年USA部分评分变化折线图", mChart);
    mChartFrame.pack();
    mChartFrame.setVisible(true);

  }
  public static CategoryDataset GetDataset()
  {
    DefaultCategoryDataset mDataset = new DefaultCategoryDataset();
    mDataset.addValue(0.09, "4.0~5.0", "2009");
    mDataset.addValue(0.25, "5.0~6.0", "2009");
    mDataset.addValue(0.33, "6.0~7.0", "2009");
    mDataset.addValue(0.25, "7.0~8.0", "2009");
    
  
    mDataset.addValue(0.09, "4.0~5.0", "2010");
    mDataset.addValue(0.21, "5.0~6.0", "2010");
    mDataset.addValue(0.44, "6.0~7.0", "2010");
    mDataset.addValue(0.17, "7.0~8.0", "2010");
    
    mDataset.addValue(0.06, "4.0~5.0", "2011");
    mDataset.addValue(0.25, "5.0~6.0", "2011");
    mDataset.addValue(0.37, "6.0~7.0", "2011");
    mDataset.addValue(0.22, "7.0~8.0", "2011");
    
    
    mDataset.addValue(0.05, "4.0~5.0", "2012");
    mDataset.addValue(0.27, "5.0~6.0", "2012");
    mDataset.addValue(0.36, "6.0~7.0", "2012");
    mDataset.addValue(0.22, "7.0~8.0", "2012");
    
    mDataset.addValue(0.06, "4.0~5.0", "2013");
    mDataset.addValue(0.21, "5.0~6.0", "2013");
    mDataset.addValue(0.38, "6.0~7.0", "2013");
    mDataset.addValue(0.25, "7.0~8.0", "2013");
    
    mDataset.addValue(0.09, "4.0~5.0", "2014");
    mDataset.addValue(0.21, "5.0~6.0", "2014");
    mDataset.addValue(0.41, "6.0~7.0", "2014");
    mDataset.addValue(0.19, "7.0~8.0", "2014");
    
    mDataset.addValue(0.14, "4.0~5.0", "2015");
    mDataset.addValue(0.23, "5.0~6.0", "2015");
    mDataset.addValue(0.29, "6.0~7.0", "2015");
    mDataset.addValue(0.21, "7.0~8.0", "2015");
    
    mDataset.addValue(0.11, "4.0~5.0", "2016");
    mDataset.addValue(0.22, "5.0~6.0", "2016");
    mDataset.addValue(0.38, "6.0~7.0", "2016");
    mDataset.addValue(0.16, "7.0~8.0", "2016");
    
    

    
    return mDataset;
  }


}