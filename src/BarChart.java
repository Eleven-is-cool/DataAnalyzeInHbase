import java.awt.Font;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import javax.swing.JFrame;
import java.awt.GridLayout;

public class BarChart {
	
	ChartPanel frame1;
	public  BarChart(){
		CategoryDataset dataset = getDataSet();
        JFreeChart chart = ChartFactory.createBarChart3D(
       		                 "", // 图表标题
                            "IMDB电影所属国家分布", // 目录轴的显示标签
                            "国家", // 数值轴的显示标签
                            dataset, // 数据集
                            PlotOrientation.VERTICAL, // 图表方向：水平、垂直
                            true,           // 是否显示图例(对于简单的柱状图必须是false)
                            false,          // 是否生成工具
                            false           // 是否生成URL链接
                            );
        
        //从这里开始
        CategoryPlot plot=chart.getCategoryPlot();//获取图表区域对象
        CategoryAxis domainAxis=plot.getDomainAxis();         //水平底部列表
         domainAxis.setLabelFont(new Font("黑体",Font.BOLD,14));         //水平底部标题
         domainAxis.setTickLabelFont(new Font("宋体",Font.BOLD,12));  //垂直标题
         ValueAxis rangeAxis=plot.getRangeAxis();//获取柱状
         rangeAxis.setLabelFont(new Font("黑体",Font.BOLD,15));
          chart.getLegend().setItemFont(new Font("黑体", Font.BOLD, 15));
          chart.getTitle().setFont(new Font("宋体",Font.BOLD,20));//设置标题字体
          
          //到这里结束，虽然代码有点多，但只为一个目的，解决汉字乱码问题
          
         frame1=new ChartPanel(chart,true);        //这里也可以用chartFrame,可以直接生成一个独立的Frame
         
	}
	   private static CategoryDataset getDataSet() {
           DefaultCategoryDataset dataset = new DefaultCategoryDataset();
           dataset.addValue(442, "UK", "UK");
           dataset.addValue(3763, "USA", "USA");
           dataset.addValue(153, "France", "France");
           dataset.addValue(96, "Germany", "Germany");
           dataset.addValue(123, "Canada", "Canada");
           dataset.addValue(53, "Australia", "Australia");
           dataset.addValue(33, "India", "India");
           dataset.addValue(45, "China", "China");
           dataset.addValue(262, "Others", "Others");
           return dataset;
	   }
	   public ChartPanel getChartPanel(){
		   	return frame1;	
	   }
	public static void main(String args[]){
		JFrame frame=new JFrame("IMDB电影所属国家分布");
		frame.setLayout(new GridLayout(1,1,10,10));
		frame.add(new BarChart().getChartPanel());           //添加柱形图
		frame.setBounds(50, 50, 800, 600);
		frame.setVisible(true);
	}
}