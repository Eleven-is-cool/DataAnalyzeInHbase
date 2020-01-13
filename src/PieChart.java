
import java.awt.Font;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import javax.swing.JPanel;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.plot.PiePlot;
import org.jfree.data.general.DefaultPieDataset;
import java.awt.GridLayout;
import javax.swing.JFrame;

public class PieChart {
	ChartPanel frame1;
	public PieChart(){
		  DefaultPieDataset data = getDataSet();
	      JFreeChart chart = ChartFactory.createPieChart3D("2015年USA:IMDB评分分布",data,true,false,false);
	      //设置百分比
	      PiePlot pieplot = (PiePlot) chart.getPlot();
	      DecimalFormat df = new DecimalFormat("0.00%");//获得一个DecimalFormat对象，主要是设置小数问题
	      NumberFormat nf = NumberFormat.getNumberInstance();//获得一个NumberFormat对象
	      StandardPieSectionLabelGenerator sp1 = new StandardPieSectionLabelGenerator("{0}  {2}", nf, df);//获得StandardPieSectionLabelGenerator对象
	      pieplot.setLabelGenerator(sp1);//设置饼图显示百分比
	  
	      //没有数据的时候显示的内容
	      pieplot.setNoDataMessage("无数据显示");
	      pieplot.setCircular(false);
	      pieplot.setLabelGap(0.02D);
	  
	      pieplot.setIgnoreNullValues(true);//设置不显示空值
	      pieplot.setIgnoreZeroValues(true);//设置不0显示负值
	      frame1=new ChartPanel (chart,true);
	      chart.getTitle().setFont(new Font("宋体",Font.BOLD,20));//设置标题字体
	      PiePlot piePlot= (PiePlot) chart.getPlot();//获取图表区域对象
	      piePlot.setLabelFont(new Font("宋体",Font.BOLD,10));//解决乱码
	      chart.getLegend().setItemFont(new Font("黑体",Font.BOLD,10));
	}
    private static DefaultPieDataset getDataSet() {
        DefaultPieDataset dataset = new DefaultPieDataset();
        dataset.setValue("评分在1.0~2.0之间",0);
        dataset.setValue("评分在2.0~3.0之间奇",2);
        dataset.setValue("评分在3.0~4.0之间拜",10);
        dataset.setValue("评分在4.0~5.0之间桔",25);
        dataset.setValue("评分在5.0~6.0之间",40);
        dataset.setValue("评分在6.0~7.0之间奇",50);
        dataset.setValue("评分在7.0~8.0之间拜",37);
        dataset.setValue("评分在8.0~9.0之间桔",6);
        dataset.setValue("评分大于等于9.0",0);
        return dataset;
}
    public ChartPanel getChartPanel(){
    	return frame1;
    	
    }   
   
    public static void main(String args[]){
    	JFrame frame=new JFrame("2015年USA:IMDB评分分布");
    	frame.setLayout(new GridLayout(1,1,10,10));
    	//frame.add(new BarChart().getChartPanel());           //添加柱形图
    	frame.add(new PieChart().getChartPanel());           //添加饼状图
    	frame.setBounds(50, 50, 800, 600);
    	frame.setVisible(true);
    }
}

