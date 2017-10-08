package ro.fagadar.daylight;

import java.io.Serializable;

// String X dimensions
public class String2D implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	// 
	public String ShowValue="";
	// 
	public String KeyValue="";
	// additional value
	public String X1Value="";
	// numeric one
	public Double X1DValue=0d;

	public String2D(){
	}

	public String2D(String s, String k){
		this.ShowValue = s;
		this.KeyValue = k;
	}

	public String2D(String s, String k, String x1, Double d1){
		this.ShowValue = s;
		this.KeyValue = k;
		this.X1Value = x1;
		this.X1DValue = d1;
	}

}
