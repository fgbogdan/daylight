package ro.fagadar.daylight;

public class DebugUtils {

	

	/**
	 * GWT log - the object with trace
	 * 
	 * @param o
	 * @param bComplex
	 */
//	public static void G(Object o, int bComplex) {
//
//		try {
//			throw new Exception("Who called me?");
//		} catch (Exception e) {
//			G("Called by " + e.getStackTrace()[1].getClassName() + "." + e.getStackTrace()[1].getMethodName() + "()!");
//		}
//		G(o.toString());
//	}
	
	
	public static void D(Object o, int bComplex) {
		if(o==null)
			o = "null";
		try {
			throw new Exception("Who called me?");
		} catch (Exception e) {
			D("Called by " + e.getStackTrace()[1].getClassName() + "." + e.getStackTrace()[1].getMethodName() + "()!");
		}
		D(o);
	}
	
	public static void D(Object o) {
		if(o==null)
			o = "null";
		System.out.println(o.toString());
	}

}
