package utils;

public class UtilsConversion {
	//rebuild better parser using regex
	public static Double parseMemory (String s){
		//return value of function is in Megabytes MB
		try{
			char unitType = s.charAt(s.length()-1);
			double value = Double.parseDouble(s.substring(0,s.length()-1));
			
			if (unitType == 't'){ //terabytes
				return value * 1024 * 1024;
			}
			else if (unitType == 'g'){ //gigabytes
				return value * 1024;
			}
			else if (unitType == 'm'){ //megabytes
				return value;
			}
			else if (unitType == 'k'){ //kilobytes
				return value / 1024;
			}
			else if (unitType == 'b'){ //bytes
				return value / 1024 / 1024;
			}
			else //just return the value first
				return Double.parseDouble(s);		
			}
		catch(NullPointerException e){
			return Double.parseDouble(s);
		}
	}

}
