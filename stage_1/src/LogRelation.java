public class LogRelation{

	private String[] data;

	public LogRelation(String line){
		this.data = line.split(",");
	}

	public String getItemId(){
		return data[1];
	}

	public String getMerchantId(){
		return data[3];
	}

	public String getAction(){
		return data[7];
	}
}