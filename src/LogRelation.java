import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/**
 * ��ʾһ����ϵ�����Թ���
 * @author KING
 *
 */
public class LogRelation implements WritableComparable<LogRelation>{
	private int user_id;
	private int item_id;
	private int cat_id;
	private int merchant_id;
	private int brand_id;
	private int month;
	private int day;
	private int action;
	private int age_range;
	private int gender;
	private String province;

	private String line;


//	user_id | ���id
//	item_id | ��Ʒid
//	cat_id | ��Ʒ���id
//	merchant_id | ����id
//	brand_id | Ʒ��id
//	month | ����ʱ��:��
//	day | �����¼�:��
//	action | ��Ϊ,ȡֵ��Χ{0,1,2,3},0��ʾ�����1��ʾ���빺�ﳵ��2��ʾ����3��ʾ��ע��Ʒ
//	age_range | �������ֶΣ�1��ʾ����<18,2��ʾ������[18,24]��3��ʾ������[25,29]��4��ʾ������[30,34]��5��ʾ������[35,39]��6��ʾ������[40,49]��7��8��ʾ����>=50��0��NULL���ʾδ֪
//	gender | �Ա�:0��ʾŮ�ԣ�1��ʾ���ԣ�2��NULL��ʾδ֪
//	province | �ջ���ַʡ��

//	line | String��ʽ��������

	public LogRelation(){}

	public LogRelation(String line){
		String[] value = line.split(",");
		user_id = Integer.parseInt(value[0]);
		item_id = Integer.parseInt(value[1]);
		cat_id = Integer.parseInt(value[2]);
		merchant_id = Integer.parseInt(value[3]);
		brand_id = Integer.parseInt(value[4]);
		month = Integer.parseInt(value[5]);
		day = Integer.parseInt(value[6]);
		action = Integer.parseInt(value[7]);
		age_range = Integer.parseInt(value[8]);
		gender = Integer.parseInt(value[9]);
		province = value[10];
		this.line = line;
	}

	int[] value = {user_id,item_id,cat_id,merchant_id,brand_id,month,day,action,age_range,gender};

	public String getProvince(){
		return province;
	}

	public int getUser_id(){
		return user_id;
	}

	public int getItem_id(){
		return item_id;
	}

	public int getCat_id(){
		return cat_id;
	}

	public int getMerchant_id(){
		return merchant_id;
	}

	public int getBrand_id(){
		return brand_id;
	}

	public int getMonth(){
		return month;
	}

	public int getDay(){
		return day;
	}

	public int getAction(){
		return action;
	}

	public int getAge_range(){
		return age_range;
	}

	public int getGender(){
		return gender;
	}


//
//	public boolean isCondition(int col, String value){
//		if(value.startsWith("l")) {
//			value = value.replace('<', '0');
//			int upperBound = Integer.parseInt(value);
//			if (col == 0 && upperBound > this.id)
//				return true;
//			else if (col == 2 && upperBound > this.age)
//				return true;
//			else if (col == 3 && upperBound > this.weight)
//				return true;
//			else
//				return false;
//		}
//		else if(value.startsWith("m")) {
//			value = value.replace('<', '0');
//			int lowerBound = Integer.parseInt(value);
//			if (col == 0 && lowerBound < this.id)
//				return true;
//			else if (col == 2 && lowerBound < this.age)
//				return true;
//			else if (col == 3 && lowerBound < this.weight)
//				return true;
//			else
//				return false;
//		}
//		else if(col == 0 && Integer.parseInt(value) == this.id)
//			return true;
//		else if(col == 1 && name.equals(value))
//			return true;
//		else if(col ==2 && Integer.parseInt(value) == this.age)
//			return true;
//		else if(col ==3 && Double.parseDouble(value) == this.weight)
//			return true;
//		else
//			return false;
//	}
//
//	public int getId() {
//		return id;
//	}
//
//	public void setId(int id) {
//		this.id = id;
//	}
//
//	public String getName() {
//		return name;
//	}
//
//	public void setName(String name) {
//		this.name = name;
//	}
//
//	public int getAge() {
//		return age;
//	}
//
//	public void setAge(int age) {
//		this.age = age;
//	}
//
//	public double getWeight() {
//		return weight;
//	}
//
//	public void setWeight(double weight) { this.weight = weight; }
//
//	public String getCol(int col){
//		switch(col){
//		case 0: return String.valueOf(id);
//		case 1: return name;
//		case 2: return String.valueOf(age);
//		case 3: return String.valueOf(weight);
//		default: return null;
//		}
//	}
//
//	public String getValueExcept(int col){
//		switch(col){
//		case 0: return name + "," + String.valueOf(age) + "," + String.valueOf(weight);
//		case 1: return String.valueOf(id) + "," + String.valueOf(age) + "," + String.valueOf(weight);
//		case 2: return String.valueOf(id) + "," + name + "," + String.valueOf(weight);
//		case 3: return String.valueOf(id) + "," + name + "," + String.valueOf(age);
//		default: return null;
//		}
//	}
	
	@Override
	public String toString(){
		return line;
	}



//	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(user_id);
		out.writeInt(item_id);
		out.writeInt(cat_id);
		out.writeInt(merchant_id);
		out.writeInt(brand_id);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(action);
		out.writeInt(age_range);
		out.writeUTF(province);
	}

//	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

		user_id = in.readInt();
		item_id = in.readInt();
		cat_id = in.readInt();
		merchant_id = in.readInt();
		brand_id = in.readInt();
		month = in.readInt();
		day = in.readInt();
		action = in.readInt();
		age_range = in.readInt();
		province = in.readUTF();

	}

//	@Override
	public int compareTo(LogRelation o) {
		return 0;
	}
}
