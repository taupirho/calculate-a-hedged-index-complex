package hedge;

public class CurrluRec {
	private String c_mnem;
	private String c_desc;
	private double c_xrate;


// Constructor
    public CurrluRec() {
	   c_mnem = "";
	   c_desc = "";
	   c_xrate = 0;
    }

    public String GetCurrDesc() {
	   return c_desc;
    }	

    public String GetCurrMnem() {
	   return c_mnem;
    }	

    public Double GetCurrXrate() {
	   return c_xrate;
    }
    
	public void SetCurrXrate(double amt) {
		c_xrate = amt;
	}

	public void SetCurrDesc(String desc) {
		c_desc = desc;
	}
	public void SetCurrMnem(String cmnem) {
		c_mnem = cmnem;
	}
}
