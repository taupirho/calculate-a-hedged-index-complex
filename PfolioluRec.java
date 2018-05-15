package hedge;

public class PfolioluRec {

	private String p_desc;
	private String p_startdate;
	private String p_enddate;
    private String i_mnem;
	private String c_mnem;
	
	// Constructor
	public void PluRec() {
		p_desc = "";
		p_startdate = "";
		p_enddate = "";
		i_mnem = "";
		c_mnem = "";
	}

	public String GetPluDesc() {
		return p_desc;
	}	
	
	public void SetPluDesc(String pdesc) {
		p_desc = pdesc;
	}

	public String GetPluStartdate() {
		return p_startdate;
	}	
	
	public void SetPluStartdate(String sdate) {
		p_startdate = sdate;
	}
	
	public String GetPluEnddate() {
		return p_enddate;
	}	
	
	public void SetPluEnddate(String edate) {
		p_enddate = edate;
	}

	public String GetPluImnem() {
		return i_mnem;
	}	
	
	public void SetPluImnem(String imnem) {
		i_mnem = imnem;
	}
	
	public String GetPluCmnem() {
		return c_mnem;
	}	
	
	public void SetPluCmnem(String cmnem) {
		c_mnem = cmnem;
	}
	
}
