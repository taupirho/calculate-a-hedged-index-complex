package hedge;

public class HedgeluRec {
	private String hedge_desc;
	private String hedge_sdesc;
	private String hedge_startdate;
	private String hedge_enddate;
    private String i_mnem;
	private String hedge_mnem;
	private String c_mnem;
	private double hedge_amt;
	
	// Constructor
	public HedgeluRec() {
		hedge_desc = "";
		hedge_sdesc = "";
		hedge_startdate = "";
		hedge_enddate = "";
		i_mnem = "";
		hedge_mnem = "";
		c_mnem = "";
		hedge_amt = 0.0;
	}

	public String GetHedgeDesc() {
		return hedge_desc;
	}	
	
	public void SetHedgeDesc(String hdesc) {
		hedge_desc = hdesc;
	}

	public String GetHedgeSdesc() {
		return hedge_sdesc;
	}	
	
	public void SetHedgeSdesc(String hsdesc) {
		hedge_sdesc = hsdesc;
	}
	
	public String GetHedgeStartdate() {
		return hedge_startdate;
	}	
	
	public void SetHedgeStartdate(String sdate) {
		hedge_startdate = sdate;
	}
	
	public String GetHedgeEnddate() {
		return hedge_enddate;
	}	
	
	public void SetHedgeEnddate(String edate) {
		hedge_enddate = edate;
	}

	public String GetHedgeImnem() {
		return i_mnem;
	}	
	
	public void SetHedgeImnem(String imnem) {
		i_mnem = imnem;
	}
	public String GetHedgeHmnem() {
		return hedge_mnem;
	}	
	
	public void SetHedgeHmnem(String hmnem) {
		hedge_mnem = hmnem;
	}
	
	public String GetHedgeCmnem() {
		return c_mnem;
	}	
	
	public void SetHedgeCmnem(String cmnem) {
		c_mnem = cmnem;
	}
	
	public double GetHedgeAmt() {
		return hedge_amt;
	}	
	
	public void SetHedgeAmt(double amt) {
		hedge_amt = amt;
	}

}