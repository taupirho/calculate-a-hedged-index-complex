package hedge;

public class CompRec {
	private String pmnem;
	private String pcurr;
	private double pmcap;
	private double pm_xrate;
	private double pmf_xrate;

	// Constructors
    public CompRec() {
	   pmnem = "";
	   pcurr = "";
	   pmcap = 0.0;
	   pm_xrate = 0;
	   pmf_xrate = 0;
    }

    public CompRec(String p,String c,double mc) {
	   pmnem = p;
	   pcurr = c;
	   pmcap = mc;
    }
    
	    public String GetPcurr() {
		   return pcurr;
	    }
	    
		public void SetPcurr(String curr) {
			pcurr = curr;
		}
		
	    public String GetPmnem() {
			   return pmnem;
        }
		    
		public void SetPmnem(String p_mnem) {
				pmnem = p_mnem;
		}
		
		public double GetPmcap() {
				   return pmcap;
		}
			    
		public void SetPmcap(double mcap) {
					pmcap = mcap;
		}
	
		public double GetPmxrate() {
			   return pm_xrate;
	}
		    
	public void SetPmxrate(double pmxrate) {
				pm_xrate = pmxrate;
	}
	
	public double GetPmfxrate() {
		   return pmf_xrate;
    }
	    
public void SetPmfxrate(double pmfxrate) {
			pmf_xrate = pmfxrate;
}
}
