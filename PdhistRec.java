package hedge;

public class PdhistRec {
	
	private double p_val;
	private double g_val;
	private double n_val;


// Constructor
    public PdhistRec() {
 	   p_val = 0;
	   n_val = 0;
	   g_val = 0;
    }

    public Double GetNval() {
	   return n_val;
    }

    public Double GetPval() {
	   return p_val;
    }
    
    public Double GetGval() {
	   return g_val;
    }
    
	public void SetPval(double pval) {
		p_val = pval;
	}
	
	public void SetNval(double nval) {
		n_val = nval;
	}

	public void SetGval(double gval) {
		g_val = gval;
	}

}
