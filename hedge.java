package hedge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;



class hedge {

	public static final String CHASURL = "jdbc:oracle:thin:@10.121.60.45:1521:jcqt";
	public static final String DAVEURL = "jdbc:oracle:thin:@10.121.60.44:1521:jcqt";
	public static final String DBUSER = "capel";
	public static final String DBPASS = "james";
	public static Connection con;
	public boolean status = false;
    public static HedgeluRec hluRec;
    public static PfolioluRec pluRec;
    public static String qdate = null;
    public static CurrluRec cluRec;
    public static PdhistRec pdhRec;   
    public static PdhistRec pmhRec;
    public static String data_date = null;
    public static double hedge_val = 0.0;
    public static double hedge_nettr = 0.0;
    public static double hedge_grosstr = 0.0;
    public static double hedgem_val = 0.0;
    public static double hedgem_nettr = 0.0;
    public static double hedgem_grosstr = 0.0;
    public static double unhedge_val = 0.0;
    public static double unhedge_nettr = 0.0;
    public static double unhedge_grosstr = 0.0;
    public static double unhedgem_val = 0.0;
    public static double unhedgem_nettr = 0.0;
    public static double unhedgem_grosstr = 0.0;
    public static double pm_val = 0.0;
    public static double pm_nettr = 0.0;
    public static double pm_grosstr = 0.0;
    public static double hm_val = 0.0;
    public static double hm_nettr = 0.0;
    public static double hm_grosstr = 0.0;
    public static double um_val = 0.0;
    public static double um_nettr = 0.0;
    public static double um_grosstr = 0.0;
    public static int debug = 0;
    
	
	
	public static void db_connect(String db) throws SQLException {


                DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

	            // Connect to Oracle Database
	    	   
	        	if(db.equals("CHAS"))
	        	{
	         	   System.out.println("Running on CHAS");
	               con = DriverManager.getConnection(CHASURL, DBUSER, DBPASS);
	        	}
	        	else
	        	{
	          	   System.out.println("Running on DAVE");
	               con = DriverManager.getConnection(DAVEURL, DBUSER, DBPASS);
	        	}
	}
	
	public static boolean get_hedgelu_data(String h_mnem) throws SQLException {
 		// build SQL query
		
		String q = "";
        int rowCount = 0;
		  
        q+= "select hedge_desc, hedge_sdesc, i_mnem, c_mnem,hedge_startdate, hedge_enddate, nvl(hedge_amt,100.0) ";
        q+= "from hedgelu where hedge_mnem = upper('" + h_mnem + "')";
        
		System.out.println("get_hedgelu_data query = " + q);
		System.out.println("");
		
    	// Load Oracle JDBC Driver;
        
		hluRec = new HedgeluRec();
		
		try {
			
		    Statement statement = con.createStatement();
        
            System.out.println(q);
        
            ResultSet rs = statement.executeQuery(q);
                   

            while (rs.next()) {
        	
        	    hluRec.SetHedgeDesc(rs.getString(1));
                hluRec.SetHedgeSdesc(rs.getString(2));
                hluRec.SetHedgeImnem(rs.getString(3));
                hluRec.SetHedgeCmnem(rs.getString(4));
                hluRec.SetHedgeStartdate(rs.getString(5));
                hluRec.SetHedgeEnddate(rs.getString(6));
                hluRec.SetHedgeAmt(rs.getDouble(7));
           
                rowCount++;
           
                break;
            }
          
	        rs.close();
            statement.close();
		}		
		catch (SQLException e) {
			return false;			
		}
		
        if(rowCount > 0) {
            return true;
        }
        else
        	return false;

   	}

	public static boolean get_pfoliolu_data(String p_mnem) throws SQLException {
 		// build SQL query
		
		String q = "";
        int rowCount = 0;
		  
        q+= "select p_desc, p_startdate, p_enddate, i_mnem, c_mnem ";
        q+= "from p_foliolu where p_mnem = upper('" + p_mnem + "')";


		System.out.println("get_pfoliolu_data query = " + q);
		System.out.println("");
		
        pluRec = new PfolioluRec();
        
		try {
			
	    	// Load Oracle JDBC Driver;
	        Statement statement = con.createStatement();
	        
	        System.out.println(q);
	        
	        ResultSet rs = statement.executeQuery(q);

	        while (rs.next()) {
	        	
	        	pluRec.SetPluDesc(rs.getString(1));
	            pluRec.SetPluStartdate(rs.getString(2));
	            pluRec.SetPluEnddate(rs.getString(3));
	            pluRec.SetPluImnem(rs.getString(4));
	            pluRec.SetPluCmnem(rs.getString(5));
	           
	            rowCount++;
	           
	            break;
	        }
	                   
	        rs.close();
	        statement.close();
	       
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			return false;
		}

		if(rowCount > 0) {
	        return true;
	    }
		else
			return false;

   	}
	
	public static boolean verify_date(String dte) throws SQLException {
		
		String q = "select to_date('" + dte + "','yyyy-mm-dd') - to_date('31-dec-85') from datelu";
		int x = 0;
		
        Statement statement;
		ResultSet rs;
        
		if(debug == 1)
		System.out.println("verify date sql = " + q);
		
		try {
			statement = con.createStatement();
		
			rs = statement.executeQuery(q);
        
			while (rs.next()) {
			   x = 1;
			   break;
			}
	    
			rs.close();
      	    statement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return false;
		}

		if(x == 1)
	        return true;
		else
			return false;

	}

	public static boolean verify_curr_mnem(String curr , String dte) throws SQLException {
		
		String q = "select c_mnem from curr where c_mnem = '" + curr + "' and c_dlystart <= to_date('" + dte + "','yyyy-mm-dd')";
		
		int x = 0;
		
        Statement statement;
		ResultSet rs;
        if(debug == 1)
		System.out.println("verify curr sql = " + q);
		
		try {
			statement = con.createStatement();
		
			rs = statement.executeQuery(q);
        
			while (rs.next()) {
			   x = 1;
			   break;
			}
	    
			rs.close();
      	    statement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return false;
		}

		if(x == 1)
	        return true;
		else
			return false;

	}

	public static boolean get_currlu_data(String curr) throws SQLException {
		
		String q = "select c_mnem, c_desc,c_xrate from curr where c_mnem = '" + curr + "'" ;
		
		int x = 0;
		
        Statement statement;
		ResultSet rs;
        
        if(debug == 1)
        	System.out.println("get_currlu sql = " + q);
	    cluRec = new CurrluRec();
		try {
			statement = con.createStatement();
		
			rs = statement.executeQuery(q);
        
			while (rs.next()) {
			   cluRec.SetCurrMnem(rs.getString(1));
			   cluRec.SetCurrDesc(rs.getString(2));
			   cluRec.SetCurrXrate(rs.getDouble(3));
			   x = x + 1;
			   break;
			}
		
	    
			rs.close();
      	    statement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return false;
		}

		if(x == 1)
	        return true;
		else
			return false;
	}

	public static boolean portfolio_values(String pmnem,String pdate) throws SQLException {
		
		String q = "select NVL(p_val,0), NVL(p_nettr,0), NVL(p_grosstr,0) from p_dhist where p_mnem = upper('" + pmnem + "') and p_valdate = to_date('" + pdate.substring(0,10) +"','yyyy-mm-dd')";

		int x = 0;
		
        Statement statement;
		ResultSet rs;
        
        if(debug == 1)
		System.out.println("portfolio_values sql = " + q);
		
	    pdhRec = new PdhistRec();
	    
		try {
			statement = con.createStatement();
		
			rs = statement.executeQuery(q);
        
			while (rs.next()) {
			   pdhRec.SetPval(rs.getDouble(1));
			   pdhRec.SetNval(rs.getDouble(2));
			   pdhRec.SetGval(rs.getDouble(3));
			   x = x + 1;
			   break;
			}
	    
			rs.close();
      	    statement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return false;
		}

		if(x == 1)
	        return true;
		else
			return false;
	}


	private static String portfolio_curr(String pmnem, String start_date) throws SQLException {

		String curr = "";
		
		String q = "";
		
		q = q + "select c_mnem from p_curr where p_mnem = upper('" + pmnem + "') and c_startdate <= to_date('" + start_date.substring(0, 10) +"','yyyy-mm-dd') ";
		q = q + "and c_enddate is null or c_enddate >= to_date('" + start_date.substring(0, 10) +"','yyyy-mm-dd')";
		
		
		int x = 0;
		
        Statement statement;
        
		ResultSet rs;
        
        if(debug == 1)
		System.out.println("portfolio_curr sql = " + q);
	
	    
		try {
			statement = con.createStatement();
		
			rs = statement.executeQuery(q);
        
			while (rs.next()) {
			   curr = rs.getString(1);
			   x = x + 1;
			   break;
			}
	    
			rs.close();
      	    statement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return null;
		}

		if(x == 1)
	        return curr;
		else
			return null;
	}
	
	private static double exchange_rate2(String c_mnem1, String c_mnem2,String c_date) throws Exception {
		        
        double top_xrate, bot_xrate, exchange_rate;


        if( !c_mnem1.equals("#") && !c_mnem1.equals(c_mnem2)) 
            top_xrate = exchange_rate(c_mnem1,c_date);
        else
            top_xrate = 1.0;        

        if( !c_mnem2.equals("#")  && !c_mnem1.equals(c_mnem2) ) 
            bot_xrate = exchange_rate(c_mnem2,c_date);
        else
            bot_xrate = 1.0;
        
        return top_xrate / bot_xrate;

	}
	
	private static double forward_rate2(String c_mnem1, String c_mnem2,String cfterm, String c_date) throws Exception {
		
		
	       double top_xrate=1, bot_xrate=1, exchange_rate=1;


	        if( !c_mnem1.equals("#") && !c_mnem1.equals(c_mnem2))
					top_xrate = forward_rate(c_mnem1,cfterm,c_date);
			else
	            top_xrate = 1.0;        

	        if( !c_mnem2.equals("#")  && !c_mnem1.equals(c_mnem2)) 
	            bot_xrate = forward_rate(c_mnem2,cfterm,c_date);
	        else
	            bot_xrate = 1.0;
	        
	        return top_xrate / bot_xrate;	
	}

    private static double forward_rate(String c_mnem, String cfterm,String base_date)  throws SQLException {
		
	    String monthend_date;
        double monthend_rate, exchange_rate, forward_xrate = 0.0;
        String mondate = null;
        Statement statement;
        ResultSet rs;
        String q = null;
        int x = 0;

        if (!c_mnem.equals("#")) {

	          mondate = monthend_date(base_date);

	          int lc = 0;

	          if (!base_date.substring(0, 10).equals(mondate.substring(0, 10))){
	        	  
	    		q = "";
	    		q = q + "select cf_xrate from cf_dhist where c_mnem = '" + c_mnem + "' and cf_term = '" + cfterm + "' ";
	    		q = q + "and cf_date > to_date('" + mondate.substring(0, 10) + "','yyyy-mm-dd') and cf_date <= to_date('" + base_date.substring(0, 10) + "','yyyy-mm-dd') order by cf_date"; 
	    		
	            if(debug == 1)
	    		System.out.println("forward_rate sql = " + q);
	    		
	    		try {
	    			statement = con.createStatement();
	    		
	    			rs = statement.executeQuery(q);
	            
	    			while (rs.next()) {
	    			   forward_xrate = rs.getDouble(1);
	    			   x = x + 1;
	    			}
	    	    
	    			rs.close();
	          	    statement.close();

	    		} 
	    		catch (SQLException e) {
	    			// TODO Auto-generated catch block
	    			return 0.0;
	    		}
	    		
	    		if(x <= 0)
	    			
  	               forward_xrate = monthend_forward(mondate,c_mnem,cfterm);
	          }
	          else
	            forward_xrate = monthend_forward(mondate,c_mnem,cfterm);
        
              return forward_xrate;
	        
        }
		return forward_xrate;
    }
	
	private static double exchange_rate(String c_mnem, String base_date)  throws SQLException {
		
	    String monthend_date;
        double monthend_rate, exchange_rate, current_xrate = 0;
        String mondate = null;
        Statement statement;
        ResultSet rs;
        String q = null;
        int x = 0;

        if (c_mnem != "#") {

	          mondate = monthend_date(base_date);

	          int lc = 0;

	          if (base_date.substring(0, 10).equals(data_date.substring(0, 10))){
	        	  
	    		q = "";
	    		q = q + "select c_xrate from curr where c_mnem = '" + c_mnem + "'";
	            if(debug == 1)
	    		System.out.println("exchange_rate sql = " + q);
	    		
	    		try {
	    			statement = con.createStatement();
	    		
	    			rs = statement.executeQuery(q);
	            
	    			while (rs.next()) {
	    			   current_xrate = rs.getDouble(1);
	    			   x = x + 1;
	    			   break;
	    			}
	    	    
	    			rs.close();
	          	    statement.close();

	    		} 
	    		catch (SQLException e) {
	    			// TODO Auto-generated catch block
	    			return 0.0;
	    		}
	    		
                if(x == 1)
   	               exchange_rate = current_xrate;
                else
                   exchange_rate = 0.0;
	          }
	          else if (base_date != mondate.substring(0, 10)) {
	            q = "";
	            q = q + "select c_xrate from c_dhist where c_date > to_date('" + mondate.substring(0,10) + "','yyyy-mm-dd') ";
	            q = q + "and c_date <= to_date('" + base_date.substring(0,10) +  "','yyyy-mm-dd') ";
	            q = q + "and c_mnem = '" + c_mnem + "' order by c_date";
	    		System.out.println("exchange_rate sql = " + q);

	            try {
	    			statement = con.createStatement();
	    		
	    			rs = statement.executeQuery(q);
	            
	    			while (rs.next()) {
	    			   current_xrate = rs.getDouble(1);
	    			   x = x + 1;
	    			}
	    	    
	    			rs.close();
	          	    statement.close();

	    		} 
	    		catch (SQLException e) {
	    			// TODO Auto-generated catch block
	    			return 0.0;
	    		}
	    		
	            if(x > 0)
	              exchange_rate = current_xrate;
	            else
	              exchange_rate = monthend_rate(mondate,c_mnem);
	          }
	          else
	            exchange_rate = monthend_rate(mondate,c_mnem);
        }     
	    else
	       exchange_rate = 1.0;
        
        return exchange_rate;
	        

	}
	
	
	private static String monthend_date(String base_date) throws SQLException {
		
		String q = "";
		
		q = q + "select last_day(add_months(to_date('" + base_date.substring(0, 10) + "','yyyy-mm-dd') +1,-1)) from dual";
		
        String mondate = null;
        
		int x = 0;
		
        Statement statement;
        
		ResultSet rs;
        
        if(debug == 1)
		System.out.println("monthend_date sql = " + q);
		
	    
		try {
			statement = con.createStatement();
		
			rs = statement.executeQuery(q);
        
			while (rs.next()) {
			   mondate = rs.getString(1);
			   x = x + 1;
			   break;
			}
	    
			rs.close();
      	    statement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return null;
		}

		if(x == 1)
	        return mondate;
		else
			return null;
	}
	
	private static String next_monthend(String prev_month) throws SQLException {
		
		String q = "";
		
		q = q + "select last_day(to_date('" + prev_month.substring(0, 10) + "','yyyy-mm-dd') +1) from dual";
		
        String mondate = null;
        
		int x = 0;
		
        Statement statement;
        
		ResultSet rs;
        
        if(debug == 1)
		System.out.println("next_monthend sql = " + q);
		
	    
		try {
			statement = con.createStatement();
		
			rs = statement.executeQuery(q);
        
			while (rs.next()) {
			   mondate = rs.getString(1);
			   x = x + 1;
			   break;
			}
	    
			rs.close();
      	    statement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return null;
		}

		if(x == 1)
	        return mondate;
		else
			return null;
	}
	
	private static double monthend_rate(String mondate, String c_mnem) throws SQLException {
		
		String q = "";
		
		q = q + "select c_xrate from c_mhist where c_mnem = '" + c_mnem + "' and c_mondate = to_date('" + mondate.substring(0,10) + "','yyyy-mm-dd')";

        if(debug == 1)
		System.out.println("monthend_rate sql = " + q);
		
		int x = 0;
		double rate = 0.0;
		
        Statement statement;
        
		ResultSet rs;        		
	    
		try {
			statement = con.createStatement();
		
			rs = statement.executeQuery(q);
        
			while (rs.next()) {
			   rate = rs.getDouble(1);
			   x = x + 1;
			   break;
			}
	    
			rs.close();
      	    statement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return 0.0;
		}

		if(x == 1)
	        return rate;
		else
			return 0.0;
	}
	
	private static double monthend_forward(String mondate, String c_mnem,String cfterm) throws SQLException {
		
		String q = "";
		
		q = q + "select cf_xrate from cf_mhist where c_mnem = '" + c_mnem + "' and cf_mondate = to_date('" + mondate.substring(0,10) + "','yyyy-mm-dd') and cf_term = '" + cfterm + "'";

        if(debug == 1)
		System.out.println("monthend_forward sql = " + q);
		
		int x = 0;
		double rate = 0.0;
		
        Statement statement;
        
		ResultSet rs;        		
	    
		try {
			statement = con.createStatement();
		
			rs = statement.executeQuery(q);
        
			while (rs.next()) {
			   rate = rs.getDouble(1);
			   x = x + 1;
			   break;
			}
	    
			rs.close();
      	    statement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return 0.0;
		}

		if(x == 1)
	        return rate;
		else
			return 0.0;
	}

	public static String data_date() throws SQLException {
 
		Statement statement;
		String q = null;
		ResultSet rs;
		int x = 0;
		
		try {
			
	        statement = con.createStatement();

			q = "select today from datelu";
		
	        if(debug == 1)
	        System.out.println("data_date sql = " + q);

	        rs = statement.executeQuery(q);

	    // extract data from the ResultSet
   	        while (rs.next()) {
	          data_date = rs.getString(1);
	          x=x+1;
	          break;
	        }
	    
	       rs.close();
	       statement.close();
		
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			return null;
		}
    
        if(x > 0)
        	return data_date;
        else
        	return null;
	}
	
	
	public static void write_values2(String hmnem,String hedge_date,double unhedge_val,double unhedge_nettr,double unhedge_grosstr, double hedge_val,double hedge_nettr,double hedge_grosstr) throws SQLException {

        
        String q = "";
                
        q = "delete from hedge_dhist where hedge_mnem = upper('" + hmnem + "') and hedge_date >= to_date('" + hedge_date.substring(0, 10) + "','YYYY-MM-DD')";
        if(debug == 1)
          System.out.println(q);

        PreparedStatement preparedStatement = con.prepareStatement(q);
   		//preparedStatement.setString(1, hmnem);
   		//preparedStatement.setString(2, hedge_date);
   		    			
   		//int deleteCount = preparedStatement.executeUpdate();


        preparedStatement = con.prepareStatement("delete from hedge_mhist where hedge_mnem = upper(?) and hedge_mondate = last_day(to_date(?,'YYYY-MM-DD'))");
   		preparedStatement.setString(1, hmnem);
   		preparedStatement.setString(2, hedge_date);
   		    			
   		//deleteCount = preparedStatement.executeUpdate();

   		q = "";
        q = q + "insert into hedge_dhist(hedge_mnem,hedge_date,hedge_val, hedge_nettr, hedge_grosstr,u_val, u_nettr, u_grosstr) ";
        q = q + "values(upper('" + hmnem + "'),to_date('" + hedge_date + "','YYYY-MM-DD')," + hedge_val + "," + hedge_nettr;
        q = q + "," + hedge_grosstr + "," + unhedge_val + "," + unhedge_nettr + "," + unhedge_grosstr + ")";
        
        if(debug == 1)
        System.out.println("write_values2 hedge_dhist sql = " + q);
        preparedStatement = con.prepareStatement(q);
       
   		//int insertCount = preparedStatement.executeUpdate();


   		q = "";
        q = q + "insert into hedge_mhist(hedge_mnem,hedge_date,hedge_mondate,hedge_val, hedge_nettr, hedge_grosstr,u_val, u_nettr, u_grosstr) ";
        q = q + "values(upper('" + hmnem + "'), to_date('" + hedge_date + "','YYYY-MM-DD'),last_day(to_date('" + hedge_date + "','YYYY-MM-DD'))" + hedge_val + "," + hedge_nettr;
        q = q + "," + hedge_grosstr + "," + unhedge_val + "," + unhedge_nettr + "," + unhedge_grosstr + ")";

        if(debug == 1)
        System.out.println("write_values2 hedge_mhist sql = " + q);
        preparedStatement = con.prepareStatement(q);
   		//insertCount = preparedStatement.executeUpdate();
	}
	
	
	private static int monthend_values(String hmnem,String start_month) {
		String q = "";
		
		q = q + "select NVL(hedge_val,0), NVL(hedge_nettr,0.0), NVL(hedge_grosstr,0.0),NVL(u_val,0.0), NVL(u_nettr,0.0), NVL(u_grosstr,0.0) ";
        q = q + "from hedge_mhist where hedge_mnem = upper('" + hmnem + "') and hedge_mondate = to_date('" + start_month.substring(0,10) + "','yyyy-mm-dd') ";

        
		System.out.println("hedge monthend values sql = " + q);
		
		int x = 0;		
        Statement statement;
        
		ResultSet rs;        		
	    
		try {
			statement = con.createStatement();
		
			rs = statement.executeQuery(q);
        
			while (rs.next()) {
				   hedgem_val = rs.getDouble(1);
				   hedgem_nettr = rs.getDouble(2);
				   hedgem_grosstr = rs.getDouble(3);
				   unhedgem_val = rs.getDouble(4);
				   unhedgem_nettr = rs.getDouble(5);
				   unhedgem_grosstr = rs.getDouble(6);
			       x = x + 1;
			       break;
			}
	    
			rs.close();
      	    statement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return 0;
		}			
		if(x == 1)
			return 1;
		else
			return 0;
		
	}

	private static int hedge_values(String hmnem,String hedge_date) {
		
		String q = "";
		
		q = q + "select NVL(hedge_val,0), NVL(hedge_nettr,0.0), NVL(hedge_grosstr,0.0),NVL(u_val,0.0), NVL(u_nettr,0.0), NVL(u_grosstr,0.0) ";
        q = q + "from hedge_dhist where hedge_mnem = upper('" + hmnem + "') and hedge_date = to_date('" + hedge_date.substring(0,10) + "','yyyy-mm-dd')";

        if(debug == 1)
		System.out.println("hedge_values sql = " + q);
		
		int x = 0;		
        Statement statement;
        
		ResultSet rs;        		
	    
		try {
			statement = con.createStatement();
		
			rs = statement.executeQuery(q);
        
			while (rs.next()) {
				   hedge_val = rs.getDouble(1);
				   hedge_nettr = rs.getDouble(2);
				   hedge_grosstr = rs.getDouble(3);
				   unhedge_val = rs.getDouble(4);
				   unhedge_nettr = rs.getDouble(5);
				   unhedge_grosstr = rs.getDouble(6);
			       x = x + 1;
			       break;
			}
	    
			rs.close();
      	    statement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return 0;
		}			
		if(x == 1)
			return 1;
		else
			return 0;
	}

	
	private static String latest_date(String hmnem) {
		
       String q = "select hedge_date from hedge_dhist where hedge_mnem = upper('" + hmnem + "') and hedge_val is not null order by 1 desc";

       String temp_qdate =null;
       System.out.println("latest date  query = " + q);

       Statement statement;
       
		ResultSet rs;        		
	    
		try {
		
		   statement = con.createStatement();

           rs = statement.executeQuery(q);

    // extract data from the ResultSet
           while (rs.next()) {
              temp_qdate = rs.getString(1);
              break;
           }
       
           rs.close();
           statement.close();
		}  catch (SQLException e) {
			// TODO Auto-generated catch block
			return null;
		}			

		return temp_qdate;
	}
       
	private static Integer days_diff(String dte1,String dte2) throws SQLException {
       
	String q = "";
    if( dte1 == null || dte1.isEmpty()) {
    	if(dte2 == null || dte2.isEmpty())
    		return -1;
    	else
        q = q + "select today - to_date('" + dte2.substring(0, 10) + "','yyyy-mm-dd'),";
        q = q + "to_date('" + dte2.substring(0, 10) + "','yyyy-mm-dd') + decode(";
        q = q + "to_char(to_date('" + dte2.substring(0, 10) + "','yyyy-mm-dd'),'DY'),'MON', -3,'SUN', -2,-1) from datelu";
     }
     else {
        q = q + "select to_date('" + dte1.substring(0, 10) + "','yyyy-mm-dd') - to_date('" + dte2.substring(0, 10) + "','yyyy-mm-dd'),";
        q = q + "to_date('" + dte2.substring(0, 10) + "','yyyy-mm-dd') + decode(";
		q = q + "to_char(to_date('" + dte2.substring(0, 10) + "','yyyy-mm-dd'),'DY'),'MON', -3,'SUN', -2,-1) from dual";
     }
     
    if(debug == 1)
     System.out.println("days_diff query = " + q);
     Statement statement;
     int d = 0;
     
     ResultSet rs;        		
	    

	try {
		    qdate = null;
			
			statement = con.createStatement();

	        rs = statement.executeQuery(q);

	    // extract data from the ResultSet
	   	    while (rs.next()) {
	   	        d = rs.getInt(1);
	   	        qdate = rs.getString(2);
	   	        break;
	   		    }
	       
	           rs.close();
	           statement.close();
			}  catch (SQLException e) {
				// TODO Auto-generated catch block
				return 0;
			}			
		     
	    
            rs.close();
            statement.close();     
            
//            System.out.println("d  = : " + d + " q_date = " + qdate);
			return d;
   }
     

	private static String next_working_day(String date1) {
		
	       String q = "select to_date('" + date1.substring(0, 10) + "','yyyy-mm-dd') + decode(to_char(to_date('" + date1.substring(0,10) + "','yyyy-mm-dd'),'DY'),'FRI', 3,'SAT', 2, 1 ) from dual";

	        if(debug == 1)
	       System.out.println("next_working_day  query = " + q);

	       Statement statement;
	       
			ResultSet rs;        		
		    
			try {
			
			   statement = con.createStatement();

	           rs = statement.executeQuery(q);

	    // extract data from the ResultSet
	           while (rs.next()) {
	              return rs.getString(1);
	           }
	       
	           rs.close();
	           statement.close();
			}  catch (SQLException e) {
				// TODO Auto-generated catch block
				return null;
			}			

			return null;
		}


public static boolean portfolio_monthend(String pmnem,String pdate) throws SQLException {
		
		String q = "select NVL(p_val,0), NVL(p_nettr,0), NVL(p_grosstr,0) from p_mhist where p_mnem = upper('" + pmnem + "') and p_mondate = to_date('" + pdate.substring(0, 10) +"','yyyy-mm-dd')";

		int x = 0;
		
        Statement statement;
		ResultSet rs;
        
        
		System.out.println("portfolio_monthend sql = " + q);
		
	    pmhRec = new PdhistRec();
	    
		try {
			statement = con.createStatement();
		
			rs = statement.executeQuery(q);
        
			while (rs.next()) {
			   pmhRec.SetPval(rs.getDouble(1));
			   pmhRec.SetNval(rs.getDouble(2));
			   pmhRec.SetGval(rs.getDouble(3));
			   x = x + 1;
			   break;
			}
	    
			rs.close();
      	    statement.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return false;
		}

		if(x == 1)
	        return true;
		else
			return false;
	}

	
    public static int components(String imnem,String hmnem,String monthend,ArrayList<CompRec> cList) {
    	
    	String pmnem = "";
    	String pcurr = "";
    	double pmcap = 0;
    	
    	String q = "";
    	    	
        q = q + "select h.p_mnem,pc.c_mnem,p_mcap*d.c_xrate/c.c_xrate from c_mhist d, c_mhist c, p_curr pc, p_mhist p, hedgelu u, hedge_portfolios h ";
        q = q + "where d.c_mnem = u.c_mnem and d.c_mondate = to_date('" + monthend.substring(0,10) + "','yyyy-mm-dd') ";
        q = q + "and c.c_mnem = pc.c_mnem and c.c_mondate  = to_date('" + monthend.substring(0,10) + "','yyyy-mm-dd') ";
        q = q + "and pc.c_startdate <= to_date('" + monthend.substring(0,10) + "','yyyy-mm-dd') ";
        q = q + "and (pc.c_enddate >= to_date('" + monthend.substring(0,10) + "','yyyy-mm-dd') or pc.c_enddate is null) ";
        q = q + "and pc.p_mnem = h.p_mnem and p.p_mnem = h.p_mnem and p.p_mondate = to_date('" + monthend.substring(0,10) + "','yyyy-mm-dd') ";
        q = q + "and u.hedge_mnem = upper('" + hmnem + "') and h.id = upper('" + imnem + "')";

        Statement statement;
		ResultSet rs;
        
       
		System.out.println("components sql = " + q);
		
	    
		try {
			statement = con.createStatement();
		
			rs = statement.executeQuery(q);
        
            while (rs.next()) {
        	
               pmnem = rs.getString(1);
               pcurr = rs.getString(2);
               pmcap =  rs.getDouble(3);
          
         	
               CompRec cRec = new CompRec(pmnem,pcurr,pmcap);
               cList.add(cRec);
            }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			return 0;
		}
		return cList.size();
    }       
    
  
    
	public static void main(String[] args) throws Exception {	         

		    String db = args[0].toUpperCase();
	    	String start_month = null;
	    	String cf_term = "1M";
	    	
	    	System.out.println(args.length);
	    	System.out.println(args[0]);
	    	if (args.length != 2) {
	    		System.out.println("Usage: @hedge dbname 1|0   - where 1 means print debug info out and 0 means do not");
	    		System.out.println("  e.g. @hedge CHAS 0");
	    		return;
	    	}
	    			 
	    	debug = Integer.parseInt(args[1]);
	        db_connect(args[0]);

	        //
	        // Read lookup data associated with hedge mnemonic if entered
	    	// equivalent to Adrian's call to hog$get_hedgelu
	        //
	        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	        
		    boolean plu_status = false;
		    boolean hlu_status = true;	    
		    String hmnem ="";
	    	System.out.print("Enter optional hedge mnemonic ['none'] : ");
	    	
	        hmnem = br.readLine();
		    
			if(hmnem.length() > 0) {
		    	
		    	   hlu_status = get_hedgelu_data(hmnem);
		    }
		    else {
			       hmnem = "none";
		    }
			
			if (hlu_status == false) {
			    System.out.print("Error getting hedgelu data ... exiting");
			    System.exit(-1);
			}
			
	        //
	        // Read lookup data associated with portfolio mnemonic 
	    	// equivalent to Adrian's call to get_portfolio_lookup

	    	if(hluRec != null && hluRec.GetHedgeImnem().length() > 0)
		    		System.out.print("Enter portfolio mnemonic [" + hluRec.GetHedgeImnem() + "] : ");
		    else
			    System.out.print("Enter portfolio mnemonic: ");
		    	
	        String imnem = br.readLine();
	        
		    if(imnem.length() > 0) {		    	
		    	plu_status = get_pfoliolu_data(imnem);
		    }
		    else if(hluRec != null)
		    {
		    	imnem = hluRec.GetHedgeImnem();
		    	plu_status = get_pfoliolu_data(imnem);
		    }
		    else  {
		    	System.out.print("No portfolio mnemonic found and no hedge data ... exiting");
		    	System.exit(-1);
		    }
		    	
		    if(plu_status == false) {
		    	System.out.print("Error getting pfoliolu data ... exiting");
			    System.exit(-1);
		    }

		    // get latest date entry in hedge_dhist table for the entered hedge mnemonic
		    // equivalent to Adrian's call to hog$latest_date
		    		    
	        Statement statement = con.createStatement();
		    
	        String temp_qdate = latest_date(hmnem);
	        
		    System.out.println("qdate 1 = : " + temp_qdate);

	        statement = con.createStatement();
		    
	        data_date = data_date();	        

	        //
	        // Equivalent to Adrians hog$days_diff and previous_working_day calls
	        //
	        int d = days_diff(temp_qdate,data_date);
	        String q_date = null;
	        
	        
	        if(d >= 0) {
	        	q_date = qdate;
	        }
	        
            if(hluRec != null && hluRec.GetHedgeStartdate().length() > 0)
       	      q_date = hluRec.GetHedgeStartdate();
	        	        
//	        System.out.println("q_date = " + q_date);

            if(temp_qdate.length() > 0)
	            System.out.print("Enter start date [" + temp_qdate.substring(0,10) + "] : ");
            else
            	System.out.print("Enter start date [" + q_date.substring(0,10) + "] : ");
	    	
	        String start_date = br.readLine();
		    boolean date_ok = false;
		    
	    	if(start_date.length() <= 0)
	    		if(temp_qdate.length()<= 0)
	    		   start_date = q_date.substring(0, 10);
	    		else
	    		   start_date = temp_qdate.substring(0, 10);
		    
	    	//
	    	// equivalent to Adrians verify_date 
	    	//
	    	date_ok = verify_date(start_date);
		    
		    if(date_ok == false) {
		    	System.out.println("Illegal start date entered ... exiting");
		    	System.exit(-1);
		    }
			
			
	        start_month = monthend_date(start_date);

	        System.out.println("start_date = " + start_date);
	        System.out.println("start_month = " + start_month);
	        
	        System.out.print("Enter end date [" + data_date.substring(0, 10) + "] : ");
	    	
	        String end_date = br.readLine();
		    date_ok = false;
		    
		    if(end_date.length() <= 0) 		    	
		    	end_date = data_date.substring(0, 10);		    	
		
	    	date_ok = verify_date(end_date);
		    
			if(date_ok == false) {
		    	System.out.println("Illegal end date entered ... exiting");
		    	System.exit(-1);
		    }
			
	        System.out.println("end_date = " + end_date);

	        boolean curr_ok = false;
	        if(hluRec != null && hluRec.GetHedgeCmnem().length() > 0)
	           System.out.print("Enter currency [" + hluRec.GetHedgeCmnem() + "] : ");
	        else
	           System.out.print("Enter currency  : ");
	        String c_mnem = br.readLine();
	        
	        if(c_mnem.length() <= 0 && hluRec != null) 
	        	c_mnem = hluRec.GetHedgeCmnem();
	        
	        //
	        // Adrians verify_currency_mnemonic2
	        //
	        curr_ok = verify_curr_mnem(c_mnem,start_date);
	        
	        if(curr_ok == false) {
		    	System.out.println("Illegal currency entered ... exiting");
		    	System.exit(-1);
	        	
	        }
	        System.out.println("currency = " + c_mnem);
	        
	        //
	        // Adrians get_currency_lookup
	        //
	        curr_ok = get_currlu_data(c_mnem);
	        
	        if(curr_ok == false) {
	        	
		    	System.out.println("Error getting currlu data  ... exiting");
		    	System.exit(-1);
	        }
	        
	        Boolean pdhist_ok = false;
	        
	        //
	        // Equivalent to Adrians hog$portfolio_values
	        //
	        pdhist_ok = portfolio_values(imnem,start_date);
	        
	        String i_curr = portfolio_curr(imnem,start_date);

	        if(i_curr.length() != 3) {
	        	System.out.println("Error getting portfolio currency ... exiting");
                System.exit(-1);
	        }
	        
	        double i_xrate = exchange_rate2(i_curr,cluRec.GetCurrMnem(),start_date);
	        
	        double x = i_xrate;

 
	        if(hmnem.equals("none") || (hluRec != null && start_date.substring(0, 10).equals(hluRec.GetHedgeStartdate().substring(0, 10)))) {
	               hedge_val = 100.0;
	               hedge_nettr = 100.0;
	               hedge_grosstr = 100.0;
	               hedgem_val = 100.0;
	               hedgem_nettr = 100.0;
	               hedgem_grosstr = 100.0;
	               unhedge_val = 100.0;
	               unhedge_nettr = 100.0;
	               unhedge_grosstr = 100.0;
	               unhedgem_val = 100.0;
	               unhedgem_nettr = 100.0;
	               unhedgem_grosstr = 100.0;

	               write_values2(hmnem,start_date,unhedge_val,unhedge_nettr,unhedge_grosstr, hedge_val,hedge_nettr,hedge_grosstr);
	        }
	        else {
	        	// hog$hedge_values
	        	// hog$monthend_values
	        	int hedge_ok = hedge_values(hmnem,start_date);
	        	monthend_values(hmnem,start_month);
	        }
	        
	        String now_date = start_date;
	        String prev_month = start_month;
	        
	        String next_month = next_monthend(prev_month);
	        String filename = "";
	        
	        if(hmnem.equals("none"))
	        	filename = pluRec.GetPluImnem() + "_HEDGE_" + pluRec.GetPluCmnem() +".HOG";
	        else
	        	filename = hmnem + ".HOG";
	        
	        File file = new File(filename);
	        
	        PrintWriter bw = new PrintWriter(file);
	          
	        try {
	           bw.println("                                                  HEDGE INDEX CALCULATION");
	           if (hluRec != null)
	              bw.println(" Index:" + hluRec.GetHedgeImnem() + ", with markets hedged " + hluRec.GetHedgeAmt() + "% in " + hluRec.GetHedgeCmnem() );
	           else
		              bw.println(" Index:" + imnem + ", with markets hedged " + 0.0 + "% in " + c_mnem );
	           bw.println(" Hedge Index: " + hmnem);
	           bw.println(""); 
	           bw.println("               Index         Spot       Odd_Day         Month       Hedge        Unhdg          Unhdg        TR_Net        TR_Grss       Hedged        Hedged        TR_Net        TR_Grss");
	           bw.println("       Date    Value         Rate         Ratio          Spot       Impact       Return         Value         Value         Value        Return         Value         Value         Value");
	         
	           
	           //bw.close();
	        } catch (Exception e) {
	            System.out.println("Unable to read file " + file.toString());
	        }
	 
	        String prev_date = null;
	        double total_mcap = 0.0;
	        double im_xrate = 0;
	        double h_val = 0.0;
	        double h_nettr = 0.0;
	        double h_grosstr = 0.0;
	        double u_val = 0.0;
	        double u_nettr = 0.0;
	        double u_grosstr = 0.0;
	        int month_days = 0;
	        int np = 0;
        	ArrayList<CompRec> compList  = new ArrayList<CompRec>();
	        		
	        // Adrians do wile hog$days_diff ...   loop
        	//
	        while(days_diff(next_working_day(now_date),end_date) <= 0) {
	        	prev_date = now_date;
	        	now_date = next_working_day(now_date);
	        	System.out.println("Calculating on: " + now_date + " / " + now_date.substring(8, 10));
	        	
	        	
	        	if( (now_date.substring(8,10).compareTo(prev_date.substring(8,10)) < 0 ) || total_mcap == 0.0) {
	        	
					if(total_mcap == 0.0) {
	        			i_curr = portfolio_curr(imnem,prev_month);
	        			im_xrate = exchange_rate2(i_curr,c_mnem,prev_month);
	        			portfolio_monthend(imnem,prev_month);
	        			System.out.println("ME: im_xrate = " + im_xrate + " i_mnem = "+imnem+" prev_month="+prev_month+" pm_val="+pmhRec.GetPval()+"p_val="+pdhRec.GetPval());

	        			pm_val = pmhRec.GetPval();
	        			pm_nettr = pmhRec.GetNval();
	        			pm_grosstr = pmhRec.GetGval();
	        			hm_val = hedgem_val;
	        			hm_nettr = hedgem_nettr;
	        			hm_grosstr = hedgem_grosstr;
	        			um_val = unhedgem_val;
	        			um_nettr = unhedgem_nettr;
	        			um_grosstr = unhedgem_grosstr;
                        bw.println("");
	     	            bw.printf(" %10s %-13.8f %-13.8f          %-13.8f                   %-13.8f %-13.8f %-13.8f           %-13.8f %-13.8f %-13.8f\n",start_date.substring(0,10),pdhRec.GetPval(),i_xrate,im_xrate,unhedge_val,unhedge_nettr,unhedge_grosstr,hedge_val,hedge_nettr,hedge_grosstr);                        
					}
	        		else {
	        			System.out.println("XX p_val="+pdhRec.GetPval()+"  pm_val="+pm_val + "  pm_val2="+pmhRec.GetPval());
	        			im_xrate = i_xrate;
	        			pm_val = pdhRec.GetPval();
	        			pm_nettr = pdhRec.GetNval();
	        			pm_grosstr = pdhRec.GetGval();
	        			hm_val = h_val;
	        			hm_nettr = h_nettr;
	        			hm_grosstr = h_grosstr;
	        			um_val = u_val;
	        			um_nettr = u_nettr;
	        			um_grosstr = u_grosstr;
	        			prev_month = next_month;
	        			next_month= next_monthend(now_date);	        				        			
	        		}					
	        		month_days = days_diff(next_month,prev_month);
	        		
	        		compList = new ArrayList<CompRec>();
	            	
	        		np = components(imnem,hmnem,prev_month,compList);
	        		
	        		System.out.println("no components = " + np);
	        		total_mcap = 0.0;
	        		
	        		for(int i= 0; i < np;i++) {
	        			total_mcap = total_mcap + compList.get(i).GetPmcap();
	        			double pmxrate = exchange_rate2(compList.get(i).GetPcurr(),c_mnem,prev_month);
	        			compList.get(i).SetPmxrate(pmxrate);
	        			double pmfxrate = forward_rate2(compList.get(i).GetPcurr(),c_mnem,cf_term,prev_month);
	        			compList.get(i).SetPmfxrate(pmfxrate);
	        			
//	        			System.out.println(" i = " + i);
	        		}	        		
	        	}
	        	i_curr = portfolio_curr(imnem,now_date);
	        	i_xrate = exchange_rate2(i_curr,c_mnem,now_date);
	        	
	        	double odd_ratio = (double)(days_diff(next_month,now_date))/(double)(month_days);
	        	System.out.println("odd_ration" + "days_diff = " + (double)(days_diff(next_month,now_date)) + "month days = " + (double)(month_days));
	        	pdhist_ok = portfolio_values(imnem,now_date);
	        	
	        	double h = 0.0;
	        	double t = 0.0;
	        	double f = 0.0;
	        	
	        	for(int i= 0;i< np; i++) {
	        		double p_xrate = exchange_rate2(compList.get(i).GetPcurr(),c_mnem,now_date);
	        		double pf_xrate = forward_rate2(compList.get(i).GetPcurr(),c_mnem,cf_term,now_date);
	        	    f = p_xrate + (pf_xrate - p_xrate) * odd_ratio;
	        	    t = (compList.get(i).GetPmcap()/total_mcap) * ( (compList.get(i).GetPmxrate()/compList.get(i).GetPmfxrate()) - (compList.get(i).GetPmxrate()/f) );
                    h = h + t;
	        	    System.out.println("h:" + (compList.get(i).GetPmcap()/total_mcap));
                    System.out.println("h:" + (compList.get(i).GetPmxrate()/compList.get(i).GetPmfxrate()));
                    System.out.println("h:" + (compList.get(i).GetPmxrate()/f));
                    
	        	    System.out.println("curr = " + compList.get(i).GetPcurr() + " i = " + i + "f= " +f + " p_xrate = " +p_xrate + " pf_xrate = " + pf_xrate + " odd ratio = " +odd_ratio);
        	        System.out.println("dte="+now_date.substring(0,10)+" t="+t + " h="+h+" f="+f+" p_mcap(i)="+compList.get(i).GetPmcap()+" total_mcap="+total_mcap+" pm_xrate(i)="+compList.get(i).GetPmxrate()+" pmf_xrate(i)="+compList.get(i).GetPmfxrate());
	        	}
	        	
//	        	double u_perf = ( (pdhRec.GetPval()/i_xrate)/(pmhRec.GetPval()/im_xrate)   ) -1;
	        	double u_perf = ( (pdhRec.GetPval()/i_xrate)/(pm_val/im_xrate)   ) -1;
	        	u_val = um_val * (u_perf + 1);
	        	double h_perf = u_perf + h*.01*hluRec.GetHedgeAmt();
	        	h_val = hm_val * (h_perf + 1);
	        	
	        	if(pdhRec.GetNval()  > 0.0) {
//	        		double u_nperf = ( (pdhRec.GetNval()/i_xrate) /(pmhRec.GetNval()/im_xrate))  - 1;
	        		double u_nperf = ( (pdhRec.GetNval()/i_xrate) /(pm_nettr/im_xrate))  - 1;
	        		u_nettr = um_nettr * (u_nperf + 1);
	        		System.out.println("um_nettr="+um_nettr + " u_nperf=" + u_nperf);
	        		double h_nperf = u_nperf + h*.01*hluRec.GetHedgeAmt();
	        		h_nettr = hm_nettr * (h_nperf + 1); 
	        	}
	        	
	        	if(pdhRec.GetGval()  > 0.0) {
//	        		double u_gperf = ( (pdhRec.GetGval()/i_xrate) /(pmhRec.GetGval()/im_xrate))  - 1;
	        		double u_gperf = ( (pdhRec.GetGval()/i_xrate) /(pm_grosstr/im_xrate))  - 1;
	        		bw.printf("\nu_gperf = %-13.8f       %-13.8f      %-13.8f        %-13.8f        %-13.8f",u_gperf,pdhRec.GetGval(),i_xrate,pm_grosstr,im_xrate);
	        		u_grosstr = um_grosstr * (u_gperf + 1);
	        		double h_gperf = u_gperf + h*.01*hluRec.GetHedgeAmt();
	        		h_grosstr = hm_grosstr * (h_gperf + 1); 
	        	    System.out.println("u_gperf="+u_gperf + "h_gperf = " + h_gperf + " h_grosstr = " + h_grosstr + " h = " +h + " hlurec.gethedgeamt=" + hluRec.GetHedgeAmt() + "p_val="+pdhRec.GetGval()+" i_xrate="+i_xrate+" p_mval="+pm_grosstr+" im_xrate="+im_xrate+" hm_grosstr=" +hm_grosstr);
	    	        bw.println("");
	    	        bw.println("");
	    	        bw.println("U_GPERF       U_GROSSTR       H_GPERF       H              HEDGE_AMT       UM_GROSS_TR    HM_GROSSTR    PDH          PMH           i_xrate          im_xrate");
	    	        bw.printf("%-13.8f       %-13.8f         %-13.8f       %-13.8f        %-13.8f           %-13.8f       %-13.8f      %-13.8f        %-13.8f        %-13.8f          %-13.8f", u_gperf,u_grosstr,h_gperf,h,hluRec.GetHedgeAmt(),um_grosstr,hm_grosstr,pdhRec.GetNval(),pmhRec.GetNval(),i_xrate,im_xrate);
	    	        bw.println("");
	        	}
	        	bw.println("");
 	            bw.printf(" %10s %-13.8f %-13.8f  %-13.8f%-13.8f%-13.8f%-13.8f%-13.8f %-13.8f %-13.8f  %-13.8f%-13.8f %-13.8f %-13.8f",now_date.substring(0,10),pdhRec.GetPval(),i_xrate,odd_ratio,im_xrate,hluRec.GetHedgeAmt()*h, 100*u_perf,u_val,u_nettr,u_grosstr,100*h_perf,h_val,h_nettr,h_grosstr);

 	            write_values2(hmnem,now_date,u_val,u_nettr,u_grosstr,h_val,h_nettr,h_grosstr);
 	            String now_month = next_monthend(now_date);
	        }
	        
	        bw.close();
	        System.out.println("Finished");
	        System.exit(0);  
	}
}
