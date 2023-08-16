package db;

public class DBIntegritException  extends RuntimeException{
	
	private static final long serialVersionUID = 1L;

	public DBIntegritException(String msg) {
		super(msg);
	}
}