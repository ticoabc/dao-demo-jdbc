package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao{
	
	private Connection conn;
	
	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}
	
	@Override
	public void insert(Seller obj) {		
	}

	@Override
	public void update(Seller obj) {		
	}

	@Override
	public void delete(Integer id) {		
	}

	//Busca por Id
	@Override
	public Seller findById(Integer id) {
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE seller.Id = ?");
			
			st.setInt(1, id);			
			rs = st.executeQuery();
			if(rs.next()) {
				
				//Chamada por M�toto Auxiliar (mais enxuto)
				Department dep = instantiateDepartment(rs);
				Seller obj = instantiateSeller(rs, dep);
				return obj;				
				
				//Chamada por Instancia��o (mais verboso)
				
				// Instancia Department
//				Department dep = new Department();
//				dep.setId(rs.getInt("DepartmentId"));
//				dep.setName(rs.getString("DepName"));
				
				// Instancia Seller				
//				Seller obj = new Seller();
//				obj.setId(rs.getInt("Id"));
//				obj.setName(rs.getString("Name"));
//				obj.setEmail(rs.getString("Email"));
//				obj.setBaseSalary(rs.getDouble("BaseSalary"));
//				obj.setBirthDate(rs.getDate("BirthDate"));
//				obj.setDepartment(dep);
//				return obj;
			}
			return null;
		} 
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}
	
	//M�todo Auxiliar                                              Propaga a Exce��o
	private Seller instantiateSeller(ResultSet rs, Department dep) throws SQLException {
		Seller obj = new Seller();
		obj.setId(rs.getInt("Id"));
		obj.setName(rs.getString("Name"));
		obj.setEmail(rs.getString("Email"));
		obj.setBaseSalary(rs.getDouble("BaseSalary"));
		obj.setBirthDate(rs.getDate("BirthDate"));
		obj.setDepartment(dep);
		return obj;
	}

	//M�todo Auxiliar                                      Propaga a Exce��o 
	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department dep = new Department();
		dep.setId(rs.getInt("DepartmentId"));
		dep.setName(rs.getString("DepName"));
		return dep;
	}

	@Override
	public List<Seller> findAll() {
		return null;
	}
}
