package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
				
				//Chamada por Métoto Auxiliar (mais enxuto)
				Department dep = instantiateDepartment(rs);
				Seller obj = instantiateSeller(rs, dep);
				return obj;				
				
				//Chamada por Instanciação (mais verboso)
				
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
	
	//Método Auxiliar                                              Propaga a Exceção
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

	//Método Auxiliar                                      Propaga a Exceção 
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

	@Override
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "WHERE DepartmentId = ? "
					+ "ORDER BY Name");
			
			st.setInt(1, department.getId());			
			rs = st.executeQuery();
			
			List<Seller> list = new ArrayList<>();
			
			//Controle para não criar objetos 'Department e sim aponta para um só
			Map<Integer, Department> map = new HashMap<>();
			
			//Percorre enquanto houver departamentos
			while(rs.next()) {
				
				//Testa e (ou) procura se o Department ja existe
				Department dep = map.get(rs.getInt("DepartmentId"));
				
				//Se DepartmentID for null ele instacia 
				if(dep == null) {
					//Chamada por Métoto Auxiliar (mais enxuto)
					dep = instantiateDepartment(rs);					
					//Salva e reaproveita
					map.put(rs.getInt("DepartmentId"), dep);
				}
				
				//Chamada por Métoto Auxiliar (mais enxuto)
				Seller obj = instantiateSeller(rs, dep);
				list.add(obj);
			}
			return list;
		} 
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}
}
