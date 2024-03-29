package model.dao.impl;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
	
	//(CREAATE)Cria novo vendedor
	@Override
	public void insert(Seller obj) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement(
					"INSERT INTO seller "
					+ "(Name, Email, BirthDate, BaseSalary, DepartmentId) "
					+ "VALUES "
					+ "(?, ?, ?, ?, ?)", 
					Statement.RETURN_GENERATED_KEYS);// Retorna o Id do novo registro
			
			st.setString(1, obj.getName());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			st.setDouble(4, obj.getBaseSalary());
			st.setInt(5, obj.getDepartment().getId());
			
			int rowsAffected = st.executeUpdate();
			
			//Testa se foi inserido o registro
			if(rowsAffected > 0) {
				ResultSet rs = st.getGeneratedKeys();
				
				//Testa se existe o id gerado
				if(rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);//popula o objeto com o id gerado
				}
				DB.closeResultSet(rs);
			}
			//Exce��o para o caso de nenhuma linha (registro) for inserido 
			else {
				throw new DbException("Unexpected erro! No rows affected!"); 
			}			
		}
		//Exce��o personalizada
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
		}
	}

	//(UPDATE) Atualiza Vendedor
	@Override
	public void update(Seller obj) {
		
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement(
					  "UPDATE seller "
					+ "SET Name = ?, Email = ?, BirthDate = ?, BaseSalary = ?, DepartmentId = ? "
					+ "WHERE Id = ?");// Retorna o Id do novo registro
			
			st.setString(1, obj.getName());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			st.setDouble(4, obj.getBaseSalary());
			st.setInt(5, obj.getDepartment().getId());
			st.setInt(6, obj.getId());
			
			st.executeUpdate();		
		}
		//Exce��o personalizada
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
		}
	}

	//(DELETE)
	@Override
	public void delete(Integer id) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement(	"DELETE FROM seller WHERE Id = ?");
			
			st.setInt(1, id);
			
			st.executeUpdate();
			
		//Exce��o personalizada
		}catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
		}
	}

	//(READ - UNICA)Busca por Id
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
	
	//(CREATE) Cria novo vendedor
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

	//(CREATE) Cria novo Departamento
	//M�todo Auxiliar                                      Propaga a Exce��o 
	private Department instantiateDepartment(ResultSet rs) throws SQLException {
		Department dep = new Department();
		dep.setId(rs.getInt("DepartmentId"));
		dep.setName(rs.getString("DepName"));
		return dep;
	}

	//(READ - GERAL)Busca todos registros
	@Override
	public List<Seller> findAll() {
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName "
					+ "FROM seller INNER JOIN department "
					+ "ON seller.DepartmentId = department.Id "
					+ "ORDER BY Name");
			
			rs = st.executeQuery();
			
			List<Seller> list = new ArrayList<>();
			
			//Controle para n�o criar objetos 'Department e sim aponta para um s�
			Map<Integer, Department> map = new HashMap<>();
			
			//Percorre enquanto houver departamentos
			while(rs.next()) {
				
				//Testa e (ou) procura se o Department ja existe
				Department dep = map.get(rs.getInt("DepartmentId"));
				
				//Se DepartmentID for null ele instancia 
				if(dep == null) {
					//Chamada por M�toto Auxiliar (mais enxuto)
					dep = instantiateDepartment(rs);					
					//Salva e reaproveita
					map.put(rs.getInt("DepartmentId"), dep);
				}
				
				//Chamada por M�toto Auxiliar (mais enxuto)
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

	//(READ - UNICA)Busca por departamento
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
			
			//Controle para n�o criar objetos 'Department e sim aponta para um s�
			Map<Integer, Department> map = new HashMap<>();
			
			//Percorre enquanto houver departamentos
			while(rs.next()) {
				
				//Testa e (ou) procura se o Department ja existe
				Department dep = map.get(rs.getInt("DepartmentId"));
				
				//Se DepartmentID for null ele instacia 
				if(dep == null) {
					//Chamada por M�toto Auxiliar (mais enxuto)
					dep = instantiateDepartment(rs);					
					//Salva e reaproveita
					map.put(rs.getInt("DepartmentId"), dep);
				}
				
				//Chamada por M�toto Auxiliar (mais enxuto)
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
