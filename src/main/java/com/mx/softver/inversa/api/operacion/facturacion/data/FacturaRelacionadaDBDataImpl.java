/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mx.softver.inversa.api.operacion.facturacion.data;

import com.mx.softver.inversa.operacion.facturacion.factura.datainterface.FacturaRelacionadaData;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.Factura;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.FacturaRelacionada;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author jhernandez
 */
public class FacturaRelacionadaDBDataImpl implements FacturaRelacionadaData{
    private final Connection connection;

    /**
     * constructor
     * @param connection 
     */
    public FacturaRelacionadaDBDataImpl(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void crear(FacturaRelacionada entidad, int idUsuario)
        throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_RELACIONADAS_C(?,?,?,?,?)}"
        );
        statement.setInt("PIDFACTURA", entidad.getIdFactura());
        statement.setInt("PIDFACTURARELACIONADA", entidad.getIdFacturaRelacionada());
        statement.setString("PCLAVETIPORELACION", entidad.getClaveTipoRelacion());
        statement.setInt("PIDUSUARIO", idUsuario);
        statement.registerOutParameter("PID", Types.INTEGER);
        statement.executeUpdate();
        
        entidad.setId(statement.getInt("PID"));
        statement.close();
    }

    @Override
    public boolean verificarId(int idRelacion) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_RELACIONADAS_R_VERIFICAR_ID(?,?)}"
        );
        statement.setInt("PID", idRelacion);
        statement.registerOutParameter("PVALIDO", Types.BOOLEAN);
        statement.executeUpdate();
        
        boolean valido = statement.getBoolean("PVALIDO");
        statement.close();
        
        return valido;
    }

    @Override
    public boolean verificarIdFactura(int idEmpresa, int idFactura)
        throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_R_VERIFICAR_ID(?,?,?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        statement.setInt("PID", idFactura);
        statement.registerOutParameter("PVALIDO", Types.BOOLEAN);
        statement.executeUpdate();
        
        boolean valido = statement.getBoolean("PVALIDO");
        statement.close();
        
        return valido;
    }

    @Override
    public void eliminar(FacturaRelacionada entidad) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_RELACIONADAS_D(?)}"
        );
        statement.setInt("PIDRELACION", entidad.getId());
        statement.executeUpdate();
        
        statement.close();
    }

    @Override
    public List<Factura> obtenerOpciones(int idEmpresa
        , Factura filtro, int idUsuario) throws Exception {
        List<Factura> lista = new ArrayList<>();
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_RELACIONADAS_R_TIMBRADAS_CANCELADAS(?,?)}"
        );
        statement.setInt("PIDCLIENTE", filtro.getIdCliente());
        statement.setInt("PIDEMPRESA", idEmpresa);
        ResultSet resultSet = statement.executeQuery();
        
        while(resultSet.next()) {
            String fechaString = resultSet.getString("FHEXPEDICION");
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date fechaExpedicion = formatter.parse(fechaString);
            
            Factura factura = new Factura();
            factura.setId(resultSet.getInt("ID"));
            factura.setSerie(resultSet.getString("SERIE"));
            factura.setFolio(resultSet.getInt("FOLIO"));
            factura.setUuid(resultSet.getString("UUID"));
            factura.setFechaExpedicion(fechaExpedicion);
            factura.setTotal(resultSet.getDouble("TOTAL"));
            factura.setTipoComprobante(resultSet.getString("TIPOCOMPROBANTE"));
            factura.setEstatus(resultSet.getString("ESTATUS"));
            lista.add(factura);
        }
        resultSet.close();
        statement.close();
        
        return lista;
    }
    
    @Override
    public String obtenerEstatus(int idFactura) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_R_ESTATUS(?)}"
        );
        statement.setInt("PIDFACTURA", idFactura);
        ResultSet resultSet = statement.executeQuery();
        
        String estatus = "";
        
        while(resultSet.next()) {
            estatus = resultSet.getString("ESTATUS");
        }
        
        resultSet.close();
        statement.close();
        
        return estatus;
    }
}
