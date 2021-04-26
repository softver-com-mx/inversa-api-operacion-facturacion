/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mx.softver.inversa.api.operacion.facturacion.data;

import com.mx.softver.inversa.operacion.facturacion.factura.datainterface.ConceptoData;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.Concepto;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.Impuesto;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jhernandez
 */
public class ConceptoDBDataImpl implements ConceptoData{
    private final Connection connection;

    /**
     * constructor
     * @param connection 
     */
    public ConceptoDBDataImpl(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void crear(int idFactura, Concepto entidad, int idUsuario)
        throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_INVERSA_FACTURAS_CONCEPTOS_C(?,?,?,?,?,?,?,?,?,?)}"
        );
        statement.setInt("PIDFACTURA", idFactura);
        if (entidad.getIdVenta() < 1) {
            statement.setNull("PIDVENTA", Types.NULL);
        } else {
            statement.setInt("PIDVENTA", entidad.getIdVenta());
        }
        if (entidad.getIdArticulo() < 1) {
            statement.setNull("PIDPRODUCTOSERVICIO", Types.NULL);
        } else {
            statement.setInt("PIDPRODUCTOSERVICIO", entidad.getIdArticulo());
        }
        statement.setDouble("PCANTIDAD", entidad.getCantidad());
        statement.setDouble("PVALORUNITARIO", entidad.getValorUnitario());
        statement.setDouble("PDESCUENTO", entidad.getDescuento());
        statement.setDouble("PIMPORTE", entidad.getImporte());
        statement.setString("POBSERVACION", entidad.getObservacion());
        statement.setInt("PIDUSUARIO", idUsuario);
        statement.registerOutParameter("PID", Types.INTEGER);
        statement.executeUpdate();
        
        entidad.setId(statement.getInt("PID"));
        statement.close();
    }

    @Override
    public void crearImpuesto(int idConcepto, Impuesto entidad, int idUsuario)
        throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_CONCEPTOS_IMPUESTOS_C(?,?,?,?,?)}"
        );
        statement.setInt("PIDCONCEPTO", idConcepto);
        if (entidad.getIdArticuloImpuesto() < 1) {
            statement.setNull("PIDIMPUESTOPRODSERV", Types.NULL);
        } else {
            statement.setInt("PIDIMPUESTOPRODSERV", entidad.getIdArticuloImpuesto());
        }
        statement.setDouble("PTOTALIMPUESTO", entidad.getTotalImpuesto());
        statement.setInt("PIDUSUARIO", idUsuario);
        statement.registerOutParameter("PID", Types.INTEGER);
        statement.executeUpdate();
        
        entidad.setId(statement.getInt("PID"));
        statement.close();
    }
    
    @Override
    public Concepto obtenerPorId(int idFactura, int idConcepto) throws Exception {
        Concepto entidad = new Concepto();
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_INVERSA_FACTURAS_CONCEPTOS_R_POR_ID(?,?)}"
        );
        statement.setInt("PIDFACTURA", idFactura);
        statement.setInt("PID", idConcepto);
        ResultSet resultSet = statement.executeQuery();
        
        while(resultSet.next()) {
            entidad.setId(resultSet.getInt("ID"));
            entidad.setIdFactura(resultSet.getInt("IDFACTURA"));
            entidad.setIdVenta(resultSet.getInt("IDVENTA"));
            entidad.setIdArticulo(resultSet.getInt("IDPRODUCTOSERVICIO"));
            entidad.setNombreArticulo(resultSet.getString("NOMBREARTICULO"));
            entidad.setCantidad(resultSet.getDouble("CANTIDAD"));
            entidad.setValorUnitario(resultSet.getDouble("VALORUNITARIO"));
            entidad.setDescuento(resultSet.getDouble("DESCUENTO"));
            entidad.setImporte(resultSet.getDouble("IMPORTE"));
            entidad.setObservacion(resultSet.getString("OBSERVACION"));
            break;
        }
        resultSet.close();
        statement.close();
        
        return entidad;
    }

    @Override
    public List<Impuesto> obtenerImpuestos(int idConcepto) throws Exception {
        List<Impuesto> lista = new ArrayList<>();
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_CONCEPTOS_IMPUESTOS_R(?)}"
        );
        statement.setInt("PIDCONCEPTO", idConcepto);
        ResultSet resultSet = statement.executeQuery();
        
        while(resultSet.next()) {
            Impuesto entidad = new Impuesto();
            entidad.setId(resultSet.getInt("ID"));
            entidad.setIdArticuloImpuesto(resultSet.getInt("IDIMPUESTOPRODSERV"));
            entidad.setTotalImpuesto(resultSet.getDouble("TOTALIMPUESTO"));
            entidad.setClaveImpuestoSat(
                resultSet.getString("CLAVEIMPUESTOSAT") == null
                ? "002"
                : resultSet.getString("CLAVEIMPUESTOSAT")
            );
            entidad.setNombre(
                resultSet.getString("NOMBRE") == null
                ? "IVA 16%"
                : resultSet.getString("NOMBRE")
            );
            entidad.setOrigen(
                resultSet.getString("ORIGEN") == null
                ? "F"
                : resultSet.getString("ORIGEN")
            );
            entidad.setBaseOAcumulado(
                resultSet.getString("BASEOACUMULADO") == null
                ? "B"
                : resultSet.getString("BASEOACUMULADO")
            );
            entidad.setTipoImpuesto(
                resultSet.getString("TIPO") == null
                ? "T"
                : resultSet.getString("TIPO")
            );
            entidad.setFactor(
                resultSet.getString("FACTOR") == null
                ? "T"
                : resultSet.getString("FACTOR")
            );
            entidad.setTasaOCuota(
                resultSet.getDouble("TASAOCUOTA") == 0
                ? 0.16
                : resultSet.getDouble("TASAOCUOTA")
            );
            lista.add(entidad);
        }
        resultSet.close();
        statement.close();
        
        return lista;
    }

    @Override
    public boolean verificarId(int idFactura, int idConcepto) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_CONCEPTOS_R_VERIFICAR_ID(?,?,?)}"
        );
        statement.setInt("PIDFACTURA", idFactura);
        statement.setInt("PID", idConcepto);
        statement.registerOutParameter("PVALIDO", Types.BOOLEAN);
        statement.executeUpdate();
        
        boolean valido = statement.getBoolean("PVALIDO");
        statement.close();
        
        return valido;
    }

    @Override
    public boolean verificarIdImpuesto(int idConcepto, int idImpuesto)
        throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_CONCEPTOS_IMPUESTOS_R_VERIFICAR_ID(?,?,?)}"
        );
        statement.setInt("PIDCONCEPTO", idConcepto);
        statement.setInt("PID", idImpuesto);
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
    public void actualizar(int idFactura, Concepto entidad, int idUsuario)
        throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_CONCEPTOS_U(?,?,?,?,?,?,?,?)}"
        );
        statement.setInt("PIDFACTURA", idFactura);
        statement.setDouble("PCANTIDAD", entidad.getCantidad());
        statement.setDouble("PVALORUNITARIO", entidad.getValorUnitario());
        statement.setDouble("PDESCUENTO", entidad.getDescuento());
        statement.setDouble("PIMPORTE", entidad.getImporte());
        statement.setString("POBSERVACION", entidad.getObservacion());
        statement.setInt("PIDUSUARIO", idUsuario);
        statement.setInt("PID", entidad.getId());
        statement.executeUpdate();
        
        statement.close();
    }

    @Override
    public void actualizarImpuesto(int idConcepto, Impuesto entidad
        , int idUsuario) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_CONCEPTOS_IMPUESTOS_U(?,?,?,?)}"
        );
        statement.setInt("PIDCONCEPTO", idConcepto);
        statement.setDouble("PTOTALIMPUESTO", entidad.getTotalImpuesto());
        statement.setInt("PIDUSUARIO", idUsuario);
        statement.setInt("PID", entidad.getId());
        statement.executeUpdate();
        
        statement.close();
    }

    @Override
    public void eliminar(int idFactura, int idConcepto) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_CONCEPTOS_D(?,?)}"
        );
        statement.setInt("PIDFACTURA", idFactura);
        statement.setInt("PID", idConcepto);
        statement.executeUpdate();
        
        statement.close();
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
