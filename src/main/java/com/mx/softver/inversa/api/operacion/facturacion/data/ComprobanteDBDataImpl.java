/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mx.softver.inversa.api.operacion.facturacion.data;

import com.mx.softver.inversa.operacion.facturacion.factura.datainterface.ComprobanteData;
import com.softver.erp.comunes.comprobantefiscal.entity.cdfi33.ConceptoComprobante;
import com.softver.erp.comunes.comprobantefiscal.entity.cdfi33.EmisorComprobante;
import com.softver.erp.comunes.comprobantefiscal.entity.cdfi33.ReceptorComprobante;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jhernandez
 */
public class ComprobanteDBDataImpl implements ComprobanteData{
    private final Connection connection;
    
    /**
     * constructor
     * @param connection 
     */
    public ComprobanteDBDataImpl(Connection connection) {
        this.connection = connection;
    }

    @Override
    public EmisorComprobante obtenerEmisor(int idEmpresa) throws Exception {
        EmisorComprobante emisor = new EmisorComprobante();
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_CFDI_INFO_EMISOR(?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        ResultSet resultSet = statement.executeQuery();
        
        while(resultSet.next()){
            emisor.setNombre(resultSet.getString("RAZONSOCIAL"));
            emisor.setRfc(resultSet.getString("RFC"));
            emisor.setRegimenFiscal(resultSet.getString("CLAVEREGIMENFISCAL"));
            break;
        }
        resultSet.close();
        statement.close();
        
        return emisor;
    }

    @Override
    public ReceptorComprobante obtenerReceptor(int idFactura) throws Exception {
        ReceptorComprobante receptor = new ReceptorComprobante();
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_CFDI_INFO_RECEPTOR(?)}"
        );
        statement.setInt("PIDFACTURA", idFactura);
        ResultSet resultSet = statement.executeQuery();
        
        while(resultSet.next()){
            receptor.setNombre(resultSet.getString("RAZONSOCIAL"));
            receptor.setRfc(resultSet.getString("RFC"));
            receptor.setUsoCFDI(resultSet.getString("CLAVEUSOCFDI"));
            break;
        }
        resultSet.close();
        statement.close();
        
        return receptor;
    }

    @Override
    public List<ConceptoComprobante> obtenerConceptos(int idFactura)
        throws Exception{
        List<ConceptoComprobante> lista = new ArrayList<>();
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_CFDI_INFO_CONCEPTOS(?)}"
        );
        statement.setInt("PIDFACTURA", idFactura);
        ResultSet resultSet = statement.executeQuery();
        
        while(resultSet.next()){
            ConceptoComprobante concepto = new ConceptoComprobante();
            concepto.setClaveProdServ(resultSet.getString("CLAVEPRODSERV"));
            concepto.setClaveUnidad(resultSet.getString("CLAVEUNIDADSAT"));
            concepto.setUnidad(resultSet.getString("UNIDAD"));
            concepto.setNoIdentificacion(resultSet.getString("NOIDENTIFICACION"));
            concepto.setDescripcion(resultSet.getString("DESCRIPCION"));
            concepto.setCantidad(resultSet.getDouble("CANTIDAD"));
            concepto.setValorUnitario(resultSet.getDouble("VALORUNITARIO"));
            if (resultSet.getDouble("DESCUENTO") != 0) {
                concepto.setDescuento(resultSet.getDouble("DESCUENTO"));                
            }
            concepto.setImporte(resultSet.getDouble("IMPORTE"));
            lista.add(concepto);
        }
        
        resultSet.close();
        statement.close();
        
        return lista;
    }

    @Override
    public String obtenerLugarExpedicion(int idEmpresa) throws Exception {
        String lugarExpedicion = null;
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_CFDI_INFO_LUGAR_EXPEDICION(?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        ResultSet resultSet = statement.executeQuery();
        
        while(resultSet.next()){
            lugarExpedicion = resultSet.getString("CLAVECODIGOPOSTAL");
            break;
        }
        
        resultSet.close();
        statement.close();
        
        return lugarExpedicion;
    }
    
}
