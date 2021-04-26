/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mx.softver.inversa.api.operacion.facturacion.data;

import com.mx.softver.inversa.operacion.facturacion.factura.datainterface.FacturaCatalogoData;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;

/**
 *
 * @author jhernandez
 */
public class FacturaCatalogoDBDataImpl implements FacturaCatalogoData{
    private final Connection connection;

    /**
     * constructor
     * @param connection 
     */
    public FacturaCatalogoDBDataImpl(Connection connection) {
        this.connection = connection;
    }

    @Override
    public boolean verificarClaveFormaPago(String claveFormaPago)
        throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FORMAS_PAGOS_SAT_VERIFICAR_CLAVE(?,?)}"
        );
        statement.setString("PCLAVE", claveFormaPago);
        statement.registerOutParameter("PVALIDO", Types.BOOLEAN);
        statement.executeUpdate();
        
        boolean valido = statement.getBoolean("PVALIDO");
        statement.close();
        
        return valido;
    }

    @Override
    public boolean verificarClaveMoneda(String claveMoneda) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_MONEDAS_SAT_R_VERIFICAR_CLAVE(?,?)}"
        );
        statement.setString("PCLAVE", claveMoneda);
        statement.registerOutParameter("PVALIDO", Types.BOOLEAN);
        statement.executeUpdate();
        
        boolean valido = statement.getBoolean("PVALIDO");
        statement.close();
        
        return valido;
    }

    @Override
    public boolean verificarClaveUsoCfdi(String claveUsoCfdi) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_USOS_COMPROBANTES_SAT_R_VERIFICAR_CLAVE(?,?)}"
        );
        statement.setString("PCLAVE", claveUsoCfdi);
        statement.registerOutParameter("PVALIDO", Types.BOOLEAN);
        statement.executeUpdate();
        
        boolean valido = statement.getBoolean("PVALIDO");
        statement.close();
        
        return valido;
    }

    @Override
    public boolean verificarClaveTipoRelacion(String claveTipoRelacion)
        throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_TIPOS_RELACIONES_SAT_R_VERIFICAR_CLAVE(?,?)}"
        );
        statement.setString("PCLAVE", claveTipoRelacion);
        statement.registerOutParameter("PVALIDO", Types.BOOLEAN);
        statement.executeUpdate();
        
        boolean valido = statement.getBoolean("PVALIDO");
        statement.close();
        
        return valido;
    }
    
}
