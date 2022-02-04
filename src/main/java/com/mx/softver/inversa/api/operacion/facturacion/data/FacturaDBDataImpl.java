/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mx.softver.inversa.api.operacion.facturacion.data;

import com.mx.softver.inversa.operacion.facturacion.factura.datainterface.FacturaData;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.Cfdi;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.Concepto;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.ConceptoCompleto;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.Factura;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.FacturaConcepto;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.FacturaFiltro;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.FacturaRelacionada;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.FacturaVistaPrevia;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.Impuesto;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.LogoCompania;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.ReporteFactura;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.UuidConceptos;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * @author jhernandez
 */
public class FacturaDBDataImpl implements FacturaData{
    private final Connection connection;

    /**
     * constructor
     * @param connection 
     */
    public FacturaDBDataImpl(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void crear(int idEmpresa, Factura entidad, int idUsuario)
        throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String fechaExpedicion = dateFormat.format(entidad.getFechaExpedicion());
        
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_C(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        statement.setInt("PIDCLIENTE", entidad.getIdCliente());
        statement.setNull("PIDCOTIZACION", Types.NULL);
        statement.setBoolean("PFACTURACOTIZACION", false);
        if (entidad.getTipoFacturacion() == null) {
            statement.setNull("PTIPOFACTURACION", Types.NULL);
        } else {
            statement.setString("PTIPOFACTURACION", entidad.getTipoFacturacion());
        }
        statement.setString("PTIPOCOMPROBANTE", entidad.getTipoComprobante());
        statement.setString("PSERIE", entidad.getSerie());
        statement.setString("PFHEXPEDICION", fechaExpedicion);
        statement.setInt("PIDSERIEFOLIO", entidad.getIdSerie());
        statement.setString("PCLAVEFORMAPAGO", entidad.getClaveFormaPago());
        statement.setString("PMETODOPAGO", entidad.getClaveMetodoPago());
        statement.setString("PCONDICIONPAGO", entidad.getCondicionPago());
        statement.setString("PCLAVEMONEDA", entidad.getClaveMoneda());
        statement.setDouble("PTIPOCAMBIO", entidad.getTipoCambio());
        statement.setString("PCLAVEUSOCFDI", entidad.getClaveUsoCfdi());
        statement.setDouble("PSUBTOTAL", entidad.getSubtotal());
        statement.setDouble("PDESCUENTO", entidad.getDescuento());
        statement.setDouble("PTOTAL", entidad.getTotal());
        if (entidad.getNumeroCuenta() == null) {
            statement.setNull("PNUMEROCUENTA", Types.NULL);
        } else {
            statement.setString("PNUMEROCUENTA", entidad.getNumeroCuenta());
        }
        statement.setString("PVERSION", entidad.getVersion());
        statement.setString("PCLAVETIPORELACION", entidad.getClaveTipoRelacion());
        statement.setInt("PIDUSUARIO", idUsuario);
        ResultSet resultSet = statement.executeQuery();
        
        while(resultSet.next()) {
            entidad.setId(resultSet.getInt("ID"));
            entidad.setFolio(resultSet.getInt("FOLIO"));
        }
        resultSet.close();
        statement.close();
    }

    @Override
    public List<Factura> obtener(int idEmpresa, FacturaFiltro filtro)
        throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Factura> lista = new ArrayList<>();
        String fecha;
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_R_INVERSA(?,?,?,?,?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        if (filtro.getEstatus().equals("0")) {
            statement.setNull("PESTATUS", Types.NULL);
        } else {
            statement.setString("PESTATUS", filtro.getEstatus());
        }
        if (filtro.getIdCliente() == 0) {
            statement.setNull("PIDCLIENTE", Types.NULL);
        } else {
            statement.setInt("PIDCLIENTE", filtro.getIdCliente());
        }
        if (filtro.getFechaDesde() == null) {
            statement.setNull("PFHINICIO", Types.NULL);
        } else {
            fecha = dateFormat.format(filtro.getFechaDesde());
            statement.setString("PFHINICIO", fecha);
        }
        if (filtro.getFechaHasta() == null) {
            statement.setNull("PFHFIN", Types.NULL);
        } else {
            fecha = dateFormat.format(filtro.getFechaHasta());
            statement.setString("PFHFIN", fecha);
        }
        ResultSet resultSet = statement.executeQuery();
        
        while(resultSet.next()) {
            String fechaString = resultSet.getString("FHEXPEDICION");
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date fechaExpedicion = formatter.parse(fechaString);
            
            Factura factura = new Factura();
            factura.setId(resultSet.getInt("ID"));
            factura.setIdCliente(resultSet.getInt("IDCLIENTE"));
            factura.setTipoFacturacion(resultSet.getString("TIPOFACTURACION"));
            factura.setTipoComprobante(resultSet.getString("TIPOCOMPROBANTE"));
            factura.setSerie(resultSet.getString("SERIE"));
            factura.setFolio(resultSet.getInt("FOLIO"));
            factura.setFechaExpedicion(fechaExpedicion);
            factura.setClaveFormaPago(resultSet.getString("CLAVEFORMAPAGO"));
            factura.setClaveMetodoPago(resultSet.getString("METODOPAGO"));
            factura.setCondicionPago(resultSet.getString("CONDICIONPAGO"));
            factura.setClaveMoneda(resultSet.getString("CLAVEMONEDA"));
            factura.setTipoCambio(resultSet.getDouble("TIPOCAMBIO"));
            factura.setClaveUsoCfdi(resultSet.getString("CLAVEUSOCFDI"));
            factura.setSubtotal(resultSet.getDouble("SUBTOTAL"));
            factura.setDescuento(resultSet.getDouble("DESCUENTO"));
            factura.setTotal(resultSet.getDouble("TOTAL"));
            factura.setNumeroCuenta(resultSet.getString("NUMEROCUENTA"));
            factura.setVersion(resultSet.getString("VERSION"));
            factura.setUuid(resultSet.getString("UUID"));
            factura.setClaveTipoRelacion(resultSet.getString("CLAVETIPORELACION"));
            factura.setNombreCliente(resultSet.getString("RAZONSOCIAL"));
            factura.setEstatus(resultSet.getString("ESTATUS"));
            factura.setFolioTicket(resultSet.getString("FOLIOTICKET"));
            lista.add(factura);
        }
        resultSet.close();
        statement.close();
        
        return lista;
    }

    @Override
    public List<Concepto> obtenerConceptos(int idFactura) throws Exception {
        List<Concepto> lista = new ArrayList<>();
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_INVERSA_FACTURAS_CONCEPTOS_R(?)}"
        );
        statement.setInt("PIDFACTURA", idFactura);
        ResultSet resultSet = statement.executeQuery();
        
        while(resultSet.next()) {
            Concepto entidad = new Concepto();
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
            lista.add(entidad);
        }
        resultSet.close();
        statement.close();
        
        return lista;
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
                entidad.getIdArticuloImpuesto() == 0
                ? "002"
                : resultSet.getString("CLAVEIMPUESTOSAT")
            );
            entidad.setNombre(
                entidad.getIdArticuloImpuesto() == 0
                ? "IVA 16%"
                : resultSet.getString("NOMBRE")
            );
            entidad.setOrigen(
                entidad.getIdArticuloImpuesto() == 0
                ? "F"
                : resultSet.getString("ORIGEN")
            );
            entidad.setBaseOAcumulado(
                entidad.getIdArticuloImpuesto() == 0
                ? "B"
                : resultSet.getString("BASEOACUMULADO")
            );
            entidad.setTipoImpuesto(
                entidad.getIdArticuloImpuesto() == 0
                ? "T"
                : resultSet.getString("TIPO")
            );
            entidad.setFactor(
                entidad.getIdArticuloImpuesto() == 0
                ? "T"
                : resultSet.getString("FACTOR")
            );
            entidad.setTasaOCuota(
                entidad.getIdArticuloImpuesto() == 0
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
    public List<FacturaRelacionada> obtenerFacturasRelacionadas(
        int idFacturaPadre) throws Exception {
        List<FacturaRelacionada> lista = new ArrayList<>();
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_RELACIONADAS_R(?)}"
        );
        statement.setInt("PIDFACTURAPADRE", idFacturaPadre);
        ResultSet resultSet = statement.executeQuery();
        
        while(resultSet.next()) {
            String fechaString = resultSet.getString("FHEXPEDICION");
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date fecha = formatter.parse(fechaString);
            
            FacturaRelacionada entidad = new FacturaRelacionada();
            entidad.setId(resultSet.getInt("ID"));
            entidad.setIdFactura(resultSet.getInt("IDFACTURA"));
            entidad.setIdFacturaRelacionada(resultSet.getInt("IDFACTURARELACIONADA"));
            entidad.setClaveTipoRelacion(resultSet.getString("CLAVETIPORELACION"));
            entidad.setUuid(resultSet.getString("UUID"));
            entidad.setSerie(resultSet.getString("SERIE"));
            entidad.setFolio(resultSet.getInt("FOLIO"));
            entidad.setFechaCreacion(fecha);
            entidad.setTipoComprobante(resultSet.getString("TIPOCOMPROBANTE"));
            lista.add(entidad);
        }
        resultSet.close();
        statement.close();
        
        return lista;
    }

    @Override
    public FacturaConcepto obtenerPorId(int idEmpresa, Factura filtro)
        throws Exception {
        FacturaConcepto factura = null;
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_R_POR_ID(?,?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        statement.setInt("PID", filtro.getId());
        ResultSet resultSet = statement.executeQuery();
        
        while (resultSet.next()) {
            String fechaString = resultSet.getString("FHEXPEDICION");
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date fechaExpedicion = formatter.parse(fechaString);
            
            factura = new FacturaConcepto();
            factura.setId(resultSet.getInt("ID"));
            factura.setIdCliente(resultSet.getInt("IDCLIENTE"));
            factura.setTipoFacturacion(resultSet.getString("TIPOFACTURACION"));
            factura.setTipoComprobante(resultSet.getString("TIPOCOMPROBANTE"));
            factura.setSerie(resultSet.getString("SERIE"));
            factura.setFolio(resultSet.getInt("FOLIO"));
            factura.setFechaExpedicion(fechaExpedicion);
            factura.setClaveFormaPago(resultSet.getString("CLAVEFORMAPAGO"));
            factura.setClaveMetodoPago(resultSet.getString("METODOPAGO"));
            factura.setCondicionPago(resultSet.getString("CONDICIONPAGO"));
            factura.setClaveMoneda(resultSet.getString("CLAVEMONEDA"));
            factura.setTipoCambio(resultSet.getDouble("TIPOCAMBIO"));
            factura.setClaveUsoCfdi(resultSet.getString("CLAVEUSOCFDI"));
            factura.setSubtotal(resultSet.getDouble("SUBTOTAL"));
            factura.setDescuento(resultSet.getDouble("DESCUENTO"));
            factura.setTotal(resultSet.getDouble("TOTAL"));
            factura.setNumeroCuenta(resultSet.getString("NUMEROCUENTA"));
            factura.setVersion(resultSet.getString("VERSION"));
            factura.setUuid(resultSet.getString("UUID"));
            factura.setClaveTipoRelacion(resultSet.getString("CLAVETIPORELACION"));
            factura.setEstatus(resultSet.getString("ESTATUS"));
            break;
        }
        resultSet.close();
        statement.close();
        
        return factura;
    }

    @Override
    public boolean verificarId(int idEmpresa, int idFactura) throws Exception {
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
    public void actualizar(int idEmpresa, Factura entidad, int idUsuario)
        throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String fechaExpedicion = dateFormat.format(entidad.getFechaExpedicion());
        
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_U(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        statement.setInt("PIDCLIENTE", entidad.getIdCliente());
        statement.setNull("PIDCOTIZACION", Types.NULL);
        statement.setBoolean("PFACTURACOTIZACION", false);
        statement.setString("PTIPOFACTURACION", entidad.getTipoFacturacion());
        statement.setString("PTIPOCOMPROBANTE", entidad.getTipoComprobante());
        statement.setString("PFHEXPEDICION", fechaExpedicion);
        statement.setString("PCLAVEFORMAPAGO", entidad.getClaveFormaPago());
        statement.setString("PMETODOPAGO", entidad.getClaveMetodoPago());
        statement.setString("PCONDICIONPAGO", entidad.getCondicionPago());
        statement.setString("PCLAVEMONEDA", entidad.getClaveMoneda());
        statement.setDouble("PTIPOCAMBIO", entidad.getTipoCambio());
        statement.setString("PCLAVEUSOCFDI", entidad.getClaveUsoCfdi());
        statement.setDouble("PSUBTOTAL", entidad.getSubtotal());
        statement.setDouble("PDESCUENTO", entidad.getDescuento());
        statement.setDouble("PTOTAL", entidad.getTotal());
        if (entidad.getNumeroCuenta() == null) {
            statement.setNull("PNUMEROCUENTA", Types.NULL);
        } else {
            statement.setString("PNUMEROCUENTA", entidad.getNumeroCuenta());
        }
        statement.setString("PVERSION", entidad.getVersion());
        statement.setString("PCLAVETIPORELACION", entidad.getClaveTipoRelacion());
        statement.setString("PESTATUS", entidad.getEstatus());
        statement.setInt("PIDUSUARIO", idUsuario);
        statement.setInt("PID", entidad.getId());
        statement.executeUpdate();
        
        statement.close();
    }

    @Override
    public void actualizarEstatus(int idEmpresa, int idFactura, String estatus
        , int idUsuario) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_U_ESTATUS(?,?,?,?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        statement.setString("PESTATUS", estatus);
        statement.setInt("PIDUSUARIO", idUsuario);
        statement.setInt("PID", idFactura);
        
        statement.executeUpdate();
        statement.close();
    }

    @Override
    public String verificarTimbrado(int idEmpresa, int idFactura)
        throws Exception {
        String uuid = null;
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_R_VERIFICA_TIMBRADO(?,?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        statement.setInt("PID", idFactura);
        ResultSet resultSet = statement.executeQuery();
        
        if (resultSet.next()) {
            uuid = resultSet.getString("UUID");
        }
        
        resultSet.close();
        statement.close();
        
        return uuid;
    }

    @Override
    public void timbrar(int idEmpresa, int idFactura, String uuid
        , int idUsuario) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_U_TIMBRAR(?,?,?,?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        statement.setString("PUUID", uuid);
        statement.setInt("PIDUSUARIO", idUsuario);
        statement.setInt("PID", idFactura);
        statement.executeUpdate();
        
        statement.close();
    }

    @Override
    public String obtenerUrlTimbrado() throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_CONFIGURACIONES_ERP_R_RUTA_TIMBRADO_CFDI()}"
        );
        ResultSet resultSet = statement.executeQuery();
        
        String url = "";
        
        while(resultSet.next()) {
            url = resultSet.getString("URLTIMBRADO");
        }
        
        resultSet.close();
        statement.close();
        
        return url;
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

    @Override
    public boolean verificarUuid(int idFactura, String uuid) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_R_VERIFICAR_UUID(?,?,?)}"
        );
        statement.setInt("PID", idFactura);
        statement.setString("PUUID", uuid);
        statement.registerOutParameter("PVALIDO", Types.BOOLEAN);
        statement.executeUpdate();
        
        boolean valido = statement.getBoolean("PVALIDO");
        statement.close();
        
        return valido;
    }

    @Override
    public String obtenerUrlDescargas() throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_CONFIGURACIONES_ERP_R_RUTA_DESCARGAS()}"
        );
        ResultSet resultSet = statement.executeQuery();
        
        String url = "";
        while(resultSet.next()) {
            url = resultSet.getString("URLDESCARGAS");
        }
        resultSet.close();
        statement.close();
        return url;
    }

    @Override
    public String obtenerTokenServicio(int idEmpresa) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_CONFIGURACIONES_SISTEMAS_R_TOKEN_EMPRESA(?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        ResultSet resultSet = statement.executeQuery();
        
        String token = "";
        while(resultSet.next()) {
            token = resultSet.getString("TOKEN");
        }
        resultSet.close();
        statement.close();
        return token;
    }

    @Override
    public List<Factura> obtenerFacturasConSaldo(int idEmpresa
        , FacturaFiltro filtro) throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Factura> lista = new ArrayList<>();
        String fecha;
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_R_CON_SALDO(?,?,?,?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        if (filtro.getIdCliente() == 0) {
            statement.setNull("PIDCLIENTE", Types.NULL);
        } else {
            statement.setInt("PIDCLIENTE", filtro.getIdCliente());
        }
        if (filtro.getFechaDesde() == null) {
            statement.setNull("PFECHADESDE", Types.NULL);
        } else {
            fecha = dateFormat.format(filtro.getFechaDesde());
            statement.setString("PFECHADESDE", fecha);
        }
        if (filtro.getFechaHasta() == null) {
            statement.setNull("PFECHAHASTA", Types.NULL);
        } else {
            fecha = dateFormat.format(filtro.getFechaHasta());
            statement.setString("PFECHAHASTA", fecha);
        }
        ResultSet resultSet = statement.executeQuery();
        
        while(resultSet.next()) {
            String fechaString = resultSet.getString("FHEXPEDICION");
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date fechaExpedicion = formatter.parse(fechaString);
            
            Factura factura = new Factura();
            factura.setId(resultSet.getInt("ID"));
            factura.setIdCliente(resultSet.getInt("IDCLIENTE"));
            factura.setTipoComprobante(resultSet.getString("TIPOCOMPROBANTE"));
            factura.setSerie(resultSet.getString("SERIE"));
            factura.setFolio(resultSet.getInt("FOLIO"));
            factura.setFechaExpedicion(fechaExpedicion);
            factura.setClaveMoneda(resultSet.getString("CLAVEMONEDA"));
            factura.setTipoCambio(resultSet.getDouble("TIPOCAMBIO"));
            factura.setClaveMetodoPago(resultSet.getString("METODOPAGO"));
            factura.setUuid(resultSet.getString("UUID"));
            factura.setTotal(resultSet.getDouble("TOTAL"));
            factura.setParcialidad(resultSet.getInt("PARCIALIDAD"));
            factura.setSaldoAPagar(resultSet.getDouble("SALDOAPAGAR"));
            lista.add(factura);
        }
        resultSet.close();
        statement.close();
        
        return lista;
    }

    @Override
    public void obtenerVistaPrevia(int idEmpresa, FacturaVistaPrevia entidad)
        throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_R_VISTA_PREVIA(?,?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        statement.setInt("PIDFACTURA", entidad.getId());
        ResultSet resultSet = statement.executeQuery();
        
        while (resultSet.next()) {
            entidad.setNombreEmisor(resultSet.getString("NOMBREEMISOR"));
            entidad.setRfcEmisor(resultSet.getString("RFCEMISOR"));
            entidad.setDireccionEmisor(resultSet.getString("DIRECCIONEMISOR"));
            entidad.setClaveRegimenFiscalEmisor(resultSet.getString("CLAVEREGIMENFISCAL"));
            entidad.setRegimenFiscalEmisor(resultSet.getString("REGIMENFISCAL"));
            entidad.setCodigoPostal(resultSet.getString("CLAVECODIGOPOSTAL"));
            entidad.setNombreReceptor(resultSet.getString("NOMBRERECEPTOR"));
            entidad.setRfcReceptor(resultSet.getString("RFCRECEPTOR"));
            entidad.setDireccionReceptor(resultSet.getString("DIRECCIONRECEPTOR"));
            entidad.setFormaPago(resultSet.getString("FORMAPAGO"));
            entidad.setUsoCfdi(resultSet.getString("USOCFDI"));
            entidad.setTipoRelacion(resultSet.getString("TIPORELACION"));
            if (resultSet.getString("RUTALOGO") != null) {
                entidad.setLogo(new LogoCompania(resultSet.getString("RUTALOGO")));
            }
            break;
        }
        resultSet.close();
        statement.close();
    }

    @Override
    public void obtenerConceptoCompleto(ConceptoCompleto entidad)
        throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_CONCEPTOS_R_VISTA_PREVIA(?)}"
        );
        statement.setInt("PIDCONCEPTO", entidad.getId());
        ResultSet resultSet = statement.executeQuery();
        
        while (resultSet.next()) {
            entidad.setClaveProdServ(resultSet.getString("CLAVEPRODSERV"));
            entidad.setDescripcionClaveProdServ(resultSet.getString("DESCRIPCIONCLAVE"));
            entidad.setClaveUnidad(resultSet.getString("CLAVEUNIDADSAT"));
            entidad.setUnidad(resultSet.getString("UNIDAD"));
            entidad.setNoIdentificacion(resultSet.getString("NOIDENTIFICACION"));
            entidad.setDescripcion(resultSet.getString("DESCRIPCION"));
            break;
        }
        resultSet.close();
        statement.close();
    }
    
    @Override
    public String obtenerRutaBase(int idEmpresa) throws Exception {
        String ruta = null;
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_CONFIGURACIONES_SISTEMAS_R_RUTA_BASE(?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        ResultSet resultSet = statement.executeQuery();
        
        while (resultSet.next()) {
            ruta = resultSet.getString("RUTA");
            break;
        }
        
        resultSet.close();
        statement.close();
        
        return ruta;
    }

    @Override
    public void obtenerDatosEncabezado(int idEmpresa, int idFactura
        , UuidConceptos entidad) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_R_VISTA_PREVIA(?,?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        statement.setInt("PIDFACTURA", idFactura);
        ResultSet resultSet = statement.executeQuery();
        
        while (resultSet.next()) {
            entidad.setNombreEmisor(resultSet.getString("NOMBREEMISOR"));
            entidad.setRfcEmisor(resultSet.getString("RFCEMISOR"));
            entidad.setDireccionEmisor(resultSet.getString("DIRECCIONEMISOR"));
            entidad.setClaveRegimenFiscalEmisor(resultSet.getString("CLAVEREGIMENFISCAL"));
            entidad.setRegimenFiscalEmisor(resultSet.getString("REGIMENFISCAL"));
            entidad.setCodigoPostal(resultSet.getString("CLAVECODIGOPOSTAL"));
            entidad.setNombreReceptor(resultSet.getString("NOMBRERECEPTOR"));
            entidad.setRfcReceptor(resultSet.getString("RFCRECEPTOR"));
            entidad.setDireccionReceptor(resultSet.getString("DIRECCIONRECEPTOR"));
            if (resultSet.getString("RUTALOGO") != null) {
                entidad.setLogo(new LogoCompania(resultSet.getString("RUTALOGO")));
            }
            break;
        }
        resultSet.close();
        statement.close();
    }

    @Override
    public List<ConceptoCompleto> obtenerConceptosCompletos(int idFactura)
        throws Exception {
        List<ConceptoCompleto> lista = new ArrayList<>();
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_CONCEPTOS_R_VISTA_PREVIA_LISTADO(?)}"
        );
        statement.setInt("PIDFACTURA", idFactura);
        ResultSet resultSet = statement.executeQuery();
        
        while(resultSet.next()) {
            ConceptoCompleto entidad = new ConceptoCompleto();
            entidad.setId(resultSet.getInt("ID"));
            entidad.setIdFactura(resultSet.getInt("IDFACTURA"));
            entidad.setIdArticulo(resultSet.getInt("IDPRODUCTOSERVICIO"));
            entidad.setNombreArticulo(resultSet.getString("NOMBREARTICULO"));
            entidad.setCantidad(resultSet.getDouble("CANTIDAD"));
            entidad.setValorUnitario(resultSet.getDouble("VALORUNITARIO"));
            entidad.setDescuento(resultSet.getDouble("DESCUENTO"));
            entidad.setImporte(resultSet.getDouble("IMPORTE"));
            entidad.setClaveProdServ(resultSet.getString("CLAVEPRODSERV"));
            entidad.setDescripcionClaveProdServ(resultSet.getString("DESCRIPCIONCLAVE"));
            entidad.setClaveUnidad(resultSet.getString("CLAVEUNIDADSAT"));
            entidad.setUnidad(resultSet.getString("UNIDAD"));
            entidad.setNoIdentificacion(resultSet.getString("NOIDENTIFICACION"));
            entidad.setDescripcion(resultSet.getString("DESCRIPCION"));
            entidad.setObservacion(resultSet.getString("OBSERVACION"));
            lista.add(entidad);
        }
        resultSet.close();
        statement.close();
        
        return lista;
    }

    @Override
    public void crearConcepto(int idFactura, Concepto entidad, int idUsuario)
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
    public void crearFacturaRelacionada(int idFactura
        , FacturaRelacionada entidad, int idUsuario) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_RELACIONADAS_C(?,?,?,?,?)}"
        );
        statement.setInt("PIDFACTURA", idFactura);
        statement.setInt("PIDFACTURARELACIONADA", entidad.getIdFacturaRelacionada());
        statement.setString("PCLAVETIPORELACION", entidad.getClaveTipoRelacion());
        statement.setInt("PIDUSUARIO", idUsuario);
        statement.registerOutParameter("PID", Types.INTEGER);
        statement.executeUpdate();
        
        entidad.setId(statement.getInt("PID"));
        statement.close();
    }

    @Override
    public void reajustarFolio(int idEmpresa, String serie, int folio
        , int idFactura) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_U_REAJUSTE_FOLIO(?,?,?,?)}"
        );
        statement.setInt("PIDEMPRESA", idEmpresa);
        statement.setString("PSERIE", serie);
        statement.setInt("PFOLIO", folio);
        statement.setInt("PID", idFactura);
        statement.executeUpdate();
        
        statement.close();
    }

       @Override
    public List<ReporteFactura> obtenerDatosReporteDescargar(int idEmpresa, FacturaFiltro filtro) throws Exception {
        List<ReporteFactura> listaFacturas = new ArrayList<>();
        CallableStatement statement = connection.prepareCall(
                "{CALL USP_FACTURAS_DESCARGAR_REPORTE_R(?,?,?,?,?)}"  
        );
        asignarParametrosReporte(statement, filtro, idEmpresa);
        ResultSet resultSet = statement.executeQuery();

        while (resultSet.next()) {
            String fechaString = resultSet.getString("FHEXPEDICION");
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date fechaFactura = formatter.parse(fechaString);

            ReporteFactura factura = new ReporteFactura();
            factura.setFechaExpedicion(fechaFactura);
            factura.setFolio(resultSet.getInt("FOLIO"));
            factura.setSerie(resultSet.getString("SERIE"));
            factura.setRfcReceptor(resultSet.getString("RFC"));
            factura.setNombreReceptor(resultSet.getString("RAZONSOCIAL"));
            factura.setNombreEntidadFederativa(resultSet.getString("ENTIDAD FEDERATIVA"));
            factura.setConcepto(resultSet.getString("CONCEPTO"));
            factura.setObservacion(resultSet.getString("OBSERVACION"));
            factura.setFormaPago(resultSet.getString("FORMA DE PAGO"));
            factura.setClaveMetodoPago(resultSet.getString("METODOPAGO"));
            factura.setSubtotal(resultSet.getDouble("SUBTOTAL"));
            factura.setTotal(resultSet.getDouble("TOTAL"));
            factura.setEstatus(resultSet.getString("ESTADO"));
            listaFacturas.add(factura);
        }
        resultSet.close();
        statement.close();

        return listaFacturas;
    }
    
    private void asignarParametrosReporte(CallableStatement statement, FacturaFiltro filtro, int idEmpresa)
            throws Exception {

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String fecha;

        statement.setInt("PIDEMPRESA", idEmpresa);
        
        if (filtro.getEstatus() == null) {
            statement.setNull("PESTATUS", Types.NULL);
        } else {
            statement.setString("PESTATUS", filtro.getEstatus());
        }
        if (filtro.getIdCliente() == 0) {
            statement.setNull("PIDCLIENTE", Types.NULL);
        } else {
            statement.setInt("PIDCLIENTE", filtro.getIdCliente());
        }
        if (filtro.getFechaDesde() == null) {
            statement.setNull("PFHINICIO", Types.NULL);
        } else {
            fecha = dateFormat.format(filtro.getFechaDesde());
            statement.setString("PFHINICIO", fecha);
        }
        if (filtro.getFechaHasta() == null) {
            statement.setNull("PFHFIN", Types.NULL);
        } else {
            fecha = dateFormat.format(filtro.getFechaHasta());
            statement.setString("PFHFIN", fecha);
        }
    }
     
    @Override
    public void cancelar(int idUsuarioAuditoria, Cfdi comprobante) throws Exception {
        CallableStatement statement = connection.prepareCall(
                "{CALL USP_FACTURAS_U_CANCELAR(?,?,?,?)}"
        );
        statement.setString("PUUID", comprobante.getUuid());
        statement.setString("PMOTIVO", comprobante.getMotivoCancelacion());
        if (comprobante.getFolioSustitucion() == null) {
            statement.setNull("PFOLIOSUSTITUCION", Types.NULL);
        } else {
            statement.setString("PFOLIOSUSTITUCION", comprobante.getFolioSustitucion());
        }
        statement.setInt("PIDUSUARIOAUDITORIA", idUsuarioAuditoria);
        statement.executeUpdate();

        statement.close();
    }

    @Override
    public void cancelacionEnProceso(int idUsuarioAuditoria, Cfdi comprobante) throws Exception {
        CallableStatement statement = connection.prepareCall(
                "{CALL USP_FACTURAS_U_CANCELACION_EN_PROCESO(?,?,?,?)}"
        );
        statement.setString("PUUID", comprobante.getUuid());
        statement.setString("PMOTIVO", comprobante.getMotivoCancelacion());
        if (comprobante.getFolioSustitucion() == null) {
            statement.setNull("PFOLIOSUSTITUCION", Types.NULL);
        } else {
            statement.setString("PFOLIOSUSTITUCION", comprobante.getFolioSustitucion());
        }
        statement.setInt("PIDUSUARIOAUDITORIA", idUsuarioAuditoria);
        statement.executeUpdate();

        statement.close();
    }

    @Override
    public void anularCancelacion(int idUsuarioAuditoria, Cfdi comprobante) throws Exception {
        CallableStatement statement = connection.prepareCall(
                "{CALL USP_FACTURAS_U_ANULAR_CANCELACION(?,?)}"
        );
        statement.setString("PUUID", comprobante.getUuid());
        statement.setInt("PIDUSUARIOAUDITORIA", idUsuarioAuditoria);
        statement.executeUpdate();

        statement.close();
    }

    @Override
    public Cfdi obtenerComprobanteACancelar(String uuid) throws Exception {
        Cfdi cfdi = new Cfdi();
        try (CallableStatement statement = connection.prepareCall(
                "{CALL USP_FACTURAS_R_COMPROBANTE_A_CANCELAR(?)}"
        )) {
            statement.setString("PUUID", uuid);
            ResultSet resultSet = statement.executeQuery();
            
            while(resultSet.next()) {
                cfdi.setUuid(resultSet.getString("UUID"));
                cfdi.setMotivoCancelacion(resultSet.getString("MOTIVOCANCELACION"));
                cfdi.setFolioSustitucion(resultSet.getString("FOLIOSUSTITUCION"));
                break;
            }
            resultSet.close();
        }
        
        return cfdi;
    }
    
    @Override
    public boolean verificarRelacionCfdi(String uuid) throws Exception {
        CallableStatement statement = connection.prepareCall(
            "{CALL USP_FACTURAS_R_VERIFICAR_RELACION(?,?)}"
        );
        statement.setString("PUUID", uuid);
        statement.registerOutParameter("PVALIDO", Types.BOOLEAN);
        statement.execute();
        
        boolean valido = statement.getBoolean("PVALIDO");
        statement.close();
        
        return valido;
    }
}
