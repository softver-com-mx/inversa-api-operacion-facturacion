/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mx.softver.inversa.api.operacion.facturacion.controllers;

import com.mx.softver.inversa.api.operacion.facturacion.data.ComprobanteDBDataImpl;
import com.mx.softver.inversa.api.operacion.facturacion.data.FacturaCatalogoDBDataImpl;
import com.mx.softver.inversa.api.operacion.facturacion.data.FacturaDBDataImpl;
import com.softver.comunes.data.TransactionDataImpl;
import com.softver.comunes.entity.Archivo;
import com.softver.comunes.entity.Id;
import com.softver.comunes.entity.InfoAuditoria;
import com.softver.comunes.entity.RespuestaBase;
import com.softver.comunes.exception.OperationNotPermittedSoftverException;
import com.softver.comunes.mensajeria.data.MensajeriaConfiguracionDataImpl;
import com.softver.comunes.mensajeria.data.MensajeriaDataFactory;
import com.softver.comunes.mensajeria.service.MensajeriaServiceImpl;
import com.softver.comunes.util.api.filters.Authenticated;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.Cfdi;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.Factura;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.FacturaConcepto;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.FacturaFiltro;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.FacturaVistaPrevia;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.IdCorreo;
import com.mx.softver.inversa.operacion.facturacion.factura.service.ComprobanteServiceImpl;
import com.mx.softver.inversa.operacion.facturacion.factura.service.FacturaServiceImpl;
import com.mx.softver.inversa.operacion.facturacion.factura.serviceinterface.FacturaService;
import java.sql.Connection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 * @author jhernandez
 */
@Path("inversa/operacion/facturacion/factura")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Authenticated
public class FacturaController {
    @Context
    private ContainerRequestContext requestContext;
    private FacturaService facturaService;
    
    /**
     * metodo de la api para crear una factura
     * @param entidad
     * @return
     */
    @POST
    @Path("")
    public Response crear(FacturaConcepto entidad) {
        RespuestaBase<FacturaConcepto> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            facturaService = new FacturaServiceImpl(
                new FacturaDBDataImpl(connection)
                , new FacturaCatalogoDBDataImpl(connection)
                , new TransactionDataImpl(connection)
            );
            facturaService.crear(
                (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA")
                , entidad
            );
            
            respuesta.setEntidad(entidad);
        } catch (OperationNotPermittedSoftverException ex) {
            estadoRespuesta = Response.Status.BAD_REQUEST;
            respuesta.setError(true);
            respuesta.setMensaje(ex.getMessage());
            
        } catch (Exception ex) {
            estadoRespuesta = Response.Status.INTERNAL_SERVER_ERROR;
            respuesta.setError(true);
            respuesta.setMensaje("Error en el procesamiento de la peticion");
            respuesta.setMensajeDetalle(ex.getMessage());
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return Response.status(estadoRespuesta).entity(respuesta).build();
    }
    
    /**
     * metodo de la api para obtener un listado de facturas
     * @param filtro
     * @return
     */
    @POST
    @Path("obtener")
    public Response obtener(FacturaFiltro filtro) {
        RespuestaBase<List<Factura>> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            facturaService = new FacturaServiceImpl(
                new FacturaDBDataImpl(connection)
            );
            
            respuesta.setEntidad(facturaService.obtener(
                (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA")
                , filtro
            ));
        } catch (OperationNotPermittedSoftverException ex) {
            estadoRespuesta = Response.Status.BAD_REQUEST;
            respuesta.setError(true);
            respuesta.setMensaje(ex.getMessage());
            
        } catch (Exception ex) {
            estadoRespuesta = Response.Status.INTERNAL_SERVER_ERROR;
            respuesta.setError(true);
            respuesta.setMensaje("Error en el procesamiento de la peticion");
            respuesta.setMensajeDetalle(ex.getMessage());
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return Response.status(estadoRespuesta).entity(respuesta).build();
    }
    
    /**
     * metodo de la api para obtener una factura por id
     * @param filtro
     * @return
     */
    @POST
    @Path("info/obtener")
    public Response obtenerPorId(FacturaConcepto filtro) {
        RespuestaBase<FacturaConcepto> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            facturaService = new FacturaServiceImpl(
                new FacturaDBDataImpl(connection)
            );
            
            respuesta.setEntidad(facturaService.obtenerPorId(
                (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA")
                , filtro
            ));
        } catch (OperationNotPermittedSoftverException ex) {
            estadoRespuesta = Response.Status.BAD_REQUEST;
            respuesta.setError(true);
            respuesta.setMensaje(ex.getMessage());
            
        } catch (Exception ex) {
            estadoRespuesta = Response.Status.INTERNAL_SERVER_ERROR;
            respuesta.setError(true);
            respuesta.setMensaje("Error en el procesamiento de la peticion");
            respuesta.setMensajeDetalle(ex.getMessage());
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return Response.status(estadoRespuesta).entity(respuesta).build();
    }
    
    /**
     * metodo de la api para actualizar una factura
     * @param entidad
     * @return 
     */
    @PUT
    @Path("")
    public Response actualizar(Factura entidad) {
        RespuestaBase<Factura> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            facturaService = new FacturaServiceImpl(
                new FacturaDBDataImpl(connection)
                , new FacturaCatalogoDBDataImpl(connection)
            );
            facturaService.actualizar(
                (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA")
                , entidad
            );
            
            respuesta.setEntidad(entidad);
        } catch (OperationNotPermittedSoftverException ex) {
            estadoRespuesta = Response.Status.BAD_REQUEST;
            respuesta.setError(true);
            respuesta.setMensaje(ex.getMessage());
            
        } catch (Exception ex) {
            estadoRespuesta = Response.Status.INTERNAL_SERVER_ERROR;
            respuesta.setError(true);
            respuesta.setMensaje("Error en el procesamiento de la peticion");
            respuesta.setMensajeDetalle(ex.getMessage());
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return Response.status(estadoRespuesta).entity(respuesta).build();
    }
    
    /**
     * metodo de la api para timbrar una factura
     * @param entidad
     * @return
     */
    @POST
    @Path("timbrar")
    public Response timbrarFactura(Id entidad) {
        RespuestaBase<FacturaConcepto> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            facturaService = new FacturaServiceImpl(
                new FacturaDBDataImpl(connection)
                , new ComprobanteServiceImpl(
                    new ComprobanteDBDataImpl(connection)
                )
            );
            respuesta.setEntidad(
                facturaService.timbrarFactura(
                    (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA")
                    , entidad
                )
            );
        } catch (OperationNotPermittedSoftverException ex) {
            estadoRespuesta = Response.Status.BAD_REQUEST;
            respuesta.setError(true);
            respuesta.setMensaje(ex.getMessage());
            
        } catch (Exception ex) {
            estadoRespuesta = Response.Status.INTERNAL_SERVER_ERROR;
            respuesta.setError(true);
            respuesta.setMensaje("Error en el procesamiento de la peticion");
            respuesta.setMensajeDetalle(ex.getMessage());
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return Response.status(estadoRespuesta).entity(respuesta).build();
    }
    
    /**
     * metodo de la api para cancelar una factura
     * @param entidad
     * @return
     */
    @POST
    @Path("cancelar")
    public Response cancelarFactura(Factura entidad) {
        RespuestaBase<Cfdi> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            facturaService = new FacturaServiceImpl(
                new FacturaDBDataImpl(connection)
            );
            respuesta.setEntidad(
                facturaService.cancelarFactura(
                    (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA")
                    , entidad
                )
            );
        } catch (OperationNotPermittedSoftverException ex) {
            estadoRespuesta = Response.Status.BAD_REQUEST;
            respuesta.setError(true);
            respuesta.setMensaje(ex.getMessage());
            
        } catch (Exception ex) {
            estadoRespuesta = Response.Status.INTERNAL_SERVER_ERROR;
            respuesta.setError(true);
            respuesta.setMensaje("Error en el procesamiento de la peticion");
            respuesta.setMensajeDetalle(ex.getMessage());
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return Response.status(estadoRespuesta).entity(respuesta).build();
    }
    
    /**
     * metodo de la api para comprobar la cancelacion de una factura
     * @param entidad
     * @return
     */
    @POST
    @Path("estado/obtener")
    public Response comprobarCancelacion(Factura entidad) {
        RespuestaBase<Cfdi> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            facturaService = new FacturaServiceImpl(
                new FacturaDBDataImpl(connection)
            );
            respuesta.setEntidad(
                facturaService.comprobarCancelacion(
                    (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA")
                    , entidad
                )
            );
        } catch (OperationNotPermittedSoftverException ex) {
            estadoRespuesta = Response.Status.BAD_REQUEST;
            respuesta.setError(true);
            respuesta.setMensaje(ex.getMessage());
            
        } catch (Exception ex) {
            estadoRespuesta = Response.Status.INTERNAL_SERVER_ERROR;
            respuesta.setError(true);
            respuesta.setMensaje("Error en el procesamiento de la peticion");
            respuesta.setMensajeDetalle(ex.getMessage());
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return Response.status(estadoRespuesta).entity(respuesta).build();
    }
    
    /**
     * metodo de la api para comprobar la cancelacion de una factura
     * @param entidad
     * @return
     */
    @POST
    @Path("cfdi/xml/obtener")
    public Response descargarXml(Factura entidad) {
        RespuestaBase<Archivo> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            facturaService = new FacturaServiceImpl(
                new FacturaDBDataImpl(connection)
            );
            respuesta.setEntidad(
                facturaService.descargarXml(
                    (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA")
                    , entidad
                )
            );
        } catch (OperationNotPermittedSoftverException ex) {
            estadoRespuesta = Response.Status.BAD_REQUEST;
            respuesta.setError(true);
            respuesta.setMensaje(ex.getMessage());
            
        } catch (Exception ex) {
            estadoRespuesta = Response.Status.INTERNAL_SERVER_ERROR;
            respuesta.setError(true);
            respuesta.setMensaje("Error en el procesamiento de la peticion");
            respuesta.setMensajeDetalle(ex.getMessage());
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return Response.status(estadoRespuesta).entity(respuesta).build();
    }
    
    /**
     * metodo de la api para comprobar la cancelacion de una factura
     * @param entidad
     * @return
     */
    @POST
    @Path("cfdi/pdf/obtener")
    public Response descargarPdf(Factura entidad) {
        RespuestaBase<Archivo> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            facturaService = new FacturaServiceImpl(
                new FacturaDBDataImpl(connection)
            );
            respuesta.setEntidad(
                facturaService.descargarPdf(
                    (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA")
                    , entidad
                )
            );
        } catch (OperationNotPermittedSoftverException ex) {
            estadoRespuesta = Response.Status.BAD_REQUEST;
            respuesta.setError(true);
            respuesta.setMensaje(ex.getMessage());
            
        } catch (Exception ex) {
            estadoRespuesta = Response.Status.INTERNAL_SERVER_ERROR;
            respuesta.setError(true);
            respuesta.setMensaje("Error en el procesamiento de la peticion");
            respuesta.setMensajeDetalle(ex.getMessage());
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return Response.status(estadoRespuesta).entity(respuesta).build();
    }
    
    /**
     * metodo de la api para comprobar la cancelacion de una factura
     * @param entidad
     * @return
     */
    @POST
    @Path("cfdi/enviar")
    public Response enviarCorreo(IdCorreo entidad) {
        RespuestaBase<Archivo> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = (Connection) requestContext.getProperty("CONNECTION");
            InfoAuditoria info = (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA");
            
            MensajeriaConfiguracionDataImpl mensajeriaData =
                new MensajeriaConfiguracionDataImpl(connection);
            
            facturaService = new FacturaServiceImpl(
                new FacturaDBDataImpl(connection)
                , new MensajeriaServiceImpl(
                    new MensajeriaDataFactory(
                        mensajeriaData.obtenerConfiguraciones(info)
                    )
                )
            );
            
            facturaService.enviarCorreo(info, entidad);
        } catch (OperationNotPermittedSoftverException ex) {
            estadoRespuesta = Response.Status.BAD_REQUEST;
            respuesta.setError(true);
            respuesta.setMensaje(ex.getMessage());
            
        } catch (Exception ex) {
            estadoRespuesta = Response.Status.INTERNAL_SERVER_ERROR;
            respuesta.setError(true);
            respuesta.setMensaje("Error en el procesamiento de la peticion");
            respuesta.setMensajeDetalle(ex.getMessage());
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return Response.status(estadoRespuesta).entity(respuesta).build();
    }
    
    /**
     * metodo de la api para obtener una factura por id
     * @param filtro
     * @return
     */
    @POST
    @Path("vistaprevia/obtener")
    public Response obtenerVistaPrevia(Id filtro) {
        RespuestaBase<FacturaVistaPrevia> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            facturaService = new FacturaServiceImpl(
                new FacturaDBDataImpl(connection)
            );
            
            respuesta.setEntidad(facturaService.obtenerVistaPrevia(
                (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA")
                , filtro
            ));
        } catch (OperationNotPermittedSoftverException ex) {
            estadoRespuesta = Response.Status.BAD_REQUEST;
            respuesta.setError(true);
            respuesta.setMensaje(ex.getMessage());
            
        } catch (Exception ex) {
            estadoRespuesta = Response.Status.INTERNAL_SERVER_ERROR;
            respuesta.setError(true);
            respuesta.setMensaje("Error en el procesamiento de la peticion");
            respuesta.setMensajeDetalle(ex.getMessage());
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return Response.status(estadoRespuesta).entity(respuesta).build();
    }
    
    /**
     * metodo de la api para obtener una factura clonada
     * @param filtro
     * @return
     */
    @POST
    @Path("info/clonar")
    public Response obtenerFacturaClonada(FacturaConcepto filtro) {
        RespuestaBase<FacturaConcepto> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            facturaService = new FacturaServiceImpl(
                new FacturaDBDataImpl(connection)
            );
            
            respuesta.setEntidad(facturaService.obtenerFacturaClonada(
                (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA")
                , filtro
            ));
        } catch (OperationNotPermittedSoftverException ex) {
            estadoRespuesta = Response.Status.BAD_REQUEST;
            respuesta.setError(true);
            respuesta.setMensaje(ex.getMessage());
            
        } catch (Exception ex) {
            estadoRespuesta = Response.Status.INTERNAL_SERVER_ERROR;
            respuesta.setError(true);
            respuesta.setMensaje("Error en el procesamiento de la peticion");
            respuesta.setMensajeDetalle(ex.getMessage());
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        return Response.status(estadoRespuesta).entity(respuesta).build();
    }
}
