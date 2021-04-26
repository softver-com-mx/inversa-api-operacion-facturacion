/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mx.softver.inversa.api.operacion.facturacion.controllers;

import com.mx.softver.inversa.api.operacion.facturacion.data.FacturaRelacionadaDBDataImpl;
import com.softver.comunes.entity.InfoAuditoria;
import com.softver.comunes.entity.RespuestaBase;
import com.softver.comunes.exception.OperationNotPermittedSoftverException;
import com.softver.comunes.util.api.filters.Authenticated;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.Factura;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.FacturaRelacionada;
import com.mx.softver.inversa.operacion.facturacion.factura.service.FacturaRelacionadaServiceImpl;
import com.mx.softver.inversa.operacion.facturacion.factura.serviceinterface.FacturaRelacionadaService;
import java.sql.Connection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
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
@Path("inversa/operacion/facturacion/factura/relacion")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Authenticated
public class FacturaRelacionadaController {
    @Context
    private ContainerRequestContext requestContext;
    private FacturaRelacionadaService facturaRelacionadaService;
    
    /**
     * metodo de la api para crear la relacion entre dos facturas
     * @param entidad
     * @return
     */
    @POST
    @Path("")
    public Response crear(FacturaRelacionada entidad) {
        RespuestaBase<Integer> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            facturaRelacionadaService = new FacturaRelacionadaServiceImpl(
                new FacturaRelacionadaDBDataImpl(connection)
            );
            facturaRelacionadaService.crear(
                (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA")
                , entidad
            );
            
            respuesta.setEntidad(entidad.getId());
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
     * metodo de la api para obtener las opciones para relacionar
     * @param filtro
     * @return
     */
    @POST
    @Path("opcion/obtener")
    public Response obtenerOpciones(Factura filtro) {
        RespuestaBase<List<Factura>> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            facturaRelacionadaService = new FacturaRelacionadaServiceImpl(
                new FacturaRelacionadaDBDataImpl(connection)
            );
            respuesta.setEntidad(
                facturaRelacionadaService.obtenerOpciones(
                    (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA")
                    , filtro
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
     * metodo de la api para eliminar la relacion entre dos facturas
     * @param entidad
     * @return
     */
    @POST
    @Path("eliminar")
    public Response eliminar(FacturaRelacionada entidad) {
        RespuestaBase<Integer> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            facturaRelacionadaService = new FacturaRelacionadaServiceImpl(
                new FacturaRelacionadaDBDataImpl(connection)
            );
            facturaRelacionadaService.eliminar(
                (InfoAuditoria) requestContext.getProperty("INFOAUDITORIA")
                , entidad
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
}
