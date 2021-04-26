/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mx.softver.inversa.api.operacion.facturacion.controllers;

import com.mx.softver.inversa.api.operacion.facturacion.data.ConceptoDBDataImpl;
import com.softver.comunes.data.TransactionDataImpl;
import com.softver.comunes.entity.InfoAuditoria;
import com.softver.comunes.entity.RespuestaBase;
import com.softver.comunes.exception.OperationNotPermittedSoftverException;
import com.softver.comunes.util.api.filters.Authenticated;
import com.mx.softver.inversa.operacion.facturacion.factura.entity.Concepto;
import com.mx.softver.inversa.operacion.facturacion.factura.service.ConceptoServiceImpl;
import com.mx.softver.inversa.operacion.facturacion.factura.serviceinterface.ConceptoService;
import java.sql.Connection;
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
@Path("inversa/operacion/facturacion/factura/concepto")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Authenticated
public class ConceptoController {
    @Context
    private ContainerRequestContext requestContext;
    private ConceptoService conceptoService;
    
    /**
     * metodo de la api para crear un concepto
     * @param entidad
     * @return
     */
    @POST
    @Path("")
    public Response crear(Concepto entidad) {
        RespuestaBase<Integer> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            conceptoService = new ConceptoServiceImpl(
                new ConceptoDBDataImpl(connection)
                , new TransactionDataImpl(connection)
            );
            conceptoService.crear(
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
     * metodo de la api para obtener un concepto por id
     * @param filtro
     * @return
     */
    @POST
    @Path("info/obtener")
    public Response obtenerPorId(Concepto filtro) {
        RespuestaBase<Concepto> respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            conceptoService = new ConceptoServiceImpl(
                new ConceptoDBDataImpl(connection)
            );
            
            respuesta.setEntidad(conceptoService.obtenerPorId(
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
     * metodo de la api para actualizar un concepto
     * @param entidad
     * @return 
     */
    @PUT
    @Path("")
    public Response actualizar(Concepto entidad) {
        RespuestaBase respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            conceptoService = new ConceptoServiceImpl(
                new ConceptoDBDataImpl(connection)
                , new TransactionDataImpl(connection)
            );
            conceptoService.actualizar(
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
    
    /**
     * metodo de la api para eliminar un concepto
     * @param entidad
     * @return 
     */
    @POST
    @Path("eliminar")
    public Response eliminar(Concepto entidad) {
        RespuestaBase respuesta = new RespuestaBase<>();
        Response.Status estadoRespuesta = Response.Status.OK;
        try {
            Connection connection = 
                (Connection) requestContext.getProperty("CONNECTION");
            conceptoService = new ConceptoServiceImpl(
                new ConceptoDBDataImpl(connection)
            );
            conceptoService.eliminar(
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
