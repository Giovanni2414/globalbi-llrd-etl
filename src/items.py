import json
import pandas as pd
from datetime import datetime
from utils.database_manager import DatabaseManager
import logging

def cargue_criterio_items(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager, logger: logging.Logger):
    print("Cargando criterio items")
    query = """
        SELECT CONVERT(INT,P.f105_id_cia) IdCompania,
            P.f105_id CodPlanItem,
            p.f105_descripcion PlanItem ,
            G.f106_id CodCriterio,
            g.f106_descripcion Criterio
        FROM [dbo].[t106_mc_criterios_item_mayores] G
        INNER JOIN [dbo].[t105_mc_criterios_item_planes] P ON p.f105_id_cia = g.f106_id_cia
        AND p.f105_id = g.f106_id_plan
        ORDER BY 1, 2
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
        WITH CTE AS (
            SELECT ? AS IdCompania,
                ? AS CodPlanItem,
                ? AS PlanItem,
                ? AS CodCriterio,
                ? AS Criterio,
                ? AS FecCreacion,
                ? AS FecModificacion
        )
        MERGE siesa.tbCriterioItems AS tgt
        USING CTE AS src
        ON tgt.CodCriterio = src.CodCriterio
            AND tgt.CodPlanItem = src.CodPlanItem
            AND tgt.IdCompania = src.IdCompania
        WHEN NOT MATCHED THEN
            INSERT (IdCompania, CodPlanItem, PlanItem,
            CodCriterio, Criterio, FecCreacion, FecModificacion)
            VALUES (src.IdCompania, src.CodPlanItem, src.PlanItem, src.CodCriterio, 
            src.Criterio, src.FecCreacion, src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.PlanItem = src.PlanItem,
            tgt.Criterio = src.Criterio,
            tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['IdCompania', 'CodPlanItem', 'PlanItem', 'CodCriterio', 'Criterio', 'FecCreacion', 'FecModificacion']].values.tolist())
    except Exception as e:
        logger.error(f"Error metodo cargue criterio items: {e}")

def limpiar_tabla_items_precios(manager: DatabaseManager, logger: logging.Logger):
    print("Limpiando tabla items precios")
    try:
        query = 'TRUNCATE TABLE siesa.tbItemsPrecios'
        manager.execute_query_no_results(query)
    except Exception as e:
        logger.error(f"Error metodo Limpiar Tabla items precios: {e}")

def limpiar_tabla_politicas_items(manager: DatabaseManager, logger: logging.Logger):
    print("Limpiando tabla politicas items")
    try:
        query = 'TRUNCATE TABLE siesa.tbItemsPoliticas'
        manager.execute_query_no_results(query)
    except Exception as e:
        logger.error(f"Error metodo Limpiar Tabla politicas items: {e}")

def limpiar_tabla_rel_criterio_items(manager: DatabaseManager, logger: logging.Logger):
    print("Limpiando tabla rel criterio items")
    try:
        query = 'TRUNCATE TABLE siesa.tbRelCriterioItems'
        manager.execute_query_no_results(query)
    except Exception as e:
        logger.error(f"Error metodo Limpiar Tabla rel criterio items: {e}")

def cargue_items(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager, logger: logging.Logger):
    print("Cargando items")
    query = """
        SELECT T120.f120_rowid IdItem,
            CONVERT(int,T120.f120_id_cia) IdCompania,
            T120.f120_id CodItem,
            t120.f120_referencia Referencia ,
            T120.f120_descripcion Item,
            T120.f120_descripcion_corta DescripcionCorta ,
            T120.f120_id_tipo_inv_serv CodTipoItemServicio,
            t149.f149_descripcion TipoItemServicio ,
            T120.f120_ind_tipo_item TipoItemInd,
            t120.f120_id_unidad_inventario UnidadInventario,
            t122.f122_peso PesoNeto ,
            t122.f122_volumen PesoProd ,
            t120.f120_id_unidad_empaque UnidadEmpaque,
            t122a.f122_peso PesoBrutoEmpaque ,
            t122a.f122_volumen PesoEmpaque,
            t120.f120_id_unidad_orden UnidadCompra,
            t122b.f122_factor FactorCompra
        FROM t120_mc_items T120
        INNER JOIN t149_mc_tipo_inv_serv t149 ON t149.f149_id = t120.f120_id_tipo_inv_serv
        AND t149.f149_id_cia = t120.f120_id_cia
        LEFT OUTER JOIN t122_mc_items_unidades t122 ON t122.f122_id_cia = T120.f120_id_cia
        AND t122.f122_rowid_item = T120.f120_rowid
        AND t122.f122_id_unidad = t120.f120_id_unidad_inventario
        LEFT OUTER JOIN t122_mc_items_unidades t122a ON t122a.f122_id_cia = T120.f120_id_cia
        AND t122a.f122_rowid_item = T120.f120_rowid
        AND t122a.f122_id_unidad = t120.f120_id_unidad_empaque
        LEFT OUTER JOIN t122_mc_items_unidades t122b ON t122b.f122_id_cia = T120.f120_id_cia
        AND t122b.f122_rowid_item = T120.f120_rowid
        AND t122b.f122_id_unidad = t120.f120_id_unidad_orden
        WHERE T120.f120_id_tipo_inv_serv NOT IN ('017') -- No se consideran repuesto paar el ppto
        ORDER BY 1
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['Ntipoitemind'] = df['TipoItemInd'].astype('int32')
    columns_to_replace_nan = ['PesoNeto', 'PesoProd', 'PesoBrutoEmpaque', 'PesoEmpaque', 'FactorCompra']
    df[['PesoNeto', 'PesoProd', 'PesoBrutoEmpaque', 'PesoEmpaque', 'FactorCompra']] = df[['PesoNeto', 'PesoProd', 'PesoBrutoEmpaque', 'PesoEmpaque', 'FactorCompra']].astype('object')
    df[columns_to_replace_nan] = df[columns_to_replace_nan].where(pd.notnull(df[columns_to_replace_nan]), None)
    query = f"""
        WITH CTE AS (
            SELECT ? AS IdItem,
                ? AS IdCompania,
                ? AS CodItem,
                ? AS Referencia,
                ? AS Item,
                ? AS DescripcionCorta,
                ? AS CodTipoItemServicio,
                ? AS TipoItemServicio,
                ? AS TipoItemInd,
                ? AS UnidadInventario,
                ? AS PesoNeto,
                ? AS PesoProd,
                ? AS FecCreacion,
                ? AS FecModificacion,
                ? AS UnidadEmpaque,
                ? AS PesoBrutoEmpaque,
                ? AS PesoNetoEmpaque,
                ? AS UnidadCompra,
                ? AS FactorCompra
        )
        MERGE siesa.tbItems AS tgt
        USING CTE AS src
        ON tgt.IdCompania = src.IdCompania
            AND tgt.IdItem = src.IdItem
        WHEN NOT MATCHED THEN
            INSERT (IdItem, IdCompania, CodItem, Referencia, Item, DescripcionCorta, CodTipoItemServicio,
            TipoItemServicio, TipoItemInd, UnidadInventario, PesoNeto, PesoProd, FecCreacion, FecModificacion,
            UnidadEmpaque, PesoBrutoEmpaque, PesoNetoEmpaque, UnidadCompra, FactorCompra)
            VALUES (src.IdItem, src.IdCompania, src.CodItem, src.Referencia, src.Item, src.DescripcionCorta, 
                    src.CodTipoItemServicio, src.TipoItemServicio, src.TipoItemInd, src.UnidadInventario, src.PesoNeto, 
                    src.PesoProd, src.FecCreacion, src.FecModificacion, src.UnidadEmpaque, src.PesoBrutoEmpaque, 
                    src.PesoNetoEmpaque, src.UnidadCompra, src.FactorCompra)
        WHEN MATCHED THEN UPDATE SET
            tgt.FecModificacion = src.FecModificacion,
            tgt.CodItem = src.CodItem,
            tgt.Referencia = src.Referencia,
            tgt.Item = src.Item,
            tgt.DescripcionCorta = src.DescripcionCorta,
            tgt.CodTipoItemServicio = src.CodTipoItemServicio,
            tgt.TipoItemServicio = src.TipoItemServicio,
            tgt.TipoItemInd = src.TipoItemInd,
            tgt.UnidadInventario = src.UnidadInventario,
            tgt.PesoNeto = src.PesoNeto,
            tgt.PesoProd = src.PesoProd,
            tgt.UnidadEmpaque = src.UnidadEmpaque,
            tgt.PesoBrutoEmpaque = src.PesoBrutoEmpaque,
            tgt.PesoNetoEmpaque = src.PesoNetoEmpaque,
            tgt.UnidadCompra = src.UnidadCompra,
            tgt.FactorCompra = src.FactorCompra;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['IdItem', 'IdCompania', 'CodItem', 'Referencia', 'Item', 'DescripcionCorta', 'CodTipoItemServicio', 'TipoItemServicio', 'Ntipoitemind', 'UnidadInventario', 'PesoNeto', 'PesoProd', 'FecCreacion', 'FecModificacion', 'UnidadEmpaque', 'PesoBrutoEmpaque', 'PesoEmpaque', 'UnidadCompra', 'FactorCompra']].values.tolist())
    except Exception as e:
        logger.error(f"Error metodo cargue items: {e}")

def cargue_rel_criterio_items(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager, logger: logging.Logger):
    print("Cargando rel criterio items")
    query = """
        SELECT CONVERT(INT,f125_id_cia) IdCia,
            f125_rowid_item IdItem ,
            f125_id_plan CodPlanItem ,
            f125_id_criterio_mayor CodCriterio
        FROM [dbo].[t125_mc_items_criterios] c
        INNER JOIN t120_mc_items T120 ON C.f125_rowid_item = T120.f120_rowid
        WHERE T120.f120_id_tipo_inv_serv NOT IN ('017') -- No se consideran repuesto paar el ppto
        ORDER BY 1, 2, 3
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['IdCompania'] = df['IdCia'].astype('int32')

    df_criterio_items = lloreda_manager.execute_query_get_pandas("SELECT IdCriterioItem, IdCompania, CodCriterio, CodPlanItem FROM siesa.tbCriterioItems")
    merged_df = df.merge(df_criterio_items, on=['IdCompania', 'CodPlanItem', 'CodCriterio'], how='inner')

    query = f"""
        WITH CTE AS (
            SELECT ? AS IdCriterioItem,
                ? AS IdItem,
                ? AS FecCreacion,
                ? AS FecModificacion
        )
        MERGE siesa.tbRelCriterioItems AS tgt
        USING CTE AS src
        ON tgt.IdCriterioItem = src.IdCriterioItem
            AND tgt.IdItem = src.IdItem
        WHEN NOT MATCHED THEN
            INSERT (IdCriterioItem, IdItem, FecCreacion, FecModificacion)
            VALUES (src.IdCriterioItem, src.IdItem, src.FecCreacion, src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, merged_df[['IdCriterioItem', 'IdItem', 'FecCreacion', 'FecModificacion']].values.tolist())
    except Exception as e:
        logger.error(f"Error metodo cargue rel criterio items: {e}")

def cargue_politicas(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager, logger: logging.Logger):
    print("Cargando politicas")
    query = """
        SELECT f120_rowid IdItem,
            f132_rowid_item_ext IdItemExt,
            f132_id_cia IdCia,
            f132_id_instalacion IdInstalacion,
            f132_mf_rowid_ruta IdRuta,
            f132_mf_ind_demanda_1 ind_demanda_1,
            f132_mf_ind_demanda_2 demanda_2,
            f132_mf_dias_corte_demanda dias_corte_demanda,
            f120_id_unidad_inventario unidad_inventario,
            f101_decimales decimales,
            f132_mf_nivel_formula nivel_formula,
            f132_mf_stock_segur_estatico stock_segur_estatico,
            f132_mf_dias_horiz_stock_min dias_horiz_stock_min,
            f132_mf_dias_stock_min dias_stock_min,
            f132_mf_dias_horiz_planea dias_horiz_planea,
            f132_mf_ind_politica_orden ind_politica_orden,
            f132_mf_tamano_lote tamano_lote,
            f132_mf_cant_incremental_lote cant_incremental_lote,
            f132_mf_porc_minimo_orden_plan porc_minimo_orden_plan,
            f132_mf_dias_periodos_fijo dias_periodos_fijo,
            f132_mf_porc_rendimiento porc_rendimiento,
            f132_mf_tiempo_repo_fijo tiempo_repo_fijo,
            f132_mf_tasa_produccion_dia tasa_produccion_dia,
            f132_mf_rowid_bodega_asigna rowid_bodega_asigna,
            f132_mf_id_planificador id_planificador,
            f132_mf_id_comprador id_comprador,
            f132_mf_rowid_tercero_prov_1 rowid_tercero_prov_1,
            f132_mf_id_sucursal_prov_1 id_sucursal_prov_1,
            f132_mf_rowid_tercero_prov_2 rowid_tercero_prov_2,
            f132_mf_id_sucursal_prov_2 id_sucursal_prov_2,
            f132_mf_ind_tipo_orden ind_tipo_orden,
            f132_mf_tasa_produccion_hora tasa_produccion_hora
        FROM t132_mc_items_instalacion
        INNER JOIN t121_mc_items_extensiones ON f132_rowid_item_ext = f121_rowid
        INNER JOIN t120_mc_items ON f121_rowid_item = f120_rowid
        INNER JOIN t101_mc_unidades_medida ON f120_id_unidad_inventario = f101_id
        AND f101_id_cia = f132_id_cia
        WHERE f132_id_cia = 2
    """
    df = siesa_manager.execute_query_get_pandas(query)
    query = """
        INSERT INTO siesa.tbItemsPoliticas (IdItem, IdItemExt, IdCia, IdInstalacion, IdRuta, 
        ind_demanda_1, demanda_2, dias_corte_demanda, unidad_inventario, decimales, 
        nivel_formula, stock_segur_estatico, dias_horiz_stock_min, dias_stock_min, 
        dias_horiz_planea, ind_politica_orden, tamano_lote, cant_incremental_lote, porc_minimo_orden_plan,
        dias_periodos_fijo, porc_rendimiento, tiempo_repo_fijo, tasa_produccion_dia, rowid_bodega_asigna,
        id_planificador, id_comprador, rowid_tercero_prov_1, id_sucursal_prov_1, rowid_tercero_prov_2,
        id_sucursal_prov_2, ind_tipo_orden, tasa_produccion_hora) VALUES 
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    df = df.astype('object')
    df = df.where(pd.notnull(df), None)
    try:
        lloreda_manager.execute_bulk_insert(query, df[['IdItem','IdItemExt','IdCia','IdInstalacion','IdRuta',
                                                       'ind_demanda_1','demanda_2','dias_corte_demanda','unidad_inventario',
                                                       'decimales','nivel_formula','stock_segur_estatico',
                                                       'dias_horiz_stock_min','dias_stock_min','dias_horiz_planea',
                                                       'ind_politica_orden','tamano_lote','cant_incremental_lote',
                                                       'porc_minimo_orden_plan','dias_periodos_fijo','porc_rendimiento',
                                                       'tiempo_repo_fijo','tasa_produccion_dia','rowid_bodega_asigna',
                                                       'id_planificador','id_comprador','rowid_tercero_prov_1',
                                                       'id_sucursal_prov_1','rowid_tercero_prov_2','id_sucursal_prov_2',
                                                       'ind_tipo_orden','tasa_produccion_hora']].values.tolist())
    except Exception as e:
        logger.error(f"Error metodo cargue politicas: {e}")

def cargue_items_precios(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager, logger: logging.Logger):
    print("Cargando items precios")
    query = """
        WITH ULTFEC AS
            (SELECT f420_id_co,
                    t421.f421_rowid_item_ext,
                    max(f420_rowid) f420_rowid
            FROM t421_cm_oc_movto t421
            INNER JOIN [dbo].[t420_cm_oc_docto] t420 ON t420.f420_rowid = t421.f421_rowid_oc_docto
            WHERE f421_id_cia = 2
                AND [f420_id_tipo_docto] = 'ODC'
            GROUP BY f420_id_co,
                        t421.f421_rowid_item_ext,
                        f421_id_co_movto)
        SELECT f150_id_instalacion IdInstalacion,
            t.f420_id_co CentroOperacion,
            T120.f120_rowid IdItem,
            t120.f120_referencia Referencia,
            t120.f120_descripcion Item ,
            f420_id_moneda_docto Moneda,
            max(f421_precio_unitario) PrecioUnitario
        FROM t421_cm_oc_movto t421
        INNER JOIN ULTFEC t420 ON t420.f420_rowid = t421.f421_rowid_oc_docto
        AND t420.f421_rowid_item_ext = t421.f421_rowid_item_ext
        INNER JOIN [dbo].[t420_cm_oc_docto] t ON t.f420_rowid = t420.f420_rowid
        INNER JOIN t121_mc_items_extensiones t121 ON t121.f121_rowid = t421.f421_rowid_item_ext
        INNER JOIN t120_mc_items t120 ON t120.f120_rowid = t121.f121_rowid_item
        INNER JOIN t150_mc_bodegas t150 ON f150_rowid = f421_rowid_bodega
        AND f150_id_cia = f421_id_cia
        GROUP BY f150_id_instalacion,
                    t.f420_id_co,
                    T120.f120_rowid,
                    t120.f120_referencia,
                    t120.f120_descripcion,
                    f420_id_moneda_docto
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['IdCompania'] = 2
    query = """
        INSERT INTO siesa.tbItemsPrecios (FecCreacion, FecModificacion, IdCompania, IdItem, Referencia, 
        Precio, CentroOperacion, IdMoneda, IdInstalacion) VALUES 
        (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    df = df.astype('object')
    df = df.where(pd.notnull(df), None)
    try:
        lloreda_manager.execute_bulk_insert(query, df[['FecCreacion','FecModificacion','IdCompania','IdItem','Referencia',
                                                       'PrecioUnitario','CentroOperacion','Moneda','IdInstalacion']].values.tolist())
    except Exception as e:
        logger.error(f"Error metodo cargue items precios: {e}")

def cargue_centros_trabajo(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager, logger: logging.Logger):
    print("Cargando centros trabajo")
    query = """
        SELECT T806.f806_id_cia IdCia,
            T806.f806_id_instalacion IdInstalacion ,
            T806.F806_ROWID ID_CENTRO_TRABAJO,
            T806.F806_ID AS COD_CENTRO_TRABAJO,
            T806.F806_DESCRIPCION AS CENTRO_TRABAJO,
            T806.f806_desc_corta AS CENTRO_TRABAJO_CORTO,
            T806.f806_num_turnos num_turnos ,
            T806.f806_horas_por_turno HorasxTurno ,
            T806.f806_factor_velocidad_maquinas factor_velocidad_maquinas ,
            T806.f806_velocidad_estandar velocidad_estandar ,
            T806.f806_rendimiento_medio rendimiento_medio ,
            T806.f806_rowid_ccosto IdCentroCosto,
            f806_ind_carga_capacidad indcargacapacidad,
            f806_num_maquinas nummaquinas,
            f806_porc_carga_deseada porccargadeseada,
            f806_ind_critico indcritico,
            f806_ind_calendario_planta indcalendarioplanta,
            T806.F806_ROWID row_ID_CENTRO_TRABAJO
        FROM T806_MF_CENTROS_TRABAJO t806
        WHERE T806.F806_ID_CIA = 2
        AND T806.F806_IND_ESTADO = 1
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['IdCompania'] = df['IdCia'].astype('int32')

    query = f"""
        WITH CTE AS (
            SELECT ? AS IdCentrosTrabajo,
            ? AS IdCompania,
            ? AS IdInstalacion,
            ? AS CodCentroTrabajo,
            ? AS CentroTrabajo,
            ? AS CentroTrabajoCorto,
            ? AS num_turnos,
            ? AS HorasxTurno,
            ? AS factor_velocidad_maquinas,
            ? AS velocidad_estandar,
            ? AS rendimiento_medio,
            ? AS rowid_ccosto,
            ? AS IndCargaCapacidad,
            ? AS NumMaquinas,
            ? AS PorcCargaDeseada,
            ? AS IndCritico,
            ? AS IndCalendarioPlanta,
            ? AS RowidCentroTrabajo,
            ? AS FecCreacion,
            ? AS FecModificacion
        )
        MERGE siesa.tbCentrosTrabajo AS tgt
        USING CTE AS src
        ON tgt.RowidCentroTrabajo = src.RowidCentroTrabajo
            AND tgt.IdCompania = src.IdCompania
        WHEN NOT MATCHED THEN
            INSERT (IdCentrosTrabajo, IdCompania, IdInstalacion, CodCentroTrabajo, 
            CentroTrabajo, CentroTrabajoCorto, num_turnos, HorasxTurno, factor_velocidad_maquinas,
            velocidad_estandar, rendimiento_medio, rowid_ccosto, IndCargaCapacidad, 
            NumMaquinas, PorcCargaDeseada, IndCritico, IndCalendarioPlanta, RowidCentroTrabajo,
            FecCreacion, FecModificacion)
            VALUES (src.IdCentrosTrabajo, src.IdCompania, src.IdInstalacion, src.CodCentroTrabajo, src.CentroTrabajo, 
                src.CentroTrabajoCorto, src.num_turnos, src.HorasxTurno, src.factor_velocidad_maquinas, 
                src.velocidad_estandar, src.rendimiento_medio, src.rowid_ccosto, src.IndCargaCapacidad, 
                src.NumMaquinas, src.PorcCargaDeseada, src.IndCritico, src.IndCalendarioPlanta, 
                src.RowidCentroTrabajo, src.FecCreacion, src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.IdInstalacion = src.IdInstalacion, tgt.CodCentroTrabajo = src.CodCentroTrabajo, 
            tgt.CentroTrabajo = src.CentroTrabajo, tgt.CentroTrabajoCorto = src.CentroTrabajoCorto, tgt.num_turnos = src.num_turnos, 
            tgt.HorasxTurno = src.HorasxTurno, tgt.factor_velocidad_maquinas = src.factor_velocidad_maquinas, 
            tgt.velocidad_estandar = src.velocidad_estandar, tgt.rendimiento_medio = src.rendimiento_medio, 
            tgt.rowid_ccosto = src.rowid_ccosto, tgt.IndCargaCapacidad = src.IndCargaCapacidad, tgt.NumMaquinas = src.NumMaquinas, 
            tgt.PorcCargaDeseada = src.PorcCargaDeseada, tgt.IndCritico = src.IndCritico, tgt.IndCalendarioPlanta = src.IndCalendarioPlanta, 
            tgt.RowidCentroTrabajo = src.RowidCentroTrabajo, tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['ID_CENTRO_TRABAJO', 'IdCompania', 'IdInstalacion', 'COD_CENTRO_TRABAJO', 'CENTRO_TRABAJO', 'CENTRO_TRABAJO_CORTO', 'num_turnos', 'HorasxTurno', 'factor_velocidad_maquinas', 'velocidad_estandar', 'rendimiento_medio', 'IdCentroCosto', 'indcargacapacidad', 'nummaquinas', 'porccargadeseada', 'indcritico', 'indcalendarioplanta', 'row_ID_CENTRO_TRABAJO', 'FecCreacion', 'FecModificacion']].values.tolist())
    except Exception as e:
        logger.error(f"Error metodo cargue centros trabajo: {e}")

def limpiar_tabla_segmentos_costos(manager: DatabaseManager, logger: logging.Logger):
    print("Limpiando tabla segmentos costos")
    try:
        query = 'TRUNCATE TABLE siesa.tbSegmentoCostos'
        manager.execute_query_no_results(query)
    except Exception as e:
        logger.error(f"Error metodo Limpiar Tabla segmentos costos: {e}")

def cargue_segmentos_costos(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager, logger: logging.Logger):
    print("Cargando segmentos costos")
    query = """
        SELECT [f804_id_cia] idcia ,
            [f804_id] idsegmentocosto ,
            [f804_descripcion] segmentocosto ,
            [f804_desc_corta] segmentocostocorto ,
            [f804_ind_tipo_costo] indtipocosto ,
            f806_id idcentrotrabajo
        FROM [UnoEE_Lloreda_Real].[dbo].[t804_mf_segmentos_costos] s
        INNER JOIN t817_mf_tarifas t ON t.f817_id_cia = s.f804_id_cia
        AND t.f817_id_segmento_costo = s.f804_id
        INNER JOIN t806_mf_centros_trabajo ct ON ct.f806_rowid = f817_rowid_ctrabajo
        WHERE f804_id_cia=2
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    query = """
        INSERT INTO siesa.tbSegmentoCostos (FecCreacion, FecModificacion, IdCompania, CodCentroTrabajo, IdSegmentoCosto, 
        SegmentoCosto, SegmentoCostoCorto, IndTtipoCosto) VALUES 
        (?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['FecCreacion','FecModificacion','idcia','idcentrotrabajo',
                                                       'idsegmentocosto','segmentocosto','segmentocostocorto','indtipocosto']].values.tolist())
    except Exception as e:
        logger.error(f"Error metodo cargue segmentos costos: {e}")
        lloreda_manager.rollback_transaction()

def cargue_rutas(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager, logger: logging.Logger):
    print("Cargando rutas")
    query = """
        SELECT r.f808_id_cia idcia,
            r.f808_id_instalacion idinstalacion,
            r.f808_id Coddruta,
            r.f808_rowId idruta ,
            r.f808_descripcion ruta,
            r.f808_ind_estado indestadoruta,
            ro.f809_numero_operacion rutaoperacion ,
            ro.f809_ind_estado indestadooperacion,
            ro.f809_cantidad_base cantidadbase,
            ro.f809_horas_ejecucion horasejecucion ,
            [f809_horas_alistamiento] horasalistamiento ,
            [f809_horas_maquina] horasmaquina ,
            [f809_horas_cola] horascola ,
            [f809_horas_traslado] horastraslado ,
            [f809_numero_operarios] numerooperarios ,
            [f809_numero_operarios_alista] operariosalistamiento ,
            [f809_unidades_equivalentes] unidadesequivalentes ,
            [f809_id_metodo] idmetodo ,
            ro.f809_rowid_ctrabajo IdCentroTrabajo,
            ct.f806_id CodCentroOperacion
        FROM t808_mf_rutas r
        INNER JOIN t809_mf_rutas_operacion ro ON r.f808_rowid = ro.f809_rowid_rutas
        INNER JOIN t806_mf_centros_trabajo ct ON ro.f809_rowid_ctrabajo = ct.f806_rowid
        WHERE (r.f808_id_cia = 2)
        AND (ro.f809_ind_estado = 1)
        AND (r.f808_ind_estado = 1)
        ORDER BY 1, 2, 4, 7
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['IdCompania'] = df['idcia'].astype('int32')

    query = f"""
        WITH CTE AS (
            SELECT ? AS IdCompania,
                ? AS IdInstalacion,
                ? AS IdRuta,
                ? AS IdCentroTrabajo,
                ? AS CodRuta,
                ? AS Ruta,
                ? AS IndEstadoRuta,
                ? AS RutaOperacion,
                ? AS IndEstadoOperacion,
                ? AS CantidadBase,
                ? AS HorasEjecucion,
                ? AS HorasAlistamiento,
                ? AS HorasMaquina,
                ? AS HorasCola,
                ? AS HorasTraslado,
                ? AS NumeroOperarios,
                ? AS OperariosAlistamiento,
                ? AS UnidadesEquivalentes,
                ? AS IdMetodo,
                ? AS FecCreacion,
                ? AS FecModificacion
        )
        MERGE siesa.tbRutas AS tgt
        USING CTE AS src
        ON tgt.CodRuta = src.CodRuta
            AND tgt.IdCompania = src.IdCompania
            AND tgt.IdInstalacion = src.IdInstalacion
            AND tgt.IdMetodo = src.IdMetodo
            AND tgt.IdRuta = src.IdRuta
            AND tgt.RutaOperacion = src.RutaOperacion
        WHEN NOT MATCHED THEN
            INSERT (IdCompania, IdInstalacion, IdRuta, IdCentroTrabajo, CodRuta, Ruta, IndEstadoRuta, RutaOperacion,
            IndEstadoOperacion, CantidadBase, HorasEjecucion, HorasAlistamiento, HorasMaquina, HorasCola, 
            HorasTraslado, NumeroOperarios, OperariosAlistamiento, UnidadesEquivalentes, IdMetodo, FecCreacion, FecModificacion)
            VALUES (src.IdCompania, src.IdInstalacion, src.IdRuta, src.IdCentroTrabajo, src.CodRuta,
                src.Ruta, src.IndEstadoRuta, src.RutaOperacion, src.IndEstadoOperacion, src.CantidadBase,
                src.HorasEjecucion, src.HorasAlistamiento, src.HorasMaquina, src.HorasCola, src.HorasTraslado,
                src.NumeroOperarios, src.OperariosAlistamiento, src.UnidadesEquivalentes, src.IdMetodo, src.FecCreacion,
                src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.IdCentroTrabajo = src.IdCentroTrabajo,
            tgt.CodRuta = src.CodRuta,
            tgt.Ruta = src.Ruta,
            tgt.IndEstadoRuta = src.IndEstadoRuta,
            tgt.IndEstadoOperacion = src.IndEstadoOperacion,
            tgt.CantidadBase = src.CantidadBase,
            tgt.HorasEjecucion = src.HorasEjecucion,
            tgt.HorasAlistamiento = src.HorasAlistamiento,
            tgt.HorasMaquina = src.HorasMaquina,
            tgt.HorasCola = src.HorasCola,
            tgt.HorasTraslado = src.HorasTraslado,
            tgt.NumeroOperarios = src.NumeroOperarios,
            tgt.OperariosAlistamiento = src.OperariosAlistamiento,
            tgt.UnidadesEquivalentes = src.UnidadesEquivalentes,
            tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['IdCompania', 'idinstalacion', 'idruta', 'IdCentroTrabajo', 'Coddruta', 'ruta', 'indestadoruta', 'rutaoperacion', 'indestadooperacion', 'cantidadbase', 'horasejecucion', 'horasalistamiento', 'horasmaquina', 'horascola', 'horastraslado', 'numerooperarios', 'operariosalistamiento', 'unidadesequivalentes', 'idmetodo', 'FecCreacion', 'FecModificacion']].values.tolist())
    except Exception as e:
        logger.error(f"Error metodo cargue rutas: {e}")

def limpiar_tabla_lista_materiales(manager: DatabaseManager, logger: logging.Logger):
    print("Limpiando tabla lista materiales")
    try:
        query = 'TRUNCATE TABLE siesa.tbListaMateriales'
        manager.execute_query_no_results(query)
    except Exception as e:
        logger.error(f"Error metodo Limpiar Tabla lista materiales: {e}")

def cargue_lista_materiales(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager, logger: logging.Logger):
    print("Cargando lista materiales")
    query = """
            SELECT lm.f820_id_cia AS Id_cia,
                lm.f820_id_instalacion idinstalacion,
                lm.f820_id_metodo idmetodo,
                it.f120_id AS iditemOp,
                lm.f820_secuencia idsecuencia,
                it.f120_descripcion AS item_op,
                it.f120_id_unidad_inventario AS um_item_op,
                lm.f820_cant_base cantidadbase,
                it1.f120_id AS iditem_comp,
                it1.f120_descripcion AS item_comp,
                it1.f120_id_unidad_inventario AS um_item_comp,
                lm.f820_cant_requerida cantidadrequerida,
                lm.f820_porcentaje_desperdicio pordesperdicio,
                f132_costo_estandar_acumulado costoestacumulado ,
                f820_rowid_ruta_oper IdRutaOperacion,
                f820_grupo_consumo GrupoConsumo,
                f820_codigo_uso CodigoUso,
                f820_porcentaje_costo_producto PorcCostoProducto,
                f820_rowid IdListaMaterial,
                f820_fecha_activacion,
                f820_fecha_inactivacion
            FROM t820_mf_lista_material lm
            INNER JOIN t121_mc_items_extensiones ie ON lm.f820_rowid_item_ext = ie.f121_rowid
            INNER JOIN t120_mc_items it ON ie.f121_rowid_item = it.f120_rowid
            INNER JOIN t121_mc_items_extensiones AS ie1 ON lm.f820_rowid_item_ext_hijo = ie1.f121_rowid
            INNER JOIN t120_mc_items AS it1 ON ie1.f121_rowid_item = it1.f120_rowid
            INNER JOIN t132_mc_items_instalacion ii ON ie1.f121_rowid = ii.f132_rowid_item_ext
            AND ii.f132_id_instalacion = lm.f820_id_instalacion
            WHERE (lm.f820_id_cia = 2)
            ORDER BY 1, 2, 4, 3, 5
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['IdCompania'] = df['Id_cia'].astype('int32')

    query = """
        INSERT INTO siesa.tbListaMateriales (IdListaMateriales, IdCompania, IdInstalacion, IdMetodo, IdItemOperacion, 
        Secuencia, ItemOperacion, UMItemOpersacion, CantidadBase, IdItemComposicion, ItemComposicion, UMItemComposicion,
        CantidadRequerida, PorcentajeDesperdicio, CostoEstandarAcumulado, FecCreacion, FecModificacion, IdRutaOperacion,
        GrupoConsumo, IdCodigoUso, PorcCostoProducto, FechaActivacion, FechaInactivacion) VALUES 
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['IdListaMaterial','IdCompania','idinstalacion','idmetodo',
                                                       'iditemOp','idsecuencia','item_op','um_item_op','cantidadbase',
                                                       'iditem_comp','item_comp','um_item_comp','cantidadrequerida',
                                                       'pordesperdicio','costoestacumulado','FecCreacion','FecModificacion',
                                                       'IdRutaOperacion','GrupoConsumo','CodigoUso','PorcCostoProducto',
                                                       'f820_fecha_activacion','f820_fecha_inactivacion']].values.tolist())
    except Exception as e:
        logger.error(f"Error metodo cargue lista materiales: {e}")

def limpiar_tabla_rel_rutas_item(manager: DatabaseManager, logger: logging.Logger):
    print("Limpiando tabla rel rutas item")
    try:
        query = 'TRUNCATE TABLE siesa.tbRelRutasItem'
        manager.execute_query_no_results(query)
    except Exception as e:
        logger.error(f"Error metodo Limpiar Tabla rel rutas item: {e}")

def cargue_rel_rutas_items(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager, logger: logging.Logger):
    print("Cargando rel rutas items")
    query = """
        SELECT t121.f121_id_cia IdCia,
            f121_rowid IdItemExt,
            f121_rowid_item IdItem ,
            t132.f132_mf_rowid_ruta IdRuta
        FROM [dbo].[t121_mc_items_extensiones] t121
        INNER JOIN [dbo].[t120_mc_items] t120 ON t120.f120_rowid = f121_rowid_item
        INNER JOIN [dbo].[t132_mc_items_instalacion] t132 ON t132.f132_rowid_item_ext = t121.f121_rowid
        WHERE t132.f132_mf_rowid_ruta IS NOT NULL
        ORDER BY 3
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['IdCompania'] = df['IdCia'].astype('int32')

    query = """
        INSERT INTO siesa.tbRelRutasItem (IdItemExt, IdItem, IdRuta, FecCreacion, FecModificacion) VALUES 
        (?, ?, ?, ?, ?)
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['IdItemExt','IdItem','IdRuta','FecCreacion','FecModificacion']].values.tolist())
    except Exception as e:
        logger.error(f"Error metodo cargue rel rutas items: {e}")

def ejecutar_items():
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename='errors.log', encoding='utf-8', level=logging.DEBUG, format='%(asctime)s %(levelname)s [%(filename)s:%(funcName)s] %(message)s')
    logger.info('Iniciando cargue items')
    with open('../config/database_credentials.json') as f:
        config_file = json.load(f)
    database_name = 'destino'
    lloreda_manager = DatabaseManager(config_file, database_name, use_pooling=False)
    lloreda_manager.connect()

    with open('../config/database_credentials.json') as f:
        config_file = json.load(f)
    database_name = 'origen'
    siesa_manager = DatabaseManager(config_file, database_name, use_pooling=False)
    siesa_manager.connect()

    # Operaciones principales
    cargue_criterio_items(lloreda_manager, siesa_manager, logger)
    cargue_items(lloreda_manager, siesa_manager, logger)
    limpiar_tabla_rel_criterio_items(lloreda_manager, logger)
    cargue_rel_criterio_items(lloreda_manager, siesa_manager, logger)
    limpiar_tabla_politicas_items(lloreda_manager, logger)
    cargue_politicas(lloreda_manager, siesa_manager, logger)
    limpiar_tabla_items_precios(lloreda_manager, logger)
    cargue_items_precios(lloreda_manager, siesa_manager, logger)
    cargue_centros_trabajo(lloreda_manager, siesa_manager, logger)
    limpiar_tabla_segmentos_costos(lloreda_manager, logger)
    cargue_segmentos_costos(lloreda_manager, siesa_manager, logger)
    cargue_rutas(lloreda_manager, siesa_manager, logger)
    limpiar_tabla_lista_materiales(lloreda_manager, logger)
    cargue_lista_materiales(lloreda_manager, siesa_manager, logger)
    limpiar_tabla_rel_rutas_item(lloreda_manager, logger)
    cargue_rel_rutas_items(lloreda_manager, siesa_manager, logger)
    ############################

    lloreda_manager.disconnect()
    siesa_manager.disconnect()

if __name__ == "__main__":
    ejecutar_items()