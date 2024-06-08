import json
import pandas as pd
from datetime import datetime
from utils.database_manager import DatabaseManager
import logging

def obtener_lista_companias(siesa_manager: DatabaseManager, logger: logging.Logger):
    query = 'SELECT f010_id IdCompania FROM [dbo].[t010_mm_companias]'
    result = siesa_manager.execute_query_get_pandas(query)
    return result['IdCompania'].tolist()

def determinar_periodos(siesa_manager: DatabaseManager, logger: logging.Logger):
    query = """
        SELECT CASE 
            WHEN MONTH(GETDATE()) = 1 THEN CONVERT(VARCHAR(6),DATEADD(MONTH,-1,GETDATE()),112) 
            ELSE CONVERT(VARCHAR(6),YEAR(GETDATE())*100 + 1 ) 
            END Periodo
    """
    result = siesa_manager.execute_query_get_pandas(query)
    return result['Periodo'][0]

def limpiar_tabla_movimientos(lloreda_manager: DatabaseManager, lista_companias: list[int], periodo: int, logger: logging.Logger):
    print("Limpiando tabla movimientos")
    try:
        for compania in lista_companias:
            query = f'DELETE FROM siesa.FactMovimientoInv WHERE IdCompania = {compania} AND IdPeriodo >= {periodo}'
            lloreda_manager.execute_query_no_results(query)
    except Exception as e:
        logger.error(f"Error metodo LimpiarTablaMovimientos: {e}")

def limpiar_tabla_saldos(lloreda_manager: DatabaseManager, lista_companias: list[int], periodo: int, logger: logging.Logger):
    print("Limpiando tabla saldos")
    try:
        for compania in lista_companias:
            query = f'DELETE FROM siesa.Fact_SaldoContable WHERE  IdCompania = {compania} AND Periodo >= {periodo}'
            lloreda_manager.execute_query_no_results(query)
    except Exception as e:
        logger.error(f"Error metodo LimpiarTablaSaldos: {e}")

def cargue_fact_inventario(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager, lista_companias: list[int], periodo: int, logger: logging.Logger):
    print("Cargando fact inventario")
    for compania in lista_companias:
        query = f"""
            SELECT t470.f470_id_cia IdCompania,
                t470.f470_id_periodo IdPeriodo,
                t470.f470_ind_estado_cm IdEstadoCM,
                t470.f470_id_concepto Concepto,
                t470.f470_id_motivo MotivoCod,
                t146.f146_descripcion Motivo,
                t470.f470_id_unidad_medida UnidadMedida,
                t470.f470_id_instalacion IdInstalacion,
                t470.f470_id_un_movto IdUNMovimiento,
                t350.f350_id_co CentroOperacionCod,
                t350.f350_id_clase_docto IdClaseDocumento,
                t350.f350_id_tipo_docto TipoDocumentoCod,
                t470.f470_cant_1 Cantidad1,
                t470.f470_cant_2 Cantidad2,
                t470.f470_cant_base CantidadBase,
                t470.f470_costo_prom_uni costo_prom_uni,
                t470.f470_costo_prom_tot costo_prom_total,
                t470.f470_costo_est_uni costo_est_uni,
                t470.f470_costo_est_tot,
                t470.f470_precio_uni precio_uni,
                t470.f470_vlr_bruto vlr_bruto,
                t470.f470_vlr_dscto_linea vlr_dscto_linea,
                t470.f470_vlr_dscto_global vlr_dscto_global,
                t470.f470_vlr_imp vlr_imp,
                t470.f470_vlr_neto vlr_neto,
                t470.f470_precio_uni_impto_asumido precio_uni_impto_asumido,
                t470.f470_ind_impto_asumido ind_impto_asumido,
                t470.f470_vlr_base_impto_asum vlr_base_impto_asum,
                t470.f470_vlr_base_alt_impto_asum vlr_base_alt_impto_asum ,
                T470.f470_ind_tipo_item,
                T470.f470_rowid_item_ext ,
                t120.f120_rowid,
                t470.f470_rowid_ccosto_movto,
                cc.f284_descripcion,
                cc.f284_id ,
                rowid_auxiliar IdAuxiliar,
                t470.f470_ind_naturaleza,
                t350.f350_rowid_tercero,
                t350.f350_id_sucursal,
                t470.f470_desc_variable,
                t350.f350_consec_docto
            FROM t470_cm_movto_invent t470
            INNER JOIN t350_co_docto_contable t350 ON t470.f470_rowid_docto = t350.f350_rowid
            INNER JOIN T021_MM_TIPOS_DOCUMENTOS ON t350.f350_id_cia = T021_MM_TIPOS_DOCUMENTOS.F021_ID_CIA
            AND t350.f350_id_tipo_docto = T021_MM_TIPOS_DOCUMENTOS.F021_ID
            INNER JOIN T028_MM_CLASES_DOCUMENTO t028 ON t350.f350_id_clase_docto = t028.F028_ID
            INNER JOIN T029_MM_CLASES_TIPOS_DOCUMENTO ON T021_MM_TIPOS_DOCUMENTOS.F021_ID_CIA = T029_MM_CLASES_TIPOS_DOCUMENTO.F029_ID_CIA
            AND T021_MM_TIPOS_DOCUMENTOS.F021_ID = T029_MM_CLASES_TIPOS_DOCUMENTO.F029_ID_TIPO_DOCTO
            AND t028.F028_ID = T029_MM_CLASES_TIPOS_DOCUMENTO.F029_ID_CLASE_DOCTO
            INNER JOIN t121_mc_items_extensiones T121 ON t470.f470_rowid_item_ext = t121.f121_rowid
            INNER JOIN t120_mc_items T120 ON t121.f121_rowid_item = t120.f120_rowid
            LEFT OUTER JOIN t146_mc_motivos t146 ON t146.f146_id_cia = t470.f470_id_cia
            AND t146.f146_id_concepto = t470.f470_id_concepto
            AND t146.f146_id = t470.f470_id_motivo
            LEFT OUTER JOIN [dbo].[t284_co_ccosto] cc ON cc.f284_rowid = t470.f470_rowid_ccosto_movto
            LEFT OUTER JOIN
            (SELECT f193_id_cia,
                    f193_id_tipo_inv_serv,
                    f193_id_concepto,
                    f193_id_motivo ,
                    ISNULL(ISNULL(ISNULL(f193_rowid_auxiliar, f193_rowid_aux_compra), f193_rowid_aux_venta_gravada), f193_rowid_aux_costo_venta) rowid_auxiliar ,
                    f193_ind_libro,
                    RANK () OVER (PARTITION BY f193_id_cia,
                                                f193_id_tipo_inv_serv,
                                                f193_id_concepto,
                                                f193_id_motivo
                                    ORDER BY f193_ind_libro DESC) FILA
            FROM t193_mc_equiv_contab_varios) t193 ON t470.f470_id_cia = t193.f193_id_cia
            AND t120.f120_id_tipo_inv_serv = t193.f193_id_tipo_inv_serv
            AND t470.f470_id_concepto = t193.f193_id_concepto
            AND t470.f470_id_motivo = t193.f193_id_motivo
            AND T193.FILA = 1
            WHERE t470.f470_id_cia = {compania}
            AND t470.f470_id_periodo >= {periodo}
        """
        df = siesa_manager.execute_query_get_pandas(query)
        df['FechaCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['FechaActualizacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['IdCompania_Int'] = df['IdCompania'].astype('int32')

        query = 'SELECT IdCentroOperacion, CodCentroOperacion AS CentroOperacionCod FROM siesa.tbCentrosOperacion'
        df_op = lloreda_manager.execute_query_get_pandas(query)
        merged_df = df.merge(df_op, on='CentroOperacionCod', how='inner')

        query = 'SELECT IdTipoDocumento, CodTipoDocumento AS TipoDocumentoCod, IdCompania AS IdCompania_Int FROM siesa.tbTiposDocumento'
        df_doc = lloreda_manager.execute_query_get_pandas(query)
        merged_df = merged_df.merge(df_doc, on=['TipoDocumentoCod','IdCompania_Int'], how='inner')

        query = """
        INSERT INTO siesa.FactMovimientoInv (IdCompania, IdPeriodo, IdEstadoCM, Motivo, UnidadMedida, IdUNMovimineto, CentroOperacionCod,
        IdClaseDocumento, TipoDocumentoCod, Cantidad1, Cantidad2, CantidadBase, costo_prom_uni, costo_prom_total, costo_est_uni,
        precio_uni, vlr_bruto, vlr_dscto_linea, vlr_dscto_global, vlr_imp, vlr_neto, precio_uni_impto_asumido, ind_impto_asumido, vlr_base_impto_asum, vlr_base_alt_impto_asum,
        FechaCreacion, FechaActualizacion, IdItem, Concepto, IdInstalacion, costo_est_tot, IdAuxiliar, IdCentroCosto, MotivoCod,
        Naturaleza, IdTercero, consec_docto, desc_variable, IdSucursal) VALUES 
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        columns_to_insert = ['IdCompania_Int','IdPeriodo','IdEstadoCM','Motivo','UnidadMedida','IdUNMovimiento','CentroOperacionCod',
                            'IdClaseDocumento','TipoDocumentoCod','Cantidad1','Cantidad2','CantidadBase','costo_prom_uni',
                            'costo_prom_total','costo_est_uni','precio_uni','vlr_bruto','vlr_dscto_linea','vlr_dscto_global',
                            'vlr_imp','vlr_neto','precio_uni_impto_asumido','ind_impto_asumido','vlr_base_impto_asum',
                            'vlr_base_alt_impto_asum','FechaCreacion','FechaActualizacion','f120_rowid','Concepto','IdInstalacion',
                            'f470_costo_est_tot','IdAuxiliar','f470_rowid_ccosto_movto','MotivoCod','f470_ind_naturaleza',
                            'f350_rowid_tercero','f350_consec_docto','f470_desc_variable','f350_id_sucursal']
        merged_df = merged_df.astype('object')
        merged_df = merged_df.where(pd.notnull(merged_df), None)
        try:
            lloreda_manager.execute_bulk_insert(query, merged_df[columns_to_insert].values.tolist())
        except Exception as e:
            logger.error(f"Error metodo cargue fact inventario: {e}")

def cargue_fact_saldo(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager, lista_companias: list[int], periodo: int, logger: logging.Logger):
    print("Cargando fact saldo")
    for compania in lista_companias:
        query = f"""
            SELECT t301.f301_id_cia IdCompania,
            t301.f301_rowid_auxiliar id_auxiliar,
            t301.f301_id_co IdCentroOperacion,
            t301.f301_id_un id_un,
            ISNULL(t301.f301_rowid_ccosto, -1) IdCentroCosto,
            t301.f301_periodo periodo,
            SUM(t301.f301_inicial) saldo_inicial,
            SUM(t301.f301_debitos) debitos,
            SUM(t301.f301_creditos) creditos,
            SUM(t301.f301_debitos - t301.f301_creditos) movimiento,
            SUM(t301.f301_inicial + t301.f301_debitos - t301.f301_creditos) saldo_final,
            SUM(t301.f301_inicial_alt) saldo_inicial_alt,
            SUM(t301.f301_debitos_alt) debitos_alt,
            SUM(t301.f301_creditos_alt) creditos_alt,
            SUM(t301.f301_debitos_alt - t301.f301_creditos_alt) movimiento_alt,
            SUM(t301.f301_inicial_alt + t301.f301_debitos_alt - t301.f301_creditos_alt) saldo_final_alt,
            SUM(t301.f301_inicial2) saldo_libro2,
            SUM(t301.f301_debitos2) deb_libro2,
            SUM(t301.f301_creditos2) cre_libro2,
            SUM(t301.f301_inicial2+t301.f301_debitos2-t301.f301_creditos2) saldo_final_lib2,
            SUM(t301.f301_debitos2-t301.f301_creditos2) movto_lib2,
            SUM(t301.f301_inicial_alt2) saldo_alt_lib2,
            SUM(t301.f301_debitos_alt2) deb_alt_lib2,
            SUM(t301.f301_creditos_alt2) cre_alt_lib2,
            SUM(t301.f301_inicial_alt2+t301.f301_debitos_alt2-t301.f301_creditos_alt2) sfinal_alt_lib2,
            SUM(t301.f301_debitos_alt2-t301.f301_creditos_alt2) movto_alt_lib2 ,
            CASE
                WHEN f301_id_cia = 1
                        OR f301_id_cia = 2 THEN CASE
                                                    WHEN f253_id = '42130501' THEN CASE
                                                                                    WHEN f200_nit = '860009787' THEN f301_rowid_tercero
                                                                                    ELSE NULL
                                                                                END
                                                END
            END IdCliente
        FROM t301_rc_aux_tercero_ccosto t301
        LEFT OUTER JOIN t200_mm_terceros ON f200_rowid = f301_rowid_tercero
        AND f200_id_cia = f301_id_cia
        LEFT OUTER JOIN t253_co_auxiliares ON f253_rowid = f301_rowid_auxiliar
        AND f253_id_cia = f301_id_cia --	INNER JOIN t300_rc_auxiliar t300 on t301.f301_rowid_auxiliar = t300.f300_rowid
        WHERE t301.f301_id_cia = {compania}
        AND f301_periodo >= {periodo}
        GROUP BY t301.f301_id_cia,
                t301.f301_rowid_auxiliar,
                t301.f301_id_co,
                t301.f301_id_un,
                ISNULL(t301.f301_rowid_ccosto, -1),
                t301.f301_periodo ,
                CASE
                    WHEN f301_id_cia = 1
                        OR f301_id_cia = 2 THEN CASE
                                                    WHEN f253_id = '42130501' THEN CASE
                                                                                        WHEN f200_nit = '860009787' THEN f301_rowid_tercero
                                                                                        ELSE NULL
                                                                                    END
                                                END
                END
        """
        df = siesa_manager.execute_query_get_pandas(query)
        df['FechaCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['FechaActualizacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['IdCompania_Int'] = df['IdCompania'].astype('int32')

        query = 'SELECT IdCentroOperacion AS IdCentroOperacionn, CodCentroOperacion AS IdCentroOperacion FROM siesa.tbCentrosOperacion'
        df_op = lloreda_manager.execute_query_get_pandas(query)
        merged_df = df.merge(df_op, on='IdCentroOperacion', how='inner')

        query = """
        INSERT INTO siesa.Fact_SaldoContable (IdCompania, id_auxiliar, IdCentroOperacion, IdCentroCosto, id_un,
        periodo, saldo_inicial, debitos, creditos, movimiento, saldo_final, saldo_inicial_alt, debitos_alt,
        creditos_alt, movimiento_alt, saldo_final_alt, saldo_libro2, deb_libro2, cre_libro2, saldo_final_lib2, movto_lib2,
        saldo_alt_lib2, deb_alt_lib2, cre_alt_lib2, sfinal_alt_lib2, movto_alt_lib2, nit) VALUES 
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        columns_to_insert = ['IdCompania_Int','id_auxiliar','IdCentroOperacionn','IdCentroCosto','id_un','periodo',
                             'saldo_inicial','debitos','creditos','movimiento','saldo_final','saldo_inicial_alt',
                             'debitos_alt','creditos_alt','movimiento_alt','saldo_final_alt','saldo_libro2',
                             'deb_libro2','cre_libro2','saldo_final_lib2','movto_lib2','saldo_alt_lib2','deb_alt_lib2',
                             'cre_alt_lib2','sfinal_alt_lib2','movto_alt_lib2','IdCliente']
        merged_df = merged_df.astype('object')
        merged_df = merged_df.where(pd.notnull(merged_df), None)
        try:
            lloreda_manager.execute_bulk_insert(query, merged_df[columns_to_insert].values.tolist())
        except Exception as e:
            logger.error(f"Error metodo cargue fact saldos: {e}")

def ejecutar_datos_lloreda():
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename='errors.log', encoding='utf-8', level=logging.DEBUG, format='%(asctime)s %(levelname)s [%(filename)s:%(funcName)s] %(message)s')
    logger.info('Iniciando cargue datos lloreda')

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
    lista_companias = obtener_lista_companias(siesa_manager, logger)
    periodo = determinar_periodos(lloreda_manager, logger)
    limpiar_tabla_movimientos(lloreda_manager, lista_companias, periodo, logger)
    limpiar_tabla_saldos(lloreda_manager, lista_companias, periodo, logger)
    cargue_fact_inventario(lloreda_manager, siesa_manager, lista_companias, periodo, logger)
    cargue_fact_saldo(lloreda_manager, siesa_manager, lista_companias, periodo, logger)
    ############################

    lloreda_manager.disconnect()
    siesa_manager.disconnect()

if __name__ == "__main__":
    ejecutar_datos_lloreda()