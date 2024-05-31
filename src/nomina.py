import json
import pandas as pd
from datetime import datetime
from utils.database_manager import DatabaseManager

def limpiar_tabla_causacion(manager: DatabaseManager):
    print("Limpiando tabla causación")
    try:
        query = 'TRUNCATE TABLE siesa.tbCausacionNomina'
        manager.execute_query_no_results(query)
    except Exception as e:
        print("Error metodo LimpiarTablaCausación: ", e)

def cargue_grupo_causacion_1(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando grupo causación 1")
    query = """
        SELECT c0590_id_cia CIA ,
        f010_razon_social NOMBRECIA ,
        1 GRUPOCAUSACION ,
        c0501_id CONCEPTO ,
        c0501_descripcion NOMBRECONCEPTO ,
        Isnull(c0590_id_grupo_ccosto, '') GPOCCOSTOSPCGA ,
        Isnull(f279_descripcion, '') NOMBREGPOCCOSTOPCGA ,
        Isnull(ccospcga.f284_id, '') CCOSTOSPCGA ,
        Isnull(ccospcga.f284_descripcion, '') NOMBRECCOSTOPCGA ,
        Isnull(c0510_rowid, 0) RowIDGPOEMPLEADOS ,
        Isnull(c0510_id, '') IDGPOEMPLEADOS ,
        Isnull(c0510_descripcion, '') DESCGPOEMPLEADOS ,
        Isnull(auxpcga.f253_id, '') IDCUENTAPCGA ,
        Isnull(auxpcga.f253_descripcion, '') DESCCUENTAPCGA ,
        c0501_porcentaje_basico_m1 PORCENTAJECAUSACION /*
        ,Isnull(c0590_id_co, '')                                                                                      IDCOPCGA
        ,Isnull(copcga.f285_descripcion, '')                                   CENTROOPPCGA
        ,Isnull(c0590_id_un, '')                                                                                      UNPCGA
        ,Isnull(negpcga.f281_descripcion, '')                 UNIDADNEGOCIOPCGA
        ,Isnull(terpcga.f200_id, '')                     TERCEROPCGA
        ,Isnull(terpcga.f200_razon_social, '')                 NOMBRETERCEROPCGA
        ,CASE
            WHEN c0590_ind_naturaleza = 1 THEN 'Debito'
            WHEN c0590_ind_naturaleza = 0 THEN 'Credito'
            END                          NATURALEZAPCGA */
        FROM w0590_equiv_causacion
        INNER JOIN w0501_conceptos ON c0501_rowid = c0590_rowid_concepto
        INNER JOIN t010_mm_companias ON f010_id = c0590_id_cia
        LEFT JOIN t279_co_grupos_ccostos ON f279_id_cia = c0590_id_cia
        AND f279_id = c0590_id_grupo_ccosto
        LEFT JOIN t284_co_ccosto ccospcga ON ccospcga.f284_rowid = c0590_rowid_ccosto
        LEFT JOIN t284_co_ccosto ccosniif ON ccosniif.f284_rowid = c0590_rowid_ccosto_l2
        LEFT JOIN w0510_grupos_empleados ON c0510_rowid = c0590_rowid_grupo_empleados
        LEFT JOIN t253_co_auxiliares auxpcga ON auxpcga.f253_rowid = c0590_rowid_cuenta
        LEFT JOIN t253_co_auxiliares auxniif ON auxniif.f253_rowid = c0590_rowid_cuenta_l2
        LEFT JOIN t285_co_centro_op copcga ON copcga.f285_id_cia = c0590_id_cia
        AND copcga.f285_id = c0590_id_co
        LEFT JOIN t285_co_centro_op coniif ON coniif.f285_id_cia = c0590_id_cia
        AND coniif.f285_id = c0590_id_co_l2
        LEFT JOIN t281_co_unidades_negocio negpcga ON negpcga.f281_id_cia = c0590_id_cia
        AND negpcga.f281_id = c0590_id_un
        LEFT JOIN t281_co_unidades_negocio negniif ON negniif.f281_id_cia = c0590_id_cia
        AND negniif.f281_id = c0590_id_un_l2
        LEFT JOIN t200_mm_terceros terpcga ON terpcga.f200_rowid = c0590_rowid_tercero
        LEFT JOIN t200_mm_terceros terniif ON terniif.f200_rowid = c0590_rowid_tercero_l2
        LEFT JOIN t243_co_rubros_ppto rubropcga ON rubropcga.f243_rowid = c0590_rowid_rubro
        WHERE c0590_id_cia in (1, 6) ORDER BY 1, c0501_id
    """
    result = siesa_manager.execute_query_get_pandas(query)
    query = """
    INSERT INTO siesa.tbCausacionNomina (CIA, NOMBRECIA, GRUPOCAUSACION, CONCEPTO, NOMBRECONCEPTO, 
    GPOCCOSTOSPCGA, NOMBREGPOCCOSTOPCGA, CCOSTOSPCGA, NOMBRECCOSTOPCGA, RowIDGPOEMPLEADOS,
    IDGPOEMPLEADOS, DESCGPOEMPLEADOS, IDCUENTAPCGA, DESCCUENTAPCGA, PORCENTAJECAUSACION) VALUES 
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    try:
        lloreda_manager.execute_bulk_insert(query, result.values.tolist())
    except Exception as e:
        print("Error metodo cargue  grupo causacion 1: ", e)

def cargue_grupo_causacion_2(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando grupo causación 2")
    query = """
        SELECT c0591_id_cia IDCIA ,
        f010_razon_social NOMBRECIA ,
        2 GRUPOCAUSACION ,
        c0570_id CONCEPTO ,
        c0570_descripcion NOMBRECONCEPTO ,
        Isnull(c0591_id_grupo_ccosto, '') GPOCCOSTOS ,
        Isnull(f279_descripcion, '') NOMBREGPOCCOSTO ,
        Isnull(c0510_rowid, 0) RowIDGPOEMPLEADOS ,
        Isnull(c0510_id, '') IDGPOEMPLEADOS ,
        Isnull(c0510_descripcion, '') DESCGPOEMPLEADOS ,
        Isnull(ctadeb.f253_id, '') IDCUENTADB ,
        Isnull(ctadeb.f253_descripcion, '') DESCCUENTADB ,
        w0570_codigos_consolidacion.c0570_porc_provision PORCENTAJECAUSACION 
        /* ,Isnull(c0591_id_co_debitar, '')                          IDCODB
        ,Isnull(codeb.f285_descripcion, '')                     CENTROOPDB
        ,Isnull(ccostodeb.f284_id, '')                               IDCCOSTODB
        ,Isnull(ccostodeb.f284_descripcion, '')                            CCOSTOSDB
        ,Isnull(c0591_id_un_debitar, '')                          UNDB
        ,Isnull(undeb.f281_descripcion, '')                    UNIDADNEGOCIODB
        ,Isnull(terdeb.f200_id, '')                                     TERCERODB
        ,Isnull(terdeb.f200_razon_social, '')                  NOMBRETERCERODB
        ,Isnull(rubro_debitar.f243_id, '')                           RUBRODB
        ,Isnull(rubro_debitar.f243_descripcion, '')  DESCRUBRODB
        ,Isnull(ctacre.f253_id, '')                                      IDCUENTACR
        ,Isnull(ctacre.f253_descripcion, '')                     DESCCUENTACR
        ,Isnull(c0591_id_co_creditar, '')                         IDCOCR
        ,Isnull(cocre.f285_descripcion, '')                      CENTROOPCR
        ,Isnull(ccostocre.f284_id, '')                                IDCCOSTOCR
        ,Isnull(ccostocre.f284_descripcion, '')                              CCOSTOSCR
        ,Isnull(c0591_id_un_creditar, '')                         UNCR
        ,Isnull(uncre.f281_descripcion, '')                      UNIDADNEGOCIOCR
        ,Isnull(tercre.f200_id, '')                                       TERCEROCR
        ,Isnull(tercre.f200_razon_social, '')                    NOMBRETERCEROCR
        ,Isnull(rubro_creditar.f243_descripcion, '') DESCRUBROCR */
        FROM w0591_equiv_consolidacion
        INNER JOIN t010_mm_companias ON f010_id = c0591_id_cia
        INNER JOIN w0570_codigos_consolidacion ON c0570_rowid = c0591_rowid_codigo_consol
        LEFT JOIN t279_co_grupos_ccostos ON f279_id_cia = c0591_id_cia
        AND f279_id = c0591_id_grupo_ccosto
        LEFT JOIN w0510_grupos_empleados ON c0510_rowid = c0591_rowid_grupo_empleados
        LEFT JOIN t253_co_auxiliares ctadeb ON ctadeb.f253_rowid = c0591_rowid_cuenta_debitar
        LEFT JOIN t253_co_auxiliares ctadebniif ON ctadebniif.f253_rowid = c0591_rowid_cuenta_debitar_l2
        LEFT JOIN t253_co_auxiliares ctacreniif ON ctacreniif.f253_rowid = c0591_rowid_cuenta_creditar_l2
        LEFT JOIN t285_co_centro_op codeb ON codeb.f285_id_cia = c0591_id_cia
        AND codeb.f285_id = c0591_id_co_debitar
        LEFT JOIN t285_co_centro_op codebniif ON codebniif.f285_id_cia = c0591_id_cia
        AND codebniif.f285_id = c0591_id_co_debitar_l2
        LEFT JOIN t285_co_centro_op cocreniif ON cocreniif.f285_id_cia = c0591_id_cia
        AND cocreniif.f285_id = c0591_id_co_creditar_l2
        LEFT JOIN t284_co_ccosto ccostodeb ON ccostodeb.f284_rowid = c0591_rowid_ccosto_debitar
        LEFT JOIN t284_co_ccosto ccostodebniif ON ccostodebniif.f284_rowid = c0591_rowid_ccosto_debitar_l2
        LEFT JOIN t284_co_ccosto ccostocreniif ON ccostocreniif.f284_rowid = c0591_rowid_ccosto_creditar_l2
        LEFT JOIN t281_co_unidades_negocio undeb ON undeb.f281_id_cia = c0591_id_cia
        AND undeb.f281_id = c0591_id_un_debitar
        LEFT JOIN t281_co_unidades_negocio undebniif ON undebniif.f281_id_cia = c0591_id_cia
        AND undebniif.f281_id = c0591_id_un_debitar_l2
        LEFT JOIN t281_co_unidades_negocio uncreniif ON uncreniif.f281_id_cia = c0591_id_cia
        AND uncreniif.f281_id = c0591_id_un_creditar_l2
        LEFT JOIN t200_mm_terceros terdeb ON terdeb.f200_rowid = c0591_rowid_tercero_debitar
        LEFT JOIN t200_mm_terceros terdebniif ON terdebniif.f200_rowid = c0591_rowid_terc_debitar_l2
        LEFT JOIN t200_mm_terceros tercreniif ON tercreniif.f200_rowid = c0591_rowid_terc_creditar_l2
        LEFT JOIN t253_co_auxiliares ctacre ON ctacre.f253_rowid = c0591_rowid_cuenta_creditar
        LEFT JOIN t285_co_centro_op cocre ON cocre.f285_id_cia = c0591_id_cia
        AND cocre.f285_id = c0591_id_co_creditar
        LEFT JOIN t284_co_ccosto ccostocre ON ccostocre.f284_rowid = c0591_rowid_ccosto_creditar
        LEFT JOIN t281_co_unidades_negocio uncre ON uncre.f281_id_cia = c0591_id_cia
        AND uncre.f281_id = c0591_id_un_creditar
        LEFT JOIN t200_mm_terceros tercre ON tercre.f200_rowid = c0591_rowid_tercero_creditar
        LEFT JOIN t243_co_rubros_ppto rubro_debitar ON rubro_debitar.f243_rowid = c0591_rowid_rubro_debitar
        LEFT JOIN t243_co_rubros_ppto rubro_creditar ON rubro_creditar.f243_rowid = c0591_rowid_rubro_creditar
        LEFT JOIN t243_co_rubros_ppto rubro_debitar_l2 ON rubro_debitar_l2.f243_rowid = c0591_rowid_rubro_debitar_l2
        LEFT JOIN t243_co_rubros_ppto rubro_creditar_l2 ON rubro_creditar_l2.f243_rowid = c0591_rowid_rubro_creditar_l2
        WHERE c0591_id_cia in (1, 6)
    """
    result = siesa_manager.execute_query_get_pandas(query)
    query = """
    INSERT INTO siesa.tbCausacionNomina (CIA, NOMBRECIA, GRUPOCAUSACION, CONCEPTO, NOMBRECONCEPTO, 
    GPOCCOSTOSPCGA, NOMBREGPOCCOSTOPCGA, RowIDGPOEMPLEADOS, IDGPOEMPLEADOS, DESCGPOEMPLEADOS, 
    IDCUENTAPCGA, DESCCUENTAPCGA, PORCENTAJECAUSACION) VALUES 
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        lloreda_manager.execute_bulk_insert(query, result.values.tolist())
    except Exception as e:
        print("Error metodo cargue  grupo causacion 2: ", e)

def cargue_grupo_causacion_3(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando grupo causación 3")
    query = """
        SELECT c0592_id_cia IDCIA ,
        f010_razon_social NOMBRECIA ,
        3 GRUPOCAUSACION ,
        c0572_id CONCEPTO ,
        c0572_descripcion NOMBRECONCEPTO ,
        Isnull(c0592_id_grupo_ccosto, '') GPOCCOSTOS ,
        Isnull(f279_descripcion, '') NOMBRECCOSTOPCGA ,
        Isnull(c0510_rowid, 0) RowIDGPOEMPLEADOS ,
        Isnull(c0510_id, '') IDGPOEMPLEADOS ,
        Isnull(c0510_descripcion, '') DESCGPOEMPLEADOS ,
        Isnull(ctadeb.f253_id, '') IDCUENTADBPCGA ,
        Isnull(ctadeb.f253_descripcion, '') DESCCUENTADBPCGA ,
        100 PORCENTAJECAUSACION 
        /* ,Isnull(c0592_id_co_debitar, '')                          IDCODBPCGA
        ,Isnull(codeb.f285_descripcion, '')                     CENTROOPDBPCGA
        ,Isnull(ccostodeb.f284_id, '')                               IDCCOSTODBPCGA
        ,Isnull(ccostodeb.f284_descripcion, '')                            CCOSTOSDBPCGA
        ,Isnull(c0592_id_un_debitar, '')                          UNDBPCGA
        ,Isnull(undeb.f281_descripcion, '')                    UNIDADNEGOCIODBPCGA
        ,Isnull(terdeb.f200_id, '')                                     TERCERODBPCGA
        ,Isnull(terdeb.f200_razon_social, '')                  NOMBRETERCERODBPCGA
        ,Isnull(rubro_debitar.f243_id, '')                           RUBRODB
        ,Isnull(rubro_debitar.f243_descripcion, '')  DESCRUBRODB
        ,Isnull(ctacre.f253_id, '')                                      IDCUENTACRPCGA
        ,Isnull(ctacre.f253_descripcion, '')                     DESCCUENTACRPCGA
        ,Isnull(c0592_id_co_creditar, '')                         IDCOCRPCGA
        ,Isnull(cocre.f285_descripcion, '')                      CENTROOPCRPCGA
        ,Isnull(ccostocre.f284_id, '')                                IDCCOSTOCRPCGA
        ,Isnull(ccostocre.f284_descripcion, '')                              CCOSTOSCRPCGA
        ,Isnull(c0592_id_un_creditar, '')                         UNCRPCGA
        ,Isnull(uncre.f281_descripcion, '')                      UNIDADNEGOCIOCRPCGA
        ,Isnull(tercre.f200_id, '')                                       TERCEROCRPCGA
        ,Isnull(tercre.f200_razon_social, '')                    NOMBRETERCEROCRPCGA
        ,Isnull(rubro_creditar.f243_id, '')                         RUBROCR
        ,Isnull(rubro_creditar.f243_descripcion, '') DESCRUBROCR */
        FROM w0592_equiv_autoliquidacion
        INNER JOIN t010_mm_companias ON f010_id = c0592_id_cia
        INNER JOIN w0572_campos_autoliquidacion ON c0572_id = c0592_id_campo_autoliquid
        LEFT JOIN t279_co_grupos_ccostos ON f279_id_cia = c0592_id_cia
        AND f279_id = c0592_id_grupo_ccosto
        LEFT JOIN w0510_grupos_empleados ON c0510_rowid = c0592_rowid_grupo_empleados
        LEFT JOIN t253_co_auxiliares ctadeb ON ctadeb.f253_rowid = c0592_rowid_cuenta_debitar
        LEFT JOIN t253_co_auxiliares ctadebniif ON ctadebniif.f253_rowid = c0592_rowid_cuenta_deb_l2
        LEFT JOIN t253_co_auxiliares ctacreniif ON ctacreniif.f253_rowid = c0592_rowid_cuenta_cred_l2
        LEFT JOIN t285_co_centro_op codeb ON codeb.f285_id_cia = c0592_id_cia
        AND codeb.f285_id = c0592_id_co_debitar
        LEFT JOIN t285_co_centro_op codebniif ON codebniif.f285_id_cia = c0592_id_cia
        AND codebniif.f285_id = c0592_id_co_deb_l2
        LEFT JOIN t285_co_centro_op cocreniif ON cocreniif.f285_id_cia = c0592_id_cia
        AND cocreniif.f285_id = c0592_id_co_cred_l2
        LEFT JOIN t284_co_ccosto ccostodeb ON ccostodeb.f284_rowid = c0592_rowid_ccosto_debitar
        LEFT JOIN t284_co_ccosto ccostodebniif ON ccostodebniif.f284_rowid = c0592_rowid_ccosto_deb_l2
        LEFT JOIN t284_co_ccosto ccostocreniif ON ccostocreniif.f284_rowid = c0592_rowid_ccosto_cred_l2
        LEFT JOIN t281_co_unidades_negocio undeb ON undeb.f281_id_cia = c0592_id_cia
        AND undeb.f281_id = c0592_id_un_debitar
        LEFT JOIN t281_co_unidades_negocio undebniif ON undebniif.f281_id_cia = c0592_id_cia
        AND undebniif.f281_id = c0592_id_un_deb_l2
        LEFT JOIN t281_co_unidades_negocio uncreniif ON uncreniif.f281_id_cia = c0592_id_cia
        AND uncreniif.f281_id = c0592_id_un_cred_l2
        LEFT JOIN t200_mm_terceros terdeb ON terdeb.f200_rowid = c0592_rowid_tercero_debitar
        LEFT JOIN t200_mm_terceros terdebniif ON terdebniif.f200_rowid = c0592_rowid_tercero_deb_l2
        LEFT JOIN t200_mm_terceros tercreniif ON tercreniif.f200_rowid = c0592_rowid_tercero_cred_l2
        LEFT JOIN t253_co_auxiliares ctacre ON ctacre.f253_rowid = c0592_rowid_cuenta_creditar
        LEFT JOIN t285_co_centro_op cocre ON cocre.f285_id_cia = c0592_id_cia
        AND cocre.f285_id = c0592_id_co_creditar
        LEFT JOIN t284_co_ccosto ccostocre ON ccostocre.f284_rowid = c0592_rowid_ccosto_creditar
        LEFT JOIN t281_co_unidades_negocio uncre ON uncre.f281_id_cia = c0592_id_cia
        AND uncre.f281_id = c0592_id_un_creditar
        LEFT JOIN t200_mm_terceros tercre ON tercre.f200_rowid = c0592_rowid_tercero_creditar
        LEFT JOIN t243_co_rubros_ppto rubro_debitar ON rubro_debitar.f243_rowid = c0592_rowid_rubro_debitar
        LEFT JOIN t243_co_rubros_ppto rubro_creditar ON rubro_creditar.f243_rowid = c0592_rowid_rubro_creditar
        LEFT JOIN t243_co_rubros_ppto rubro_debitar_l2 ON rubro_debitar_l2.f243_rowid = c0592_rowid_rubro_debitar_l2
        LEFT JOIN t243_co_rubros_ppto rubro_creditar_l2 ON rubro_creditar_l2.f243_rowid = c0592_rowid_rubro_creditar_l2
        WHERE c0592_id_cia in (1, 6)
    """
    result = siesa_manager.execute_query_get_pandas(query)
    query = """
    INSERT INTO siesa.tbCausacionNomina (CIA, NOMBRECIA, GRUPOCAUSACION, CONCEPTO, NOMBRECONCEPTO, 
    GPOCCOSTOSPCGA, NOMBREGPOCCOSTOPCGA, RowIDGPOEMPLEADOS, IDGPOEMPLEADOS, DESCGPOEMPLEADOS, 
    IDCUENTAPCGA, DESCCUENTAPCGA, PORCENTAJECAUSACION) VALUES 
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        lloreda_manager.execute_bulk_insert(query, result.values.tolist())
    except Exception as e:
        print("Error metodo cargue  grupo causacion 3: ", e)

def cargue_grupo_empleados(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando grupos empleados")
    query = """
        SELECT a.c0510_id_cia idcompania,
        a.c0510_rowid idgrupoempleados,
        a.c0510_id codgrupoempleados,
        a.c0510_descripcion grupoempleados,
        b.c0501_id codconceptosueldo,
        c.c0501_id codconceptofestivo,
        d.c0501_id codconceptodominical
        FROM w0510_grupos_empleados a
        INNER JOIN w0501_conceptos b ON b.c0501_rowid = a.c0510_rowid_cpto_sueldo
        INNER JOIN w0501_conceptos c ON c.c0501_rowid = a.c0510_rowid_cpto_dominical
        INNER JOIN w0501_conceptos d ON d.c0501_rowid = a.c0510_rowid_cpto_festivo
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['C_IdCompania'] = df['idcompania'].astype('int32')
    
    query = f"""
        WITH CTE AS (
            SELECT ? AS idcompania,
            ? AS idgrupoempleados,
            ? AS codgrupoempleados,
            ? AS grupoempleados,
            ? AS codconceptosueldo,
            ? AS codconceptofestivo,
            ? AS codconceptodominical,
            ? AS FecCreacion,
            ? AS FecModificacion
        )
        MERGE siesa.tbGrupoEmpleados AS tgt
        USING CTE AS src
        ON tgt.idGrupoEmpleados = src.idgrupoempleados
        WHEN NOT MATCHED THEN
            INSERT (IdCompania, idGrupoEmpleados, CodGrupoEmpleados, GrupoEmpleados,
            CodConceptoSueldo, CodConceptoDominical, CodConceptoFestivo, FecCreacion,
            FecModificacion)
            VALUES (src.idcompania, src.idgrupoempleados, src.codgrupoempleados, 
            src.grupoempleados, src.codconceptosueldo, src.codconceptodominical, 
            src.codconceptofestivo, src.FecCreacion, src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.IdCompania = src.idcompania,
            tgt.CodGrupoEmpleados = src.codgrupoempleados,
            tgt.GrupoEmpleados = src.grupoempleados,
            tgt.CodConceptoSueldo = src.codconceptosueldo,
            tgt.CodConceptoDominical = src.codconceptodominical,
            tgt.CodConceptoFestivo = src.codconceptofestivo,
            tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['C_IdCompania','idgrupoempleados','codgrupoempleados',
                                                       'grupoempleados','codconceptosueldo','codconceptofestivo',
                                                       'codconceptodominical','FecCreacion','FecModificacion']].values.tolist())
    except Exception as e:
        print("Error metodo cargue  grupo causacion 3: ", e)

if __name__ == "__main__":
    with open('../config/database_credentials.json') as f:
        config_file = json.load(f)
    database_name = 'lloreda'
    lloreda_manager = DatabaseManager(config_file, database_name, use_pooling=False)
    lloreda_manager.connect()

    with open('../config/database_credentials.json') as f:
        config_file = json.load(f)
    database_name = 'siesa'
    siesa_manager = DatabaseManager(config_file, database_name, use_pooling=False)
    siesa_manager.connect()

    # Operaciones principales
    limpiar_tabla_causacion(lloreda_manager)
    cargue_grupo_causacion_1(lloreda_manager, siesa_manager)
    cargue_grupo_causacion_2(lloreda_manager, siesa_manager)
    cargue_grupo_causacion_3(lloreda_manager, siesa_manager)
    cargue_grupo_empleados(lloreda_manager, siesa_manager)
    ############################

    lloreda_manager.disconnect()
    siesa_manager.disconnect()