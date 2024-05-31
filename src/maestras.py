import json
import pandas as pd
from datetime import datetime
from utils.database_manager import DatabaseManager

def cargue_companias(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando companias")
    query = """
        SELECT [f010_id] IdCompania ,
        [f010_razon_social] RazonSocial ,
        [f010_nit] Nit ,
        [f010_ind_estado] IndEstado ,
        [f010_id_moneda_local] MonedaLocal ,
        [f010_id_plan_cuentas] PlanCuenta
        FROM [t010_mm_companias]
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['NewIdCompania'] = df['IdCompania'].astype('int32')
    query = f"""
        WITH CTE AS (
            SELECT ? AS IdCompania,
            ? AS RazonSocial,
            ? AS Nit,
            ? AS IndEstado,
            ? AS MonedaLocal,
            ? AS PlanCuenta,
            ? AS FecCreacion,
            ? AS FecModificacion
        )
        MERGE siesa.tbCompanias AS tgt
        USING CTE AS src
        ON tgt.IdCompania = src.IdCompania
        WHEN NOT MATCHED THEN
            INSERT (IdCompania, RazonSocial, Nit, IndEstado,
            MonedaLocal, PlanCuenta, FecCreacion, FecModificacion)
            VALUES (src.IdCompania, src.RazonSocial, src.Nit, src.IndEstado, 
            src.MonedaLocal, src.PlanCuenta, src.FecCreacion, src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.RazonSocial = src.RazonSocial,
            tgt.Nit = src.Nit,
            tgt.IndEstado = src.IndEstado,
            tgt.MonedaLocal = src.MonedaLocal,
            tgt.PlanCuenta = src.PlanCuenta,
            tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['NewIdCompania', 'RazonSocial', 'Nit', 'IndEstado', 'MonedaLocal', 'PlanCuenta', 'FecCreacion', 'FecModificacion']].values.tolist())
    except Exception as e:
        print("Error metodo cargue companias: ", e)

def cargue_regionales(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando regionales")
    query = """
        SELECT f280_id CodRegion,
        f280_descripcion Regional, 
        CONVERT(INT,f280_id_cia) IdCompania
        FROM [dbo].t280_co_regionales
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    query = f"""
        WITH CTE AS (
            SELECT ? AS CodRegion,
            ? AS Regional,
            ? AS IdCompania,
            ? AS FecCreacion,
            ? AS FecModificacion
        )
        MERGE siesa.tbRegionales AS tgt
        USING CTE AS src
        ON tgt.CodRegion = src.CodRegion
        WHEN NOT MATCHED THEN
            INSERT (IdCompania, CodRegion, Regional, 
            FecCreacion, FecModificacion)
            VALUES (src.IdCompania, src.CodRegion, src.Regional, 
            src.FecCreacion, src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.Regional = src.Regional,
            tgt.IdCompania = src.IdCompania,
            tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['CodRegion', 'Regional', 'IdCompania', 'FecCreacion', 'FecModificacion']].values.tolist())
    except Exception as e:
        print("Error metodo cargue regionales: ", e)

def cargue_tipos_documento(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando tipos documento")
    query = """
        SELECT convert(int,t.f021_id_cia) IdCompania,
        t.f021_id TipoDocumentoCod,
        t.f021_descripcion TipoDocumento,
        t.f021_id_flia_docto FamiliaCod,
        f.f020_descripcion Familia
        FROM T021_MM_TIPOS_DOCUMENTOS t
        INNER JOIN [dbo].[t020_mm_flias_documentos] f ON f.f020_id_cia = t.f021_id_cia
        AND f.f020_id = t.f021_id_flia_docto
        ORDER BY 1, 2
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    query = f"""
        WITH CTE AS (
            SELECT ? AS IdCompania,
            ? AS CodTipoDocumento,
            ? AS TipoDocumento,
            ? AS CodFamilia,
            ? AS Familia,
            ? AS FecCreacion,
            ? AS FecModificacion
        )
        MERGE siesa.tbTiposDocumento AS tgt
        USING CTE AS src
        ON tgt.CodTipoDocumento = src.CodTipoDocumento AND 
            tgt.IdCompania = src.IdCompania
        WHEN NOT MATCHED THEN
            INSERT (IdCompania, CodTipoDocumento, TipoDocumento, CodFamilia,
            Familia, FecCreacion, FecModificacion)
            VALUES (src.IdCompania, src.CodTipoDocumento, src.TipoDocumento, src.CodFamilia,
            src.Familia, src.FecCreacion, src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.CodTipoDocumento = src.CodTipoDocumento,
            tgt.TipoDocumento = src.TipoDocumento,
            tgt.CodFamilia = src.CodFamilia,
            tgt.Familia = src.Familia,
            tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['IdCompania', 'TipoDocumentoCod', 'TipoDocumento', 'FamiliaCod', 'Familia', 'FecCreacion', 'FecModificacion']].values.tolist())
    except Exception as e:
        print("Error metodo cargue tipos documento: ", e)

def cargue_centros_operacion(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando centros operacion")
    
    query = """
        SELECT f285_id CentroOperacionCod,
        CONVERT(INT,f285_id_cia) IdCompania,
        f285_id_regional CodRegion,
        f285_descripcion CentroOperacion,
        f285_ts FechaCrea,
        f285_ind_estado Activo
        FROM [dbo].t285_co_centro_op
        ORDER BY 2, 3, 1
    """

    result = siesa_manager.execute_query_get_pandas(query)
    result['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    query = """
        SELECT IdRegional, IdCompania, CodRegion
        FROM siesa.tbRegionales
    """
    regionales_df = lloreda_manager.execute_query_get_pandas(query)
    merged_df = result.merge(regionales_df, on=['IdCompania', 'CodRegion'], how='inner')

    query = f"""
        WITH CTE AS (
            SELECT ? AS CentroOperacionCod,
                ? AS IdCompania,
                ? AS CodRegion,
                ? AS CentroOperacion,
                ? AS Activo,
                ? AS IdRegional,
                ? AS FechaCrea,
                ? AS FecModificacion
        )
        MERGE siesa.tbCentrosOperacion AS tgt
        USING CTE AS src
        ON tgt.CodCentroOperacion = src.CentroOperacionCod AND 
            tgt.IdRegional = src.IdRegional
        WHEN NOT MATCHED THEN
            INSERT (IdCompania, IdRegional, CodCentroOperacion, CentroOperacion,
            Activo, FecCreacion, FecModificacion)
            VALUES (src.IdCompania, src.IdRegional, src.CentroOperacionCod, src.CentroOperacion,
            src.Activo, src.FechaCrea, src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.IdCompania = src.IdCompania,
            tgt.CentroOperacion = src.CentroOperacion,
            tgt.Activo = src.Activo,
            tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, merged_df[['CentroOperacionCod', 'IdCompania', 'CodRegion', 'CentroOperacion', 'Activo', 'IdRegional', 'FechaCrea', 'FecModificacion']].values.tolist())
    except Exception as e:
        print("Error metodo cargue centros operacion: ", e)

def cargue_centros_costo(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando centros costo")
    query = """
        SELECT DISTINCT cc.f284_rowid IdCentroCosto,
        CONVERT(int,cc.f284_id_cia) IdCompania,
        cc.f284_id CentroCostosCod,
        cc.f284_descripcion CentroCosto ,
        cc.f284_id_co CentroOperacionesCod,
        co.f285_descripcion CentroOperaciones ,
        cc.f284_id_grupo_ccosto GrupoCentroCostoCod,
        gc.f279_descripcion GrupoCentroCosto ,
        CC.f284_id_ccosto_mayor IdCentroCostoMayor ,
        cc.f284_ind_estado Estado ,
        cc.f284_ts FechaCreacion ,
        bi.ccosto_n1 CodNivel1,
        bi.nombre_ccosto_n1 Nivel1 ,
        bi.ccosto_n2 CodNivel2,
        bi.nombre_ccosto_n2 Nivel2 ,
        bi.ccosto_n3 CodNivel3,
        bi.nombre_ccosto_n3 Nivel3 ,
        bi.ccosto_n4 CodNivel4,
        bi.nombre_ccosto_n4 Nivel4 ,
        bi.ccosto_n5 CodNivel5,
        bi.nombre_ccosto_n5 Nivel5 ,
        bi.ccosto_n6 CodNivel6,
        bi.nombre_ccosto_n6 Nivel6 ,
        bi.ccosto_n7 CodNivel7,
        bi.nombre_ccosto_n7 Nivel7
        FROM [dbo].[BI_T284] bi
        INNER JOIN [dbo].[t284_co_ccosto] cc ON cc.f284_rowid = bi.rowid_ccosto
        AND cc.f284_id_cia = bi.id_cia
        LEFT OUTER JOIN [dbo].[t285_co_centro_op] co ON cc.f284_id_co = co.f285_id
        AND cc.f284_id_cia = co.f285_id_cia
        LEFT OUTER JOIN [dbo].[t279_co_grupos_ccostos] gc ON cc.f284_id_grupo_ccosto = gc.f279_id
        AND cc.f284_id_cia = gc.f279_id_cia
        ORDER BY 1
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
        WITH CTE AS (
            SELECT ? AS IdCentroCosto,
            ? AS IdCompania,
            ? AS CentroCostosCod,
            ? AS CentroCosto,
            ? AS CentroOperacionesCod,
            ? AS CentroOperaciones,
            ? AS GrupoCentroCostoCod,
            ? AS GrupoCentroCosto,
            ? AS IdCentroCostoMayor,
            ? AS Estado,
            ? AS FechaCreacion,
            ? AS CodNivel1,
            ? AS Nivel1,
            ? AS CodNivel2,
            ? AS Nivel2,
            ? AS CodNivel3,
            ? AS Nivel3,
            ? AS CodNivel4,
            ? AS Nivel4,
            ? AS CodNivel5,
            ? AS Nivel5,
            ? AS CodNivel6,
            ? AS Nivel6,
            ? AS CodNivel7,
            ? AS Nivel7,
            ? AS FecModificacion
        )
        MERGE siesa.tbCentrosCosto AS tgt
        USING CTE AS src
        ON tgt.IdCentroCosto = src.IdCentroCosto
        WHEN NOT MATCHED THEN
            INSERT (IdCentroCosto, IdCompania, CentroCostosCod, CentroCosto,
            CentroOperacionesCod, GrupoCentroCostoCod, GrupoCentroCosto,
            Estado, IdCentroCostoMayor, CodNivel1, Nivel1, CodNivel2, Nivel2,
            CodNivel3, Nivel3, CodNivel4, Nivel4, CodNivel5, Nivel5,
            CodNivel6, Nivel6, CodNivel7, Nivel7, FechaCreacion, FecModificacion)
            VALUES (src.IdCentroCosto, src.IdCompania, src.CentroCostosCod, src.CentroCosto,
            src.CentroOperacionesCod, src.GrupoCentroCostoCod, src.GrupoCentroCosto,
            src.Estado, src.IdCentroCostoMayor, src.CodNivel1, src.Nivel1,
            src.CodNivel2, src.Nivel2, src.CodNivel3, src.Nivel3, src.CodNivel4, src.Nivel4,
            src.CodNivel5, src.Nivel5, src.CodNivel6, src.Nivel6, src.CodNivel7, src.Nivel7,
            src.FechaCreacion, src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.IdCompania = src.IdCompania,
            tgt.CentroCostosCod = src.CentroCostosCod,
            tgt.CentroCosto = src.CentroCosto,
            tgt.CentroOperacionesCod = src.CentroOperacionesCod,
            tgt.GrupoCentroCostoCod = src.GrupoCentroCostoCod,
            tgt.GrupoCentroCosto = src.GrupoCentroCosto,
            tgt.Estado = src.Estado,
            tgt.IdCentroCostoMayor = src.IdCentroCostoMayor,
            tgt.CodNivel1 = src.CodNivel1,
            tgt.CodNivel2 = src.CodNivel2,
            tgt.CodNivel3 = src.CodNivel3,
            tgt.CodNivel4 = src.CodNivel4,
            tgt.CodNivel5 = src.CodNivel5,
            tgt.CodNivel6 = src.CodNivel6,
            tgt.CodNivel7 = src.CodNivel7,
            tgt.Nivel1 = src.Nivel1,
            tgt.Nivel2 = src.Nivel2,
            tgt.Nivel3 = src.Nivel3,
            tgt.Nivel4 = src.Nivel4,
            tgt.Nivel5 = src.Nivel5,
            tgt.Nivel6 = src.Nivel6,
            tgt.Nivel7 = src.Nivel7,
            tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['IdCentroCosto', 'IdCompania', 'CentroCostosCod', 'CentroCosto', 'CentroOperacionesCod', 'CentroOperaciones', 'GrupoCentroCostoCod', 'GrupoCentroCosto', 'IdCentroCostoMayor', 'Estado', 'FechaCreacion', 'CodNivel1', 'Nivel1', 'CodNivel2', 'Nivel2', 'CodNivel3', 'Nivel3', 'CodNivel4', 'Nivel4', 'CodNivel5', 'Nivel5', 'CodNivel6', 'Nivel6', 'CodNivel7', 'Nivel7', 'FecModificacion']].values.tolist())
    except Exception as e:
        print("Error metodo cargue centros costo: ", e)

# Pending to fix
def cargue_contratos(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando contratos")
    query = """
        SELECT [c0550_id],
        t.f200_nit nitEmpleado,
        concat(f200_nombres, ' ', f200_apellido1, ' ', f200_apellido2) Nombres ,
        [c0550_id_cia] IdCompania ,
        [c0550_id_co] IdCentroOperacion ,
        [c0550_id_cargo] IdCargo ,
        [c0550_rowid_turno] RowidTurno ,
        [c0550_rowid_grupo_empleados] RowidGrupoEmpleados ,
        [c0550_rowid_ccosto] RowidCCosto ,
        [c0550_rowid_centros_trabajo] RowidCentroTrabajo ,
        c0504_id TipoNomina ,
        c0504_descripcion DescripcionTipoNomina ,
        [c0550_rowid_tiempo_basico] RowidTiempoBasico ,
        [c0550_fecha_ingreso] FechaIngreso ,
        [c0550_fecha_ingreso_ley50] FechaIngresoLey50 ,
        [c0550_fecha_retiro] FechaRetiro ,
        [c0550_fecha_contrato_hasta] FechaContratoHasta ,
        [c0550_fecha_prima_hasta] FechaPrimaHasta ,
        [c0550_fecha_vacaciones_hasta] FechaVacacionesHasta ,
        [c0550_fecha_ult_aumento] FechaUltimoAumento ,
        [c0550_fecha_ult_vacaciones] FechaUltVacaciones ,
        [c0550_fecha_ult_pension] FechaUltPension ,
        [c0550_fecha_ult_cesantias] FechaUltCesantias ,
        [c0550_salario] Salario ,
        [c0550_salario_anterior] SalarioAnterior ,
        [c0550_nro_personas_cargo] NroPersonasCargo ,
        [c0550_ind_regimen_laboral] IndRegimenLaboral ,
        [c0550_ind_auxilio_transporte] IndAuxilioTransporte ,
        [c0550_ind_pacto_colectivo] IndPactoColectivo ,
        [c0550_ind_subsidio] IndSubsidio ,
        [c0550_ind_salario_integral] IndSalarioIntegral ,
        [c0550_ind_ley789] IndLey789 ,
        [c0550_ind_tipo_cuenta] IndtipoCuenta ,
        [c0550_ind_clase_contrato] IndClaseContrato ,
        [c0550_ind_termino_contrato] IndTerminoContrato ,
        [c0550_ind_estado] IndEstado ,
        [c0550_rowid_cargo] RowidCargo ,
        [c0550_rowid_convencion] RowidConvencion ,
        [c0550_id_compensacion_flex] IdCompensacionFlex ,
        [c0550_ind_contra_temp] IndContrTemp ,
        [c0550_ind_tipo_salario] IndTipoSalario ,
        f200_fecha_nacimiento FechaNacimiento ,
        c0540_ind_sexo IndSexo ,
        CASE
            WHEN arp.c0517_id = '999' THEN 1
            WHEN afp.c0516_id = '9999' THEN 1
            ELSE 0
        END IndPensionado,
        0 Retirado
        FROM w0550_contratos c
        INNER JOIN t200_mm_terceros t ON t.f200_rowid = c.c0550_rowid_tercero
        INNER JOIN w0504_tipos_nomina tn ON c0504_rowid = c.c0550_rowid_tipo_nomina
        INNER JOIN w0540_empleados em ON em.c0540_rowid_tercero = t.f200_rowid
        INNER JOIN w0517_entidades_arp arp ON arp.c0517_rowid = c.c0550_rowid_entidad_arp
        INNER JOIN w0516_entidades_afp afp ON afp.c0516_rowid = c.c0550_rowid_entidad_pension
        WHERE [c0550_id_cia] not in(5)
        AND [c0550_fecha_retiro] IS NULL
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['Cidcompf'] = df['IdCompensacionFlex'].astype('int16')
    df['CCid'] = df['c0550_id'].astype('int16')
    df['cindreg'] = df['IndRegimenLaboral'].astype('int16')
    df['cindauxt'] = df['IndAuxilioTransporte'].astype('int16')
    df['cindpc'] = df['IndPactoColectivo'].astype('int16')
    df['cindsub'] = df['IndSubsidio'].astype('int16')
    df['cindsi'] = df['IndSalarioIntegral'].astype('int16')
    df['cindle7'] = df['IndLey789'].astype('int16')
    df['cintcuenta'] = df['IndtipoCuenta'].astype('int16')
    df['cindccont'] = df['IndClaseContrato'].astype('int16')
    df['cintcont'] = df['IndTerminoContrato'].astype('int16')
    df['cindes'] = df['IndEstado'].astype('int16')
    df['Crcar'] = df['RowidCargo'].astype('int16')
    df['RowidConvencion'] = df['RowidConvencion'].fillna(0)
    df['Crconv'] = df['RowidConvencion'].astype('int16')
    df['Cindct'] = df['IndContrTemp'].astype('int16')
    df['Cindts'] = df['IndTipoSalario'].astype('int16')
    df['CIndSexo'] = df['IndSexo'].astype('int16')
    df['IndPensionado'] = df['IndPensionado'].astype('int16')
    df['Retirado'] = df['Retirado'].astype('int16')
    query = f"""
        WITH CTE AS (
            SELECT ? AS nitEmpleado,
                ? AS Nombres,
                ? AS IdCompania,
                ? AS IdCentroOperacion,
                ? AS IdCargo,
                ? AS RowidTurno,
                ? AS RowidGrupoEmpleados,
                ? AS RowidCCosto,
                ? AS RowidCentroTrabajo,
                ? AS TipoNomina,
                ? AS DescripcionTipoNomina,
                ? AS RowidTiempoBasico,
                ? AS FechaIngreso,
                ? AS FechaIngresoLey50,
                ? AS FechaRetiro,
                ? AS FechaContratoHasta,
                ? AS FechaPrimaHasta,
                ? AS FechaVacacionesHasta,
                ? AS FechaUltimoAumento,
                ? AS FechaUltVacaciones,
                ? AS FechaUltPension,
                ? AS FechaUltCesantias,
                ? AS Salario,
                ? AS SalarioAnterior,
                ? AS NroPersonasCargo,
                ? AS IndRegimenLaboral,
                ? AS IndAuxilioTransporte,
                ? AS IndPactoColectivo,
                ? AS IndSubsidio,
                ? AS IndSalarioIntegral,
                ? AS IndLey789,
                ? AS IndtipoCuenta,
                ? AS IndClaseContrato,
                ? AS IndTerminoContrato,
                ? AS IndEstado,
                ? AS RowidCargo,
                ? AS RowidConvencion,
                ? AS IdCompensacionFlex,
                ? AS IndContrTemp,
                ? AS IndTipoSalario,
                ? AS FechaNacimiento,
                ? AS IndSexo,
                ? AS IndPensionado,
                ? AS Retirado
        )
        MERGE siesa.tbContratos AS tgt
        USING CTE AS src
        ON tgt.nitEmpleado = src.nitEmpleado AND 
            tgt.IdCompania = src.IdCompania
        WHEN NOT MATCHED THEN
            INSERT (nitEmpleado, Nombres, IdCompania, IdCentroOperacion, 
            IdCargo, RowidTurno, RowidGrupoEmpleados, RowidCCosto, 
            RowidCentroTrabajo, TipoNomina, DescripcionTipoNomina, 
            RowidTiempoBasico, FechaIngreso, FechaIngresoLey50, 
            FechaRetiro, FechaContratoHasta, FechaPrimaHasta, 
            FechaVacacionesHasta, FechaUltimoAumento, FechaUltVacaciones, 
            FechaUltPension, FechaUltCesantias, Salario, SalarioAnterior, 
            NroPersonasCargo, IndRegimenLaboral, IndAuxilioTransporte, 
            IndPactoColectivo, IndSubsidio, IndSalarioIntegral, IndLey789, 
            IndtipoCuenta, IndClaseContrato, IndTerminoContrato, IndEstado, 
            RowidCargo, RowidConvencion, IdCompensacionFlex, IndContrTemp, 
            IndTipoSalario, FechaNacimiento, IndSexo, IndPensionado, 
            Retirado)
            VALUES (src.nitEmpleado, src.Nombres, src.IdCompania, src.IdCentroOperacion, 
                src.IdCargo, src.RowidTurno, src.RowidGrupoEmpleados, src.RowidCCosto, 
                src.RowidCentroTrabajo, src.TipoNomina, src.DescripcionTipoNomina, 
                src.RowidTiempoBasico, src.FechaIngreso, src.FechaIngresoLey50, 
                src.FechaRetiro, src.FechaContratoHasta, src.FechaPrimaHasta, 
                src.FechaVacacionesHasta, src.FechaUltimoAumento, src.FechaUltVacaciones, 
                src.FechaUltPension, src.FechaUltCesantias, src.Salario, src.SalarioAnterior, 
                src.NroPersonasCargo, src.IndRegimenLaboral, src.IndAuxilioTransporte, 
                src.IndPactoColectivo, src.IndSubsidio, src.IndSalarioIntegral, src.IndLey789, 
                src.IndtipoCuenta, src.IndClaseContrato, src.IndTerminoContrato, src.IndEstado, 
                src.RowidCargo, src.RowidConvencion, src.IdCompensacionFlex, src.IndContrTemp, 
                src.IndTipoSalario, src.FechaNacimiento, src.IndSexo, src.IndPensionado, 
                src.Retirado
            )
        WHEN MATCHED THEN UPDATE SET
            tgt.nitEmpleado = src.nitEmpleado, 
            tgt.Nombres = src.Nombres, 
            tgt.IdCompania = src.IdCompania, 
            tgt.IdCentroOperacion = src.IdCentroOperacion, 
            tgt.IdCargo = src.IdCargo, 
            tgt.RowidTurno = src.RowidTurno, 
            tgt.RowidGrupoEmpleados = src.RowidGrupoEmpleados, 
            tgt.RowidCCosto = src.RowidCCosto, 
            tgt.RowidCentroTrabajo = src.RowidCentroTrabajo, 
            tgt.TipoNomina = src.TipoNomina, 
            tgt.DescripcionTipoNomina = src.DescripcionTipoNomina, 
            tgt.RowidTiempoBasico = src.RowidTiempoBasico, 
            tgt.FechaIngreso = src.FechaIngreso, 
            tgt.FechaIngresoLey50 = src.FechaIngresoLey50, 
            tgt.FechaRetiro = src.FechaRetiro, 
            tgt.FechaContratoHasta = src.FechaContratoHasta, 
            tgt.FechaPrimaHasta = src.FechaPrimaHasta, 
            tgt.FechaVacacionesHasta = src.FechaVacacionesHasta, 
            tgt.FechaUltimoAumento = src.FechaUltimoAumento, 
            tgt.FechaUltVacaciones = src.FechaUltVacaciones, 
            tgt.FechaUltPension = src.FechaUltPension, 
            tgt.FechaUltCesantias = src.FechaUltCesantias, 
            tgt.Salario = src.Salario, 
            tgt.SalarioAnterior = src.SalarioAnterior, 
            tgt.NroPersonasCargo = src.NroPersonasCargo, 
            tgt.IndRegimenLaboral = src.IndRegimenLaboral, 
            tgt.IndAuxilioTransporte = src.IndAuxilioTransporte, 
            tgt.IndPactoColectivo = src.IndPactoColectivo, 
            tgt.IndSubsidio = src.IndSubsidio, 
            tgt.IndSalarioIntegral = src.IndSalarioIntegral, 
            tgt.IndLey789 = src.IndLey789, 
            tgt.IndtipoCuenta = src.IndtipoCuenta, 
            tgt.IndClaseContrato = src.IndClaseContrato, 
            tgt.IndTerminoContrato = src.IndTerminoContrato, 
            tgt.IndEstado = src.IndEstado, 
            tgt.RowidCargo = src.RowidCargo, 
            tgt.RowidConvencion = src.RowidConvencion, 
            tgt.IdCompensacionFlex = src.IdCompensacionFlex, 
            tgt.IndContrTemp = src.IndContrTemp, 
            tgt.IndTipoSalario = src.IndTipoSalario, 
            tgt.FechaNacimiento = src.FechaNacimiento, 
            tgt.IndSexo = src.IndSexo, 
            tgt.IndPensionado = src.IndPensionado, 
            tgt.Retirado = src.Retirado;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['nitEmpleado', 'Nombres', 'IdCompania', 'IdCentroOperacion', 'IdCargo', 'RowidTurno', 'RowidGrupoEmpleados', 'RowidCCosto', 'RowidCentroTrabajo', 'TipoNomina', 'DescripcionTipoNomina', 'RowidTiempoBasico', 'FechaIngreso', 'FechaIngresoLey50', 'FechaRetiro', 'FechaContratoHasta', 'FechaPrimaHasta', 'FechaVacacionesHasta', 'FechaUltimoAumento', 'FechaUltVacaciones', 'FechaUltPension', 'FechaUltCesantias', 'Salario', 'SalarioAnterior', 'NroPersonasCargo', 'cindreg', 'cindauxt', 'cindpc', 'cindsub', 'cindsi', 'cindle7', 'cintcuenta', 'cindccont', 'cintcont', 'cindes', 'Crcar', 'Crconv', 'Cidcompf', 'Cindct', 'Cindts', 'FechaNacimiento', 'IndSexo', 'IndPensionado', 'Retirado']].values.tolist())
    except Exception as e:
        print("Error metodo cargue contratos: ", e)

def cargue_plan_cuentas(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando plan cuentas")
    query = """
        WITH PL AS
        (SELECT 'PUC' AS PL),
            N1 AS
        (SELECT t252_co_mayores.f252_id AS IdNivel,
                t252_co_mayores.f252_descripcion AS Nivel,
                t252_co_mayores.f252_ind_tipo AS Tipo,
                t252_co_mayores.f252_ind_naturaleza AS Naturaleza
        FROM t252_co_mayores
        INNER JOIN PL AS PL_4 ON t252_co_mayores.f252_id_plan = PL_4.PL
        WHERE (t252_co_mayores.f252_nivel = 1)),
            N2 AS
        (SELECT n.f252_id AS IdNivel,
                n.f252_descripcion AS Nivel,
                n.f252_ind_tipo AS Tipo,
                n.f252_ind_naturaleza AS Naturaleza,
                n.f252_id_padre AS IdPadre
        FROM t252_co_mayores AS n
        INNER JOIN PL AS PL_3 ON n.f252_id_plan = PL_3.PL
        WHERE (n.f252_nivel = 2)),
            N3 AS
        (SELECT t252_co_mayores_2.f252_id AS IdNivel,
                t252_co_mayores_2.f252_descripcion AS Nivel,
                t252_co_mayores_2.f252_ind_tipo AS Tipo,
                t252_co_mayores_2.f252_ind_naturaleza AS Naturaleza,
                t252_co_mayores_2.f252_id_padre AS IdPadre,
                t252_co_mayores_2.f252_id_plan AS IdPlan
        FROM t252_co_mayores AS t252_co_mayores_2
        INNER JOIN PL AS PL_2 ON t252_co_mayores_2.f252_id_plan = PL_2.PL
        WHERE (t252_co_mayores_2.f252_nivel = 3)),
            N4 AS
        (SELECT t252_co_mayores_1.f252_id AS IdNivel,
                t252_co_mayores_1.f252_descripcion AS Nivel,
                t252_co_mayores_1.f252_ind_tipo AS Tipo,
                t252_co_mayores_1.f252_ind_naturaleza AS Naturaleza,
                t252_co_mayores_1.f252_id_padre AS IdPadre,
                t252_co_mayores_1.f252_id_plan AS IdPlan
        FROM t252_co_mayores AS t252_co_mayores_1
        INNER JOIN PL AS PL_1 ON t252_co_mayores_1.f252_id_plan = PL_1.PL
        WHERE (t252_co_mayores_1.f252_nivel = 4)),
            SUBC AS
        (SELECT N4_1.IdPlan,
                N4_1.IdNivel AS IdSubcuenta,
                N4_1.Nivel AS Subcuenta,
                N4_1.Naturaleza,
                N4_1.Tipo,
                N3_1.IdNivel AS IdCuenta,
                N3_1.Nivel AS Cuenta,
                N2_1.IdNivel AS IdClase,
                N2_1.Nivel AS Clase,
                N1_1.IdNivel AS IdGrupo,
                N1_1.Nivel AS Grupo
        FROM N4 AS N4_1
        LEFT OUTER JOIN N3 AS N3_1 ON N3_1.IdNivel = N4_1.IdPadre
        LEFT OUTER JOIN N2 AS N2_1 ON N2_1.IdNivel = N3_1.IdPadre
        LEFT OUTER JOIN N1 AS N1_1 ON N1_1.IdNivel = N2_1.IdPadre
        UNION ALL SELECT N4_1.IdPlan,
                            N4_1.IdNivel AS IdSubcuenta,
                            N4_1.Nivel AS Subcuenta,
                            N4_1.Naturaleza,
                            N4_1.Tipo,
                            N3_1.IdNivel AS IdCuenta,
                            N3_1.Nivel AS Cuenta,
                            N2_1.IdNivel AS IdClase,
                            N2_1.Nivel AS Clase,
                            N1_1.IdNivel AS IdGrupo,
                            N1_1.Nivel AS Grupo
        FROM N3 AS N4_1
        LEFT OUTER JOIN N3 AS N3_1 ON N3_1.IdNivel = N4_1.IdNivel
        LEFT OUTER JOIN N2 AS N2_1 ON N2_1.IdNivel = N3_1.IdPadre
        LEFT OUTER JOIN N1 AS N1_1 ON N1_1.IdNivel = N2_1.IdPadre
        WHERE NOT EXISTS
            (SELECT 1
                FROM N4
                WHERE N3_1.IdNivel = N4.IdPadre) )
        SELECT CONVERT(int, A.f253_id_cia) IdCompania,
            ISNULL(c.IdPlan, f254_id_plan) PlanContable ,
            A.f253_id CodMayor,
            A.f253_descripcion AuxMayor ,
            IdSubcuenta,
            Subcuenta,
            Naturaleza,
            Tipo,
            IdCuenta,
            Cuenta,
            IdClase,
            Clase,
            IdGrupo,
            Grupo
        FROM t254_co_mayores_auxiliares M
        INNER JOIN t253_co_auxiliares A ON M.f254_rowid_auxiliar = A.f253_rowid
        AND M.f254_id_cia = A.f253_id_cia
        LEFT OUTER JOIN SUBC C ON C.IdSubcuenta = M.f254_id_mayor ,
                                PL AS P1
        WHERE (f254_id_plan = P1.PL)
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
        WITH CTE AS (
            SELECT ? AS IdCompania,
            ? AS PlanContable,
            ? AS CodMayor,
            ? AS AuxMayor,
            ? AS IdSubCuenta,
            ? AS Subcuenta,
            ? AS IdCuenta,
            ? AS Cuenta,
            ? AS IdClase,
            ? AS Clase,
            ? AS IdGrupo,
            ? AS Grupo,
            ? AS Naturaleza,
            ? AS Tipo,
            ? AS FecCreacion,
            ? AS FecModificacion
        )
        MERGE siesa.tbPlanCuentas AS tgt
        USING CTE AS src
        ON tgt.CodMayor = src.CodMayor AND
            tgt.IdCompania = src.IdCompania AND 
            tgt.PlanContable = src.PlanContable
        WHEN NOT MATCHED THEN
            INSERT (IdCompania, PlanContable, CodMayor, AuxiliarMayor, CodSubcuenta, Subcuenta, 
            CoddCuenta, Cuenta, CodClase, Clase, CodGrupo, Grupo, Naturaleza, Tipo, 
            FecCreacion, FecModificacion)
            VALUES (src.IdCompania, src.PlanContable, src.CodMayor, src.AuxMayor,
            src.IdSubcuenta, src.Subcuenta, src.IdCuenta, src.Cuenta,
            src.IdClase, src.Clase, src.IdGrupo, src.Grupo, src.Naturaleza, src.Tipo,
            src.FecCreacion, src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.IdCompania = src.IdCompania,
            tgt.PlanContable = src.PlanContable,
            tgt.CodMayor = src.CodMayor,
            tgt.AuxiliarMayor = src.AuxMayor,
            tgt.CodSubcuenta = src.IdSubcuenta,
            tgt.Subcuenta = src.Subcuenta,
            tgt.CoddCuenta = src.IdCuenta,
            tgt.Cuenta = src.Cuenta,
            tgt.CodClase = src.IdClase,
            tgt.Clase = src.Clase,
            tgt.CodGrupo = src.IdGrupo,
            tgt.Grupo = src.Grupo,
            tgt.Naturaleza = src.Naturaleza,
            tgt.Tipo = src.Tipo,
            tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['IdCompania', 'PlanContable', 'CodMayor', 'AuxMayor', 'IdSubcuenta', 'Subcuenta', 'IdCuenta', 'Cuenta', 'IdClase', 'Clase', 'IdGrupo', 'Grupo', 'Naturaleza', 'Tipo', 'FecCreacion', 'FecModificacion']].values.tolist())
    except Exception as e:
        print("Error metodo cargue plan cuentas: ", e)

def cargue_auxiliar_contable(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando auxiliares contables")
    query = """
        SELECT CONVERT(INT,f681_rowid_auxiliar) AS IdAuxiliar,
        f681_id_plan AS PlanContable,
        CONVERT(INT,f681_id_cia) AS IdCompania ,
        f681_id_auxiliar AS CodAuxiliar,
        f681_descripcion_auxiliar AS Auxiliar,
        LTRIM(RTRIM(f681_id_mayor_n4)) AS IdAuxMayor ,
        a.f253_id_moneda IdMoneda,
        a.f253_id_grupo_ccosto IdGrupo_CCosto,
        CONVERT(INT,a.f253_ind_ccostos) IndCCosto
        FROM t681_in_auxiliares_bi bi
        INNER JOIN t253_co_auxiliares a ON a.f253_rowid = bi.f681_rowid_auxiliar
        WHERE (f681_id_plan = 'PUC')
        ORDER BY IdAuxiliar
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    query = """
        SELECT IdPlanCuenta, 
        IdCompania, 
        PlanContable, 
        rtrim(ltrim(CodMayor)) CodAuxiliar
        FROM SIESA.tbPlanCuentas
    """
    df2 = lloreda_manager.execute_query_get_pandas(query)

    merged_df = df.merge(df2, on=['IdCompania', 'PlanContable', 'CodAuxiliar'], how='inner')

    query = f"""
        WITH CTE AS (
            SELECT ? AS IdAuxiliarContable,
            ? AS IdPlanCuenta,
            ? AS CodAuxiliar,
            ? AS Auxiliar,
            ? AS FecCreacion,
            ? AS FecModificacion,
            ? AS IdMoneda,
            ? AS IndGrupo_CCosto,
            ? AS IdGrupo_CCostos
        )
        MERGE siesa.tbAuxiliaresContable AS tgt
        USING CTE AS src
        ON tgt.IdAuxiliarContable = src.IdAuxiliarContable
        WHEN NOT MATCHED THEN
            INSERT (IdAuxiliarContable, IdPlanCuenta, CodAuxiliar, Auxiliar,
            FecCreacion, FecModificacion, IdMoneda, IndGrupo_CCosto, IdGrupo_CCostos)
            VALUES (src.IdAuxiliarContable, src.IdPlanCuenta, src.CodAuxiliar, src.Auxiliar, src.FecCreacion, 
            src.FecModificacion, src.IdMoneda, src.IndGrupo_CCosto, src.IdGrupo_CCostos)
        WHEN MATCHED THEN UPDATE SET
            tgt.IdAuxiliarContable = src.IdAuxiliarContable,
            tgt.IdPlanCuenta = src.IdPlanCuenta,
            tgt.CodAuxiliar = src.CodAuxiliar,
            tgt.Auxiliar = src.Auxiliar,
            tgt.FecModificacion = src.FecModificacion,
            tgt.IdMoneda = src.IdMoneda,
            tgt.IndGrupo_CCosto = src.IndGrupo_CCosto,
            tgt.IdGrupo_CCostos = src.IdGrupo_CCostos;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, merged_df[['IdAuxiliar', 'IdPlanCuenta', 'CodAuxiliar', 'Auxiliar', 'FecCreacion', 'FecModificacion', 'IdMoneda', 'IndCCosto', 'IdGrupo_CCosto']].values.tolist())
    except Exception as e:
        print("Error metodo cargue auxiliares contables: ", e)

def limpiar_tabla_cargos(manager: DatabaseManager):
    print("Limpiando tabla cargos")
    try:
        query = 'TRUNCATE TABLE siesa.tbCargos'
        manager.execute_query_no_results(query)
    except Exception as e:
        print("Error metodo Limpiar Tabla Cargos: ", e)

def cargue_cargos(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando cargos")
    query = """
        SELECT c0763_rowid IdCargo,
            c0763_id CodCargo,
            c0763_descripcion descripcioncargo,
            CASE
                WHEN c0763_id_cia IS NULL THEN 1
                ELSE c0763_id_cia
            END idcompania
        FROM w0763_gh01_cargos
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['idcia'] = df['idcompania'].astype('int32')

    query = """
    INSERT INTO siesa.tbCargos (IdCompania, IdCargo, CodCargo, DescripcionCargo, 
    FecCreacion, FecModificacion) 
    VALUES (?, ?, ?, ?, ?, ?)
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['idcompania','IdCargo','CodCargo','descripcioncargo','FecCreacion','FecModificacion']].values.tolist())
    except Exception as e:
        print("Error metodo cargue  grupo causacion 1: ", e)

def limpiar_tabla_cargos_centros(manager: DatabaseManager):
    print("Limpiando tabla cargos centros")
    try:
        query = 'TRUNCATE TABLE siesa.tbCargosCentros'
        manager.execute_query_no_results(query)
    except Exception as e:
        print("Error metodo Limpiar Tabla Cargos Centros: ", e)

def cargue_cargos_centros(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando cargos centros")
    query = """
        SELECT wco.c0550_id_cia idcompania,
            wco.c0550_id idcontrato,
            wco.c0550_id_co idcentrooperacion,
            wco.c0550_rowid_ccosto idcentrocosto,
            wco.c0550_rowid_centros_trabajo idcentrotrabajo,
            c0504_id idtiponomina,
            c0504_descripcion descripciontiponomina,
            wco.c0550_rowid_cargo idcargo,
            wco.c0550_salario salario
        FROM w0550_contratos wco
        INNER JOIN w0504_tipos_nomina wtn ON wtn.c0504_rowid= wco.c0550_rowid_tipo_nomina
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['idcia'] = df['idcompania'].astype('int32')

    query = """
    INSERT INTO siesa.tbCargosCentros (IdCompania, IdContrato, IdCetroOPeracion, rowid_ccosto, 
    rowid_centros_trabajo, IdTipoNomina, DescripcionTipoNomina, rowid_cargo,
    Salario, FecCreacion, FecModificacion) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['idcompania','idcontrato','idcentrooperacion',
                                                       'idcentrocosto','idcentrotrabajo','idtiponomina',
                                                       'descripciontiponomina','idcargo','salario','FecCreacion',
                                                       'FecModificacion']].values.tolist())
    except Exception as e:
        print("Error metodo cargue  grupo causacion 1: ", e)

def cargue_ciudades(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando ciudades")
    query = """
        SELECT [f013_id_pais] IdPais ,
            [f013_id_depto] IdDepartamento ,
            [f013_id] IdCiudad ,
            [f013_descripcion] Ciudad ,
            d.f012_descripcion Departamento ,
            UPPER(p.f011_descripcion) Pais
        FROM [t013_mm_ciudades] c
        LEFT OUTER JOIN [dbo].[t012_mm_deptos] d ON c.f013_id_depto = d.f012_id
        AND c.[f013_id_pais] = d.f012_id_pais
        LEFT OUTER JOIN [dbo].[t011_mm_paises] P ON d.f012_id_pais = p.f011_id
        ORDER BY 1, 2, 3
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
        WITH CTE AS (
            SELECT ? AS CoddPais,
            ? AS CoddDepartamento,
            ? AS CodCiudad,
            ? AS Ciudad,
            ? AS Departamento,
            ? AS Pais,
            ? AS FecCreacion,
            ? AS FecModificacion
        )
        MERGE siesa.tbCiudades AS tgt
        USING CTE AS src
        ON tgt.CodCiudad = src.CodCiudad
            AND tgt.CoddDepartamento = src.CoddDepartamento
            AND tgt.CoddPais = src.CoddPais
        WHEN NOT MATCHED THEN
            INSERT (CoddPais, CoddDepartamento, CodCiudad, Ciudad,
            Departamento, Pais, FecCreacion, FecModificacion)
            VALUES (src.CoddPais, src.CoddDepartamento, src.CodCiudad, src.Ciudad, 
            src.Departamento, src.Pais, src.FecCreacion, src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.CoddPais = src.CoddPais,
            tgt.CoddDepartamento = src.CoddDepartamento,
            tgt.CodCiudad = src.CodCiudad,
            tgt.Ciudad = src.Ciudad,
            tgt.Departamento = src.Departamento,
            tgt.Pais = src.Pais,
            tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['IdPais', 'IdDepartamento', 'IdCiudad', 'Ciudad', 'Departamento', 'Pais', 'FecCreacion', 'FecModificacion']].values.tolist())
    except Exception as e:
        print("Error metodo cargue ciudades: ", e)

def cargue_clases_documento(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando clases documento")
    query = """
        SELECT C.f028_id IdClaseDocumento,
            C.f028_descripcion ClaseDocumento,
            C.f028_id_modulo Modulo,
            C.f028_prefijo_formato Formato,
            G.f034_id IdGrupoDocumento,
            G.f034_descripcion GrupoDocumento
        FROM T028_MM_CLASES_DOCUMENTO C
        INNER JOIN [dbo].[t034_mm_grupos_clases_docto] G ON G.f034_id = C.f028_id_grupo_clase_docto
        ORDER BY 1
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
        WITH CTE AS (
            SELECT ? AS IdClaseDocumento,
                ? AS ClaseDocumento,
                ? AS Modulo,
                ? AS Formato,
                ? AS IdGrupoDocumento,
                ? AS GrupoDocumento,
                ? AS FecCreacion,
                ? AS FecModificacion
        )
        MERGE siesa.tbClasesDocumento AS tgt
        USING CTE AS src
        ON tgt.IdClaseDocumento = src.IdClaseDocumento
        WHEN NOT MATCHED THEN
            INSERT (IdClaseDocumento, ClaseDocumento, Modulo, Formato,
            IdGrupoDocumento, GrupoDocumento, FecCreacion, FecModificacion)
            VALUES (src.IdClaseDocumento, src.ClaseDocumento, src.Modulo, src.Formato, 
            src.IdGrupoDocumento, src.GrupoDocumento, src.FecCreacion, src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.ClaseDocumento = src.ClaseDocumento,
            tgt.Modulo = src.Modulo,
            tgt.Formato = src.Formato,
            tgt.IdGrupoDocumento = src.IdGrupoDocumento,
            tgt.GrupoDocumento = src.GrupoDocumento,
            tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['IdClaseDocumento', 'ClaseDocumento', 'Modulo', 'Formato', 'IdGrupoDocumento', 'GrupoDocumento', 'FecCreacion', 'FecModificacion']].values.tolist())
    except Exception as e:
        print("Error metodo cargue clases documento: ", e)

# Ask for this method, in the package the slowly changing dimension isn't complete
def cargue_monedas(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando monedas")
    query = """
        SELECT [f017_id_cia] IdCompania ,
            [f017_id] IdMoneda ,
            [f017_descripcion] Descripcion ,
            [f017_simbolo] Simbolo
        FROM [UnoEE_Lloreda_Real].[dbo].[t017_mm_monedas]
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['CidCompania'] = df['IdCompania'].astype('int32')

    for index, row in df.iterrows():
        query = f"""
            WITH CTE AS (
                SELECT {row['IdClaseDocumento']} AS IdClaseDocumento,
                '{row['ClaseDocumento']}' AS ClaseDocumento,
                {row['Modulo']} AS Modulo,
                '{row['Formato']}' AS Formato,
                {row['IdGrupoDocumento']} AS IdGrupoDocumento,
                '{row['GrupoDocumento']}' AS GrupoDocumento,
                '{row['FecCreacion']}' AS FecCreacion,
                '{row['FecModificacion']}' AS FecModificacion
            )
            MERGE siesa.tbMonedas AS tgt
            USING CTE AS src
            ON tgt.IdClaseDocumento = src.IdClaseDocumento
            WHEN NOT MATCHED THEN
                INSERT (IdClaseDocumento, ClaseDocumento, Modulo, Formato,
                IdGrupoDocumento, GrupoDocumento, FecCreacion, FecModificacion)
                VALUES (src.IdClaseDocumento, src.ClaseDocumento, src.Modulo, src.Formato, 
                src.IdGrupoDocumento, src.GrupoDocumento, src.FecCreacion, src.FecModificacion)
            WHEN MATCHED THEN UPDATE SET
                tgt.ClaseDocumento = src.ClaseDocumento,
                tgt.Modulo = src.Modulo,
                tgt.Formato = src.Formato,
                tgt.IdGrupoDocumento = src.IdGrupoDocumento,
                tgt.GrupoDocumento = src.GrupoDocumento,
                tgt.FecModificacion = src.FecModificacion
        """
        try:
            lloreda_manager.begin_transaction()
            lloreda_manager.execute_query(query)
            lloreda_manager.commit_transaction()
        except Exception as e:
            print("Error metodo cargue monedas: ", e)
            lloreda_manager.rollback_transaction()

def cargue_tasa_cambio_real(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando tasa cambio real")
    query = """
        SELECT [f018_id_cia] IdCompania ,
            [f018_id_moneda] IdMoneda ,
            [f018_fecha] FechaTasa ,
            [f018_tasa] Tasa
        FROM [UnoEE_Lloreda_Real].[dbo].[t018_mm_tasas_cambio]
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['CidCompania'] = df['IdCompania'].astype('int32')

    query = f"""
        WITH CTE AS (
            SELECT ? AS IdCompania,
            ? AS IdMoneda,
            ? AS Fecha,
            ? AS tasa,
            ? AS FecCreacion,
            ? AS FecModificacion
        )
        MERGE siesa.tbTasaCambioReal AS tgt
        USING CTE AS src
        ON tgt.IdCompania = src.IdCompania
            AND tgt.IdMoneda = src.IdMoneda
        WHEN NOT MATCHED THEN
            INSERT (IdCompania, IdMoneda, Fecha, tasa,
            FecCreacion, FecModificacion)
            VALUES (src.IdCompania, src.IdMoneda, src.Fecha, src.tasa, 
            src.FecCreacion, src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.IdCompania = src.IdCompania,
            tgt.IdMoneda = src.IdMoneda,
            tgt.Fecha = src.Fecha,
            tgt.tasa = src.tasa,
            tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['CidCompania', 'IdMoneda', 'FechaTasa', 'Tasa', 'FecCreacion', 'FecModificacion']].values.tolist())
    except Exception as e:
        print("Error metodo cargue tasa cambio real: ", e)

# Ask for the reason why the slowly changing dimension is empty and pending to fix
def cargue_contratos_retirados(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando contratos retirados")
    query = """
        SELECT [c0550_id],
            t.f200_nit nitEmpleado,
            concat(f200_nombres, ' ', f200_apellido1, ' ', f200_apellido2) Nombres ,
            [c0550_id_cia] IdCompania ,
            [c0550_id_co] IdCentroOperacion ,
            [c0550_id_cargo] IdCargo ,
            [c0550_rowid_turno] RowidTurno ,
            [c0550_rowid_grupo_empleados] RowidGrupoEmpleados ,
            [c0550_rowid_ccosto] RowidCCosto ,
            [c0550_rowid_centros_trabajo] RowidCentroTrabajo ,
            c0504_id TipoNomina ,
            c0504_descripcion DescripcionTipoNomina ,
            [c0550_rowid_tiempo_basico] RowidTiempoBasico ,
            [c0550_fecha_ingreso] FechaIngreso ,
            [c0550_fecha_ingreso_ley50] FechaIngresoLey50 ,
            [c0550_fecha_retiro] FechaRetiro ,
            [c0550_fecha_contrato_hasta] FechaContratoHasta ,
            [c0550_fecha_prima_hasta] FechaPrimaHasta ,
            [c0550_fecha_vacaciones_hasta] FechaVacacionesHasta ,
            [c0550_fecha_ult_aumento] FechaUltimoAumento ,
            [c0550_fecha_ult_vacaciones] FechaUltVacaciones ,
            [c0550_fecha_ult_pension] FechaUltPension ,
            [c0550_fecha_ult_cesantias] FechaUltCesantias ,
            [c0550_salario] Salario ,
            [c0550_salario_anterior] SalarioAnterior ,
            [c0550_nro_personas_cargo] NroPersonasCargo ,
            [c0550_ind_regimen_laboral] IndRegimenLaboral ,
            [c0550_ind_auxilio_transporte] IndAuxilioTransporte ,
            [c0550_ind_pacto_colectivo] IndPactoColectivo ,
            [c0550_ind_subsidio] IndSubsidio ,
            [c0550_ind_salario_integral] IndSalarioIntegral ,
            [c0550_ind_ley789] IndLey789 ,
            [c0550_ind_tipo_cuenta] IndtipoCuenta ,
            [c0550_ind_clase_contrato] IndClaseContrato ,
            [c0550_ind_termino_contrato] IndTerminoContrato ,
            [c0550_ind_estado] IndEstado ,
            [c0550_rowid_cargo] RowidCargo ,
            [c0550_rowid_convencion] RowidConvencion ,
            [c0550_id_compensacion_flex] IdCompensacionFlex ,
            [c0550_ind_contra_temp] IndContrTemp ,
            [c0550_ind_tipo_salario] IndTipoSalario ,
            f200_fecha_nacimiento FechaNacimiento ,
            c0540_ind_sexo IndSexo ,
            CASE
                WHEN arp.c0517_id = '999' THEN 1
                ELSE 0
            END IndPensionado,
            1 Retirado
        FROM w0550_contratos c
        INNER JOIN t200_mm_terceros t ON t.f200_rowid = c.c0550_rowid_tercero
        INNER JOIN w0504_tipos_nomina tn ON c0504_rowid = c.c0550_rowid_tipo_nomina
        INNER JOIN w0540_empleados em ON em.c0540_rowid_tercero = t.f200_rowid
        INNER JOIN w0517_entidades_arp arp ON arp.c0517_rowid = c.c0550_rowid_entidad_arp
        WHERE [c0550_id_cia] not in(5)
        AND YEAR([c0550_fecha_retiro]) = YEAR(GETDATE())
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FecCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FecModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['Cidcompf'] = df['IdCompensacionFlex'].astype('int16')
    df['CCid'] = df['c0550_id'].astype('int16')
    df['cindreg'] = df['IndRegimenLaboral'].astype('int16')
    df['cindauxt'] = df['IndAuxilioTransporte'].astype('int16')
    df['cindpc'] = df['IndPactoColectivo'].astype('int16')
    df['cindsub'] = df['IndSubsidio'].astype('int16')
    df['cindsi'] = df['IndSalarioIntegral'].astype('int16')
    df['cindle7'] = df['IndLey789'].astype('int16')
    df['cintcuenta'] = df['IndtipoCuenta'].astype('int16')
    df['cindccont'] = df['IndClaseContrato'].astype('int16')
    df['cintcont'] = df['IndTerminoContrato'].astype('int16')
    df['cindes'] = df['IndEstado'].astype('int16')
    df['Crcar'] = df['RowidCargo'].astype('int16')
    df['Crconv'] = df['RowidConvencion'].astype('int16')
    df['Cindct'] = df['IndContrTemp'].astype('int16')
    df['Cindts'] = df['IndTipoSalario'].astype('int16')
    df['CIndSexo'] = df['IndSexo'].astype('int16')
    df['IndPensionado'] = df['IndPensionado'].astype('int16')
    df['Retirado'] = df['Retirado'].astype('int16')

    for index, row in df.iterrows():
        query = f"""
            WITH CTE AS (
                SELECT {row['CidCompania']} AS IdCompania,
                '{row['IdMoneda']}' AS IdMoneda,
                '{row['FechaTasa']}' AS Fecha,
                {row['Tasa']} AS tasa,
                '{row['FecCreacion']}' AS FecCreacion,
                '{row['FecModificacion']}' AS FecModificacion
            )
            MERGE siesa.tbTasaCambioReal AS tgt
            USING CTE AS src
            ON tgt.IdCompania = src.IdCompania
                AND tgt.IdMoneda = src.IdMoneda
            WHEN NOT MATCHED THEN
                INSERT (IdCompania, IdMoneda, Fecha, tasa,
                FecCreacion, FecModificacion)
                VALUES (src.IdCompania, src.IdMoneda, src.Fecha, src.tasa, 
                src.FecCreacion, src.FecModificacion)
            WHEN MATCHED THEN UPDATE SET
                tgt.IdCompania = src.IdCompania,
                tgt.IdMoneda = src.IdMoneda,
                tgt.Fecha = src.Fecha,
                tgt.tasa = src.tasa,
                tgt.FecModificacion = src.FecModificacion
        """
        try:
            lloreda_manager.begin_transaction()
            lloreda_manager.execute_query(query)
            lloreda_manager.commit_transaction()
        except Exception as e:
            print("Error metodo cargue tasa cambio real: ", e)
            lloreda_manager.rollback_transaction()

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
    #cargue_companias(lloreda_manager, siesa_manager)
    #cargue_regionales(lloreda_manager, siesa_manager)
    #cargue_tipos_documento(lloreda_manager, siesa_manager)
    #cargue_centros_operacion(lloreda_manager, siesa_manager)
    #cargue_centros_costo(lloreda_manager, siesa_manager)
    #cargue_plan_cuentas(lloreda_manager, siesa_manager)
    #cargue_auxiliar_contable(lloreda_manager, siesa_manager)
    #limpiar_tabla_cargos(lloreda_manager)
    #cargue_cargos(lloreda_manager, siesa_manager)
    #limpiar_tabla_cargos_centros(lloreda_manager)
    #cargue_cargos_centros(lloreda_manager, siesa_manager)
    #cargue_ciudades(lloreda_manager, siesa_manager)
    #cargue_clases_documento(lloreda_manager, siesa_manager)
    #cargue_tasa_cambio_real(lloreda_manager, siesa_manager)
    ############################

    lloreda_manager.disconnect()
    siesa_manager.disconnect()