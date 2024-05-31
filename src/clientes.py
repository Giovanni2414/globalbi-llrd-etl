import json
import pandas as pd
from datetime import datetime
from utils.database_manager import DatabaseManager
import warnings

def cargue_clientes(lloreda_manager: DatabaseManager, siesa_manager: DatabaseManager):
    print("Cargando clientes")
    query = """
        SELECT f201_id_cia IdCompania ,
            f201_rowid_tercero IdCliente ,
            f200_nit NitCliente ,
            f200_dv_nit DvNitCliente ,
            f200_id_tipo_ident IdTipoIdentidad ,
            f200_ind_tipo_tercero IdTipoTercero ,
            f200_razon_social RazonSocial ,
            f200_apellido1 Apellido1 ,
            f200_apellido2 Apellido2 ,
            f200_nombres Nombres ,
            f201_id_sucursal IdSucursal ,
            f201_descripcion_sucursal DescripcionSucursal ,
            f200_nombre_est NombreEstado ,
            f201_id_vendedor IdVendedor ,
            f201_id_lista_precio IdListaPrecio ,
            f201_fecha_ingreso FechaIngreso ,
            f201_ind_estado_activo IndEstadoActivo ,
            f201_rowid_tercero_corp RowidTerceroCorporativo ,
            f201_id_sucursal_corp IdSucursalCorporativo ,
            f201_rowid_tercero RowidTercero
        FROM dbo.t201_mm_clientes
        INNER JOIN t200_mm_terceros ON f200_rowid = f201_rowid_tercero
    """
    df = siesa_manager.execute_query_get_pandas(query)
    df['FechaCreacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['FechaModificacion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['NIDCOMPANIA'] = df['IdCompania'].astype('int32')
    df['NIDTIPOTERCERO'] = df['IdTipoTercero'].astype('int32')
    df['NINDESTADOACTIVO'] = df['IndEstadoActivo'].astype('int32')
    columns_to_replace_nan = ['RowidTercero','RowidTerceroCorporativo']
    df[columns_to_replace_nan] = df[columns_to_replace_nan].astype('object')
    df[columns_to_replace_nan] = df[columns_to_replace_nan].where(pd.notnull(df[columns_to_replace_nan]), None)
    query = f"""
        WITH CTE AS (
            SELECT ? AS IdCliente,
                ? AS IdCompania,
                ? AS IdSucursal,
                ? AS NitCliente,
                ? AS DvNitCliente,
                ? AS IdTipoIdentidad,
                ? AS IndTipoTercero,
                ? AS RazonSocial,
                ? AS Apellido1,
                ? AS Apellido2,
                ? AS Nombres,
                ? AS DescripcionSucursal,
                ? AS NombreEstado,
                ? AS IdVendedor,
                ? AS IdListaPrecios,
                ? AS FechaIngreso,
                ? AS IndEstadoActivo,
                ? AS RowidTercero_corp,
                ? AS IdSucursal_corp,
                ? AS RowidTercero,
                ? AS FecCreacion,
                ? AS FecModificacion
        )
        MERGE siesa.tbClientes AS tgt
        USING CTE AS src
        ON tgt.IdCliente = src.IdCliente
            AND tgt.IdCompania = src.IdCompania 
            AND tgt.IdSucursal = src.IdSucursal
        WHEN NOT MATCHED THEN
            INSERT (IdCliente, IdCompania, IdSucursal, NitCliente, DvNitCliente, IdTipoIdentidad,
            IndTipoTercero, RazonSocial, Apellido1, Apellido2, Nombres, DescripcionSucursal,
            NombreEstado, IdVendedor, IdListaPrecios, FechaIngreso, IndEstadoActivo,
            RowidTercero_corp, IdSucursal_corp, RowidTercero, FecCreacion, FecModificacion)
            VALUES (src.IdCliente, src.IdCompania, src.IdSucursal, src.NitCliente, 
            src.DvNitCliente, src.IdTipoIdentidad, src.IndTipoTercero, src.RazonSocial, 
            src.Apellido1, src.Apellido2, src.Nombres, src.DescripcionSucursal, 
            src.NombreEstado, src.IdVendedor, src.IdListaPrecios, src.FechaIngreso, 
            src.IndEstadoActivo, src.RowidTercero_corp, src.IdSucursal_corp, src.RowidTercero, 
            src.FecCreacion, src.FecModificacion)
        WHEN MATCHED THEN UPDATE SET
            tgt.NitCliente = src.NitCliente,
            tgt.DvNitCliente = src.DvNitCliente,
            tgt.IdTipoIdentidad = src.IdTipoIdentidad,
            tgt.IndTipoTercero = src.IndTipoTercero,
            tgt.RazonSocial = src.RazonSocial,
            tgt.Apellido1 = src.Apellido1,
            tgt.Apellido2 = src.Apellido2,
            tgt.Nombres = src.Nombres,
            tgt.DescripcionSucursal = src.DescripcionSucursal,
            tgt.NombreEstado = src.NombreEstado,
            tgt.IdVendedor = src.IdVendedor,
            tgt.IdListaPrecios = src.IdListaPrecios,
            tgt.FechaIngreso = src.FechaIngreso,
            tgt.IndEstadoActivo = src.IndEstadoActivo,
            tgt.RowidTercero_corp = src.RowidTercero_corp,
            tgt.IdSucursal_corp = src.IdSucursal_corp,
            tgt.RowidTercero = src.RowidTercero,
            tgt.FecModificacion = src.FecModificacion;
    """
    try:
        lloreda_manager.execute_bulk_insert(query, df[['IdCliente','NIDCOMPANIA','IdSucursal','NitCliente',
                                                       'DvNitCliente','IdTipoIdentidad','NIDTIPOTERCERO',
                                                       'RazonSocial','Apellido1','Apellido2','Nombres',
                                                       'DescripcionSucursal','NombreEstado','IdVendedor',
                                                       'IdListaPrecio','FechaIngreso','NINDESTADOACTIVO',
                                                       'RowidTerceroCorporativo','IdSucursalCorporativo',
                                                       'RowidTercero','FechaCreacion','FechaModificacion']].values.tolist())
    except Exception as e:
        print("Error metodo cargue clientes: ", e)

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
    warnings.simplefilter(action='ignore', category=FutureWarning)

    # Operaciones principales
    #cargue_clientes(lloreda_manager, siesa_manager)
    ############################

    lloreda_manager.disconnect()
    siesa_manager.disconnect()