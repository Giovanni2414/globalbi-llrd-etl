from clientes import ejecutar_clientes
from datosLloreda import ejecutar_datos_lloreda
from factExistencialInventario import ejecutar_fact_existencia
from items import ejecutar_items
from maestras import ejecutar_maestras
from nomina import ejecutar_nomina

if __name__ == "__main__":
    ejecutar_maestras()
    ejecutar_clientes()
    ejecutar_items()
    ejecutar_datos_lloreda()
    ejecutar_fact_existencia()
    ejecutar_nomina()