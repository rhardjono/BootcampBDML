USE ODS; 

##########################################################################################################################
# Al analizar la tabla STG_CONTACTOS_IVR observamos que hay llamdas referencias a numeros de telefono de clientes que no 
# existen, es decir, no estan dados de alta en la tabla ODS_HC_CLIENTES (STG_CLIENTES_CRM <= ODS_HC_CLIENTES)
# En la siguiente query se muestra el listado de id_clientes inexistentes:
#
#SELECT DISTINCT
#TRIM(LLAMADAS.PHONE_NUMBER) TELEFONO
#FROM STAGE.STG_CONTACTOS_IVR LLAMADAS
#LEFT JOIN ODS.ODS_HC_CLIENTES CLI ON CAST(LLAMADAS.PHONE_NUMBER AS UNSIGNED INT) = CLI.TELEFONO_CLIENTE
#WHERE TELEFONO_CLIENTE IS NULL 
#;
# ---> INF row(s) returned

# Buscamos algunos de los clientes inexistentes por Telefono tanto en la tabla ODS_HC_CLIENTES:
#
#SELECT * FROM ODS.ODS_HC_CLIENTES
#WHERE TELEFONO_CLIENTE IN (4075503542, 9722617992, 4021945400,2087246547)
#;
# ---> 0 row(s) returned
#






##########################################################################################################################

#========================================================================================================================#
# Insertar datos nuevos de clientes en ODS_HC_CLIENTES a partir de la tabla STG_CONTACTOS_IVR
#========================================================================================================================#
#========================================================================================================================#
# NO APLICA
#========================================================================================================================#
/*
INSERT INTO ODS_HC_CLIENTES
# TODO: COMPLETAR - NO ES NECESARIO??
FROM STAGE.STG_CONTACTOS_IVR LLAMADAS
COMMIT;
ANALYZE TABLE ODS_HC_CLIENTES;
*/