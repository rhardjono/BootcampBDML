USE ODS; 

##########################################################################################################################
# Al analizar la tabla STG_PRODUCTOS_CRM observamos que hay servicios referenciados a id_cliente que no existen, es decir,
# no estan dados de alta en la tabla ODS_HC_CLIENTES (STG_CLIENTES_CRM <= ODS_HC_CLIENTES)
# En la siguiente query se muestra el listado de id_clientes inexistentes:
#
#SELECT DISTINCT
#TRIM(PRO.CUSTOMER_ID) ID_CLIENTE
#FROM STAGE.STG_PRODUCTOS_CRM PRO
#LEFT JOIN ODS.ODS_HC_CLIENTES CLI ON PRO.CUSTOMER_ID = CLI.ID_CLIENTE
#WHERE ID_CLIENTE IS NULL 
#;
# ---> 3 row(s) returned

# Buscamos algunos de los clientes inexistentes por ID tanto en la tabla ODS_HC_CLIENTES como en la tablaSTAGE.STG_CLIENTES_CRM:
#
#SELECT * FROM ODS.ODS_HC_CLIENTES
#WHERE ID_CLIENTE IN (10000, 14826, 16689)
#;
# ---> 0 row(s) returned
#
#SELECT * FROM STAGE.STG_CLIENTES_CRM
#WHERE CUSTOMER_ID IN (10000, 14826, 16689)
#;
# ---> 0 row(s) returned


##########################################################################################################################

#========================================================================================================================#
# Insertar datos nuevos de clientes en ODS_HC_CLIENTES a partir de la tabla STG_PRODUCTOS_CRM
#========================================================================================================================#
#========================================================================================================================#
# NO APLICA
#========================================================================================================================#
/*
INSERT INTO ODS_HC_CLIENTES
# TODO:
FROM STAGE.STG_PRODUCTOS_CRM PRODUCTOS
COMMIT;
ANALYZE TABLE ODS_HC_CLIENTES;
*/