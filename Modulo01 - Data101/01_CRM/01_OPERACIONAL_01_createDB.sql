CREATE DATABASE CRM;
CREATE DATABASE FACTURADOR;
CREATE DATABASE IVR;

/*
CREATE DATABASE CRM;
CREATE DATABASE FACTURADOR;
CREATE DATABASE IVR;
CREATE DATABASE STAGE;
CREATE DATABASE ODS;
CREATE DATABASE DDS;

CREATE DATABASE BASE_CRM;
CREATE DATABASE BASE_FACTURADOR;
CREATE DATABASE BASE_IVR;
CREATE DATABASE BASE_STAGE;
CREATE DATABASE BASE_ODS;
CREATE DATABASE BASE_DDS;

COMMIT;

#Para optimizar el cruce 
ALTER TABLE ODS.ODS_HC_CLIENTES ADD INDEX cli_id_idx (ID_CLIENTE ASC);
ALTER TABLE ODS.TMP_DIRECCIONES_CLIENTES2 ADD INDEX temdir_id_idx (ID_CLIENTE ASC);
ALTER TABLE STAGE.STG_PRODUCTOS_CRM ADD INDEX pro_id_idx (CUSTOMER_ID ASC);
COMMIT;

*/