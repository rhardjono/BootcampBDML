USE ODS;

##########################################################################################################################

INSERT INTO ODS_DM_METODOS_PAGO (DE_METODO_PAGO, FC_INSERT, FC_MODIFICACION)
SELECT DISTINCT UPPER(TRIM(BILL_METHOD)) METODO_PAGO, NOW(), NOW()
FROM STAGE.STG_FACTURAS_FCT
WHERE TRIM(BILL_METHOD)<>'';
# ---> 3 row(s) returned

COMMIT;

INSERT INTO ODS_DM_METODOS_PAGO VALUES (999, 'DESCONOCIDO', NOW(), NOW());
INSERT INTO ODS_DM_METODOS_PAGO VALUES (998, 'NO APLICA', NOW(), NOW());
COMMIT;

ANALYZE TABLE ODS_DM_METODOS_PAGO;

##########################################################################################################################

INSERT INTO ODS_DM_CICLOS_FACTURACION (DE_CICLO_FACTURACION, FC_INSERT, FC_MODIFICACION)
SELECT DISTINCT UPPER(TRIM(BILL_CYCLE)) CICLO_FACTURACION, NOW(), NOW()
FROM STAGE.STG_FACTURAS_FCT
WHERE TRIM(BILL_CYCLE)<>'';
# ---> 2 row(s) returned

COMMIT;

INSERT INTO ODS_DM_CICLOS_FACTURACION VALUES (999, 'DESCONOCIDO', NOW(), NOW());
INSERT INTO ODS_DM_CICLOS_FACTURACION VALUES (998, 'NO APLICA', NOW(), NOW());
COMMIT;

ANALYZE TABLE ODS_DM_CICLOS_FACTURACION;

##########################################################################################################################
