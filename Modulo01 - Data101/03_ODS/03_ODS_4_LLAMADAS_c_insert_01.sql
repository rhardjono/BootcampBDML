USE ODS;

##########################################################################################################################

INSERT INTO ODS_DM_AGENTES_CC (DE_AGENTE_CC, FC_INSERT, FC_MODIFICACION)
SELECT DISTINCT UPPER(TRIM(AGENT)) AGENTE, NOW(), NOW()
FROM STAGE.STG_CONTACTOS_IVR
WHERE TRIM(AGENT)<>''
ORDER BY AGENTE;
# ---> 593 row(s) returned

COMMIT;

INSERT INTO ODS_DM_AGENTES_CC VALUES (99999, 'DESCONOCIDO', NOW(), NOW());
INSERT INTO ODS_DM_AGENTES_CC VALUES (99998, 'NO APLICA', NOW(), NOW());
COMMIT;

ANALYZE TABLE ODS_DM_AGENTES_CC;

##########################################################################################################################

INSERT INTO ODS_DM_DEPARTAMENTOS_CC (DE_DEPARTAMENTO_CC, FC_INSERT, FC_MODIFICACION)
SELECT DISTINCT UPPER(TRIM(SERVICE)) DEPARTAMENTO_CC, NOW(), NOW()
FROM STAGE.STG_CONTACTOS_IVR
WHERE TRIM(SERVICE)<>'';
# ---> 6 row(s) returned

COMMIT;

INSERT INTO ODS_DM_DEPARTAMENTOS_CC VALUES (999, 'DESCONOCIDO', NOW(), NOW());
INSERT INTO ODS_DM_DEPARTAMENTOS_CC VALUES (998, 'NO APLICA', NOW(), NOW());
COMMIT;

ANALYZE TABLE ODS_DM_DEPARTAMENTOS_CC;

##########################################################################################################################