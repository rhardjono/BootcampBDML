USE ODS;

#ALTER TABLE ODS.ODS_HC_CLIENTES MODIFY COLUMN ID_SEXO INT(10) UNSIGNED;
ALTER TABLE ODS.ODS_HC_CLIENTES ADD INDEX fk_cli_sexo_idx (ID_SEXO ASC);
ALTER TABLE ODS.ODS_HC_CLIENTES ADD CONSTRAINT fk_cli_sexo FOREIGN KEY (ID_SEXO) 
	REFERENCES ODS.ODS_DM_SEXOS (ID_SEXO);

#ALTER TABLE ODS.ODS_HC_CLIENTES MODIFY COLUMN ID_PROFESION INT(10) UNSIGNED;
ALTER TABLE ODS.ODS_HC_CLIENTES ADD INDEX fk_cli_prof_idx (ID_PROFESION ASC);
ALTER TABLE ODS.ODS_HC_CLIENTES ADD CONSTRAINT fk_cli_prof FOREIGN KEY (ID_PROFESION) 
	REFERENCES ODS.ODS_DM_PROFESIONES (ID_PROFESION);

#ALTER TABLE ODS.ODS_HC_CLIENTES MODIFY COLUMN ID_COMPANYA INT(10) UNSIGNED;
ALTER TABLE ODS.ODS_HC_CLIENTES ADD INDEX fk_cli_comp_idx (ID_DIRECCION_CLIENTE ASC);
ALTER TABLE ODS.ODS_HC_CLIENTES ADD CONSTRAINT fk_cli_comp FOREIGN KEY (ID_COMPANYA) 
	REFERENCES ODS.ODS_DM_COMPANYAS (ID_COMPANYA);

#ALTER TABLE ODS.ODS_HC_CLIENTES MODIFY COLUMN ID_DIRECCION_CLIENTE INT(10) UNSIGNED;
ALTER TABLE ODS.ODS_HC_CLIENTES ADD INDEX fk_cli_dir_idx (ID_DIRECCION_CLIENTE ASC);
ALTER TABLE ODS.ODS_HC_CLIENTES ADD CONSTRAINT fk_cli_dir FOREIGN KEY (ID_DIRECCION_CLIENTE) 
	REFERENCES ODS.ODS_HC_DIRECCIONES (ID_DIRECCION);

#ALTER TABLE ODS.ODS_DM_CIUDADES_ESTADOS MODIFY COLUMN ID_PAIS INT(10) UNSIGNED;
ALTER TABLE ODS.ODS_DM_CIUDADES_ESTADOS ADD INDEX fk_ciu_pai_idx (ID_PAIS ASC);
ALTER TABLE ODS.ODS_DM_CIUDADES_ESTADOS ADD CONSTRAINT fk_ciu_pai FOREIGN KEY (ID_PAIS) 
	REFERENCES ODS.ODS_DM_PAISES (ID_PAIS);

#ALTER TABLE ODS.ODS_HC_DIRECCIONES MODIFY COLUMN ID_CIUDAD_ESTADO INT(10) UNSIGNED;
ALTER TABLE ODS.ODS_HC_DIRECCIONES ADD INDEX fk_dir_ciu_idx (ID_CIUDAD_ESTADO ASC);
ALTER TABLE ODS.ODS_HC_DIRECCIONES ADD CONSTRAINT fk_dir_ciu FOREIGN KEY (ID_CIUDAD_ESTADO) 
	REFERENCES ODS.ODS_DM_CIUDADES_ESTADOS (ID_CIUDAD_ESTADO);

#Para optimizar el cruce entre ODS.ODS_HC_CLIENTES y STAGE.STG_CONTACTOS_IVR
ALTER TABLE ODS.ODS_HC_CLIENTES ADD INDEX cli_tel_idx (TELEFONO_CLIENTE ASC);
