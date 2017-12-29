USE ODS;

ALTER TABLE ODS.ODS_HC_FACTURAS ADD INDEX fk_fac_cli_idx (ID_CLIENTE ASC);
ALTER TABLE ODS.ODS_HC_FACTURAS ADD CONSTRAINT fk_fac_cli FOREIGN KEY (ID_CLIENTE) 
	REFERENCES ODS.ODS_HC_CLIENTES (ID_CLIENTE);

ALTER TABLE ODS.ODS_HC_FACTURAS ADD INDEX fk_fac_metpag_idx (ID_METODO_PAGO ASC);
ALTER TABLE ODS.ODS_HC_FACTURAS ADD CONSTRAINT fk_fac_metpag FOREIGN KEY (ID_METODO_PAGO) 
	REFERENCES ODS.ODS_DM_METODOS_PAGO (ID_METODO_PAGO);

ALTER TABLE ODS.ODS_HC_FACTURAS ADD INDEX fk_fac_ciclofac_idx (ID_CICLO_FACTURACION ASC);
ALTER TABLE ODS.ODS_HC_FACTURAS ADD CONSTRAINT fk_fac_ciclofac FOREIGN KEY (ID_CICLO_FACTURACION) 
	REFERENCES ODS.ODS_DM_CICLOS_FACTURACION (ID_CICLO_FACTURACION);