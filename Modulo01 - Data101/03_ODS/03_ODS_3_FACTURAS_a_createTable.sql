USE ODS;

DROP TABLE IF EXISTS ODS_HC_FACTURAS;
CREATE TABLE ODS_HC_FACTURAS
(ID_FACTURA INT NOT NULL PRIMARY KEY
, ID_CLIENTE INT(11)
, FC_INICIO DATETIME
, FC_FIN DATETIME
, FC_ESTADO DATETIME
, FC_PAGO DATETIME
, ID_CICLO_FACTURACION INT(10) UNSIGNED
, ID_METODO_PAGO INT(10) UNSIGNED
, CANTIDAD INT(11)
, FC_INSERT DATETIME
, FC_MODIFICACION DATETIME
);

DROP TABLE IF EXISTS ODS_DM_METODOS_PAGO;
CREATE TABLE ODS_DM_METODOS_PAGO
(ID_METODO_PAGO INT UNSIGNED AUTO_INCREMENT PRIMARY KEY
, DE_METODO_PAGO VARCHAR(512)
, FC_INSERT DATETIME
, FC_MODIFICACION DATETIME
);

DROP TABLE IF EXISTS ODS_DM_CICLOS_FACTURACION;
CREATE TABLE ODS_DM_CICLOS_FACTURACION
(ID_CICLO_FACTURACION INT UNSIGNED AUTO_INCREMENT PRIMARY KEY
, DE_CICLO_FACTURACION VARCHAR(512)
, FC_INSERT DATETIME
, FC_MODIFICACION DATETIME
);
