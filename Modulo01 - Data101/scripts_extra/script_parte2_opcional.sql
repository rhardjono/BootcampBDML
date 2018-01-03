
# Separar el campo DE_DIRECCION de la tabla de ODS.ODS_HC_DIRECCIONES en dos campos: NOMBRE_VIA y NUM_VIA
# Para realizar esta operacion hemos comprobado que el campo DE_DIRECCION tiene un formato comun consistente en:
# numero_via(espacio)nombre_via
# Se utiliza la función SUBSTRING_INDEX() para extraer las dos partes que componen la dirección.

SELECT 
SUBSTRING_INDEX(DE_DIRECCION, ' ', -1) NOMBRE_VIA,
SUBSTRING_INDEX(DE_DIRECCION, ' ', 1) NUM_VIA
FROM ODS.ODS_HC_DIRECCIONES 
WHERE ID_DIRECCION NOT IN (999999, 999998)
ORDER BY NOMBRE_VIA; 

###########################################################################################################

#OPCION MENOS OPTIMA
/*
SELECT 
CASE 
	WHEN ID_DIRECCION=999999 THEN 'DESCONOCIDO'
    WHEN ID_DIRECCION=999998 THEN 'NO APLICA'
    ELSE SUBSTRING_INDEX(DE_DIRECCION, ' ', -1) END NOMBRE_VIA,
CASE 
	WHEN ID_DIRECCION=999999 THEN 'DESCONOCIDO'
    WHEN ID_DIRECCION=999998 THEN 'NO APLICA'
    ELSE SUBSTRING_INDEX(DE_DIRECCION, ' ', 1) END NUM_VIA
FROM ODS.ODS_HC_DIRECCIONES 
ORDER BY NUM_VIA DESC;
*/