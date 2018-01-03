
# Para establecer en el modelo de Direcciones (ODS_HC_DIRECCIONES) una relacion de jerarquia: PAIS --> ESTADO --> CIUDAD --> DIRECCION
# debe existir una relacion 1 a 1 entre la entidad hijo con respecto a la entidad padre, por ejemplo debe suceder que una Ciudad pertenezca a un solo Estado y que un Estado pertenezca a su vez a un solo Pais.

# Sin embargo, tras analizar los campos City y State de la tabla STAGE.STG_CLIENTES_CRM se ha observado la existencia de una Ciudad que pertenece a dos Estados diferentes. De este modo si consultasemos o filtrasemos
# por dicha ciudad obtendriamos dos valores de Estados distintos.

# No se puede establecer una relación de jerarquía entre Estado y Ciudad porque no hay una relación 1 a 1 entre la entidad hija con respecto a la entidad padre, en concreto existe una Ciudad que pertenece a dos Estados diferentes.

# Esto lo podemos comprobar con la siguiente query que devuelve en numero de estados al que pertenece una ciudad:
SELECT DISTINCT CITY, COUNT(*) TOTAL_STATES FROM (
	SELECT DISTINCT CITY, STATE FROM STAGE.STG_CLIENTES_CRM GROUP BY CITY, STATE HAVING CITY<>'' OR STATE<>'') CITIES_STATES
GROUP BY CITY
ORDER BY TOTAL_STATES DESC;
# CITY, TOTAL_STATES
# Glendale, 2
# Henderson, 1
# Oakland, 1
# Sacramento, 1
# Simi Valley, 1
# Brea, 1
# Huntington Beach, 1
# Oceanside, 1
# Salinas, 1
# South Lake Tahoe, 1

# Para el modelo de Direcciones se ha planteado una tabla auxiliar (ODS_DM_CIUDADES_ESTADOS) que relaciona la Ciudad y el Estado generando un nuevo campo identificados/clave.
# Esto seria equivalente a crear las tablas de dimensiones de Ciudad y Estado y luego definir una tabla intermedia de Ciudades_Estados pero esto se puede considerar menos óptimo o eficiente pues habría que mantener 3 tablas.

#COMENTARIO:
/*
Cabe indicar que dentro de esa jerarquía de Direcciones podemos encontrarnos también que existen direcciones(descripción con nombre y número) que pertenecen a una misma ciudad-estado...
Puede ser una opción alternativa de diseño, definir las dimensiones de Pais, Estado, Ciudad y CodigoPostal para luego a partir de ellas crear el modelo de Direcciones puesto que tendremos un campo PK autoincremental o como alternativa tener una PK compuesta.
Aunque hay que tener un conocimiento del modelo de negocio... este planteamiento puede cubrir situaciones futuras en el que tuviesemos por ejemplo un mismo estado para diferentes paises.
Por tanto, en el modelo de Dirección no 'importaría' que hubiese una misma Ciudad para varios Estados o un Estado para varios Paises puesto una Dirección quedaría identificado de forma univoca por estos campos
*/

#EJEMPLOS:
/*
SELECT DE_DIRECCION, COUNT(*) TOTAL FROM ODS.ODS_HC_DIRECCIONES GROUP BY DE_DIRECCION ORDER BY TOTAL DESC;
SELECT * FROM ODS.ODS_HC_DIRECCIONES WHERE DE_DIRECCION LIKE '9 TOBAN WAY';

SELECT DE_DIRECCION, ID_CIUDAD_ESTADO, COUNT(*) TOTAL FROM ODS.ODS_HC_DIRECCIONES GROUP BY DE_DIRECCION, ID_CIUDAD_ESTADO ORDER BY TOTAL DESC;
SELECT * FROM ODS.ODS_HC_DIRECCIONES WHERE DE_DIRECCION LIKE '15 KENNEDY PARK';
*/