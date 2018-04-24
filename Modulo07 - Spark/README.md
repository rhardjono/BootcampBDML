# Módulo 7 - Spark & Scala

Práctica Final de Spark & Scala
===============================

Este proyecto consiste en el desarrollo de una pseudo arquitectura lambda en el que se ha implementado la capa batch y la capa speed(streaming) y se ha dejado  la capa servicie como pendiente.

A continuación se detallan algunos de los aspectos y funcionalidades más importantes a destacar:
En la clase Metrics se recoge el cálculo de las métricas (KPIs) mediante consultas SQL (no se ha utilizado el API Dataframe) y las funciones de escriturá y lectura en los principales formatos de fichero. También ofrece ciertas funciones para enriquecer el dato durante el proceso de paseo:

...Determinar la categoría a la que pertenece una transacción a partir de su descripción bajo criterio personal y manual.

...Determinar el país en el que se ha efectuado la transacción a partir de sus coordenadas geográficas: basado eln el proyecto Geocoder de KodiDev (https://github.com/KoddiDev/geocoder/blob/master/README.md) que crea un cliente para la APIde Google Maps.

**Nota:** el uso de de este cliente requiere de una clave personal de Google que estará activa hasta el final de la práctica.

* CAPA BATCH
Están las clases MetricsSQL y TestBatchSQL. El primero corresponde al Core y el segundo es una prueba para definir otro modelo de datos(incompleto)

* CAPA STREAMING
Están las clases MetricsStreaming y MetricsStructuredStreaming. El primero corresponde al Core y el.segundo es una prueba para trabajar con Structured Streaming (incompleto)

Generador Kafka aleatorio de transacciones: basado en el proyecto de @fjpiqueras. Básicamente solo se ha modificado el formato del mensaje a generar.

**Nota:** las coordenadas generadas aleatoriamente no son válidas para poder realizar las peticiones de geolocalizacion inversa mediante el Geocoder. (Se obtienen respuestas 400).
Se recomienda deshabilitar el Uso de Geocoder si se quiere trabajar con los datos aleatorios de transacciones.
