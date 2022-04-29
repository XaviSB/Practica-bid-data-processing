STREAMING PART: 
 
Leemos los datos del topic de Kafka.
 
Parseamos los datos del JSON de Kafka.
 
Leemos los datos de la tabla que hemos creado en Postgres con el JdbcProvisioner.
 
Hacemos Join de las dos tablas.
 
 
Calculamos en una ventana de 5 minutos: 
•	Total, de bytes recibidos por antena.
•	Total, de bytes transmitidos por id de usuario.
•	Total, de bytes transmitidos por aplicación
 
Formato de escritura al Postgres.


 
Escribimos en local, en formato parquet, los datos, particionados por año, mes, día y hora. 
 
Imprimimos por consola los counts de antena, usuarios y app.
 
Enviamos a postgres los datos que hemos calculado (y los almacenamos en la tabla “bytes” que hemos creado previamente) y a storage los datos en formato parquet. 

BATCH PART: 
 
Leemos/recuperamos los datos que hemos almacenado en el storage en formato parquet. 
 
Igual que en Streaming
 
 
Igual que en Streaming pero en lugar del usuario, en el segundo lo hacemos con el email.
 
Calculamos el total de bytes que se han consumido por email de usuario, y reflejamos los que han sobrepasado la cuota por hora.
 
Similar a Streaming.
 
Enviamos a Postgres los datos de los métodos/cálculos que hemos realizado en batch, y los almacenamos en sus respectivas tablas ("bytes_hourly" y "user_quota_limit")





