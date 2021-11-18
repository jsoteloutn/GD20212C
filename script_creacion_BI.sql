/*******************
*** BASE DE DATOS **
********************/
USE GD2C2021;
GO

/************************
*** CREACIÓN DE TABLAS **
*************************/
CREATE TABLE MONKEY_D_BASE.BI_Rango_Edad (
	id					INT IDENTITY PRIMARY KEY NOT NULL,
	edad_ini			INT,
	edad_fin			INT);	

CREATE TABLE MONKEY_D_BASE.BI_Tiempo (
	fecha				DATETIME2 PRIMARY KEY NOT NULL,
	dia					INT NOT NULL,
	mes					INT NOT NULL,
	cuarto				CHAR(2) NOT NULL,
	anio				INT NOT NULL);

/********************
*** CREACION SP ***** 
*********************/
--Este procedure migrara los datos a las dimensiones correspondientes
		DECLARE @tabla nvarchar(100);
--Tiempo
		SET @tabla = 'Tiempo';

		INSERT INTO MONKEY_D_BASE.BI_Tiempo (
					fecha,
					dia,
					mes,
					anio,
					cuarto)
		SELECT	
		CONVERT(DATETIME2,tabla.VIAJE_FECHA_INICIO) fecha,
		DAY(tabla.VIAJE_FECHA_INICIO) dia,
		MONTH(tabla.VIAJE_FECHA_INICIO) mes,
		YEAR(tabla.VIAJE_FECHA_INICIO) anio,
		(CASE 	WHEN	 MONTH(tabla.VIAJE_FECHA_INICIO) = 1 OR MONTH(tabla.VIAJE_FECHA_INICIO) = 2 
					  OR MONTH(tabla.VIAJE_FECHA_INICIO) = 3 OR MONTH(tabla.VIAJE_FECHA_INICIO) = 4 THEN '1Q'
				WHEN     MONTH(tabla.VIAJE_FECHA_INICIO) = 5 OR MONTH(tabla.VIAJE_FECHA_INICIO) = 6 
					  OR MONTH(tabla.VIAJE_FECHA_INICIO) = 7 OR MONTH(tabla.VIAJE_FECHA_INICIO) = 8 THEN '2Q'
				WHEN     MONTH(tabla.VIAJE_FECHA_INICIO) = 9 OR MONTH(tabla.VIAJE_FECHA_INICIO) = 10 
					  OR MONTH(tabla.VIAJE_FECHA_INICIO) = 11 OR MONTH(tabla.VIAJE_FECHA_INICIO) = 12  THEN '3Q'
				END) as cuarto
		FROM 
		(SELECT DISTINCT m.VIAJE_FECHA_INICIO FROM gd_esquema.Maestra m 
		WHERE m.VIAJE_FECHA_INICIO is not null
		--ORDER BY 1
		UNION
		SELECT DISTINCT m.VIAJE_FECHA_FIN
		FROM gd_esquema.Maestra m
		WHERE m.VIAJE_FECHA_FIN is not null
		--order by 1
		) as tabla order by tabla.VIAJE_FECHA_INICIO

		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

--Rango_Edad
		SET @tabla = 'Rango_Edad';

		INSERT INTO MONKEY_D_BASE.BI_Rango_Edad(
					edad_ini,
					edad_fin
					)
		VALUES (18,30);

		INSERT INTO MONKEY_D_BASE.BI_Rango_Edad(
					edad_ini,
					edad_fin
					)
		VALUES (31,50);

		INSERT INTO MONKEY_D_BASE.BI_Rango_Edad(
					edad_ini
					)
		VALUES (50);
		
	EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

/************************
*** CREACIÓN DE VISTAS **
*************************/
--Desvio Promedio x Tarea x Taller
CREATE VIEW MONKEY_D_BASE.BI_VW_Desvio_Promedio_x_Tarea_x_Taller AS
SELECT o.taller_id,ot.tarea_id,AVG(DATEDIFF(day,ot.fecha_planificada,ot.fecha_ini_real)) as desvio_promedio FROM MONKEY_D_BASE.Orden_Tarea ot
JOIN MONKEY_D_BASE.Orden_Trabajo o ON o.id = ot.orden_id
GROUP BY o.taller_id,ot.tarea_id;


--Costo Promedio x rango etario de choferes
CREATE VIEW MONKEY_D_BASE.BI_VW_Costo_Promedio_x_RangoEtario AS
SELECT re.id,re.edad_ini,re.edad_fin,avg(e.costo_hora) as costo_promedio
FROM MONKEY_D_BASE.Empleado e
JOIN MONKEY_D_BASE.Viaje v ON v.chofer_legajo = e.legajo
JOIN MONKEY_D_BASE.BI_Rango_Edad re ON DATEDIFF(YEAR,e.fecha_nacimiento,v.fecha_fin) BETWEEN re.edad_ini AND re.edad_fin 
WHERE e.tipo_id = 1
GROUP BY re.id,re.edad_ini,re.edad_fin;
