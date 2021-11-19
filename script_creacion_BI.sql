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

CREATE TABLE MONKEY_D_BASE.BI_Promedio_x_Tarea_x_Taller (
	taller_id				INT NOT NULL,
	tarea_id				INT NOT NULL,
	tarea_tiempo_estimado	INT NOT NULL,
	promedio				decimal(12,2));

CREATE TABLE MONKEY_D_BASE.BI_Tareas_mas_realizadas_x_Modelo_Camion (
	modelo_id			INT NOT NULL,
	modelo_desc			nvarchar(255) NOT NULL,
	tarea_id			INT NOT NULL,
	cantidad			INT NOT NULL);

CREATE TABLE MONKEY_D_BASE.BI_Ingresos_por_camion (
	camion_id			INT NOT NULL,
	ingresos			decimal(18,2));

CREATE TABLE MONKEY_D_BASE.BI_Costo_viaje (
	camion_id			INT NOT NULL,
	costo				decimal(18,2));

CREATE TABLE MONKEY_D_BASE.BI_camion_servicio(
	camion_id int NOT NULL,
	cuarto CHAR(2) NOT NULL,
	tiempo_sin_servicio int NOT NULL,
	CONSTRAINT PK_BI_camion_servicio PRIMARY KEY (camion_id, cuarto)
)

CREATE TABLE MONKEY_D_BASE.BI_camion_costo(
    camion_id int NOT NULL,
    cuarto char(2) NOT NULL,
    taller_id int NOT NULL,
    costo_total DECIMAL(18,2) NOT NULL
    CONSTRAINT PK_BI_camion_costo PRIMARY KEY (camion_id, cuarto, taller_id)
)

CREATE TABLE MONKEY_D_BASE.BI_costo_mantenimiento(
    camion_id    int NOT NULL,
    costo_total decimal(38, 2) NULL
)
GO

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

	--Promedio x Tarea x Taller
	INSERT INTO MONKEY_D_BASE.BI_Promedio_x_Tarea_x_Taller (taller_id,tarea_id,tarea_tiempo_estimado,promedio)
	SELECT o.taller_id,ot.tarea_id,t.tiempo_estimado,AVG(DATEDIFF(day,ot.fecha_ini_real,ot.fecha_fin_real)) as promedio FROM MONKEY_D_BASE.Orden_Tarea ot
	JOIN MONKEY_D_BASE.Orden_Trabajo o ON o.id = ot.orden_id
	JOIN MONKEY_D_BASE.Tarea t ON t.id = ot.tarea_id
	GROUP BY o.taller_id,ot.tarea_id,t.tiempo_estimado
	ORDER BY 1,2;

	--Las 5 tareas que más se realizan por modelo de camión
	INSERT INTO MONKEY_D_BASE.BI_Tareas_mas_realizadas_x_Modelo_Camion (modelo_id,modelo_desc,tarea_id,cantidad)
	SELECT cm.id,cm.descripcion,t.id as tarea,COUNT(t.id) as cantidad
	FROM MONKEY_D_BASE.Tarea t
	JOIN MONKEY_D_BASE.Orden_Tarea ot ON t.id = ot.tarea_id
	JOIN MONKEY_D_BASE.Orden_Trabajo o ON o.id = ot.orden_id
	JOIN MONKEY_D_BASE.Camion c ON c.id = o.camion_id
	JOIN MONKEY_D_BASE.Camion_Modelo cm ON c.modelo_id = cm.id
	GROUP BY cm.id,cm.descripcion,t.id;

	--Máximo tiempo fuera de servicio de cada camión por cuatrimestre
	SELECT 
     ot.camion_id,
     ot.id,
     t.cuarto,
     DATEDIFF(DAY, MIN(ott.fecha_ini_real), MAX(ott.fecha_fin_real)) tiempo_sin_servicio
	INTO #tmp
	FROM 
		MONKEY_D_BASE.Orden_trabajo ot,
		MONKEY_D_BASE.Orden_tarea ott,
		MONKEY_D_BASE.BI_Tiempo t
	WHERE ot.id = ott.orden_id
	AND ott.fecha_fin_real = t.fecha
	GROUP BY    ot.camion_id,
				ot.id,
				t.cuarto

	INSERT INTO MONKEY_D_BASE.BI_camion_servicio (camion_id, cuarto, tiempo_sin_servicio)
	SELECT 
		camion_id,
		cuarto,
		MAX(tiempo_sin_servicio) tiempo_sin_servicio
	FROM 
		#tmp
	GROUP BY camion_id,
			 cuarto

	DROP TABLE #tmp
	GO

	--Costo total de mantenimiento por camión, por taller, por cuatrimestre
	SELECT 
     ot.camion_id,
     ot.taller_id,
     t.cuarto,
     DATEDIFF(DAY, ott.fecha_ini_real, ott.fecha_fin_real) * 8 * e.costo_hora empleadoTotaCosto,
     ISNULL((SELECT SUM(m.precio * tm.material_cantidad) empleadoCostoTotal
                FROM    MONKEY_D_BASE.Tarea_material tm, 
                        MONKEY_D_BASE.Material m 
                WHERE ott.tarea_id = tm.tarea_id 
                AND tm.material_id = m.id),0) precioTotaTarea
INTO #tmp
FROM 
    MONKEY_D_BASE.Orden_trabajo ot,
    MONKEY_D_BASE.Orden_tarea ott,
    MONKEY_D_BASE.BI_Tiempo t,
    MONKEY_D_BASE.Empleado e
WHERE ot.id = ott.orden_id
AND ott.fecha_fin_real = t.fecha
AND ott.mecanico_legajo = e.legajo

INSERT INTO MONKEY_D_BASE.BI_camion_costo (camion_id, taller_id, cuarto, costo_total)
SELECT 
    camion_id,
    taller_id,
    cuarto,
    SUM(empleadoTotaCosto + precioTotaTarea)
FROM 
    #tmp
GROUP BY 
    camion_id,
    taller_id,
    cuarto

DROP TABLE #tmp
GO

--Ganancia por camión
--Ingresos
INSERT INTO MONKEY_D_BASE.BI_Ingresos_por_camion (camion_id,ingresos)
select 
v.camion_codigo,
 sum(vp.paquete_cantidad * vp.paquete_precio_hist) + SUM(v.precio_recorrido_his) as [Ingresos] 
from MONKEY_D_BASE.Viaje v
inner join MONKEY_D_BASE.viaje_paquete vp
on v.id = vp.viaje_id
group by v.camion_codigo
ORDER BY 1;

--Costo de viaje
INSERT INTO MONKEY_D_BASE.BI_Costo_viaje (camion_id,costo)
Select	viaje.camion_codigo, 
		Sum(((DATEDIFF(hour, viaje.fecha_inicio, viaje.fecha_fin) * empl.costo_hora) + (viaje.combustible_consumido * 100)))
From MONKEY_D_BASE.Viaje viaje 
Join MONKEY_D_BASE.Empleado empl on viaje.chofer_legajo = empl.legajo
group by viaje.camion_codigo
order by 1

--Costo de mantenimiento
INSERT INTO MONKEY_D_BASE.BI_costo_mantenimiento (camion_id, costo_total)
SELECT camion_id, sum(costo_total) costo_total
FROM MONKEY_D_BASE.BI_camion_costo
GROUP BY camion_id

/************************
*** CREACIÓN DE VISTAS **
*************************/
--Máximo tiempo fuera de servicio de cada camión por cuatrimestre
CREATE VIEW MONKEY_D_BASE.BI_camion_sin_servicio
AS
    SELECT
        b.cuarto,
        c.patente,
        b.tiempo_sin_servicio
    FROM 
        MONKEY_D_BASE.BI_camion_servicio b,
        MONKEY_D_BASE.Camion c
    WHERE
        b.camion_id = c.id

GO

--Costo total de mantenimiento por camión, por taller, por cuatrimestre
CREATE VIEW MONKEY_D_BASE.BI_VW_camion_costo_total
AS
    SELECT
        b.cuarto cuatrimestre,
        c.patente camion_patente,
        tl.nombre taller_nombre,
        b.costo_total
    FROM 
        MONKEY_D_BASE.BI_camion_costo b,
        MONKEY_D_BASE.camion c,
        MONKEY_D_BASE.Taller tl
    WHERE
        b.camion_id = c.id
    AND b.taller_id = tl.id 
GO

--Desvio promedio por tarea por taller
CREATE VIEW MONKEY_D_BASE.BI_VW_Desvio_Promedio_x_Tarea_x_Taller AS
SELECT ptt.taller_id,ptt.tarea_id,ABS(ptt.tarea_tiempo_estimado - ptt.promedio) as desvio_promedio 
FROM MONKEY_D_BASE.BI_Promedio_x_Tarea_x_Taller ptt;

--Las 5 tareas que más se realizan por modelo de camión
CREATE VIEW MONKEY_D_BASE.BI_VW_Tareas_mas_realizadas_x_Modelo_Camion AS
SELECT * FROM (
SELECT tr.modelo_desc,tr.tarea_id,tr.cantidad,RANK() OVER (PARTITION BY tr.modelo_desc ORDER BY tr.cantidad DESC) as ranking
FROM MONKEY_D_BASE.BI_Tareas_mas_realizadas_x_Modelo_Camion tr) AS T
WHERE ranking <=5

--Costo Promedio x rango etario de choferes
CREATE VIEW MONKEY_D_BASE.BI_VW_Costo_Promedio_x_RangoEtario AS
SELECT re.id,re.edad_ini,re.edad_fin,avg(e.costo_hora) as costo_promedio
FROM MONKEY_D_BASE.Empleado e
JOIN MONKEY_D_BASE.Viaje v ON v.chofer_legajo = e.legajo
JOIN MONKEY_D_BASE.BI_Rango_Edad re ON DATEDIFF(YEAR,e.fecha_nacimiento,v.fecha_fin) BETWEEN re.edad_ini AND re.edad_fin 
WHERE e.tipo_id = 1
GROUP BY re.id,re.edad_ini,re.edad_fin;
--Se asume que el costo por hora de los empleados no cambio en el tiempo

--Los 10 materiales más utilizados por taller
CREATE FUNCTION MONKEY_D_BASE.BI_MATERIAL_X_USADO (@ORDEN_MAS_USADO int, @TALLER_ID int)
RETURNS int
AS
BEGIN
    Return (Select tm.material_id 
	From MONKEY_D_BASE.Tarea_Material tm 
	Join MONKEY_D_BASE.Tarea t on tm.tarea_id = t.id 
	Join MONKEY_D_BASE.Orden_Tarea ordt on ordt.tarea_id = t.id 
    Join MONKEY_D_BASE.Orden_Trabajo ot2 on ordt.orden_id = ot2.id 
	where ot2.taller_id = @TALLER_ID 
	Group by tm.material_id 
	Order by SUM(tm.material_cantidad) desc OFFSET @ORDEN_MAS_USADO ROWS FETCH NEXT 1 ROWS ONLY);
END;
GO

CREATE VIEW MONKEY_D_BASE.BI_VW_Materiales_mas_usados AS
Select   ot.taller_id
		,MONKEY_D_BASE.BI_MATERIAL_X_USADO (0, ot.taller_id) as [1° Material mas usado], 
		 MONKEY_D_BASE.BI_MATERIAL_X_USADO (1, ot.taller_id) as [2° Material mas usado], 
		 MONKEY_D_BASE.BI_MATERIAL_X_USADO (2, ot.taller_id) as [3° Material mas usado], 
		 MONKEY_D_BASE.BI_MATERIAL_X_USADO (3, ot.taller_id) as [4° Material mas usado], 
		 MONKEY_D_BASE.BI_MATERIAL_X_USADO (4, ot.taller_id) as [5° Material mas usado],
		 MONKEY_D_BASE.BI_MATERIAL_X_USADO (5, ot.taller_id) as [6° Material mas usado], 
		 MONKEY_D_BASE.BI_MATERIAL_X_USADO (6, ot.taller_id) as [7° Material mas usado], 
		 MONKEY_D_BASE.BI_MATERIAL_X_USADO (7, ot.taller_id) as [8° Material mas usado], 
		 MONKEY_D_BASE.BI_MATERIAL_X_USADO (8, ot.taller_id) as [9° Material mas usado], 
		 MONKEY_D_BASE.BI_MATERIAL_X_USADO (9, ot.taller_id) as [10° Material mas usado] 
From     MONKEY_D_BASE.Orden_Trabajo ot
Group by ot.taller_id;

--Facturación total por recorrido por cuatrimestre
CREATE View MONKEY_D_BASE.BI_VW_Facturacion_Total_x_Recorrido_x_Cuatrimestre as
select 
r.id,
r.ciudad_origen,
r.ciudad_destino,
t.cuarto,
 sum((vp.paquete_cantidad * p.precio)) + r.precio as [Facturacion total] 
from MONKEY_D_BASE.recorrido r
inner join MONKEY_D_BASE.viaje v
left join MONKEY_D_BASE.BI_Tiempo t
on v.fecha_inicio = t.fecha
on r.id = v.id
inner join MONKEY_D_BASE.viaje_paquete vp
on v.id = vp.viaje_id
inner join MONKEY_D_BASE.paquete_tipo p
on vp.tipo_id = p.id
group by r.id ,t.cuarto,r.precio,r.ciudad_origen,r.ciudad_destino

--Ganancia por camión
CREATE VIEW MONKEY_D_BASE.BI_VW_Ganancia_x_camion AS
SELECT c.camion_id,(i.ingresos - c.costo - cm.costo_total) as Ganancia FROM MONKEY_D_BASE.BI_Ingresos_por_camion i
JOIN MONKEY_D_BASE.BI_Costo_viaje c ON i.camion_id = c.camion_id
JOIN MONKEY_D_BASE.BI_costo_mantenimiento cm ON cm.camion_id = c.camion_id

