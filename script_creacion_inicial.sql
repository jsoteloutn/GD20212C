/*******************
*** BASE DE DATOS **
********************/
USE GD2C2021;
GO

/***********************
*** DROP DE OBJETOS ***
***********************/
DECLARE @objeto nvarchar(100), 
		@tabla nvarchar(100), 
		@tipo nvarchar(2), 
		@sql nvarchar(2000);

DECLARE objetos CURSOR FOR 
	SELECT name, OBJECT_NAME(parent_object_id), type
	FROM sys.objects
	WHERE schema_id = SCHEMA_ID('MONKEY_D_BASE') 
	AND type IN ('F', 'P', 'U', 'V', 'FN' )
	AND name like 'BI_%'
	ORDER BY type;

OPEN objetos;

FETCH NEXT FROM objetos INTO @objeto, @tabla, @tipo;

WHILE @@fetch_status = 0
BEGIN

	IF @tipo = 'P'
		SET @sql = 'DROP PROCEDURE MONKEY_D_BASE.' + @objeto;

	IF @tipo = 'FN'
		SET @sql = 'DROP FUNCTION MONKEY_D_BASE.' + @objeto;

	IF @tipo = 'F'
		SET @sql = 'ALTER TABLE MONKEY_D_BASE.' + @tabla + ' DROP CONSTRAINT ' + @objeto;

	IF @tipo = 'U'
		SET @sql = 'DROP TABLE MONKEY_D_BASE.' + @objeto;

	IF @tipo = 'V'
		SET @sql = 'DROP VIEW MONKEY_D_BASE.' + @objeto;

	EXEC (@sql)

	FETCH NEXT FROM objetos INTO @objeto, @tabla, @tipo;

END
CLOSE objetos;
DEALLOCATE objetos;

GO

/************************
*** CREACION DE TABLAS **
*************************/
-- Creacion de tabla de que guarda los datos correspondiente con la dimension Rango Edad 
CREATE TABLE MONKEY_D_BASE.BI_Rango_Edad (
	id					INT IDENTITY PRIMARY KEY NOT NULL,
	edad_ini			INT,
	edad_fin			INT
    );	

-- Creacion de tabla que guarda los datos correspondiente con la dimension Tiempo
CREATE TABLE MONKEY_D_BASE.BI_Tiempo (
	fecha				DATETIME2 PRIMARY KEY NOT NULL,
	dia					INT NOT NULL,
	mes					INT NOT NULL,
	cuarto				CHAR(2) NOT NULL,
	anio				INT NOT NULL
    );
-- Creacion de tabla que guarda los datos correspondiente con el promedio de dias de demora de realizacion de tareas por taller
CREATE TABLE MONKEY_D_BASE.BI_Promedio_x_Tarea_x_Taller (
	taller_id				INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Taller(id),
	tarea_id				INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Tarea(id),
	tarea_tiempo_estimado	INT NOT NULL,
	promedio				decimal(12,2)
    CONSTRAINT PK_BI_Promedio_x_Tarea_x_Taller PRIMARY KEY (taller_id, tarea_id)
    );
-- Creacion de tabla que guarda los datos correspondiente con la cantidad de tareas realizadas por modelo de camion
CREATE TABLE MONKEY_D_BASE.BI_Tareas_mas_realizadas_x_Modelo_Camion (
	modelo_id			INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Camion_Modelo(id),
	modelo_desc			nvarchar(255) NOT NULL,
	tarea_id			INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Taller(id),
	cantidad			INT NOT NULL
    CONSTRAINT PK_BI_Tareas_mas_realizadas_x_Modelo_Camion PRIMARY KEY (modelo_id, tarea_id)
    );
-- Creacion de tabla que guarda los datos correspondiente con el total de ingresos por cada camion
CREATE TABLE MONKEY_D_BASE.BI_Ingresos_por_camion (
	camion_id			INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Camion(id),
	ingresos			decimal(18,2)
    CONSTRAINT PK_BI_Ingresos_por_camion PRIMARY KEY (camion_id)
    );
-- Creacion de tabla que guarda los datos correspondiente con el costos total de los viajes de cada camion 
CREATE TABLE MONKEY_D_BASE.BI_Costo_viaje (
	camion_id			INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Camion(id),
	costo				decimal(18,2)
    CONSTRAINT PK_BI_Costo_viaje PRIMARY KEY (camion_id)
    );
-- Creacion de tabla que guarda los datos correspondiente con el maximo tiempo fuera de servicio de cada camion por cuatrimestre
CREATE TABLE MONKEY_D_BASE.BI_camion_x_cuatri_sin_servicio(
	camion_id INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Camion(id),
	cuarto CHAR(2) NOT NULL,
	tiempo_sin_servicio int NOT NULL,
	CONSTRAINT PK_BI_camion_x_cuatri_sin_servicio PRIMARY KEY (camion_id, cuarto)
	);
-- Creacion de tabla que guarda los datos correspondiente con el costo de mantenimiendo de cada camion por taller y cuatrimestre
CREATE TABLE MONKEY_D_BASE.BI_camion_x_taller_x_cuatri_costo(
    camion_id INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Camion(id),
    cuarto char(2) NOT NULL,
    taller_id INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Taller(id),
    costo_total DECIMAL(18,2) NOT NULL
    CONSTRAINT PK_BI_camion_costo PRIMARY KEY (camion_id, cuarto, taller_id)
	);
-- Creacion de tabla que guarda los datos correspondiente con el costo total de mantenimiendo de cada camion
CREATE TABLE MONKEY_D_BASE.BI_costo_mantenimiento(
    camion_id   INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Camion(id),
    costo_total decimal(38, 2) NULL
    CONSTRAINT PK_BI_costo_mantenimiento PRIMARY KEY (camion_id)
	);
GO

/********************
*** CREACION SP *****
*********************/
-- llenado de tablas BI

-- Este procedimiento es el encargado de realizar la migración de los datos correspondientes al modelo relacional transaccional al modelo de inteligencia de negocios.
CREATE PROCEDURE MONKEY_D_BASE.BI_SP_migracion_olap
AS
BEGIN
	
	DECLARE @tabla VARCHAR(255);

	BEGIN TRY

/*DIMENSIONES*/		
--Tiempo
		SET @tabla = 'BI_Tiempo';

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
			WHERE m.VIAJE_FECHA_INICIO IS NOT NULL
			UNION
			SELECT DISTINCT m.VIAJE_FECHA_FIN
			FROM gd_esquema.Maestra m
			WHERE m.VIAJE_FECHA_FIN IS NOT NULL
			) AS tabla ORDER BY tabla.VIAJE_FECHA_INICIO;

		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

--Rango_Edad
		SET @tabla = 'BI_Rango_Edad';

		INSERT INTO MONKEY_D_BASE.BI_Rango_Edad(
					edad_ini,
					edad_fin
					)
			SELECT 
				18,30
			UNION
			SELECT 
				31,50
			UNION 
			SELECT 
				50, NULL;		
		
        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

/*HECHOS*/
--Promedio x Tarea x Taller
        SET @tabla = 'BI_Promedio_x_Tarea_x_Taller';

        INSERT INTO MONKEY_D_BASE.BI_Promedio_x_Tarea_x_Taller (
            taller_id,
            tarea_id,
            tarea_tiempo_estimado,
            promedio	)
        SELECT 
            o.taller_id,ot.
            tarea_id,
            t.tiempo_estimado,
            AVG(DATEDIFF(day,ot.fecha_ini_real,ot.fecha_fin_real)) as promedio 
        FROM 
            MONKEY_D_BASE.Orden_Tarea ot
        INNER JOIN MONKEY_D_BASE.Orden_Trabajo o ON o.id = ot.orden_id
        INNER JOIN MONKEY_D_BASE.Tarea t ON t.id = ot.tarea_id
        GROUP BY o.taller_id,ot.tarea_id,t.tiempo_estimado
        ORDER BY 1,2;

        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

--Las 5 tareas que mas se realizan por modelo de camion
        SET @tabla = 'BI_Tareas_mas_realizadas_x_Modelo_Camion';

        INSERT INTO MONKEY_D_BASE.BI_Tareas_mas_realizadas_x_Modelo_Camion (
            modelo_id,
            modelo_desc,
            tarea_id,
            cantidad	)
        SELECT 
            cm.id,
            cm.descripcion,
            t.id as tarea,
            COUNT(t.id) as cantidad
        FROM 
            MONKEY_D_BASE.Tarea t
        INNER JOIN MONKEY_D_BASE.Orden_Tarea ot ON t.id = ot.tarea_id
        INNER JOIN MONKEY_D_BASE.Orden_Trabajo o ON o.id = ot.orden_id
        INNER JOIN MONKEY_D_BASE.Camion c ON c.id = o.camion_id
        INNER JOIN MONKEY_D_BASE.Camion_Modelo cm ON c.modelo_id = cm.id
        GROUP BY cm.id,cm.descripcion,t.id;

        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

--Maximo tiempo fuera de servicio de cada camion por cuatrimestre
        SET @tabla = 'BI_camion_x_cuatri_sin_servicio';

        SELECT 
        ot.camion_id,
        ot.id,
        t.cuarto,
        DATEDIFF(DAY, MIN(ott.fecha_ini_real), MAX(ott.fecha_fin_real)) tiempo_sin_servicio
        INTO #camionSinServicio
        FROM 
            MONKEY_D_BASE.Orden_trabajo ot
        INNER JOIN MONKEY_D_BASE.Orden_tarea ott ON ot.id = ott.orden_id
        INNER JOIN MONKEY_D_BASE.BI_Tiempo t ON ott.fecha_fin_real = t.fecha
        GROUP BY    ot.camion_id,
                    ot.id,
                    t.cuarto;

        INSERT INTO MONKEY_D_BASE.BI_camion_x_cuatri_sin_servicio (
            camion_id, 
            cuarto, 
            tiempo_sin_servicio)
        SELECT 
            camion_id,
            cuarto,
            MAX(tiempo_sin_servicio) tiempo_sin_servicio
        FROM 
            #camionSinServicio
        GROUP BY camion_id,
                cuarto;

        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

        DROP TABLE #camionSinServicio;

--Costo total de mantenimiento por camion, por taller, por cuatrimestre
        SET @tabla = 'BI_camion_x_taller_x_cuatri_costo';

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
        INTO #CamionCosto
        FROM 
            MONKEY_D_BASE.Orden_trabajo ot
        INNER JOIN MONKEY_D_BASE.Orden_tarea ott ON ot.id = ott.orden_id
        INNER JOIN MONKEY_D_BASE.BI_Tiempo t ON ott.fecha_fin_real = t.fecha
        INNER JOIN MONKEY_D_BASE.Empleado e ON ott.mecanico_legajo = e.legajo;

        INSERT INTO MONKEY_D_BASE.BI_camion_x_taller_x_cuatri_costo (
            camion_id, 
            taller_id, 
            cuarto, 
            costo_total)
        SELECT 
            camion_id,
            taller_id,
            cuarto,
            SUM(empleadoTotaCosto + precioTotaTarea)
        FROM 
            #CamionCosto
        GROUP BY 
            camion_id,
            taller_id,
            cuarto;

        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;
        
        DROP TABLE #CamionCosto

--Ganancia por camion
        SET @tabla = 'BI_Ingresos_por_camion';
--Ingresos
        INSERT INTO MONKEY_D_BASE.BI_Ingresos_por_camion (camion_id,ingresos)
        SELECT 
            v.camion_codigo,
            SUM(vp.paquete_cantidad * vp.paquete_precio_hist) + SUM(v.precio_recorrido_his) AS [Ingresos] 
        FROM 
            MONKEY_D_BASE.Viaje v
        INNER JOIN MONKEY_D_BASE.viaje_paquete vp ON v.id = vp.viaje_id
        GROUP BY v.camion_codigo

        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

--Costo de viaje
        SET @tabla = 'BI_Costo_viaje';
        
        INSERT INTO MONKEY_D_BASE.BI_Costo_viaje (camion_id,costo)
        SELECT	
            viaje.camion_codigo, 
            Sum(((DATEDIFF(hour, viaje.fecha_inicio, viaje.fecha_fin) * empl.costo_hora) + (viaje.combustible_consumido * 100)))
        FROM 
            MONKEY_D_BASE.Viaje viaje 
            INNER JOIN MONKEY_D_BASE.Empleado empl on viaje.chofer_legajo = empl.legajo
        GROUP BY 
            viaje.camion_codigo

        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

--Costo de mantenimiento
        SET @tabla = 'BI_costo_mantenimiento';

        INSERT INTO MONKEY_D_BASE.BI_costo_mantenimiento (
            camion_id, 
            costo_total	)
        SELECT	
            camion_id, 
            sum(costo_total) costo_total
        FROM 
            MONKEY_D_BASE.BI_camion_x_taller_x_cuatri_costo
        GROUP BY 
            camion_id

        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

	END TRY

	BEGIN CATCH -- Esta porción es la que contempla los errores en caso de ocurrir
		
		DECLARE @Message varchar(255) = 'Insert tabla '  + UPPER(@tabla) + '; Motivo: '  + UPPER(ERROR_MESSAGE()),
				@Severity int = ERROR_SEVERITY(),
				@State smallint = ERROR_STATE()					
		RAISERROR(@Message, @Severity, @State);

	END CATCH
END

GO

/************************
*** CREACION DE VISTAS **
*************************/
--Maximo tiempo fuera de servicio de cada camion por cuatrimestre
CREATE VIEW MONKEY_D_BASE.BI_VW_camion_sin_servicio
AS
    SELECT
        c.patente,
		b.cuarto cuatrimestre,   
        b.tiempo_sin_servicio
    FROM 
        MONKEY_D_BASE.BI_camion_x_cuatri_sin_servicio b
    INNER JOIN MONKEY_D_BASE.Camion c ON b.camion_id = c.id;        

GO

--Costo total de mantenimiento por camion, por taller, por cuatrimestre
CREATE VIEW MONKEY_D_BASE.BI_VW_camion_costo_total
AS
    SELECT
		c.patente camion_patente,
		b.cuarto cuatrimestre,
        tl.nombre taller_nombre,
        b.costo_total
    FROM 
        MONKEY_D_BASE.BI_camion_x_taller_x_cuatri_costo b
    INNER JOIN MONKEY_D_BASE.camion c ON b.camion_id = c.id
    INNER JOIN MONKEY_D_BASE.Taller tl ON b.taller_id = tl.id;

GO

--Desvio promedio por tarea por taller
CREATE VIEW MONKEY_D_BASE.BI_VW_Desvio_Promedio_x_Tarea_x_Taller 
AS
	SELECT 
		t.nombre taller,
		tt.descripcion tarea,
		ABS(ptt.tarea_tiempo_estimado - ptt.promedio) as desvio_promedio 
	FROM MONKEY_D_BASE.BI_Promedio_x_Tarea_x_Taller ptt
	INNER JOIN MONKEY_D_BASE.Taller t ON t.id = ptt.taller_id
	INNER JOIN MONKEY_D_BASE.Tarea tt ON tt.id = ptt.tarea_id;

GO

--Las 5 tareas que mas se realizan por modelo de camian
CREATE VIEW MONKEY_D_BASE.BI_VW_Tareas_mas_realizadas_x_Modelo_Camion 
AS
	SELECT 
		 T.modelo_desc,
		 tt.descripcion tarea,
		 T.cantidad,
		 T.ranking
	FROM (	SELECT 
				tr.modelo_desc,
				tr.tarea_id,
				tr.cantidad,
				RANK() OVER (PARTITION BY tr.modelo_desc ORDER BY tr.cantidad DESC) as ranking
			FROM MONKEY_D_BASE.BI_Tareas_mas_realizadas_x_Modelo_Camion tr) AS T
	INNER JOIN MONKEY_D_BASE.Tarea tt ON tt.id = T.tarea_id
	WHERE ranking <=5;

GO

--Costo Promedio x rango etario de choferes
CREATE VIEW MONKEY_D_BASE.BI_VW_Costo_Promedio_x_RangoEtario 
AS
	SELECT 
		CONVERT(VARCHAR(3), re.edad_ini) + ' - ' +
		CONVERT(VARCHAR(3), re.edad_fin) rango_etario,
		AVG(e.costo_hora) as costo_promedio		--Se asume que el costo por hora de los empleados no cambio en el tiempo
	FROM 
		MONKEY_D_BASE.Empleado e
	INNER JOIN MONKEY_D_BASE.Viaje v ON v.chofer_legajo = e.legajo
	INNER JOIN MONKEY_D_BASE.BI_Rango_Edad re ON DATEDIFF(YEAR,e.fecha_nacimiento,v.fecha_fin) BETWEEN re.edad_ini AND re.edad_fin 
	WHERE 
		e.tipo_id = 1
	GROUP BY 
		CONVERT(VARCHAR(3), re.edad_ini) + ' - ' +
		CONVERT(VARCHAR(3), re.edad_fin);
	
GO

-- funcion que utilizamos para la creacion de la vista BI_VW_Materiales_mas_usados
-- dado un taller devuelve el material mas usado por ese taller
CREATE FUNCTION MONKEY_D_BASE.BI_MATERIAL_X_USADO (
	@ORDEN_MAS_USADO INT, 
	@TALLER_ID INT)
RETURNS int
AS
BEGIN
    Return (
			SELECT 
				tm.material_id 
			FROM 
				MONKEY_D_BASE.Tarea_Material tm 
			INNER JOIN MONKEY_D_BASE.Tarea t ON tm.tarea_id = t.id 
			INNER JOIN MONKEY_D_BASE.Orden_Tarea ordt ON ordt.tarea_id = t.id 
			INNER JOIN MONKEY_D_BASE.Orden_Trabajo ot2 ON ordt.orden_id = ot2.id 
			WHERE 
				ot2.taller_id = @TALLER_ID 
			GROUP BY 
				tm.material_id 
			ORDER BY 
				SUM(tm.material_cantidad) DESC OFFSET @ORDEN_MAS_USADO ROWS FETCH NEXT 1 ROWS ONLY);
END

GO
--Los 10 materiales mas utilizados por taller
CREATE VIEW MONKEY_D_BASE.BI_VW_Materiales_mas_usados 
AS
	SELECT
		t.nombre taller,
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (0, ot.taller_id) as [1� Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (1, ot.taller_id) as [2� Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (2, ot.taller_id) as [3� Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (3, ot.taller_id) as [4� Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (4, ot.taller_id) as [5� Material mas usado],
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (5, ot.taller_id) as [6� Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (6, ot.taller_id) as [7� Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (7, ot.taller_id) as [8� Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (8, ot.taller_id) as [9� Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (9, ot.taller_id) as [10� Material mas usado] 
	FROM     
		MONKEY_D_BASE.Orden_Trabajo ot
	INNER JOIN MONKEY_D_BASE.Taller t ON ot.taller_id = t.id
	GROUP BY 
		t.nombre,
		ot.taller_id;

GO

--Facturacion total por recorrido por cuatrimestre
CREATE VIEW MONKEY_D_BASE.BI_VW_Facturacion_Total_x_Recorrido_x_Cuatrimestre 
AS
	SELECT 
		r.ciudad_origen +' - ' + r.ciudad_destino 'origen - destino',
		t.cuarto cuatrimestre,
		SUM((vp.paquete_cantidad * p.precio)) + r.precio AS [Facturacion total] 
	FROM 
		MONKEY_D_BASE.recorrido r
	INNER JOIN MONKEY_D_BASE.viaje v
	LEFT JOIN MONKEY_D_BASE.BI_Tiempo t on v.fecha_inicio = t.fecha	on r.id = v.id
	INNER JOIN MONKEY_D_BASE.viaje_paquete vp on v.id = vp.viaje_id
	INNER JOIN MONKEY_D_BASE.paquete_tipo p	on vp.tipo_id = p.id
	GROUP BY 
		t.cuarto,
		r.precio,
		r.ciudad_origen +' - ' + r.ciudad_destino;

GO

--Ganancia por camion
CREATE VIEW MONKEY_D_BASE.BI_VW_Ganancia_x_camion 
AS
	SELECT 
		Camion.patente,
		(i.ingresos - c.costo - cm.costo_total) as ganancia 
	FROM MONKEY_D_BASE.BI_Ingresos_por_camion i
	INNER JOIN MONKEY_D_BASE.BI_Costo_viaje c ON i.camion_id = c.camion_id
	INNER JOIN MONKEY_D_BASE.BI_costo_mantenimiento cm ON cm.camion_id = c.camion_id
	INNER JOIN MONKEY_D_BASE.Camion ON Camion.id = c.camion_id;

GO
/************************
*** LLENADO DE TABLAS ***
*************************/
BEGIN TRY 
	
	DELETE MONKEY_D_BASE.ControlTablas WHERE 1=1; -- borro los datos de la tabla de control

	EXEC MONKEY_D_BASE.BI_SP_migracion_olap;	-- BI_camion_x_cuatri_sin_servicio;BI_camion_x_taller_x_cuatri_costo;BI_costo_mantenimiento
												-- BI_Costo_viaje;BI_Ingresos_por_camion;BI_Promedio_x_Tarea_x_Taller;BI_Rango_Edad;
												-- BI_Tareas_mas_realizadas_x_Modelo_Camion;BI_Tiempo		

	SELECT * FROM MONKEY_D_BASE.ControlTablas ORDER BY nombre;	-- Se imprime por pantalla el resultado (para control interno)

END TRY

BEGIN CATCH	-- Esta porción es la que contempla los errores en caso de ocurrir. Corta la ejecución.

	SELECT * FROM MONKEY_D_BASE.ControlTablas ORDER BY nombre;	-- Se imprime por pantalla el resultado (para control interno)

	THROW;

END CATCH

GO
