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
	AND name like '%BI_%'
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
-- Creación de tabla que guarda los datos correspondientes con la dimension Tarea
CREATE TABLE MONKEY_D_BASE.BI_Tarea (
	id				INT PRIMARY KEY,
	descripcion		NVARCHAR(255) NOT NULL
);

-- Creación de tabla que guarda los datos correspondientes con la dimension Recorrido
CREATE TABLE MONKEY_D_BASE.BI_Recorrido (
	id				INT PRIMARY KEY,
	ciudad_origen	NVARCHAR(255) NOT NULL,
	ciudad_destino	NVARCHAR(255) NOT NULL
);

-- Creación de tabla que guarda los datos correspondientes con la dimension taller
CREATE TABLE MONKEY_D_BASE.BI_Taller (
	id			INT PRIMARY KEY,
	nombre		NVARCHAR(255)	NOT NULL
);

-- Creación de tabla que guarda los datos correspondientes con la dimension Marca							
CREATE TABLE MONKEY_D_BASE.BI_Marca (
	id				INT PRIMARY KEY,
	descripcion		NVARCHAR(255) NOT NULL 
);

-- Creación de tabla que guarda los datos correspondientes con la dimension Camion Modelo
CREATE TABLE MONKEY_D_BASE.BI_Camion_Modelo (
	id					INT PRIMARY KEY,
	descripcion			NVARCHAR(255) NOT NULL
);

-- Creación de tabla que guarda los datos correspondientes con la dimension Camion
CREATE TABLE MONKEY_D_BASE.BI_Camion (
	id			INT PRIMARY KEY,
	patente		NVARCHAR(255) NOT NULL
);

-- Creacion de tabla que guarda los datos correspondiente con la dimension Rango Edad 
CREATE TABLE MONKEY_D_BASE.BI_Rango_Edad (
	id					INT IDENTITY PRIMARY KEY NOT NULL,
	edad_ini			INT,
	edad_fin			INT
    );	

-- Creacion de tabla que guarda los datos correspondiente con la dimension Tiempo
CREATE TABLE MONKEY_D_BASE.BI_Tiempo (
			id_tiempo           INT IDENTITY PRIMARY KEY NOT NULL,
			cuatrimestre        CHAR(2) NOT NULL,
			anio                INT NOT NULL,
			fecha_inicio_cuatri DATE NOT NULL,
			fecha_fin_cuatri    DATE NOT NULL
);

--Creación de la tabla de Hecho de Ordenes Tarea
CREATE TABLE MONKEY_D_BASE.BI_Hechos_Ordenes_tarea (
        tiempo_id                   INT FOREIGN KEY REFERENCES MONKEY_D_BASE.BI_Tiempo(id_tiempo),
        taller_id                   INT FOREIGN KEY REFERENCES MONKEY_D_BASE.BI_Taller(id),
        tarea_id                    INT FOREIGN KEY REFERENCES MONKEY_D_BASE.BI_Tarea(id),
        camion_id                   INT FOREIGN KEY REFERENCES MONKEY_D_BASE.BI_Camion(id),
        modelo_id                   INT FOREIGN KEY REFERENCES MONKEY_D_BASE.BI_Camion_Modelo(id),
        marca_id                    INT FOREIGN KEY REFERENCES MONKEY_D_BASE.BI_Marca(id),
        camion_dias_sin_servicio    INT NOT NULL,
        costo                       DECIMAL(18,2) NOT NULL,
        desvio_promedio             DECIMAL(12,2) NOT NULL,
        CONSTRAINT PK_BI_Hechos_Ordenes_tarea PRIMARY KEY (tiempo_id, taller_id, tarea_id, camion_id, modelo_id, marca_id)
);


--Creación de la tabla de hecho para Viajes
CREATE TABLE MONKEY_D_BASE.BI_Hechos_Viajes (
			tiempo_id                   INT FOREIGN KEY REFERENCES MONKEY_D_BASE.BI_Tiempo(id_tiempo),
			camion_id                   INT FOREIGN KEY REFERENCES MONKEY_D_BASE.BI_Camion(id),
			marca_id                    INT FOREIGN KEY REFERENCES MONKEY_D_BASE.BI_Marca(id),
			modelo_id                   INT FOREIGN KEY REFERENCES MONKEY_D_BASE.BI_Camion_modelo(id),
			recorrido_id                INT FOREIGN KEY REFERENCES MONKEY_D_BASE.BI_recorrido(id),
			rango_edad_id               INT FOREIGN KEY REFERENCES MONKEY_D_BASE.BI_Rango_Edad(id),
			facturacion_total           DECIMAL(18,2) NOT NULL,
			costo_chofer                DECIMAL(18,2) NOT NULL,
			costo_combustible           DECIMAL(18,2) NOT NULL,
			CONSTRAINT PK_BI_Hechos_Viajes PRIMARY KEY (tiempo_id, camion_id, marca_id, modelo_id,  recorrido_id, rango_edad_id)
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
--Tarea
	    SET @tabla = 'BI_Tarea';

        INSERT INTO MONKEY_D_BASE.BI_Tarea (id, descripcion)
        SELECT id, descripcion
        FROM Tarea
        
        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

--Recorrido
	    SET @tabla = 'BI_Recorrido';

        INSERT INTO MONKEY_D_BASE.BI_Recorrido (id, ciudad_origen, ciudad_destino)
        SELECT id, ciudad_origen, ciudad_destino
        FROM Recorrido
        
        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

--Taller
        SET @tabla = 'BI_Taller';

        INSERT INTO MONKEY_D_BASE.BI_Taller (id, nombre)
        SELECT id, nombre
        FROM Taller
        
        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

--Marca
        SET @tabla = 'BI_Marca';

        INSERT INTO MONKEY_D_BASE.BI_Marca (id, descripcion)
        SELECT id, descripcion
        FROM Marca
        
        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

--Modelo
        SET @tabla = 'BI_Camion_Modelo';

        INSERT INTO MONKEY_D_BASE.BI_Camion_Modelo (id, descripcion)
        SELECT id, descripcion
        FROM Camion_Modelo
        
        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

--Camion
        SET @tabla = 'BI_Camion';

        INSERT INTO MONKEY_D_BASE.BI_Camion (id, patente)
        SELECT id, patente
        FROM Camion
        
        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;        

--Tiempo
		SET @tabla = 'BI_Tiempo';

		INSERT INTO MONKEY_D_BASE.BI_Tiempo (cuatrimestre,anio,fecha_inicio_cuatri,fecha_fin_cuatri)
		SELECT DISTINCT
			CASE
			WHEN MONTH(fecha_inicio) BETWEEN 1 AND 4 THEN '1Q' 
			WHEN MONTH(fecha_inicio) BETWEEN 5 AND 8 THEN '2Q' 
			WHEN MONTH(fecha_inicio) BETWEEN 9 AND 12 THEN '3Q' END cuatrimestre,
			YEAR(fecha_inicio) anio,
			CASE
			WHEN MONTH(fecha_inicio) BETWEEN 1 AND 4 THEN CONVERT(DATE, CONVERT(CHAR(4),YEAR(fecha_inicio)) + '0101')
			WHEN MONTH(fecha_inicio) BETWEEN 5 AND 8 THEN CONVERT(DATE, CONVERT(CHAR(4),YEAR(fecha_inicio)) + '0501')
			WHEN MONTH(fecha_inicio) BETWEEN 9 AND 12 THEN CONVERT(DATE, CONVERT(CHAR(4),YEAR(fecha_inicio)) + '0901') 
			END fecha_inicio_cuatrimestre,
			CASE
			WHEN MONTH(fecha_inicio) BETWEEN 1 AND 4 THEN CONVERT(DATE, CONVERT(CHAR(4),YEAR(fecha_inicio)) + '0430')
			WHEN MONTH(fecha_inicio) BETWEEN 5 AND 8 THEN CONVERT(DATE, CONVERT(CHAR(4),YEAR(fecha_inicio)) + '0831')
			WHEN MONTH(fecha_inicio) BETWEEN 9 AND 12 THEN CONVERT(DATE, CONVERT(CHAR(4),YEAR(fecha_inicio)) + '1231') 
			END fecha_fin_cuatrimestre
		FROM MONKEY_D_BASE.Viaje;
		
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

/************************
*** TABLA DE HECHOS ***
*************************/
--Insert para la tabla de hecho de Ordenes de Trabajo
		SET @tabla = 'BI_Hechos_Ordenes_tarea'

        SELECT 
        ot.camion_id camion_id,
        cm.id modelo_id,
        cm.marca_id marca_id,
        ot.id,
        t.id_tiempo,
        DATEDIFF(DAY, MIN(ott.fecha_ini_real), MAX(ott.fecha_fin_real)) tiempo_sin_servicio
        INTO #camionSinServicio2
        FROM MONKEY_D_BASE.Orden_trabajo ot
        INNER JOIN MONKEY_D_BASE.Orden_tarea ott ON ot.id = ott.orden_id
        INNER JOIN MONKEY_D_BASE.BI_Tiempo t ON ott.fecha_fin_real BETWEEN t.fecha_inicio_cuatri AND t.fecha_fin_cuatri AND t.anio = YEAR(ott.fecha_fin_real)
        JOIN MONKEY_D_BASE.Camion c ON c.id = ot.camion_id
        JOIN MONKEY_D_BASE.Camion_Modelo cm ON cm.id = c.modelo_id
        GROUP BY    ot.camion_id,
                    cm.id,
                    cm.marca_id,
                    ot.id,
                    t.id_tiempo;
					
		INSERT INTO MONKEY_D_BASE.BI_Hechos_Ordenes_tarea (
                            tiempo_id,
                            taller_id,
                            tarea_id,
                            camion_id,
                            modelo_id,
                            marca_id,
                            camion_dias_sin_servicio,
                            costo,
                            desvio_promedio)
		SELECT  tiemp.id_tiempo as tiempo_id, 
                tal.id as taller_id, 
                tar.id as tarea_id, 
                cam.id as camion_id,
                cm.id as modelo_id,
                cm.marca_id as marca_id,                
                (Select MAX(tiempo_sin_servicio) From #camionSinServicio2 temp where temp.camion_id = cam.id and temp.id_tiempo = tiemp.id_tiempo) as camion_dias_sin_servicio,
                Sum(DATEDIFF(DAY, ordt.fecha_ini_real, ordt.fecha_fin_real) * 8 * emp.costo_hora) + Sum(ISNULL(tm.material_cantidad * mat.precio,0)) as costo,
                AVG(tar.tiempo_estimado) - AVG(DATEDIFF(day,ordt.fecha_ini_real,ordt.fecha_fin_real)) desvio_promedio
        From MONKEY_D_BASE.Orden_Tarea ordt 
        Join MONKEY_D_BASE.Orden_Trabajo ot on ordt.orden_id = ot.id
        Join MONKEY_D_BASE.Taller tal on tal.id = ot.taller_id
        Join MONKEY_D_BASE.Empleado emp on emp.legajo = ordt.mecanico_legajo
        Join MONKEY_D_BASE.Tarea tar on tar.id = ordt.tarea_id
        Join MONKEY_D_BASE.Tarea_Material tm on tm.tarea_id = tar.id
        Join MONKEY_D_BASE.Material mat on mat.id = tm.material_id
        Join MONKEY_D_BASE.Camion cam on cam.id = ot.camion_id
        Join MONKEY_D_BASE.Camion_Modelo cm on cm.id = cam.modelo_id
        Join MONKEY_D_BASE.Marca mar on mar.id = cm.marca_id
        JOIN MONKEY_D_BASE.BI_Tiempo tiemp ON ordt.fecha_fin_real BETWEEN tiemp.fecha_inicio_cuatri AND tiemp.fecha_fin_cuatri AND YEAR(ordt.fecha_fin_real) = tiemp.anio
        Group by    tal.id,
                    tar.id, 
                    cam.id,
                    cm.id,
                    cm.marca_id, 
                    --emp.legajo, 
                    tiemp.id_tiempo
        order by 1, 2, 3 ,4, 5;

        EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

        DROP TABLE #camionSinServicio2;
		
		SET @tabla = 'BI_Hechos_Viajes';

--Insert para la tabla de hecho de Viaje
        INSERT INTO MONKEY_D_BASE.BI_Hechos_Viajes
        SELECT 
            t.id_tiempo tiempo_id,
            v.camion_codigo,
            cm.marca_id,
            c.modelo_id,
            v.recorrido_codigo,
            r.id rango_edad_id,
            0,                      -- ponemos 0 para luego actualizar
            SUM(((DATEDIFF(HOUR, v.fecha_inicio, v.fecha_fin) * e.costo_hora))) costo_chofer,
            SUM(v.combustible_consumido) * 100 costo_combustible
        FROM 
            MONKEY_D_BASE.Viaje v
            INNER JOIN MONKEY_D_BASE.BI_Tiempo t ON v.fecha_fin BETWEEN t.fecha_inicio_cuatri AND t.fecha_fin_cuatri 
                                                    AND YEAR(v.fecha_fin) = t.anio
            INNER JOIN MONKEY_D_BASE.Camion c ON c.id = v.camion_codigo
            INNER JOIN MONKEY_D_BASE.Camion_modelo cm ON cm.id = c.modelo_id
            INNER JOIN MONKEY_D_BASE.Empleado e ON v.chofer_legajo = e.legajo
            INNER JOIN MONKEY_D_BASE.BI_Rango_Edad r ON DATEDIFF(YEAR, e.fecha_nacimiento, v.fecha_fin) BETWEEN r.edad_ini AND r.edad_fin
        GROUP BY     t.id_tiempo,
                    v.camion_codigo,
                    cm.marca_id,
                    c.modelo_id,
                    v.recorrido_codigo,
                    r.id;
        
        -- tabla auxiliar para calcular el total facturado
        SELECT 
            t.id_tiempo tiempo_id,
            v.camion_codigo,
            c.modelo_id,
            cm.marca_id,
            v.recorrido_codigo,
            r.id rango_edad_id,
            SUM(vp.paquete_cantidad * vp.paquete_precio_hist) + v.precio_recorrido_his facturacion_Total
        INTO #tmp
        FROM 
            MONKEY_D_BASE.Viaje v
            INNER JOIN MONKEY_D_BASE.Viaje_Paquete vp ON vp.viaje_id = v.id
            INNER JOIN MONKEY_D_BASE.BI_Tiempo t ON v.fecha_fin BETWEEN t.fecha_inicio_cuatri AND t.fecha_fin_cuatri 
                                                    AND YEAR(v.fecha_fin) = t.anio
            INNER JOIN MONKEY_D_BASE.Camion c ON c.id = v.camion_codigo
            INNER JOIN MONKEY_D_BASE.Camion_modelo cm ON cm.id = c.modelo_id
            INNER JOIN MONKEY_D_BASE.Empleado e ON v.chofer_legajo = e.legajo
            INNER JOIN MONKEY_D_BASE.BI_Rango_Edad r ON DATEDIFF(YEAR, e.fecha_nacimiento, v.fecha_fin) BETWEEN r.edad_ini AND r.edad_fin
        GROUP BY    t.id_tiempo ,
                    v.camion_codigo,
                    c.modelo_id,
                    cm.marca_id,
                    v.recorrido_codigo,
                    r.id,
                    v.precio_recorrido_his;

            -- actualizo la facturacion total de BI_Hechos_Viajes
            UPDATE MONKEY_D_BASE.BI_Hechos_Viajes
            SET facturacion_Total = t.facturacion_Total
            FROM MONKEY_D_BASE.BI_Hechos_Viajes v , #tmp t
            WHERE v.tiempo_id = t.tiempo_id
            AND v.camion_id = t.camion_codigo
            AND v.marca_id = t.marca_id
            AND v.modelo_id = t.modelo_id
            AND v.recorrido_id = t.recorrido_codigo
            AND v.rango_edad_id = t.rango_edad_id;

			EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

            DROP TABLE #tmp;

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
CREATE VIEW MONKEY_D_BASE.BI_VW_camion_sin_servicio2
AS
    SELECT
        c.patente patente,
		t.cuatrimestre cuatrimestre,   
        max (hechos_ot.camion_dias_sin_servicio) as maximo_dias_sin_servicio
    FROM 
        MONKEY_D_BASE.BI_Hechos_Ordenes_tarea hechos_ot
		JOIN MONKEY_D_BASE.BI_Camion c ON hechos_ot.camion_id = c.id    
		JOIN MONKEY_D_BASE.BI_Tiempo t ON t.id_tiempo = hechos_ot.tiempo_id
	Group by   c.patente,t.cuatrimestre;

GO

--Costo total de mantenimiento por camion, por taller, por cuatrimestre
CREATE VIEW MONKEY_D_BASE.BI_VW_camion_costo_total2
AS
    SELECT
		c.patente camion_patente,
		ti.cuatrimestre cuatrimestre,
        t.nombre taller_nombre,
        Sum(hechos_ot.costo) AS costo
    FROM 
        MONKEY_D_BASE.BI_Hechos_Ordenes_tarea hechos_ot
		JOIN MONKEY_D_BASE.BI_Camion c ON hechos_ot.camion_id = c.id   
		JOIN MONKEY_D_BASE.BI_Taller t ON hechos_ot.taller_id = t.id   
		JOIN MONKEY_D_BASE.BI_Tiempo ti ON ti.id_tiempo = hechos_ot.tiempo_id
	GROUP BY 
		c.patente,
		ti.cuatrimestre,
        t.nombre;
GO

--Desvio promedio por tarea por taller
CREATE VIEW MONKEY_D_BASE.BI_VW_Desvio_Promedio_x_Tarea_x_Taller2 
AS
	SELECT 
		t.nombre taller,
		tt.descripcion tarea,
		AVG(hechos_ot.desvio_promedio) as desvio_promedio 
        FROM MONKEY_D_BASE.BI_Hechos_Ordenes_tarea hechos_ot
		JOIN MONKEY_D_BASE.BI_Taller t ON t.id = hechos_ot.taller_id
		JOIN MONKEY_D_BASE.BI_Tarea tt ON tt.id = hechos_ot.tarea_id
		GROUP BY    t.nombre,
		            tt.descripcion;
GO

--Las 5 tareas que mas se realizan por modelo de camión
CREATE VIEW MONKEY_D_BASE.BI_VW_Tareas_mas_realizadas_x_Modelo_Camion2 
AS
    SELECT    cm.descripcion as Modelo, 
            (Select TOP 1 hechos_ot2.tarea_id From MONKEY_D_BASE.BI_Hechos_Ordenes_tarea hechos_ot2 JOIN MONKEY_D_BASE.BI_Camion c2 ON hechos_ot2.camion_id = c2.id
             where hechos_ot2.modelo_id = cm.id Group by hechos_ot2.tarea_id Order by Count(*) desc) as Tarea_mas_realizada,
             (Select hechos_ot2.tarea_id From MONKEY_D_BASE.BI_Hechos_Ordenes_tarea hechos_ot2 JOIN MONKEY_D_BASE.BI_Camion c2 ON hechos_ot2.camion_id = c2.id
             where hechos_ot2.modelo_id = cm.id Group by hechos_ot2.tarea_id Order by Count(*) desc OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY) as Segunda_Tarea_mas_realizada,
             (Select hechos_ot2.tarea_id From MONKEY_D_BASE.BI_Hechos_Ordenes_tarea hechos_ot2 JOIN MONKEY_D_BASE.BI_Camion c2 ON hechos_ot2.camion_id = c2.id
             where hechos_ot2.modelo_id = cm.id Group by hechos_ot2.tarea_id Order by Count(*) desc OFFSET 2 ROWS FETCH NEXT 1 ROWS ONLY) as Tercera_Tarea_mas_realizada,
             (Select hechos_ot2.tarea_id From MONKEY_D_BASE.BI_Hechos_Ordenes_tarea hechos_ot2 JOIN MONKEY_D_BASE.BI_Camion c2 ON hechos_ot2.camion_id = c2.id
             where hechos_ot2.modelo_id = cm.id Group by hechos_ot2.tarea_id Order by Count(*) desc OFFSET 3 ROWS FETCH NEXT 1 ROWS ONLY) as Cuarta_Tarea_mas_realizada,
             (Select hechos_ot2.tarea_id From MONKEY_D_BASE.BI_Hechos_Ordenes_tarea hechos_ot2 JOIN MONKEY_D_BASE.BI_Camion c2 ON hechos_ot2.camion_id = c2.id
             where hechos_ot2.modelo_id = cm.id Group by hechos_ot2.tarea_id Order by Count(*) desc OFFSET 4 ROWS FETCH NEXT 1 ROWS ONLY) as Quinta_Tarea_mas_realizada
    FROM MONKEY_D_BASE.BI_Hechos_Ordenes_tarea hechos_ot
    Join MONKEY_D_BASE.BI_Camion_Modelo cm on cm.id = hechos_ot.modelo_id
    Group by cm.id,cm.descripcion;
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
				JOIN MONKEY_D_BASE.BI_Hechos_Ordenes_tarea hot ON hot.tarea_id = tm.tarea_id
			WHERE 
				hot.taller_id = @TALLER_ID 
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
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (0, hot.taller_id) as [1er Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (1, hot.taller_id) as [2do Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (2, hot.taller_id) as [3er Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (3, hot.taller_id) as [4to Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (4, hot.taller_id) as [5to Material mas usado],
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (5, hot.taller_id) as [6to Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (6, hot.taller_id) as [7mo Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (7, hot.taller_id) as [8vo Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (8, hot.taller_id) as [9no Material mas usado], 
		MONKEY_D_BASE.BI_MATERIAL_X_USADO (9, hot.taller_id) as [10mo Material mas usado] 
	FROM     
		MONKEY_D_BASE.BI_Hechos_Ordenes_tarea hot
	INNER JOIN MONKEY_D_BASE.BI_Taller t ON hot.taller_id = t.id
	GROUP BY 
		t.nombre,
		hot.taller_id;

GO

-- Ganancia por camion
CREATE VIEW MONKEY_D_BASE.BI_VW_Ganancia_x_camion 
AS
	SELECT 
	c.patente,
	SUM(v.facturacion_total) - SUM(v.costo_chofer) - SUM(v.costo_combustible) - SUM(o.costo) Ganancia
FROM 
	MONKEY_D_BASE.BI_Hechos_Ordenes_tarea o
	INNER JOIN MONKEY_D_BASE.BI_Hechos_Viajes v ON o.camion_id = v.camion_id
	INNER JOIN MONKEY_D_BASE.BI_Camion c ON o.camion_id = c.id
GROUP BY c.patente

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
