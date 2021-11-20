/*******************
*** BASE DE DATOS **
********************/
USE GD2C2021;
GO

/************************
*** CREACION DE SCHEMA **
*************************/
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'MONKEY_D_BASE')
	EXEC ('CREATE SCHEMA MONKEY_D_BASE');
GO

/***********************************
*** DROP DE OBJETOS EN EL SCHEMA ***
************************************/
DECLARE @objeto nvarchar(100), 
		@tabla nvarchar(100), 
		@tipo nvarchar(2), 
		@sql nvarchar(2000);

DECLARE objetos CURSOR FOR 
	SELECT	name, OBJECT_NAME(parent_object_id), type
	FROM sys.objects
	WHERE schema_id = SCHEMA_ID('MONKEY_D_BASE') 
	AND type IN ('F', 'P', 'U', 'FN' )
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

	EXEC (@sql)

	FETCH NEXT FROM objetos INTO @objeto, @tabla, @tipo;

END
CLOSE objetos;
DEALLOCATE objetos;

GO
/************************
*** CRECION DE TABLAS ***
*************************/
-- Tabla de revisión interna creada para ver la cantidad de registros insertados en cada una al momento de la carga
CREATE TABLE MONKEY_D_BASE.ControlTablas
		 (nombre	VARCHAR(250) PRIMARY KEY,
		  CantRegis	INT	);

-- Creación de tabla de que guarda todos los recorridos vigentes
CREATE TABLE MONKEY_D_BASE.Recorrido (
	id				INT IDENTITY PRIMARY KEY,
	ciudad_origen	NVARCHAR(255)	NOT NULL,
	ciudad_destino	NVARCHAR(255)	NOT NULL,
	precio			DECIMAL(18,2)	NOT NULL,
	kms				INT		NOT NULL);

-- Creación de tabla que guarda los distintos tipos de paquetes
CREATE TABLE MONKEY_D_BASE.Paquete_Tipo (		
	id				INT IDENTITY PRIMARY KEY,
	descripcion		NVARCHAR(255) NOT NULL,
	peso_max		DECIMAL(18,2) NOT NULL,
	alto_max		DECIMAL(18,2) NOT NULL,
	ancho_max		DECIMAL(18,2) NOT NULL,
	largo_max		DECIMAL(18,2) NOT NULL,
	precio			DECIMAL(18,2) NOT NULL	);
				
-- Creación de tabla donde se guarda las Marcas de los modelos de camiones								
CREATE TABLE MONKEY_D_BASE.Marca (
	id				INT IDENTITY PRIMARY KEY,
	descripcion		NVARCHAR(255) NOT NULL );

-- Creación de tabla donde se guardan los modelos de los camiones
CREATE TABLE MONKEY_D_BASE.Camion_Modelo (
	id					INT IDENTITY PRIMARY KEY,
	descripcion			NVARCHAR(255)	NOT NULL,
	capacidad_tanque	INT		NOT NULL,
	capacidad_carga		INT		NOT NULL,
	velocidad_max		INT		NOT NULL,
	marca_id			INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Marca(id) );

-- Creación de tabla donde se guardan todos los camiones de la flota
CREATE TABLE MONKEY_D_BASE.Camion (
	id			INT IDENTITY PRIMARY KEY,
	nro_chasis	NVARCHAR(255)	NOT NULL,
	nro_motor	NVARCHAR(255)	NOT NULL,
	fecha_alta	Datetime2	NOT NULL,
	patente		NVARCHAR(255)	NOT NULL,
	modelo_id	INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Camion_Modelo(id) );

-- Creación de tabla que guarda los distintos tipos de Empleados
CREATE TABLE MONKEY_D_BASE.Empleado_Tipo (
	id					INT IDENTITY PRIMARY KEY,
	tipo_descripcion	NVARCHAR(255) NOT NULL	);

-- Creación de tabla que guarda todos los empleados de la organización, sea Mecánico o Chofer
CREATE TABLE MONKEY_D_BASE.Empleado (
	legajo				INT IDENTITY PRIMARY KEY,
	nombre				NVARCHAR(255)	NOT NULL,
	apellido			NVARCHAR(255)	NOT NULL,	
	dni					DECIMAL(18)	NOT NULL,
	direccion			NVARCHAR(255)	NOT NULL,	
	telefono			INT		NOT NULL,
	mail				NVARCHAR(255)	NOT NULL,	
	fecha_nacimiento	DATETIME2	NOT NULL,	
	costo_hora			DECIMAL(18,2)	NOT NULL,
	tipo_id				INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Empleado_Tipo(id)	);

--Creación de tabla que guarda todos los viajes realizados
CREATE TABLE MONKEY_D_BASE.Viaje (
	id						INT IDENTITY PRIMARY KEY,
	fecha_inicio			DATETIME2	NOT NULL,	
	fecha_fin				DATETIME2	NOT NULL,	
	combustible_consumido	INT		NOT NULL,	
	precio_recorrido_his	DECIMAL(18,2)	NOT NULL,
	chofer_legajo			INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Empleado(legajo),	
	recorrido_codigo		INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Recorrido(id),	
	camion_codigo			INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Camion(id)	);

-- Creación de tabla que guarda los paquetes que se llevaron en cada viaje
CREATE TABLE MONKEY_D_BASE.Viaje_Paquete (
	id						INT IDENTITY PRIMARY KEY,
	paquete_cantidad		INT NOT NULL,
	paquete_precio_hist		DECIMAL(18,2) NOT NULL,
	tipo_id					INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Paquete_tipo(id),
	viaje_id				INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Viaje(id)	);

-- Creación de tabla donde se guardan todos los Talleres con los que trabaja la organización
CREATE TABLE MONKEY_D_BASE.Taller (
	id			INT IDENTITY PRIMARY KEY,
	nombre		NVARCHAR(255)	NOT NULL,
	ciudad		NVARCHAR(255)	NOT NULL,
	mail		NVARCHAR(255)	NOT NULL,
	telefono	DECIMAL(18)		NOT NULL,	
	direccion	NVARCHAR(255)	NOT NULL	);

-- Creación de tabla que guarda todos los materiales que requiere cada tarea para su realización
CREATE TABLE MONKEY_D_BASE.Material (
	id			INT IDENTITY PRIMARY KEY,
	codigo		NVARCHAR(100) NOT NULL,
	descripcion	NVARCHAR(255) NOT NULL,
	precio      DECIMAL(18,2) NOT NULL	);

-- Creación de tabla que guarda los distintos tipos de tareas que se pueden realizar para las Ordenes de trabajo
CREATE TABLE MONKEY_D_BASE.Tarea_Tipo (
	id			INT IDENTITY PRIMARY KEY,
	descripcion	NVARCHAR(255)	NOT NULL	);

-- Creación de tabla que guarda todas las tareas que se pueden realizar en las ordenes de trabajo
CREATE TABLE MONKEY_D_BASE.Tarea (
	id				INT IDENTITY PRIMARY KEY,
	descripcion		NVARCHAR(255)	NOT NULL,
	tiempo_estimado	INT		NOT NULL,
	tipo_id			INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Tarea_Tipo(id)	);

-- Creación de tabla que guarda cuales materiales y con qué cantidad deben realizarse para cada tarea
CREATE TABLE MONKEY_D_BASE.Tarea_Material (
	id					INT IDENTITY PRIMARY KEY,
	material_cantidad	INT	NULL,
	material_id			INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Material(id),
	tarea_id			INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Tarea(id)	);

-- Creación de tabla que guarda los distintos Estados en los que se puede encontrar una Orden de Trabajo
CREATE TABLE MONKEY_D_BASE.Estado_OT (
	id			INT IDENTITY PRIMARY KEY,
	descripcion	NVARCHAR(255)	NOT NULL	);

-- Creación de tabla donde se guardan todas las ordenes de trabajo
CREATE TABLE MONKEY_D_BASE.Orden_Trabajo (
	id			INT IDENTITY PRIMARY KEY,
	fecha		DATETIME2 NOT NULL,
	camion_id	INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Camion(id),
	taller_id	INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Taller(id),
	estado_id	INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Estado_OT(id)	);
	
-- Creación de tabla donde se guardan las tareas que se deben realizar o realizaron para cada Orden de Trabajo
CREATE TABLE MONKEY_D_BASE.Orden_Tarea (
	id					INT IDENTITY PRIMARY KEY,
	fecha_planificada	DATETIME2 NOT NULL,
	fecha_ini_real		DATETIME2 NOT NULL,
	fecha_fin_real		DATETIME2 NOT NULL,
	orden_id			INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Orden_Trabajo(id),
	tarea_id			INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Tarea(id),
	mecanico_legajo		INT FOREIGN KEY REFERENCES MONKEY_D_BASE.Empleado(legajo)	);
			
GO

/********************
*** CREACION SP *****
*********************/
-- auxiliares

-- Definición del procedure que es el encargado de insertar en la tabla 
-- controlTablas los resultados de cada INSERT realizado sobre las tablas del dominio
CREATE PROCEDURE MONKEY_D_BASE.Sp_registrarTabla (@tabla VARCHAR(255))
AS
BEGIN
		INSERT INTO MONKEY_D_BASE.ControlTablas
			SELECT 	@tabla,
					@@ROWCOUNT
END
GO

-- llenado de tablas del dominio

-- Definición del procedure que es el encargado de migrar las tablas paramétricas, aquellas tablas que no requieran de JOINS.
CREATE PROCEDURE MONKEY_D_BASE.Sp_LlenadoParametricas
AS 
BEGIN
	
	DECLARE @tabla VARCHAR(255)

	BEGIN TRY
		
		--recorrido
		SET @tabla = 'Recorrido';

		INSERT INTO MONKEY_D_BASE.Recorrido (
			ciudad_origen,
			ciudad_destino,
			precio,
			kms	)
		SELECT DISTINCT 
			m.RECORRIDO_CIUDAD_ORIGEN,
			m.RECORRIDO_CIUDAD_DESTINO,
			m.RECORRIDO_PRECIO,
			m.RECORRIDO_KM
		FROM GD2C2021.gd_esquema.Maestra m
		WHERE m.RECORRIDO_CIUDAD_ORIGEN IS NOT NULL
		AND	m.RECORRIDO_CIUDAD_DESTINO IS NOT NULL;

		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

		--Marca
		SET @tabla = 'Marca';

		INSERT INTO MONKEY_D_BASE.Marca (
			descripcion	)
		SELECT DISTINCT 
			m.MARCA_CAMION_MARCA
		FROM gd_esquema.Maestra m
		WHERE m.MARCA_CAMION_MARCA IS NOT NULL;

		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

		--Empleado_Tipo
		SET @tabla = 'Empleado_Tipo';
		
		INSERT INTO MONKEY_D_BASE.Empleado_Tipo
				SELECT 'CHOFER'
			UNION
				SELECT 'MECANICO'
		
		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

		--Paquete_Tipo
		SET @tabla = 'Paquete_Tipo';

		INSERT INTO MONKEY_D_BASE.Paquete_Tipo (
			descripcion,
			peso_max,
			alto_max,
			ancho_max,
			largo_max,
			precio	)
		SELECT DISTINCT 
			m.PAQUETE_DESCRIPCION,
			m.PAQUETE_PESO_MAX,
			m.PAQUETE_ALTO_MAX,
			m.PAQUETE_ANCHO_MAX,
			m.PAQUETE_LARGO_MAX,
			m.PAQUETE_PRECIO
		FROM gd_esquema.Maestra m
		WHERE m.PAQUETE_DESCRIPCION IS NOT NULL;

		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

		--Estado_OT
		SET @tabla = 'Estado_OT';
		
		INSERT INTO MONKEY_D_BASE.Estado_OT (
			descripcion	)
		SELECT DISTINCT 
			m.ORDEN_TRABAJO_ESTADO
		FROM gd_esquema.Maestra m
		WHERE m.ORDEN_TRABAJO_ESTADO IS NOT NULL

		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

		--Material
		SET @tabla = 'Material';

		INSERT INTO MONKEY_D_BASE.Material (
				codigo,
				descripcion,
				precio	)
		SELECT DISTINCT 
			material_cod,
			(CASE WHEN ISNUMERIC(right(material_descripcion,1)) = 1 THEN LEFT(m.MATERIAL_DESCRIPCION, LEN(m.material_descripcion) - 1) ELSE m.MATERIAL_DESCRIPCION END),
			material_precio 
		FROM gd_esquema.Maestra m 
		WHERE material_cod IS NOT NULL

		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;
		
		--Tarea_tipo
		SET @tabla = 'Tarea_tipo';

		INSERT INTO MONKEY_D_BASE.Tarea_Tipo (
			descripcion)
		SELECT DISTINCT 
			m.TIPO_TAREA
		FROM GD2C2021.gd_esquema.Maestra m
		WHERE m.TIPO_TAREA IS NOT NULL;

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

-- Definición de procedure que es el encargado de migrar las tablas “secundarias”, 
-- aquellas que requieran un JOIN para su migración y tengan un volúmen de datos intermedio.
CREATE PROCEDURE MONKEY_D_BASE.Sp_LlenadoSecundarias
AS 
BEGIN
	
	DECLARE @tabla VARCHAR(255)

	BEGIN TRY
		
		--Camion_Modelo
		SET @tabla = 'Camion_Modelo';

		INSERT INTO MONKEY_D_BASE.Camion_Modelo (
			descripcion,
			capacidad_tanque,
			capacidad_carga,
			velocidad_max,
			marca_id	)
		SELECT DISTINCT 
			m.MODELO_CAMION, 
			m.MODELO_CAPACIDAD_TANQUE,
			m.MODELO_CAPACIDAD_CARGA,
			m.MODELO_VELOCIDAD_MAX,
			mar.id
		FROM gd_esquema.Maestra m
		JOIN MONKEY_D_BASE.Marca mar on m.MARCA_CAMION_MARCA = mar.descripcion;

		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

		--Camion
		SET @tabla = 'Camion';

		INSERT INTO MONKEY_D_BASE.Camion (
			nro_chasis,
			nro_motor,
			fecha_alta,
			patente,
			modelo_id)
		SELECT DISTINCT 
			m.CAMION_NRO_CHASIS,
			m.CAMION_NRO_MOTOR,
			m.CAMION_FECHA_ALTA,
			m.CAMION_PATENTE,
			cm.id
		FROM gd_esquema.Maestra m
		JOIN MONKEY_D_BASE.Camion_Modelo cm on m.MODELO_CAMION = cm.descripcion
		AND m.MODELO_CAPACIDAD_TANQUE = cm.capacidad_tanque AND m.MODELO_CAPACIDAD_CARGA = cm.capacidad_carga
		AND m.MODELO_VELOCIDAD_MAX = cm.velocidad_max;

		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;
		
		--Taller
		SET @tabla = 'Taller';

		INSERT INTO MONKEY_D_BASE.Taller (
			nombre,
			direccion,
			ciudad,
			mail,
			telefono	)
		SELECT DISTINCT 
			m.TALLER_NOMBRE,
			m.TALLER_DIRECCION,
			m.TALLER_CIUDAD,
			m.TALLER_MAIL,
			m.TALLER_TELEFONO  
		FROM GD2C2021.gd_esquema.Maestra m
		WHERE m.TALLER_MAIL IS NOT NULL;

		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

		--Empleado
		SET @tabla = 'Chofer';

		SET IDENTITY_INSERT MONKEY_D_BASE.Empleado ON; -- Desactivo la propiedad de autoincremento

		INSERT INTO MONKEY_D_BASE.Empleado (
			legajo,nombre,
			apellido,
			dni,
			direccion,
			telefono,
			mail,
			fecha_nacimiento,
			costo_hora,
			tipo_id	)
		SELECT DISTINCT 
			m.CHOFER_NRO_LEGAJO,
			m.CHOFER_NOMBRE,
			m.CHOFER_APELLIDO,
			m.CHOFER_DNI,
			m.CHOFER_DIRECCION,
			m.CHOFER_TELEFONO,
			m.CHOFER_MAIL,
			m.CHOFER_FECHA_NAC,
			m.CHOFER_COSTO_HORA,
			et.id
		FROM gd_esquema.Maestra m
		JOIN MONKEY_D_BASE.Empleado_Tipo et ON et.tipo_descripcion = 'CHOFER'
		WHERE m.CHOFER_NRO_LEGAJO IS NOT NULL
		ORDER BY m.CHOFER_NRO_LEGAJO ASC;
		
		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;
		
		--Mecanico
		SET @tabla = 'Mecanico';

		INSERT INTO MONKEY_D_BASE.Empleado (
			legajo,nombre,
			apellido,
			dni,
			direccion,
			telefono,
			mail,
			fecha_nacimiento,
			costo_hora,
			tipo_id	)
		SELECT DISTINCT 
			m.MECANICO_NRO_LEGAJO,
			m.MECANICO_NOMBRE,
			m.MECANICO_APELLIDO,
			m.MECANICO_DNI,
			m.MECANICO_DIRECCION,
			m.MECANICO_TELEFONO,
			m.MECANICO_MAIL,
			m.MECANICO_FECHA_NAC,
			m.MECANICO_COSTO_HORA,
			et.id
		FROM gd_esquema.Maestra m 
		JOIN MONKEY_D_BASE.Empleado_Tipo et ON et.tipo_descripcion = 'MECANICO'
		WHERE m.MECANICO_NRO_LEGAJO IS NOT NULL
		ORDER BY m.MECANICO_NRO_LEGAJO ASC;
		
		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

		SET IDENTITY_INSERT MONKEY_D_BASE.Empleado OFF; -- Reactivo la propiedad de autoincremento

		--Tarea
		SET @tabla = 'Tarea';

		SET IDENTITY_INSERT MONKEY_D_BASE.Tarea ON; -- Desactivo la propiedad de autoincremento

		INSERT INTO MONKEY_D_BASE.Tarea (
			id,
			descripcion,
			tipo_id,
			tiempo_estimado	)
		SELECT DISTINCT 
			m.TAREA_CODIGO,
			m.TAREA_DESCRIPCION,
			tt.id,
			m.TAREA_TIEMPO_ESTIMADO 		
		FROM GD2C2021.gd_esquema.Maestra m
		JOIN MONKEY_D_BASE.Tarea_Tipo tt ON tt.descripcion = m.TIPO_TAREA
		WHERE m.TAREA_CODIGO IS NOT NULL;

		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

		SET IDENTITY_INSERT MONKEY_D_BASE.Tarea OFF; -- Reactivo la propiedad de autoincremento

		--Tarea_Material
		SET @tabla = 'Tarea_Material';

		INSERT INTO MONKEY_D_BASE.Tarea_Material (
			tarea_id,
			material_id,
			material_cantidad	)
		SELECT DISTINCT 
			m.TAREA_CODIGO,
			mat.id,
			NULL 
		FROM gd_esquema.Maestra m
		JOIN MONKEY_D_BASE.Material mat ON mat.codigo = m.MATERIAL_COD
		WHERE m.MATERIAL_COD IS NOT NULL AND m.TAREA_CODIGO IS NOT NULL
		ORDER BY 1,2 ASC;

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


-- Definición de procedure que es el encargado de migrar las tablas “principales”, 
-- aquellas que requieran varios JOINS para su inserción y tengan un volumen de 
-- datos alto respecto a las restantes
CREATE PROCEDURE MONKEY_D_BASE.Sp_LlenadoPrincipales
AS 
BEGIN
	
	DECLARE @tabla VARCHAR(255)

	BEGIN TRY
		
		--Viaje
		SET @tabla = 'Viaje';

		INSERT INTO MONKEY_D_BASE.Viaje (
			fecha_inicio,
			fecha_fin,
			combustible_consumido,
			recorrido_codigo,
			camion_codigo,
			chofer_legajo,
			precio_recorrido_his	)
		SELECT DISTINCT
			m.VIAJE_FECHA_INICIO, 
			m.VIAJE_FECHA_FIN,
			m.VIAJE_CONSUMO_COMBUSTIBLE, 
			r.id recorrido,
			c.id camion,
			m.CHOFER_NRO_LEGAJO chofer,
			m.RECORRIDO_PRECIO precio_hist
		FROM GD2C2021.gd_esquema.Maestra m
		JOIN MONKEY_D_BASE.Recorrido r ON r.ciudad_origen = m.RECORRIDO_CIUDAD_ORIGEN
		AND r.ciudad_destino = m.RECORRIDO_CIUDAD_DESTINO
		JOIN MONKEY_D_BASE.Camion c ON c.patente = m.CAMION_PATENTE
		WHERE m.VIAJE_FECHA_INICIO IS NOT NULL;

		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

		--Viaje_Paquete
		SET @tabla = 'Viaje_Paquete';

		INSERT INTO MONKEY_D_BASE.Viaje_Paquete (
			paquete_cantidad,
			paquete_precio_hist,
			tipo_id,
			viaje_id	)
		SELECT  
			sum(m.PAQUETE_CANTIDAD),
			m.PAQUETE_PRECIO,
			pt.id,
			v.id
		FROM gd_esquema.Maestra m
		JOIN MONKEY_D_BASE.Paquete_Tipo pt ON pt.descripcion = m.PAQUETE_DESCRIPCION
		JOIN MONKEY_D_BASE.Viaje v ON v.fecha_inicio = m.VIAJE_FECHA_INICIO AND v.fecha_fin = m.VIAJE_FECHA_FIN AND v.combustible_consumido = m.VIAJE_CONSUMO_COMBUSTIBLE
		AND v.precio_recorrido_his = m.RECORRIDO_PRECIO AND v.chofer_legajo = m.CHOFER_NRO_LEGAJO
		JOIN MONKEY_D_BASE.Recorrido r ON r.ciudad_origen = m.RECORRIDO_CIUDAD_ORIGEN AND r.ciudad_destino = m.RECORRIDO_CIUDAD_DESTINO AND v.recorrido_codigo = r.id
		JOIN MONKEY_D_BASE.Camion c ON c.patente = m.CAMION_PATENTE AND c.id = v.camion_codigo
		WHERE m.VIAJE_FECHA_INICIO IS NOT NULL
		GROUP BY m.PAQUETE_PRECIO,pt.id,v.id; -- paquete_precio_hist representa lo que se cobro por tipo de paquete por unidad
		
		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

		--Orden_Trabajo
		SET @tabla = 'Orden_Trabajo';

		INSERT INTO MONKEY_D_BASE.Orden_Trabajo (
			fecha,
			estado_id,
			camion_id,
			taller_id	)
		SELECT DISTINCT 
			m.ORDEN_TRABAJO_FECHA,
			e.id,
			c.id, 
			tal.id
		FROM GD2C2021.gd_esquema.Maestra m
		JOIN MONKEY_D_BASE.Estado_OT e ON e.descripcion = m.ORDEN_TRABAJO_ESTADO 
		JOIN MONKEY_D_BASE.Taller tal ON tal.mail = m.TALLER_MAIL AND tal.direccion = m.TALLER_DIRECCION
		JOIN MONKEY_D_BASE.Camion c ON c.patente = m.CAMION_PATENTE
		WHERE m.ORDEN_TRABAJO_FECHA IS NOT NULL;

		EXEC MONKEY_D_BASE.Sp_registrarTabla @tabla;

		--Orden_Tarea
		SET @tabla = 'Orden_Tarea';

		INSERT INTO MONKEY_D_BASE.Orden_Tarea (
			fecha_planificada,
			fecha_ini_real,
			fecha_fin_real,
			orden_id,
			tarea_id,
			mecanico_legajo	)
		SELECT DISTINCT 
			m.TAREA_FECHA_INICIO_PLANIFICADO,
			m.TAREA_FECHA_INICIO,
			m.TAREA_FECHA_FIN,
			ot.id,
			t.id,
			e.legajo
		FROM gd_esquema.Maestra m
		JOIN MONKEY_D_BASE.Orden_Trabajo ot ON ot.fecha = m.ORDEN_TRABAJO_FECHA 
		JOIN MONKEY_D_BASE.Camion c ON ot.camion_id = c.id AND c.patente = m.CAMION_PATENTE
		JOIN MONKEY_D_BASE.Tarea t ON t.descripcion = m.TAREA_DESCRIPCION
		JOIN MONKEY_D_BASE.Empleado e ON e.legajo = m.MECANICO_NRO_LEGAJO;
		
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
*** LLENADO DE TABLAS ***
*************************/
BEGIN TRY 
	
	EXEC MONKEY_D_BASE.Sp_LlenadoParametricas;		-- Recorrido;Marca;Empleado_Tipo;Paquete_Tipo;Estado_OT;Material;Tarea_Tipo
	
	EXEC MONKEY_D_BASE.Sp_LlenadoSecundarias;		-- Empleado;Taller;Camion;Camion_Modelo;Tarea;Tarea_Material

	EXEC MONKEY_D_BASE.Sp_LlenadoPrincipales;		-- Viaje;Viaje_Paquete;Orden_trabajo;Orden_Tarea
	
	SELECT * FROM MONKEY_D_BASE.ControlTablas ORDER BY nombre; -- Se imprime por pantalla el resultado (para control interno)

END TRY

BEGIN CATCH

	SELECT * FROM MONKEY_D_BASE.ControlTablas ORDER BY nombre; -- Se imprime por pantalla el resultado (para control interno)

	THROW;

END CATCH
