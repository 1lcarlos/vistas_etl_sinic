-- Script para crear vistas e índices en todos los esquemas cunXXXXX
DO $$
DECLARE
    schema_name TEXT;
    sql_command TEXT;
    schemas TEXT[] := ARRAY[
    'cun25489'
];
BEGIN
    FOREACH schema_name IN ARRAY schemas
    LOOP
        RAISE NOTICE 'Procesando esquema: %', schema_name;
        
        -- Verificar si el esquema existe
        --IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = schema_name) THEN
          IF EXISTS (SELECT 1 FROM information_schema.schemata s WHERE s.schema_name = current_schema) THEN 
            RAISE NOTICE '  Creando vistas en esquema: %', schema_name;
            
            -- 1. Vista base
            sql_command := format('
            CREATE OR REPLACE VIEW %s.vw_base AS
            SELECT 
                -- col_responsablefuente atributos NOT NULL
                crf.t_id AS responsable_fuente_id,
                crf.fuente_administrativa AS crf_fuente_admin_id,
                
                -- col_rrrfuente atributos NOT NULL
                crrrf.t_id AS rrr_fuente_id,
                crrrf.fuente_administrativa AS crrrf_fuente_admin_id,
                
                -- col_unidadfuente atributos NOT NULL
                cuf.t_id AS unidad_fuente_id,
                cuf.fuente_administrativa AS cuf_fuente_admin_id,
                cuf.unidad AS unidad_predio_id,
                
                -- gc_fuenteadministrativa atributos NOT NULL
                COALESCE(gfa_unidad.t_id, gfa_resp.t_id, gfa_rrr.t_id) AS fuente_admin_id,
                COALESCE(gfa_unidad.tipo, gfa_resp.tipo, gfa_rrr.tipo) AS fuente_tipo,
                COALESCE(gfa_unidad.estado_disponibilidad, gfa_resp.estado_disponibilidad, gfa_rrr.estado_disponibilidad) AS estado_disponibilidad,
                COALESCE(gfa_unidad.espacio_de_nombres, gfa_resp.espacio_de_nombres, gfa_rrr.espacio_de_nombres) AS fuente_espacio_nombres,
                COALESCE(gfa_unidad.local_id, gfa_resp.local_id, gfa_rrr.local_id) AS fuente_local_id,
                
                -- gc_interesado atributos NOT NULL
                gi.t_id AS interesado_id,
                gi.tipo_documento,
                gi.sexo,
                gi.comienzo_vida_util_version AS interesado_cvu_version,
                gi.espacio_de_nombres AS interesado_espacio_nombres,
                gi.local_id AS interesado_local_id,
                
                -- gc_predio_tramitecatastral atributos NOT NULL
                gptc.t_id AS predio_tramite_id,
                gptc.cr_predio,
                gptc.cr_tramite_catastral,
                
                -- gc_tramitecatastral atributos NOT NULL
                gtc.t_id AS tramite_id,
                gtc.clasificacion_mutacion,
                gtc.numero_resolucion,
                gtc.fecha_resolucion,
                gtc.fecha_radicacion,
                gtc.fecha_inscripcion,
                
                -- gc_derechocatastral atributos NOT NULL
                gdc.t_id AS derecho_id,
                gdc.tipo AS derecho_tipo,
                gdc.comienzo_vida_util_version AS derecho_cvu_version,
                gdc.espacio_de_nombres AS derecho_espacio_nombres,
                gdc.local_id AS derecho_local_id,
                
                -- gc_predio atributos NOT NULL
                gp.t_id AS predio_id,
                gp.departamento,
                gp.municipio,
                gp.numero_predial_nacional,
                gp.codigo_homologado,
                gp.tipo_predio,
                gp.condicion_predio,
                gp.destinacion_economica,
                gp.area_catastral_terreno,
                gp.vigencia_actualizacion_catastral,
                gp.estado,
                gp.clase_suelo,
                gp.tipo AS predio_tipo,
                gp.comienzo_vida_util_version AS predio_cvu_version,
                gp.espacio_de_nombres AS predio_espacio_nombres,
                gp.local_id AS predio_local_id

            FROM %s.gc_predio gp
            LEFT JOIN %s.gc_predio_tramitecatastral gptc ON gp.t_id = gptc.cr_predio
            LEFT JOIN %s.gc_tramitecatastral gtc ON gptc.cr_tramite_catastral = gtc.t_id
            LEFT JOIN %s.gc_derechocatastral gdc ON gp.t_id = gdc.unidad
            LEFT JOIN %s.gc_interesado gi ON gdc.interesado_gc_interesado = gi.t_id
            LEFT JOIN %s.col_unidadfuente cuf ON gp.t_id = cuf.unidad
            LEFT JOIN %s.col_responsablefuente crf ON gi.t_id = crf.interesado_gc_interesado
            LEFT JOIN %s.col_rrrfuente crrrf ON gdc.t_id = crrrf.rrr_gc_derechocatastral
            LEFT JOIN %s.gc_fuenteadministrativa gfa_unidad ON cuf.fuente_administrativa = gfa_unidad.t_id
            LEFT JOIN %s.gc_fuenteadministrativa gfa_resp ON crf.fuente_administrativa = gfa_resp.t_id
            LEFT JOIN %s.gc_fuenteadministrativa gfa_rrr ON crrrf.fuente_administrativa = gfa_rrr.t_id;',
            schema_name, schema_name, schema_name, schema_name, schema_name, 
            schema_name, schema_name, schema_name, schema_name, schema_name,
            schema_name, schema_name);
            
            EXECUTE sql_command;
            
            -- Índices para la vista base
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gc_predio_tid ON %s.gc_predio(t_id);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gptc_cr_predio ON %s.gc_predio_tramitecatastral(cr_predio);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gptc_cr_tramite ON %s.gc_predio_tramitecatastral(cr_tramite_catastral);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gtc_tid ON %s.gc_tramitecatastral(t_id);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gdc_unidad ON %s.gc_derechocatastral(unidad);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gdc_tid ON %s.gc_derechocatastral(t_id);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gdc_interesado ON %s.gc_derechocatastral(interesado_gc_interesado);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gi_tid ON %s.gc_interesado(t_id);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_cuf_unidad ON %s.col_unidadfuente(unidad);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_cuf_fuente_admin ON %s.col_unidadfuente(fuente_administrativa);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_crf_interesado ON %s.col_responsablefuente(interesado_gc_interesado);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_crf_fuente_admin ON %s.col_responsablefuente(fuente_administrativa);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_crrrf_rrr ON %s.col_rrrfuente(rrr_gc_derechocatastral);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_crrrf_fuente_admin ON %s.col_rrrfuente(fuente_administrativa);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gfa_tid ON %s.gc_fuenteadministrativa(t_id);', schema_name);
            
            -- 2. Vista mutación de primera
            EXECUTE format('CREATE OR REPLACE VIEW %s.vw_mutacion_de_primera AS SELECT * FROM %s.vw_base;', schema_name, schema_name);
            
            -- 3. Vista mutación de segunda
            sql_command := format('
            CREATE OR REPLACE VIEW %s.vw_mutacion_de_segunda AS
            SELECT 
                vb.*,
                cav.t_id AS areavalor_id,
                cav.tipo AS areavalor_tipo,
                cav.area AS areavalor_area,
                cue.t_id AS uebaunit_id,
                cue.baunit AS uebaunit_baunit_id,
                ed.t_id AS direccion_id,
                ed.tipo_direccion,
                gea.t_id AS estructura_avaluo_id,
                gea.fecha_avaluo,
                gea.avaluo_catastral,
                gea.autoestimacion,
                gepm.t_id AS estructura_predio_matriz_id,
                gepm.numero_predial_nacional AS npn_predio_matriz,
                gt.t_id AS terreno_id,
                gt.geometria,
                gt.comienzo_vida_util_version AS terreno_cvu_version,
                gt.espacio_de_nombres AS terreno_espacio_nombres,
                gt.local_id AS terreno_local_id
            FROM %s.vw_base vb
            LEFT JOIN %s.col_uebaunit cue_terreno ON vb.predio_id = cue_terreno.baunit
            LEFT JOIN %s.gc_terreno gt ON cue_terreno.ue_gc_terreno = gt.t_id
            LEFT JOIN %s.col_areavalor cav ON gt.t_id = cav.gc_terreno_area  
            LEFT JOIN %s.col_uebaunit cue ON vb.predio_id = cue.baunit
            LEFT JOIN %s.extdireccion ed ON vb.predio_id = ed.gc_predio_direccion
            LEFT JOIN %s.gc_estructuraavaluo gea ON vb.predio_id = gea.gc_predio_avaluo
            LEFT JOIN %s.gc_estructuraprediomatriznpn gepm ON vb.predio_id = gepm.gc_predio_predio_matriz_npn;',
            schema_name, schema_name, schema_name, schema_name, schema_name, 
            schema_name, schema_name, schema_name, schema_name);
            
            EXECUTE sql_command;
            
            -- Índices para vw_mutacion_de_segunda
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_cav_terreno ON %s.col_areavalor(gc_terreno_area);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_cav_tid ON %s.col_areavalor(t_id);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_cue_baunit ON %s.col_uebaunit(baunit);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_cue_ue_terreno ON %s.col_uebaunit(ue_gc_terreno);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_ed_predio ON %s.extdireccion(gc_predio_direccion);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_ed_tid ON %s.extdireccion(t_id);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gea_predio ON %s.gc_estructuraavaluo(gc_predio_avaluo);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gea_tid ON %s.gc_estructuraavaluo(t_id);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gepm_predio ON %s.gc_estructuraprediomatriznpn(gc_predio_predio_matriz_npn);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gepm_tid ON %s.gc_estructuraprediomatriznpn(t_id);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gt_tid ON %s.gc_terreno(t_id);', schema_name);
            
            -- 4. Vista mutación de segunda 2
            sql_command := format('
            CREATE OR REPLACE VIEW %s.vw_mutacion_de_segunda_2 AS
            SELECT 
                vb.*,
                cav.t_id AS areavalor_id,
                cav.tipo AS areavalor_tipo,
                cav.area AS areavalor_area,
                cue.t_id AS uebaunit_id,
                cue.baunit AS uebaunit_baunit_id,
                ed.t_id AS direccion_id,
                ed.tipo_direccion,
                gea.t_id AS estructura_avaluo_id,
                gea.fecha_avaluo,
                gea.avaluo_catastral,
                gea.autoestimacion,
                gepm.t_id AS estructura_predio_matriz_id,
                gepm.numero_predial_nacional AS npn_predio_matriz,
                gt.t_id AS terreno_id,
                gt.geometria,
                gt.comienzo_vida_util_version AS terreno_cvu_version,
                gt.espacio_de_nombres AS terreno_espacio_nombres,
                gt.local_id AS terreno_local_id,
                gdm.t_id AS datos_matriz_id,
                gdm.cr_predio AS datos_matriz_predio_id,
                gpc.t_id AS copropiedad_id,
                gpc.unidad_predial AS copropiedad_unidad_predial,
                gpc.matriz AS copropiedad_matriz,
                gpc.coeficiente AS copropiedad_coeficiente,
                gpc.area_catastral_terreno_coeficiente AS copropiedad_area_coeficiente
            FROM %s.vw_base vb
            LEFT JOIN %s.col_uebaunit cue_terreno ON vb.predio_id = cue_terreno.baunit
            LEFT JOIN %s.gc_terreno gt ON cue_terreno.ue_gc_terreno = gt.t_id
            LEFT JOIN %s.col_areavalor cav ON gt.t_id = cav.gc_terreno_area  
            LEFT JOIN %s.col_uebaunit cue ON vb.predio_id = cue.baunit
            LEFT JOIN %s.extdireccion ed ON vb.predio_id = ed.gc_predio_direccion
            LEFT JOIN %s.gc_estructuraavaluo gea ON vb.predio_id = gea.gc_predio_avaluo
            LEFT JOIN %s.gc_estructuraprediomatriznpn gepm ON vb.predio_id = gepm.gc_predio_predio_matriz_npn
            LEFT JOIN %s.gc_datosmatriz gdm ON vb.predio_id = gdm.cr_predio
            LEFT JOIN %s.gc_predio_copropiedad gpc ON vb.predio_id = gpc.unidad_predial;',
            schema_name, schema_name, schema_name, schema_name, schema_name,
            schema_name, schema_name, schema_name, schema_name, schema_name,
            schema_name, schema_name);
            
            EXECUTE sql_command;
            
            -- Índices adicionales para vw_mutacion_de_segunda_2
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gdm_predio ON %s.gc_datosmatriz(cr_predio);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gdm_tid ON %s.gc_datosmatriz(t_id);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gpc_unidad_predial ON %s.gc_predio_copropiedad(unidad_predial);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gpc_tid ON %s.gc_predio_copropiedad(t_id);', schema_name);
            
            -- 5. Vista mutación tercera 1
            sql_command := format('
            CREATE OR REPLACE VIEW %s.vw_mutacion_tercera_1 AS
            SELECT 
                vb.predio_id,
                vb.departamento,
                vb.municipio,
                vb.numero_predial_nacional,
                vb.codigo_homologado,
                vb.tipo_predio,
                vb.condicion_predio,
                vb.destinacion_economica,
                vb.area_catastral_terreno,
                vb.vigencia_actualizacion_catastral,
                vb.estado,
                vb.clase_suelo,
                vb.predio_tipo,
                vb.predio_cvu_version,
                vb.predio_espacio_nombres,
                vb.predio_local_id,
                vb.tramite_id,
                vb.clasificacion_mutacion,
                vb.numero_resolucion,
                vb.fecha_resolucion,
                vb.fecha_radicacion,
                vb.fecha_inscripcion,
                vb.predio_tramite_id,
                vb.cr_predio,
                vb.cr_tramite_catastral,
                cue.t_id AS uebaunit_id,
                cue.baunit AS uebaunit_baunit_id,
                gea.t_id AS estructura_avaluo_id,
                gea.fecha_avaluo,
                gea.avaluo_catastral,
                gea.autoestimacion,
                ed.t_id AS direccion_id,
                ed.tipo_direccion
            FROM %s.vw_base vb
            LEFT JOIN %s.col_uebaunit cue ON vb.predio_id = cue.baunit
            LEFT JOIN %s.gc_estructuraavaluo gea ON vb.predio_id = gea.gc_predio_avaluo
            LEFT JOIN %s.extdireccion ed ON vb.predio_id = ed.gc_predio_direccion;',
            schema_name, schema_name, schema_name, schema_name, schema_name);
            
            EXECUTE sql_command;
            
            -- 6. Vista mutación tercera 2
            sql_command := format('
            CREATE OR REPLACE VIEW %s.vw_mutacion_tercera_2 AS
            SELECT 
                vb.predio_id,
                vb.departamento,
                vb.municipio,
                vb.numero_predial_nacional,
                vb.codigo_homologado,
                vb.tipo_predio,
                vb.condicion_predio,
                vb.destinacion_economica,
                vb.area_catastral_terreno,
                vb.vigencia_actualizacion_catastral,
                vb.estado,
                vb.clase_suelo,
                vb.predio_tipo,
                vb.predio_cvu_version,
                vb.predio_espacio_nombres,
                vb.predio_local_id,
                vb.tramite_id,
                vb.clasificacion_mutacion,
                vb.numero_resolucion,
                vb.fecha_resolucion,
                vb.fecha_radicacion,
                vb.fecha_inscripcion,
                vb.predio_tramite_id,
                vb.cr_predio,
                vb.cr_tramite_catastral,
                cue.t_id AS uebaunit_id,
                cue.baunit AS uebaunit_baunit_id,
                guc.t_id AS unidad_construccion_id,
                guc.tipo_planta,
                guc.planta_ubicacion,
                guc.cr_caracteristicasunidadconstruccion,
                guc.comienzo_vida_util_version AS unidad_construccion_cvu_version,
                guc.espacio_de_nombres AS unidad_construccion_espacio_nombres,
                guc.local_id AS unidad_construccion_local_id,
                ccuc.t_id AS caracteristicas_construccion_id,
                ccuc.identificador,
                ccuc.tipo_unidad_construccion,
                ccuc.total_plantas,
                ccuc.uso,
                ccuc.anio_construccion,
                ccuc.area_construida,
                ccuc.estado_conservacion,
                gea.t_id AS estructura_avaluo_id,
                gea.fecha_avaluo,
                gea.avaluo_catastral,
                gea.autoestimacion,
                geac.t_id AS avaluo_caracterizacion_id,
                geac.fecha_avaluo AS fecha_avaluo_caracterizacion,
                geac.avaluo_catastral AS avaluo_catastral_caracterizacion
            FROM %s.vw_base vb
            LEFT JOIN %s.col_uebaunit cue ON vb.predio_id = cue.baunit
            LEFT JOIN %s.gc_unidadconstruccion guc ON cue.ue_gc_unidadconstruccion = guc.t_id
            LEFT JOIN %s.gc_caracteristicasunidadconstruccion ccuc ON guc.cr_caracteristicasunidadconstruccion = ccuc.t_id
            LEFT JOIN %s.gc_estructuraavaluo gea ON vb.predio_id = gea.gc_predio_avaluo
            LEFT JOIN %s.gc_estructuraavaluocaracterizacion geac ON ccuc.t_id = geac.gc_crctrstcdcnstrccion_avaluo_caracterizacion;',
            schema_name, schema_name, schema_name, schema_name, schema_name,
            schema_name, schema_name);
            
            EXECUTE sql_command;
            
            -- Índices adicionales para vw_mutacion_tercera_2
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_cue_ue_construccion ON %s.col_uebaunit(ue_gc_unidadconstruccion);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_guc_tid ON %s.gc_unidadconstruccion(t_id);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_guc_caracteristicas ON %s.gc_unidadconstruccion(cr_caracteristicasunidadconstruccion);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_ccuc_tid ON %s.gc_caracteristicasunidadconstruccion(t_id);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_geac_caracteristicas ON %s.gc_estructuraavaluocaracterizacion(gc_crctrstcdcnstrccion_avaluo_caracterizacion);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_geac_tid ON %s.gc_estructuraavaluocaracterizacion(t_id);', schema_name);
            
            -- 7. Vista mutación tercera 3
            sql_command := format('
            CREATE OR REPLACE VIEW %s.vw_mutacion_tercera_3 AS
            SELECT 
                vb.predio_id,
                vb.departamento,
                vb.municipio,
                vb.numero_predial_nacional,
                vb.codigo_homologado,
                vb.tipo_predio,
                vb.condicion_predio,
                vb.destinacion_economica,
                vb.area_catastral_terreno,
                vb.vigencia_actualizacion_catastral,
                vb.estado,
                vb.clase_suelo,
                vb.predio_tipo,
                vb.predio_cvu_version,
                vb.predio_espacio_nombres,
                vb.predio_local_id,
                vb.predio_tramite_id,
                vb.cr_predio,
                vb.cr_tramite_catastral,
                vb.tramite_id,
                vb.clasificacion_mutacion,
                vb.numero_resolucion,
                vb.fecha_resolucion,
                vb.fecha_radicacion,
                vb.fecha_inscripcion
            FROM %s.vw_base vb;',
            schema_name, schema_name);
            
            EXECUTE sql_command;
            
            -- 8. Vista mutación cuarta 1
            sql_command := format('
            CREATE OR REPLACE VIEW %s.vw_mutacion_de_cuarta_1 AS
            SELECT 
                vb.predio_id,
                vb.departamento,
                vb.municipio,
                vb.numero_predial_nacional,
                vb.codigo_homologado,
                vb.tipo_predio,
                vb.condicion_predio,
                vb.destinacion_economica,
                vb.area_catastral_terreno,
                vb.vigencia_actualizacion_catastral,
                vb.estado,
                vb.clase_suelo,
                vb.predio_tipo,
                vb.predio_cvu_version,
                vb.predio_espacio_nombres,
                vb.predio_local_id,
                vb.tramite_id,
                vb.clasificacion_mutacion,
                vb.numero_resolucion,
                vb.fecha_resolucion,
                vb.fecha_radicacion,
                vb.fecha_inscripcion,
                vb.predio_tramite_id,
                vb.cr_predio,
                vb.cr_tramite_catastral,
                vb.derecho_id,
                vb.derecho_tipo,
                vb.derecho_cvu_version,
                vb.derecho_espacio_nombres,
                vb.derecho_local_id,
                vb.interesado_id,
                vb.tipo_documento,
                vb.sexo,
                vb.interesado_cvu_version,
                vb.interesado_espacio_nombres,
                vb.interesado_local_id,
                vb.unidad_fuente_id,
                vb.cuf_fuente_admin_id,
                vb.unidad_predio_id,
                vb.rrr_fuente_id,
                vb.crrrf_fuente_admin_id,  
                vb.fuente_admin_id,
                vb.fuente_tipo,
                vb.estado_disponibilidad,
                vb.fuente_espacio_nombres,
                vb.fuente_local_id,
                cav.t_id AS areavalor_id,
                cav.tipo AS areavalor_tipo,
                cav.area AS areavalor_area,
                cue.t_id AS uebaunit_id,
                cue.baunit AS uebaunit_baunit_id,
                gea.t_id AS estructura_avaluo_id,
                gea.fecha_avaluo,
                gea.avaluo_catastral,
                gea.autoestimacion,
                ed.t_id AS direccion_id,
                ed.tipo_direccion
            FROM %s.vw_base vb
            LEFT JOIN %s.col_areavalor cav ON vb.predio_id = cav.gc_terreno_area
            LEFT JOIN %s.col_uebaunit cue ON vb.predio_id = cue.baunit
            LEFT JOIN %s.gc_estructuraavaluo gea ON vb.predio_id = gea.gc_predio_avaluo
            LEFT JOIN %s.extdireccion ed ON vb.predio_id = ed.gc_predio_direccion;',
            schema_name, schema_name, schema_name, schema_name, schema_name,
            schema_name);
            
            EXECUTE sql_command;
            
            -- 9. Vista mutación cuarta 2
            sql_command := format('
            CREATE OR REPLACE VIEW %s.vw_mutacion_de_cuarta_2 AS
            SELECT 
                vb.predio_id,
                vb.departamento,
                vb.municipio,
                vb.numero_predial_nacional,
                vb.codigo_homologado,
                vb.tipo_predio,
                vb.condicion_predio,
                vb.destinacion_economica,
                vb.area_catastral_terreno,
                vb.vigencia_actualizacion_catastral,
                vb.estado,
                vb.clase_suelo,
                vb.predio_tipo,
                vb.predio_cvu_version,
                vb.predio_espacio_nombres,
                vb.predio_local_id,
                vb.tramite_id,
                vb.clasificacion_mutacion,
                vb.numero_resolucion,
                vb.fecha_resolucion,
                vb.fecha_radicacion,
                vb.fecha_inscripcion,
                vb.predio_tramite_id,
                vb.cr_predio,
                vb.cr_tramite_catastral,
                vb.unidad_fuente_id,
                vb.cuf_fuente_admin_id AS unidad_fuente_admin_id,
                vb.unidad_predio_id,
                vb.fuente_admin_id,
                vb.fuente_tipo,
                vb.estado_disponibilidad,
                vb.fuente_espacio_nombres,
                vb.fuente_local_id,
                gea.t_id AS estructura_avaluo_id,
                gea.fecha_avaluo,
                gea.avaluo_catastral,
                gea.autoestimacion
            FROM %s.vw_base vb
            LEFT JOIN %s.gc_estructuraavaluo gea ON vb.predio_id = gea.gc_predio_avaluo;',
            schema_name, schema_name, schema_name);
            
            EXECUTE sql_command;
            
            -- 10. Vista mutación quinta
            sql_command := format('
            CREATE OR REPLACE VIEW %s.vw_mutacion_de_quinta AS
            SELECT 
                vb.*,
                cav.t_id AS areavalor_id,
                cav.tipo AS areavalor_tipo,
                cav.area AS areavalor_area,
                cue.t_id AS uebaunit_id,
                cue.baunit AS uebaunit_baunit_id,
                gea.t_id AS estructura_avaluo_id,
                gea.fecha_avaluo,
                gea.avaluo_catastral,
                gea.autoestimacion,
                ed.t_id AS direccion_id,
                ed.tipo_direccion
            FROM %s.vw_base vb
            LEFT JOIN %s.col_areavalor cav ON vb.predio_id = cav.gc_terreno_area
            LEFT JOIN %s.col_uebaunit cue ON vb.predio_id = cue.baunit
            LEFT JOIN %s.gc_estructuraavaluo gea ON vb.predio_id = gea.gc_predio_avaluo
            LEFT JOIN %s.extdireccion ed ON vb.predio_id = ed.gc_predio_direccion;',
            schema_name, schema_name, schema_name, schema_name, schema_name,
            schema_name);
            
            EXECUTE sql_command;
            
            -- 11. Vista mutación quinta 1
            sql_command := format('
            CREATE OR REPLACE VIEW %s.vw_mutacion_de_quinta_1 AS
            SELECT 
                vb.*,
                cav.t_id AS areavalor_id,
                cav.tipo AS areavalor_tipo,
                cav.area AS areavalor_area,
                cue.t_id AS uebaunit_id,
                cue.baunit AS uebaunit_baunit_id,
                gea.t_id AS estructura_avaluo_id,
                gea.fecha_avaluo,
                gea.avaluo_catastral,
                gea.autoestimacion,
                ed.t_id AS direccion_id,
                ed.tipo_direccion,
                gpi.t_id AS predio_informalidad_id,
                gpi.cr_predio_formal,
                gpi.cr_predio_informal,
                gpi.area_terreno_interseccion,
                gpi.area_construida_interseccion,
                geai.t_id AS avaluo_interseccion_id,
                geai.avaluo_catastral_interseccion_terreno,
                geai.avaluo_catastral_interseccion_unidades_construccion,
                geai.avaluo_catastral_interseccion_total
            FROM %s.vw_base vb
            LEFT JOIN %s.col_areavalor cav ON vb.predio_id = cav.gc_terreno_area
            LEFT JOIN %s.col_uebaunit cue ON vb.predio_id = cue.baunit
            LEFT JOIN %s.gc_estructuraavaluo gea ON vb.predio_id = gea.gc_predio_avaluo
            LEFT JOIN %s.extdireccion ed ON vb.predio_id = ed.gc_predio_direccion
            LEFT JOIN %s.gc_predio_informalidad gpi ON vb.predio_id = gpi.cr_predio_formal 
                OR vb.predio_id = gpi.cr_predio_informal
            LEFT JOIN %s.gc_estructuraavaluointerseccion geai 
                ON gpi.t_id = geai.gc_predio_informalidad_avaluo_catastral_interseccion;',
            schema_name, schema_name, schema_name, schema_name, schema_name,
            schema_name, schema_name, schema_name);
            
            EXECUTE sql_command;
            
            -- Índices adicionales para vw_mutacion_de_quinta_1
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gpi_formal ON %s.gc_predio_informalidad(cr_predio_formal);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gpi_informal ON %s.gc_predio_informalidad(cr_predio_informal);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_gpi_tid ON %s.gc_predio_informalidad(t_id);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_geai_informalidad ON %s.gc_estructuraavaluointerseccion(gc_predio_informalidad_avaluo_catastral_interseccion);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_geai_tid ON %s.gc_estructuraavaluointerseccion(t_id);', schema_name);
            
            -- 12. Vista rectificaciones EIC
            sql_command := format('
            CREATE OR REPLACE VIEW %s.vw_Rectificaciones_EIC AS
            SELECT 
                vb.*,
                cav.t_id AS areavalor_id,
                cav.tipo AS areavalor_tipo,
                cav.area AS areavalor_area,
                cue.t_id AS uebaunit_id,
                cue.baunit AS uebaunit_baunit_id,
                gea.t_id AS estructura_avaluo_id,
                gea.fecha_avaluo,
                gea.avaluo_catastral,
                gea.autoestimacion,
                ed.t_id AS direccion_id,
                ed.tipo_direccion
            FROM %s.vw_base vb
            LEFT JOIN %s.col_areavalor cav ON vb.predio_id = cav.gc_terreno_area
            LEFT JOIN %s.col_uebaunit cue ON vb.predio_id = cue.baunit
            LEFT JOIN %s.gc_estructuraavaluo gea ON vb.predio_id = gea.gc_predio_avaluo
            LEFT JOIN %s.extdireccion ed ON vb.predio_id = ed.gc_predio_direccion;',
            schema_name, schema_name, schema_name, schema_name, schema_name,
            schema_name);
            
            EXECUTE sql_command;
            
            -- 13. Vista modificaciones
            sql_command := format('
            CREATE OR REPLACE VIEW %s.vw_modificaciones AS
            SELECT 
                vb.*,
                cue.t_id AS uebaunit_id,
                cue.baunit AS uebaunit_baunit_id,
                gea.t_id AS estructura_avaluo_id,
                gea.fecha_avaluo,
                gea.avaluo_catastral,
                gea.autoestimacion,
                genpn.t_id AS origen_npn_id,
                genpn.numero_predial_nacional AS npn_anterior,
                genpn.fecha_cambio_npn
            FROM %s.vw_base vb
            LEFT JOIN %s.col_uebaunit cue ON vb.predio_id = cue.baunit
            LEFT JOIN %s.gc_estructuraavaluo gea ON vb.predio_id = gea.gc_predio_avaluo
            LEFT JOIN %s.gc_estructurapredioorigennpn genpn ON vb.predio_id = genpn.gc_predio_predio_origen_npn;',
            schema_name, schema_name, schema_name, schema_name, schema_name);
            
            EXECUTE sql_command;
            
            -- Índices adicionales para vw_modificaciones
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_genpn_predio ON %s.gc_estructurapredioorigennpn(gc_predio_predio_origen_npn);', schema_name);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_genpn_tid ON %s.gc_estructurapredioorigennpn(t_id);', schema_name);
            
            -- 14. Vista modificaciones y cancelaciones de gc_predio
            EXECUTE format('
            CREATE OR REPLACE VIEW %s.vw_mc_gc_predio AS
            SELECT 
                t_id,
                departamento,
                municipio,
                numero_predial_nacional,
                codigo_homologado,
                tipo_predio,
                condicion_predio,
                destinacion_economica,
                area_catastral_terreno,
                vigencia_actualizacion_catastral,
                estado,
                clase_suelo,
                tipo,
                comienzo_vida_util_version,
                espacio_de_nombres,
                local_id
            FROM %s.gc_predio;',
            schema_name, schema_name);
            
            RAISE NOTICE '  ✓ Vistas creadas en esquema: %', schema_name;
        ELSE
            RAISE NOTICE '  ✗ Esquema % no existe, omitiendo...', schema_name;
        END IF;
    END LOOP;
    
    RAISE NOTICE '';
    RAISE NOTICE '=== PROCESO COMPLETADO ===';
    RAISE NOTICE 'Se procesaron todos los esquemas.';
END;
$$;



select * from cun25489.vw_mutacion_de_primera