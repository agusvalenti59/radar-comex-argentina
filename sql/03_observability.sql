-- Copyright (c) 2026 Agustin Valenti Boursiac. Todos los derechos reservados.
-- Prohibido copiar, modificar o redistribuir sin permiso escrito del autor. Ver LICENSE.
-- Observabilidad: queries para dashboard operativo en Databricks SQL + alertas.

-- 1) Éxito/duración de tareas propias (auditoría del pipeline)
SELECT task, status, count(*) AS runs, round(avg(duration_s), 1) AS avg_s, max(started_at) AS last_run
FROM ops.pipeline_runs
WHERE started_at >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY task, status
ORDER BY task;

-- 2) Tendencia de calidad: % de filas en cuarentena por corrida
SELECT measured_at,
       max(CASE WHEN metric = 'rows_quarantined' THEN value END) /
       max(CASE WHEN metric = 'rows_in' THEN value END) AS quarantine_ratio
FROM ops.dq_metrics
WHERE `table` = 'silver.trade_flows'
GROUP BY measured_at
ORDER BY measured_at DESC;
-- ALERTA sugerida: quarantine_ratio > 0.02

-- 3) Frescura por fuente (la tarea freshness falla el job; esto es para el tablero)
SELECT `table`, status, age_h, sla_h, checked_at
FROM ops.freshness
QUALIFY row_number() OVER (PARTITION BY `table` ORDER BY checked_at DESC) = 1;

-- 4) Costo por job (system tables)
SELECT usage_metadata.job_id, j.name, sum(usage_quantity) AS dbus, usage_date
FROM system.billing.usage u
LEFT JOIN system.lakeflow.jobs j ON j.job_id = u.usage_metadata.job_id
WHERE u.custom_tags['cost_center'] = 'trade-intel'
  AND usage_date >= current_date() - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY usage_date DESC;

-- 5) Corridas fallidas de Workflows
SELECT job_id, run_id, result_state, period_start_time, period_end_time
FROM system.lakeflow.job_run_timeline
WHERE result_state IN ('FAILED', 'TIMED_OUT')
  AND period_start_time >= current_timestamp() - INTERVAL 7 DAYS;
