const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
const dotenv = require('dotenv');

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3001;
const DEMO_EMAIL = process.env.DEMO_EMAIL || 'admin@leadflow.com';
const DEMO_PASSWORD = process.env.DEMO_PASSWORD || 'Admin1234!';
const DEMO_TOKEN = process.env.DEMO_TOKEN || 'leadflow-demo-token';
const allowedOrigins = (process.env.CORS_ORIGINS || process.env.FRONTEND_URL || '')
  .split(',')
  .map(origin => origin.trim())
  .filter(Boolean);

const corsOptions = {
  origin: (origin, callback) => {
    if (!origin || allowedOrigins.length === 0 || allowedOrigins.includes(origin)) {
      return callback(null, true);
    }
    return callback(new Error('Origen no permitido por CORS'));
  },
  credentials: true,
};

app.use(cors(corsOptions));
app.options('*', cors(corsOptions));
app.use(express.json());

const WORKSHOP_DAY_ORDER_SQL = `
  CASE dia
    WHEN 'lunes' THEN 1
    WHEN 'martes' THEN 2
    WHEN 'miércoles' THEN 3
    WHEN 'jueves' THEN 4
    WHEN 'viernes' THEN 5
    WHEN 'sábado' THEN 6
    WHEN 'domingo' THEN 7
    ELSE 99
  END
`;

// ─── CONEXIÓN POSTGRESQL ──────────────────────────────────────────
// Railway inyecta DATABASE_URL automáticamente.
// Para desarrollo local: crea un .env con DATABASE_URL=postgresql://user:pass@localhost:5432/leadflow
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production'
    ? { rejectUnauthorized: false }   // Railway / Render requieren SSL
    : false,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

const query = (text, params) => pool.query(text, params);
let dbReady = false;
let dbInitError = null;

if (!process.env.DATABASE_URL) {
  console.warn('DATABASE_URL no está configurada. El backend quedará en modo degradado hasta conectarse a una DB.');
}

// ─── INICIALIZAR TABLAS ───────────────────────────────────────────
async function initDB() {
  await query(`
    CREATE TABLE IF NOT EXISTS leads (
      id              TEXT PRIMARY KEY,
      name            TEXT NOT NULL,
      company         TEXT,
      email           TEXT,
      phone           TEXT,
      channel         TEXT DEFAULT 'Web',
      status          TEXT DEFAULT 'Nuevo',
      priority        TEXT DEFAULT 'Media',
      estimated_value NUMERIC DEFAULT 0,
      notes           TEXT,
      agent           TEXT,
      created_at      TIMESTAMPTZ DEFAULT NOW(),
      updated_at      TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS activities (
      id         TEXT PRIMARY KEY,
      lead_id    TEXT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
      type       TEXT NOT NULL,
      content    TEXT,
      channel    TEXT,
      direction  TEXT DEFAULT 'inbound',
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS appointments (
      id           TEXT PRIMARY KEY,
      lead_id      TEXT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
      scheduled_at TIMESTAMPTZ NOT NULL,
      channel      TEXT,
      notes        TEXT,
      status       TEXT DEFAULT 'pending',
      created_at   TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS tasks (
      id         TEXT PRIMARY KEY,
      lead_id    TEXT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
      title      TEXT NOT NULL,
      due_date   TIMESTAMPTZ,
      completed  BOOLEAN DEFAULT FALSE,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS notes (
      id         TEXT PRIMARY KEY,
      lead_id    TEXT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
      content    TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS workshops (
      id                  TEXT PRIMARY KEY,
      razon_social        TEXT NOT NULL,
      nombre_comercial    TEXT NOT NULL,
      rut                 TEXT,
      encargado_nombre    TEXT,
      encargado_email     TEXT,
      encargado_phone     TEXT,
      finanzas_nombre     TEXT,
      finanzas_email      TEXT,
      finanzas_phone      TEXT,
      direccion           TEXT,
      comuna              TEXT,
      comunas_adicionales JSONB,
      latitud             NUMERIC,
      longitud            NUMERIC,
      maps_url            TEXT,
      puestos             INTEGER DEFAULT 1,
      turnos_por_puesto   INTEGER DEFAULT 1,
      aro_min             INTEGER DEFAULT 13,
      aro_max             INTEGER DEFAULT 22,
      instala_runflat     BOOLEAN DEFAULT FALSE,
      tipos_vehiculo      JSONB,
      marcas_neumaticos   JSONB,
      todas_marcas        BOOLEAN DEFAULT TRUE,
      active              BOOLEAN DEFAULT TRUE,
      created_at          TIMESTAMPTZ DEFAULT NOW(),
      updated_at          TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS workshop_schedules (
      id          TEXT PRIMARY KEY,
      workshop_id TEXT NOT NULL REFERENCES workshops(id) ON DELETE CASCADE,
      dia         TEXT NOT NULL,
      activo      BOOLEAN DEFAULT TRUE,
      hora_inicio TEXT DEFAULT '09:00',
      hora_fin    TEXT DEFAULT '18:00',
      horas       TEXT,
      created_at  TIMESTAMPTZ DEFAULT NOW(),
      updated_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE UNIQUE INDEX IF NOT EXISTS ux_workshop_schedules_workshop_dia
      ON workshop_schedules(workshop_id, dia);

    CREATE TABLE IF NOT EXISTS workshop_services (
      id          TEXT PRIMARY KEY,
      workshop_id TEXT NOT NULL REFERENCES workshops(id) ON DELETE CASCADE,
      nombre      TEXT NOT NULL,
      descripcion TEXT,
      created_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS workshop_prices (
      id          TEXT PRIMARY KEY,
      workshop_id TEXT NOT NULL REFERENCES workshops(id) ON DELETE CASCADE,
      tipo        TEXT NOT NULL,
      descripcion TEXT,
      aro_min     INTEGER,
      aro_max     INTEGER,
      precio      NUMERIC DEFAULT 0,
      created_at  TIMESTAMPTZ DEFAULT NOW(),
      updated_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS workshop_appointments (
      id          TEXT PRIMARY KEY,
      workshop_id TEXT NOT NULL REFERENCES workshops(id) ON DELETE CASCADE,
      lead_id     TEXT REFERENCES leads(id) ON DELETE SET NULL,
      fecha       DATE NOT NULL,
      hora        TEXT NOT NULL,
      puesto      INTEGER DEFAULT 1,
      notas       TEXT,
      order_id    TEXT,
      status      TEXT DEFAULT 'pending',
      created_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_leads_status    ON leads(status);
    CREATE INDEX IF NOT EXISTS idx_leads_channel   ON leads(channel);
    CREATE INDEX IF NOT EXISTS idx_activities_lead ON activities(lead_id);
    CREATE INDEX IF NOT EXISTS idx_tasks_lead      ON tasks(lead_id);
    CREATE INDEX IF NOT EXISTS idx_workshops_active ON workshops(active);
    CREATE INDEX IF NOT EXISTS idx_workshop_prices_workshop ON workshop_prices(workshop_id);
    CREATE INDEX IF NOT EXISTS idx_workshop_services_workshop ON workshop_services(workshop_id);
    CREATE INDEX IF NOT EXISTS idx_workshop_appointments_workshop_fecha ON workshop_appointments(workshop_id, fecha);
  `);

  const { rows } = await query('SELECT COUNT(*) as count FROM leads');
  if (parseInt(rows[0].count) === 0) await seedData();

  console.log('Tablas listas');
}

// ─── SEED DATA ────────────────────────────────────────────────────
async function seedData() {
  const leads = [
    { name: 'Maria Gonzalez',   company: 'Tech Innovate MX',   email: 'maria@techinnovate.mx',   phone: '+52 55 1234 5678', channel: 'WhatsApp', status: 'Nuevo',       priority: 'Alta',  estimated_value: 15000, notes: 'Interesada en plan enterprise. Contactar antes del viernes.', agent: 'AI Agent - WhatsApp' },
    { name: 'Carlos Hernandez', company: 'Startup Co',          email: 'carlos@startup.co',        phone: '+52 55 2345 6789', channel: 'Facebook', status: 'Contactado',  priority: 'Media', estimated_value: 8500,  notes: 'Solicita demo del producto.',                                agent: 'AI Agent - Facebook' },
    { name: 'Ana Martinez',     company: 'Boutique Digital',    email: 'ana@boutiquedigital.mx',   phone: '+52 55 3456 7890', channel: 'Instagram',status: 'Calificado',  priority: 'Alta',  estimated_value: 22000, notes: 'Lista para propuesta formal.',                               agent: 'AI Agent - Instagram' },
    { name: 'Roberto Lopez',    company: 'Empresa Global SA',   email: 'roberto@empresaglobal.mx', phone: '+52 55 4567 8901', channel: 'Chat',     status: 'Propuesta',   priority: 'Alta',  estimated_value: 45000, notes: 'Revisando propuesta con equipo.',                            agent: 'AI Agent - Chat' },
    { name: 'Laura Sanchez',    company: 'Consultoria Sanchez', email: 'laura@consultoria.mx',     phone: '+52 55 5678 9012', channel: 'Telefono', status: 'Negociacion', priority: 'Media', estimated_value: 32000, notes: 'Negociando precio final.',                                    agent: 'AI Agent - Telefono' },
    { name: 'Diego Ramirez',    company: 'WebFlow MX',          email: 'diego@webflow.mx',         phone: '+52 55 6789 0123', channel: 'Web',      status: 'Nuevo',       priority: 'Baja',  estimated_value: 3500,  notes: 'Lead frio del blog.',                                        agent: 'AI Agent - Web' },
    { name: 'Patricia Flores',  company: 'Marketing 360',       email: 'patricia@mkt360.mx',       phone: '+52 55 7890 1234', channel: 'WhatsApp', status: 'Ganado',      priority: 'Alta',  estimated_value: 28000, notes: 'Cliente cerrado. Onboarding iniciado.',                      agent: 'AI Agent - WhatsApp' },
    { name: 'Fernando Torres',  company: 'Retail MX',           email: 'fernando@retailmx.com',    phone: '+52 55 8901 2345', channel: 'Facebook', status: 'Perdido',     priority: 'Baja',  estimated_value: 5000,  notes: 'Eligio competidor.',                                         agent: 'AI Agent - Facebook' },
    { name: 'Alejandro Ruiz',   company: 'Innova Tech',         email: 'alejandro@innovatech.mx',  phone: '+52 55 9012 3456', channel: 'Telefono', status: 'Contactado',  priority: 'Media', estimated_value: 9500,  notes: 'Segunda llamada pendiente.',                                 agent: 'AI Agent - Telefono' },
    { name: 'Sofia Mendoza',    company: 'FinTech Solutions',   email: 'sofia@fintech.mx',         phone: '+52 55 0123 4567', channel: 'Instagram',status: 'Nuevo',       priority: 'Alta',  estimated_value: 18000, notes: 'Referida por Patricia Flores.',                              agent: 'AI Agent - Instagram' },
  ];

  let firstLeadId = null;
  for (const l of leads) {
    const id = uuidv4();
    if (!firstLeadId) firstLeadId = id;
    await query(
      `INSERT INTO leads (id, name, company, email, phone, channel, status, priority, estimated_value, notes, agent)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
      [id, l.name, l.company, l.email, l.phone, l.channel, l.status, l.priority, l.estimated_value, l.notes, l.agent]
    );
  }

  await query(
    `INSERT INTO activities (id, lead_id, type, content, channel, direction) VALUES ($1,$2,$3,$4,$5,$6)`,
    [uuidv4(), firstLeadId, 'Message', 'Hola, estoy interesada en conocer mas sobre sus servicios enterprise.', 'WhatsApp', 'inbound']
  );
  await query(
    `INSERT INTO activities (id, lead_id, type, content, channel, direction) VALUES ($1,$2,$3,$4,$5,$6)`,
    [uuidv4(), firstLeadId, 'Message', 'Hola! Con gusto te ayudo. Cuantos usuarios necesitarias?', 'WhatsApp', 'outbound']
  );
  console.log('Datos de ejemplo cargados');
}

// ─── ASYNC HANDLER ────────────────────────────────────────────────
const asyncHandler = fn => (req, res, next) =>
  Promise.resolve(fn(req, res, next)).catch(err => {
    console.error('API Error:', err.message);
    res.status(500).json({ error: 'Error interno del servidor', detail: err.message });
  });

const parseBoolean = (value, defaultValue = false) => {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['true', '1', 'yes', 'si', 'sí', 'on'].includes(normalized)) return true;
    if (['false', '0', 'no', 'off'].includes(normalized)) return false;
  }
  if (value === undefined || value === null) return defaultValue;
  return Boolean(value);
};

const parseInteger = (value, defaultValue) => {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : defaultValue;
};

const parseFloatNumber = (value, defaultValue) => {
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : defaultValue;
};

const normalizeTime = (value, fallback) => {
  if (value === undefined || value === null || value === '') return fallback;
  const match = String(value).trim().match(/^([01]\d|2[0-3]):([0-5]\d)/);
  if (!match) return fallback;
  return `${match[1]}:${match[2]}`;
};

const normalizeDia = value => {
  const normalized = String(value || '').trim().toLowerCase();
  const dayMap = {
    monday: 'lunes',
    mon: 'lunes',
    lunes: 'lunes',
    tuesday: 'martes',
    tue: 'martes',
    martes: 'martes',
    wednesday: 'miércoles',
    wed: 'miércoles',
    miercoles: 'miércoles',
    'miércoles': 'miércoles',
    thursday: 'jueves',
    thu: 'jueves',
    jueves: 'jueves',
    friday: 'viernes',
    fri: 'viernes',
    viernes: 'viernes',
    saturday: 'sábado',
    sat: 'sábado',
    sabado: 'sábado',
    'sábado': 'sábado',
    sunday: 'domingo',
    sun: 'domingo',
    domingo: 'domingo',
  };
  return dayMap[normalized] || normalized;
};

const getScheduleInput = body => body?.schedules || body?.horarios || [];

async function upsertWorkshopSchedules(workshopId, scheduleInput) {
  const schedules = (Array.isArray(scheduleInput) ? scheduleInput : [])
    .map(s => ({
      dia: normalizeDia(s.dia || s.day || s.weekday),
      activo: parseBoolean(s.activo ?? s.active ?? s.enabled, false),
      hora_inicio: normalizeTime(s.hora_inicio ?? s.horaInicio ?? s.start_time ?? s.start, '09:00'),
      hora_fin: normalizeTime(s.hora_fin ?? s.horaFin ?? s.end_time ?? s.end, '18:00'),
      horas: s.horas || null,
    }))
    .filter(s => s.dia);

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    for (const s of schedules) {
      await client.query(
        `INSERT INTO workshop_schedules (id, workshop_id, dia, activo, hora_inicio, hora_fin, horas)
         VALUES ($1,$2,$3,$4,$5,$6,$7)
         ON CONFLICT (workshop_id,dia)
         DO UPDATE SET
           activo=EXCLUDED.activo,
           hora_inicio=EXCLUDED.hora_inicio,
           hora_fin=EXCLUDED.hora_fin,
           horas=EXCLUDED.horas,
           updated_at=NOW()`,
        [uuidv4(), workshopId, s.dia, s.activo, s.hora_inicio, s.hora_fin, s.horas]
      );
    }

    const { rows } = await client.query(
      `SELECT * FROM workshop_schedules
       WHERE workshop_id = $1
       ORDER BY ${WORKSHOP_DAY_ORDER_SQL}`,
      [workshopId]
    );

    await client.query('COMMIT');
    return rows;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

// ─── HEALTH CHECK ─────────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.status(dbReady ? 200 : 503).json({
    status: dbReady ? 'ok' : 'degraded',
    db: dbReady ? 'connected' : 'disconnected',
    detail: dbInitError,
    timestamp: new Date().toISOString(),
  });
});

// ─── AUTH DEMO ────────────────────────────────────────────────────
const demoUser = {
  email: DEMO_EMAIL,
  name: 'LeadFlow Admin',
  role: 'admin',
};

const sendAuthPayload = res => res.json({ token: DEMO_TOKEN, user: demoUser });

const loginHandler = (req, res) => {
  const { email, password } = req.body || {};

  if (!email || !password) {
    return res.status(400).json({ error: 'Email y contraseña son requeridos' });
  }

  if (email.toLowerCase() !== DEMO_EMAIL.toLowerCase() || password !== DEMO_PASSWORD) {
    return res.status(401).json({ error: 'Credenciales inválidas' });
  }

  return sendAuthPayload(res);
};

app.post('/api/auth/login', loginHandler);
app.post('/api/login', loginHandler);

app.get('/api/auth/me', (req, res) => {
  const authHeader = req.headers.authorization || '';
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';

  if (token !== DEMO_TOKEN) {
    return res.status(401).json({ error: 'No autorizado' });
  }

  return res.json({ user: demoUser });
});

app.use('/api', (req, res, next) => {
  if (req.path === '/login' || req.path === '/auth/login' || req.path === '/auth/me') {
    return next();
  }

  if (!dbReady) {
    return res.status(503).json({
      error: 'Servicio temporalmente no disponible',
      detail: dbInitError || 'La base de datos aún no está disponible',
    });
  }

  return next();
});

// ─── DASHBOARD ────────────────────────────────────────────────────
app.get('/api/dashboard', asyncHandler(async (req, res) => {
  const [total, nuevos, ganados, valor, byChannel, recientes] = await Promise.all([
    query('SELECT COUNT(*) as count FROM leads'),
    query("SELECT COUNT(*) as count FROM leads WHERE status = 'Nuevo'"),
    query("SELECT COUNT(*) as count FROM leads WHERE status = 'Ganado'"),
    query('SELECT COALESCE(SUM(estimated_value), 0) as total FROM leads'),
    query('SELECT channel, COUNT(*) as count FROM leads GROUP BY channel ORDER BY count DESC'),
    query('SELECT * FROM leads ORDER BY created_at DESC LIMIT 10'),
  ]);

  res.json({
    totalLeads:  parseInt(total.rows[0].count),
    newLeads:    parseInt(nuevos.rows[0].count),
    wonLeads:    parseInt(ganados.rows[0].count),
    totalValue:  parseFloat(valor.rows[0].total),
    byChannel:   byChannel.rows,
    recentLeads: recientes.rows,
  });
}));

// ─── LEADS CRUD ───────────────────────────────────────────────────
app.get('/api/leads', asyncHandler(async (req, res) => {
  const { status, channel, search } = req.query;
  const conditions = ['1=1'];
  const params = [];
  let i = 1;

  if (status)  { conditions.push(`status = $${i++}`);  params.push(status); }
  if (channel) { conditions.push(`channel = $${i++}`); params.push(channel); }
  if (search)  {
    conditions.push(`(name ILIKE $${i} OR company ILIKE $${i} OR email ILIKE $${i})`);
    params.push(`%${search}%`);
    i++;
  }

  const { rows } = await query(
    `SELECT * FROM leads WHERE ${conditions.join(' AND ')} ORDER BY created_at DESC`,
    params
  );
  res.json(rows);
}));

app.get('/api/leads/:id', asyncHandler(async (req, res) => {
  const { id } = req.params;
  const { rows: [lead] } = await query('SELECT * FROM leads WHERE id = $1', [id]);
  if (!lead) return res.status(404).json({ error: 'Lead no encontrado' });

  const [activities, appointments, tasks, notes] = await Promise.all([
    query('SELECT * FROM activities   WHERE lead_id = $1 ORDER BY created_at DESC', [id]),
    query('SELECT * FROM appointments WHERE lead_id = $1 ORDER BY scheduled_at ASC', [id]),
    query('SELECT * FROM tasks        WHERE lead_id = $1 ORDER BY created_at ASC',   [id]),
    query('SELECT * FROM notes        WHERE lead_id = $1 ORDER BY created_at DESC',  [id]),
  ]);

  res.json({
    ...lead,
    activities:   activities.rows,
    appointments: appointments.rows,
    tasks:        tasks.rows,
    notes:        notes.rows,
  });
}));

app.post('/api/leads', asyncHandler(async (req, res) => {
  const { name, company, email, phone, channel, status, priority, estimated_value, notes, agent } = req.body;
  if (!name) return res.status(400).json({ error: 'El nombre es requerido' });

  const { rows: [lead] } = await query(
    `INSERT INTO leads (id, name, company, email, phone, channel, status, priority, estimated_value, notes, agent)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
    [uuidv4(), name, company, email, phone,
     channel  || 'Web',
     status   || 'Nuevo',
     priority || 'Media',
     parseFloat(estimated_value) || 0,
     notes, agent]
  );
  res.status(201).json(lead);
}));

app.put('/api/leads/:id', asyncHandler(async (req, res) => {
  const { name, company, email, phone, channel, status, priority, estimated_value, notes, agent } = req.body;
  const { rows: [lead] } = await query(
    `UPDATE leads
     SET name=$1, company=$2, email=$3, phone=$4, channel=$5,
         status=$6, priority=$7, estimated_value=$8, notes=$9, agent=$10,
         updated_at=NOW()
     WHERE id=$11 RETURNING *`,
    [name, company, email, phone, channel, status, priority,
     parseFloat(estimated_value) || 0, notes, agent, req.params.id]
  );
  if (!lead) return res.status(404).json({ error: 'Lead no encontrado' });
  res.json(lead);
}));

app.delete('/api/leads/:id', asyncHandler(async (req, res) => {
  await query('DELETE FROM leads WHERE id = $1', [req.params.id]);
  res.json({ success: true });
}));

// ─── PIPELINE ─────────────────────────────────────────────────────
app.get('/api/pipeline', asyncHandler(async (req, res) => {
  const stages = ['Nuevo','Contactado','Calificado','Propuesta','Negociacion','Ganado','Perdido'];
  const { rows } = await query(
    `SELECT * FROM leads WHERE status = ANY($1) ORDER BY estimated_value DESC`,
    [stages]
  );

  const pipeline = {};
  stages.forEach(stage => {
    const leads = rows.filter(l => l.status === stage);
    pipeline[stage] = {
      leads,
      count: leads.length,
      total: leads.reduce((s, l) => s + parseFloat(l.estimated_value || 0), 0),
    };
  });
  res.json(pipeline);
}));

app.patch('/api/leads/:id/status', asyncHandler(async (req, res) => {
  const { status } = req.body;
  await query('UPDATE leads SET status=$1, updated_at=NOW() WHERE id=$2', [status, req.params.id]);
  res.json({ success: true });
}));

// ─── ACTIVIDADES ──────────────────────────────────────────────────
app.post('/api/leads/:id/activities', asyncHandler(async (req, res) => {
  const { type, content, channel, direction } = req.body;
  const { rows: [act] } = await query(
    `INSERT INTO activities (id, lead_id, type, content, channel, direction)
     VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
    [uuidv4(), req.params.id, type, content, channel, direction || 'outbound']
  );
  res.status(201).json(act);
}));

// ─── CITAS ────────────────────────────────────────────────────────
app.post('/api/leads/:id/appointments', asyncHandler(async (req, res) => {
  const { scheduled_at, channel, notes } = req.body;
  const { rows: [appt] } = await query(
    `INSERT INTO appointments (id, lead_id, scheduled_at, channel, notes)
     VALUES ($1,$2,$3,$4,$5) RETURNING *`,
    [uuidv4(), req.params.id, scheduled_at, channel, notes]
  );
  res.status(201).json(appt);
}));

// ─── TAREAS ───────────────────────────────────────────────────────
app.post('/api/leads/:id/tasks', asyncHandler(async (req, res) => {
  const { title, due_date } = req.body;
  const { rows: [task] } = await query(
    `INSERT INTO tasks (id, lead_id, title, due_date) VALUES ($1,$2,$3,$4) RETURNING *`,
    [uuidv4(), req.params.id, title, due_date || null]
  );
  res.status(201).json(task);
}));

app.patch('/api/tasks/:id/complete', asyncHandler(async (req, res) => {
  await query('UPDATE tasks SET completed = TRUE WHERE id = $1', [req.params.id]);
  res.json({ success: true });
}));

// ─── NOTAS ────────────────────────────────────────────────────────
app.post('/api/leads/:id/notes', asyncHandler(async (req, res) => {
  const { content } = req.body;
  const { rows: [note] } = await query(
    `INSERT INTO notes (id, lead_id, content) VALUES ($1,$2,$3) RETURNING *`,
    [uuidv4(), req.params.id, content]
  );
  res.status(201).json(note);
}));

// ─── TALLERES ─────────────────────────────────────────────────────
app.get('/api/workshops', asyncHandler(async (req, res) => {
  const { search } = req.query;
  const active = req.query.active ?? 'true';
  let queryText = `SELECT w.*,
    COUNT(DISTINCT ws.dia) FILTER (WHERE ws.activo = true) as dias_activos,
    COUNT(DISTINCT wp.id) as servicios_precios
    FROM workshops w
    LEFT JOIN workshop_schedules ws ON ws.workshop_id = w.id
    LEFT JOIN workshop_prices wp ON wp.workshop_id = w.id
    WHERE w.active = $1`;

  const params = [parseBoolean(active, true)];

  if (search) {
    queryText += ` AND (w.nombre_comercial ILIKE $2 OR w.comuna ILIKE $2)`;
    params.push(`%${search}%`);
  }

  queryText += ' GROUP BY w.id ORDER BY w.nombre_comercial';
  const { rows } = await query(queryText, params);
  res.json(rows);
}));

app.get('/api/workshops/:id', asyncHandler(async (req, res) => {
  const [workshop, schedules, services, prices] = await Promise.all([
    query('SELECT * FROM workshops WHERE id = $1', [req.params.id]),
    query(`SELECT * FROM workshop_schedules WHERE workshop_id = $1 ORDER BY ${WORKSHOP_DAY_ORDER_SQL}`, [req.params.id]),
    query('SELECT * FROM workshop_services WHERE workshop_id = $1 ORDER BY nombre', [req.params.id]),
    query('SELECT * FROM workshop_prices WHERE workshop_id = $1 ORDER BY tipo, aro_min', [req.params.id]),
  ]);

  if (!workshop.rows[0]) return res.status(404).json({ error: 'Taller no encontrado' });
  res.json({ ...workshop.rows[0], schedules: schedules.rows, services: services.rows, prices: prices.rows });
}));

app.post('/api/workshops', asyncHandler(async (req, res) => {
  const {
    razon_social, nombre_comercial, rut, encargado_nombre, encargado_email, encargado_phone,
    finanzas_nombre, finanzas_email, finanzas_phone, direccion, comuna, comunas_adicionales,
    latitud, longitud, maps_url, puestos, turnos_por_puesto, aro_min, aro_max,
    instala_runflat, tipos_vehiculo, marcas_neumaticos, todas_marcas,
  } = req.body;

  if (!nombre_comercial) return res.status(400).json({ error: 'Nombre comercial requerido' });

  const { rows: [workshop] } = await query(
    `INSERT INTO workshops (id, razon_social, nombre_comercial, rut, encargado_nombre, encargado_email,
      encargado_phone, finanzas_nombre, finanzas_email, finanzas_phone, direccion, comuna,
      comunas_adicionales, latitud, longitud, maps_url, puestos, turnos_por_puesto, aro_min, aro_max,
      instala_runflat, tipos_vehiculo, marcas_neumaticos, todas_marcas)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
     RETURNING *`,
    [uuidv4(), razon_social || nombre_comercial, nombre_comercial, rut || null,
      encargado_nombre || null, encargado_email || null, encargado_phone || null,
      finanzas_nombre || null, finanzas_email || null, finanzas_phone || null,
      direccion || null, comuna || null, comunas_adicionales || null,
      latitud || null, longitud || null, maps_url || null,
      parseInteger(puestos, 1), parseInteger(turnos_por_puesto, 1),
      parseInteger(aro_min, 13), parseInteger(aro_max, 22),
      parseBoolean(instala_runflat, false), tipos_vehiculo || null,
      marcas_neumaticos || null, parseBoolean(todas_marcas, true)]
  );

  const scheduleInput = getScheduleInput(req.body);
  const schedules = scheduleInput.length > 0
    ? await upsertWorkshopSchedules(workshop.id, scheduleInput)
    : [];

  res.status(201).json({ ...workshop, schedules });
}));

app.put('/api/workshops/:id', asyncHandler(async (req, res) => {
  const {
    razon_social, nombre_comercial, rut, encargado_nombre, encargado_email, encargado_phone,
    finanzas_nombre, finanzas_email, finanzas_phone, direccion, comuna, comunas_adicionales,
    latitud, longitud, maps_url, puestos, turnos_por_puesto, aro_min, aro_max,
    instala_runflat, tipos_vehiculo, marcas_neumaticos, todas_marcas, active,
  } = req.body;

  const { rows: [workshop] } = await query(
    `UPDATE workshops
     SET razon_social=$1,nombre_comercial=$2,rut=$3,encargado_nombre=$4,encargado_email=$5,
         encargado_phone=$6,finanzas_nombre=$7,finanzas_email=$8,finanzas_phone=$9,direccion=$10,
         comuna=$11,comunas_adicionales=$12,latitud=$13,longitud=$14,maps_url=$15,puestos=$16,
         turnos_por_puesto=$17,aro_min=$18,aro_max=$19,instala_runflat=$20,tipos_vehiculo=$21,
         marcas_neumaticos=$22,todas_marcas=$23,active=$24,updated_at=NOW()
     WHERE id=$25
     RETURNING *`,
    [razon_social, nombre_comercial, rut || null, encargado_nombre || null, encargado_email || null,
      encargado_phone || null, finanzas_nombre || null, finanzas_email || null, finanzas_phone || null,
      direccion || null, comuna || null, comunas_adicionales || null, latitud || null, longitud || null,
      maps_url || null, parseInteger(puestos, 1), parseInteger(turnos_por_puesto, 1),
      parseInteger(aro_min, 13), parseInteger(aro_max, 22), parseBoolean(instala_runflat, false),
      tipos_vehiculo || null, marcas_neumaticos || null, parseBoolean(todas_marcas, true),
      parseBoolean(active, true), req.params.id]
  );

  if (!workshop) return res.status(404).json({ error: 'Taller no encontrado' });

  const scheduleInput = getScheduleInput(req.body);
  if (scheduleInput.length > 0) {
    await upsertWorkshopSchedules(req.params.id, scheduleInput);
  }

  res.json(workshop);
}));

// Horarios
app.put('/api/workshops/:id/schedules', asyncHandler(async (req, res) => {
  const { rows } = await query('SELECT id FROM workshops WHERE id = $1', [req.params.id]);
  if (!rows[0]) return res.status(404).json({ error: 'Taller no encontrado' });

  const schedules = await upsertWorkshopSchedules(req.params.id, getScheduleInput(req.body));
  res.json({ success: true, schedules });
}));

// Servicios
app.post('/api/workshops/:id/services', asyncHandler(async (req, res) => {
  const { nombre, descripcion } = req.body;
  const { rows: [service] } = await query(
    `INSERT INTO workshop_services (id, workshop_id, nombre, descripcion)
     VALUES ($1,$2,$3,$4) RETURNING *`,
    [uuidv4(), req.params.id, nombre, descripcion || null]
  );
  res.status(201).json(service);
}));

app.delete('/api/workshops/:id/services/:sid', asyncHandler(async (req, res) => {
  await query('DELETE FROM workshop_services WHERE id = $1 AND workshop_id = $2', [req.params.sid, req.params.id]);
  res.json({ success: true });
}));

// Precios
app.post('/api/workshops/:id/prices', asyncHandler(async (req, res) => {
  const { tipo, descripcion, aro_min, aro_max, precio } = req.body;
  const { rows: [price] } = await query(
    `INSERT INTO workshop_prices (id, workshop_id, tipo, descripcion, aro_min, aro_max, precio)
     VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
    [uuidv4(), req.params.id, tipo, descripcion, aro_min || null, aro_max || null, parseFloatNumber(precio, 0)]
  );
  res.status(201).json(price);
}));

app.put('/api/workshops/:id/prices/:pid', asyncHandler(async (req, res) => {
  const { tipo, descripcion, aro_min, aro_max, precio } = req.body;
  const { rows: [price] } = await query(
    `UPDATE workshop_prices
     SET tipo=$1, descripcion=$2, aro_min=$3, aro_max=$4, precio=$5, updated_at=NOW()
     WHERE id=$6 AND workshop_id=$7
     RETURNING *`,
    [tipo, descripcion, aro_min || null, aro_max || null, parseFloatNumber(precio, 0), req.params.pid, req.params.id]
  );
  res.json(price);
}));

app.delete('/api/workshops/:id/prices/:pid', asyncHandler(async (req, res) => {
  await query('DELETE FROM workshop_prices WHERE id = $1 AND workshop_id = $2', [req.params.pid, req.params.id]);
  res.json({ success: true });
}));

// Agenda
app.get('/api/workshops/:id/appointments', asyncHandler(async (req, res) => {
  const { fecha_inicio, fecha_fin } = req.query;
  let queryText = `SELECT wa.*, l.name as lead_name, l.phone as lead_phone
    FROM workshop_appointments wa
    LEFT JOIN leads l ON l.id = wa.lead_id
    WHERE wa.workshop_id = $1`;
  const params = [req.params.id];

  if (fecha_inicio) {
    queryText += ` AND wa.fecha >= $${params.length + 1}`;
    params.push(fecha_inicio);
  }
  if (fecha_fin) {
    queryText += ` AND wa.fecha <= $${params.length + 1}`;
    params.push(fecha_fin);
  }
  queryText += ' ORDER BY wa.fecha, wa.hora';

  const { rows } = await query(queryText, params);
  res.json(rows);
}));

app.post('/api/workshops/:id/appointments', asyncHandler(async (req, res) => {
  const { lead_id, fecha, hora, puesto, notas, order_id } = req.body;
  const { rows: [appointment] } = await query(
    `INSERT INTO workshop_appointments (id, workshop_id, lead_id, fecha, hora, puesto, notas, order_id)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
    [uuidv4(), req.params.id, lead_id || null, fecha, hora, parseInteger(puesto, 1), notas || null, order_id || null]
  );
  res.status(201).json(appointment);
}));

app.patch('/api/workshops/:id/appointments/:aid/status', asyncHandler(async (req, res) => {
  const { status } = req.body;
  const { rows: [appointment] } = await query(
    `UPDATE workshop_appointments
     SET status=$1
     WHERE id=$2 AND workshop_id=$3
     RETURNING *`,
    [status, req.params.aid, req.params.id]
  );
  res.json(appointment);
}));

// ─── WEBHOOK AGENTES ──────────────────────────────────────────────
app.post('/api/webhook/agent', asyncHandler(async (req, res) => {
  const {
    agent_name, channel, contact_name, contact_phone,
    contact_email, company, message, direction = 'inbound',
  } = req.body;

  if (!contact_phone && !contact_email) {
    return res.status(400).json({ error: 'Se requiere contact_phone o contact_email' });
  }

  const { rows: [existing] } = await query(
    `SELECT * FROM leads WHERE phone = $1 OR email = $2 LIMIT 1`,
    [contact_phone || '', contact_email || '']
  );

  let lead = existing;
  let created = false;

  if (!lead) {
    const { rows: [newLead] } = await query(
      `INSERT INTO leads (id, name, company, email, phone, channel, status, priority, agent)
       VALUES ($1,$2,$3,$4,$5,$6,'Nuevo','Media',$7) RETURNING *`,
      [uuidv4(),
       contact_name  || 'Desconocido',
       company       || '',
       contact_email || '',
       contact_phone || '',
       channel       || 'Web',
       agent_name    || 'AI Agent']
    );
    lead = newLead;
    created = true;
    console.log(`Nuevo lead desde agente: ${lead.name} via ${channel}`);
  }

  if (message) {
    await query(
      `INSERT INTO activities (id, lead_id, type, content, channel, direction)
       VALUES ($1,$2,'Message',$3,$4,$5)`,
      [uuidv4(), lead.id, message, channel, direction]
    );
  }

  res.json({ success: true, lead_id: lead.id, lead, created });
}));

// ─── START ────────────────────────────────────────────────────────
async function bootstrapDatabase() {
  try {
    await query('SELECT 1');
    console.log('PostgreSQL conectado');
    await initDB();
    dbReady = true;
    dbInitError = null;
  } catch (err) {
    dbReady = false;
    dbInitError = err.message || 'Error desconocido';
    console.error('Error DB:', dbInitError);
    setTimeout(bootstrapDatabase, 10000);
  }
}

app.listen(PORT, () => {
  console.log(`LeadFlow API en puerto ${PORT} [${process.env.NODE_ENV || 'development'}]`);
  bootstrapDatabase();
});
