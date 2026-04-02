const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());

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

pool.connect()
  .then(client => { console.log('PostgreSQL conectado'); client.release(); })
  .catch(err => { console.error('Error DB:', err.message); process.exit(1); });

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

    CREATE INDEX IF NOT EXISTS idx_leads_status    ON leads(status);
    CREATE INDEX IF NOT EXISTS idx_leads_channel   ON leads(channel);
    CREATE INDEX IF NOT EXISTS idx_activities_lead ON activities(lead_id);
    CREATE INDEX IF NOT EXISTS idx_tasks_lead      ON tasks(lead_id);
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

// ─── HEALTH CHECK ─────────────────────────────────────────────────
app.get('/health', asyncHandler(async (req, res) => {
  await query('SELECT 1');
  res.json({ status: 'ok', db: 'connected', timestamp: new Date().toISOString() });
}));

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
initDB().then(() => {
  app.listen(PORT, () => console.log(`LeadFlow API en puerto ${PORT} [${process.env.NODE_ENV || 'development'}]`));
}).catch(err => {
  console.error('Error al iniciar:', err);
  process.exit(1);
});
